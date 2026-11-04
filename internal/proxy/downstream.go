package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

// DefaultCallTimeout bounds how long the proxy waits for a downstream reply.
const DefaultCallTimeout = 60 * time.Second

// DownstreamMCP is a JSON-RPC client over the stdio of a child MCP server.
//
// A single reader goroutine owns stdout and routes every response to the
// waiting caller by id. Requests are written with proxy-internal ids, so ids
// chosen by the upstream client can never collide with the proxy's own calls.
// Lines that are not JSON-RPC responses (log output printed to stdout,
// server-initiated notifications and requests) are skipped.
type DownstreamMCP struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser

	writeMu sync.Mutex
	mu      sync.Mutex
	nextID  int64
	pending map[string]chan []byte
	readErr error
	done    chan struct{}
}

func StartDownstreamMCP(command string, args ...string) (*DownstreamMCP, error) {
	cmd := exec.Command(command, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	return newDownstream(cmd, stdin, stdoutPipe), nil
}

func newDownstream(cmd *exec.Cmd, stdin io.WriteCloser, stdout io.Reader) *DownstreamMCP {
	d := &DownstreamMCP{
		cmd:     cmd,
		stdin:   stdin,
		pending: make(map[string]chan []byte),
		done:    make(chan struct{}),
	}
	go d.readLoop(stdout)
	return d
}

func (d *DownstreamMCP) readLoop(r io.Reader) {
	reader := bufio.NewReaderSize(r, 1024*1024)
	var err error
	for {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if len(line) > 0 {
			d.dispatch(line)
		}
		if err != nil {
			break
		}
	}
	if errors.Is(err, io.EOF) {
		err = errors.New("downstream MCP server closed its stdout")
	}
	d.mu.Lock()
	d.readErr = err
	d.mu.Unlock()
	close(d.done)
}

func (d *DownstreamMCP) dispatch(line []byte) {
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		log.Printf("downstream stdout (non-JSON, ignored): %s", trimmed)
		return
	}
	var msg struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if err := json.Unmarshal(trimmed, &msg); err != nil {
		log.Printf("downstream stdout (invalid JSON, ignored): %s", trimmed)
		return
	}
	if msg.Method != "" || len(msg.ID) == 0 || string(msg.ID) == "null" {
		log.Printf("downstream message without matching request (ignored): %s", trimmed)
		return
	}
	key := string(msg.ID)
	d.mu.Lock()
	ch, ok := d.pending[key]
	delete(d.pending, key)
	d.mu.Unlock()
	if !ok {
		log.Printf("downstream response for unknown id %s (ignored)", key)
		return
	}
	ch <- append([]byte(nil), trimmed...)
}

// roundTrip assigns an internal id to msg, sends it and waits for the reply.
func (d *DownstreamMCP) roundTrip(ctx context.Context, msg map[string]any) ([]byte, error) {
	d.mu.Lock()
	if d.readErr != nil {
		err := d.readErr
		d.mu.Unlock()
		return nil, err
	}
	d.nextID++
	id := d.nextID
	key := strconv.FormatInt(id, 10)
	ch := make(chan []byte, 1)
	d.pending[key] = ch
	d.mu.Unlock()

	cleanup := func() {
		d.mu.Lock()
		delete(d.pending, key)
		d.mu.Unlock()
	}

	msg["id"] = id
	if err := d.write(msg); err != nil {
		cleanup()
		return nil, err
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultCallTimeout)
		defer cancel()
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		cleanup()
		return nil, fmt.Errorf("downstream did not respond: %w", ctx.Err())
	case <-d.done:
		cleanup()
		d.mu.Lock()
		err := d.readErr
		d.mu.Unlock()
		return nil, err
	}
}

func (d *DownstreamMCP) write(msg map[string]any) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	_, err = d.stdin.Write(append(raw, '\n'))
	return err
}

// CallTool issues a tools/call request and returns the raw JSON-RPC response.
func (d *DownstreamMCP) CallTool(ctx context.Context, name string, args json.RawMessage) ([]byte, error) {
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	return d.roundTrip(ctx, map[string]any{
		"jsonrpc": "2.0",
		"method":  "tools/call",
		"params":  map[string]any{"name": name, "arguments": args},
	})
}

// ForwardRaw relays an upstream message unchanged except for its id, which is
// replaced for the round trip and restored on the response.
func (d *DownstreamMCP) ForwardRaw(ctx context.Context, raw []byte, isNotification bool) ([]byte, error) {
	var msg map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&msg); err != nil {
		return nil, err
	}
	if isNotification {
		delete(msg, "id")
		return nil, d.write(msg)
	}
	clientID, hasID := msg["id"]
	resp, err := d.roundTrip(ctx, msg)
	if err != nil {
		return nil, err
	}
	if !hasID {
		return resp, nil
	}
	return replaceID(resp, clientID)
}

func replaceID(resp []byte, id any) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(resp, &m); err != nil {
		return nil, err
	}
	idRaw, err := json.Marshal(id)
	if err != nil {
		return nil, err
	}
	m["id"] = idRaw
	return json.Marshal(m)
}

func (d *DownstreamMCP) Close() error {
	_ = d.stdin.Close()
	if d.cmd == nil || d.cmd.Process == nil {
		return nil
	}
	err := d.cmd.Process.Kill()
	_ = d.cmd.Wait()
	return err
}
