package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"sync"
)

type DownstreamMCP struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	mu     sync.Mutex
	reqID  int64
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

	return &DownstreamMCP{
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReader(stdoutPipe),
	}, nil
}

func (d *DownstreamMCP) CallTool(ctx context.Context, name string, args json.RawMessage) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.reqID++
	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      d.reqID,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      name,
			"arguments": args,
		},
	}

	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	if _, err := d.stdin.Write(append(raw, '\n')); err != nil {
		return nil, err
	}

	lineChan := make(chan []byte, 1)
	errChan := make(chan error, 1)

	go func() {
		line, err := d.stdout.ReadBytes('\n')
		if err != nil {
			errChan <- err
			return
		}
		lineChan <- line
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errChan:
		return nil, err
	case line := <-lineChan:
		return line, nil
	}
}

func (d *DownstreamMCP) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_ = d.stdin.Close()
	return d.cmd.Process.Kill()
}
