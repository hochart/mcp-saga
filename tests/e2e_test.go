package tests

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// proxyClient drives the real mcp-saga binary over stdio, in front of the
// fakemcp downstream server, exactly as an MCP host would.
type proxyClient struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	lines  chan string
	nextID int
}

func buildBinaries(t *testing.T) (proxyBin, fakeBin string) {
	t.Helper()
	dir := t.TempDir()
	proxyBin = filepath.Join(dir, "mcp-saga")
	fakeBin = filepath.Join(dir, "fakemcp")
	for _, b := range [][2]string{{proxyBin, "mcp-saga/cmd/mcp-saga"}, {fakeBin, "mcp-saga/tests/fakemcp"}} {
		out, err := exec.Command("go", "build", "-ldflags=-linkmode=external -s -w", "-o", b[0], b[1]).CombinedOutput()
		if err != nil {
			t.Fatalf("go build %s: %v\n%s", b[1], err, out)
		}
	}
	return
}

func startProxy(t *testing.T, appDB string) *proxyClient {
	t.Helper()
	proxyBin, fakeBin := buildBinaries(t)
	wal := filepath.Join(t.TempDir(), "wal.db")
	cmd := exec.Command(proxyBin, "-db", wal, "-app-db", appDB, "--", fakeBin, "-db", appDB)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c := &proxyClient{t: t, cmd: cmd, stdin: stdin, lines: make(chan string, 64)}
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 1<<20), 4<<20)
		for sc.Scan() {
			c.lines <- sc.Text()
		}
		close(c.lines)
	}()
	t.Cleanup(func() { stdin.Close(); cmd.Process.Kill(); cmd.Wait() })
	return c
}

func (c *proxyClient) send(v map[string]any) {
	b, _ := json.Marshal(v)
	if _, err := c.stdin.Write(append(b, '\n')); err != nil {
		c.t.Fatal(err)
	}
}

func (c *proxyClient) call(method string, params any) map[string]any {
	c.t.Helper()
	c.nextID++
	id := c.nextID
	c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	select {
	case line, ok := <-c.lines:
		if !ok {
			c.t.Fatalf("proxy exited while waiting for %s", method)
		}
		var resp map[string]any
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			c.t.Fatalf("proxy wrote non-JSON line %q: %v", line, err)
		}
		if got := fmt.Sprint(resp["id"]); got != fmt.Sprint(id) {
			c.t.Fatalf("%s: response id %s, want %d: %s", method, got, id, line)
		}
		return resp
	case <-time.After(15 * time.Second):
		c.t.Fatalf("timeout waiting for %s", method)
	}
	return nil
}

func (c *proxyClient) tool(name string, args map[string]any) (text string, isError bool) {
	c.t.Helper()
	resp := c.call("tools/call", map[string]any{"name": name, "arguments": args})
	res, ok := resp["result"].(map[string]any)
	if !ok {
		c.t.Fatalf("tools/call %s: no result: %v", name, resp)
	}
	for _, item := range res["content"].([]any) {
		text += item.(map[string]any)["text"].(string) + "\n"
	}
	isError, _ = res["isError"].(bool)
	return text, isError
}

func (c *proxyClient) handshake() {
	c.t.Helper()
	c.call("initialize", map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "e2e", "version": "1"}})
	c.send(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
}

func seedApp(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range []string{
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT NOT NULL, tier TEXT NOT NULL, note TEXT)`,
		`CREATE TABLE ledgers (account_id INTEGER PRIMARY KEY REFERENCES accounts(id), credits INTEGER NOT NULL)`,
		`CREATE TABLE audit_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER NOT NULL REFERENCES accounts(id), action TEXT NOT NULL)`,
		`INSERT INTO accounts VALUES (42, 'client@enterprise.corp', 'free', 'it''s; tricky'), (43, 'b@x.io', 'free', NULL), (44, 'c@x.io', 'pro', 'keep')`,
		`INSERT INTO ledgers VALUES (42, 100), (43, 5), (44, 7)`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func snapshot(t *testing.T, path string) string {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var b strings.Builder
	for _, q := range []string{
		"SELECT id, email, tier, quote(note) FROM accounts ORDER BY id",
		"SELECT account_id, credits, 0, 0 FROM ledgers ORDER BY account_id",
		"SELECT id, account_id, action, 0 FROM audit_logs ORDER BY id",
	} {
		rows, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var a, b2, c, d any
			rows.Scan(&a, &b2, &c, &d)
			fmt.Fprintf(&b, "%v|%v|%v|%v\n", a, b2, c, d)
		}
		rows.Close()
		b.WriteString("--\n")
	}
	return b.String()
}

func TestE2E_FailedStepRollsBackWholeSaga(t *testing.T) {
	app := seedApp(t)
	baseline := snapshot(t, app)
	c := startProxy(t, app)
	c.handshake()

	tools := c.call("tools/list", map[string]any{})
	list, _ := json.Marshal(tools["result"])
	for _, name := range []string{`"execute"`, `"saga_begin"`, `"saga_commit"`, `"saga_abort"`} {
		if !strings.Contains(string(list), name) {
			t.Fatalf("tools/list missing %s: %s", name, list)
		}
	}

	if txt, isErr := c.tool("saga_begin", map[string]any{}); isErr {
		t.Fatalf("saga_begin: %s", txt)
	}
	steps := []string{
		// Multi-row update whose predicate references the changed column.
		"UPDATE accounts SET tier = 'enterprise', note = 'promo' WHERE tier = 'free'",
		"UPDATE ledgers SET credits = credits + 10000 WHERE account_id = 42",
		"INSERT INTO audit_logs (account_id, action) VALUES (42, 'TIER_CHANGED')",
		"DELETE FROM ledgers WHERE account_id = 44",
	}
	for _, s := range steps {
		if txt, isErr := c.tool("execute", map[string]any{"sql": s}); isErr {
			t.Fatalf("step %q failed: %s", s, txt)
		}
	}
	if snapshot(t, app) == baseline {
		t.Fatal("steps had no effect")
	}

	// Foreign-key violation: downstream answers isError, proxy must roll back.
	txt, isErr := c.tool("execute", map[string]any{"sql": "INSERT INTO audit_logs (account_id, action) VALUES (99999, 'PROMO_APPLIED')"})
	if !isErr || !strings.Contains(txt, "rolled back (4 step(s) compensated)") {
		t.Fatalf("expected rollback note, got isError=%v: %s", isErr, txt)
	}
	if got := snapshot(t, app); got != baseline {
		t.Fatalf("state not restored.\nwant:\n%s\ngot:\n%s", baseline, got)
	}

	// The saga is closed; a new one can start.
	if txt, isErr := c.tool("saga_begin", map[string]any{}); isErr {
		t.Fatalf("second saga_begin: %s", txt)
	}
}

func TestE2E_AbortAndCommit(t *testing.T) {
	app := seedApp(t)
	baseline := snapshot(t, app)
	c := startProxy(t, app)
	c.handshake()

	c.tool("saga_begin", map[string]any{})
	c.tool("execute", map[string]any{"sql": "INSERT INTO audit_logs (account_id, action) VALUES (43, 'A'), (44, 'B')"})
	c.tool("execute", map[string]any{"sql": "DELETE FROM accounts WHERE id = 43 AND 0"})
	c.tool("query", map[string]any{"sql": "SELECT COUNT(*) FROM audit_logs"})
	if txt, isErr := c.tool("saga_abort", map[string]any{}); isErr || !strings.Contains(txt, "rolled back (1 step(s) compensated)") {
		t.Fatalf("abort: %v %s", isErr, txt)
	}
	if got := snapshot(t, app); got != baseline {
		t.Fatalf("abort did not restore state.\nwant:\n%s\ngot:\n%s", baseline, got)
	}

	c.tool("saga_begin", map[string]any{})
	c.tool("execute", map[string]any{"sql": "UPDATE ledgers SET credits = 1 WHERE account_id = 42"})
	txt, isErr := c.tool("saga_commit", map[string]any{})
	if isErr || !strings.Contains(txt, "Merkle root: ") || strings.HasSuffix(strings.TrimSpace(txt), "Merkle root:") {
		t.Fatalf("commit: %v %s", isErr, txt)
	}

	// Unbounded mutations cannot be compensated and are refused inside a saga.
	c.tool("saga_begin", map[string]any{})
	if txt, isErr := c.tool("execute", map[string]any{"sql": "DELETE FROM audit_logs"}); !isErr || !strings.Contains(txt, "refused") {
		t.Fatalf("expected refusal, got %v %s", isErr, txt)
	}

	// Outside a saga, calls pass straight through.
	c.tool("saga_abort", map[string]any{})
	if txt, isErr := c.tool("query", map[string]any{"sql": "SELECT credits FROM ledgers WHERE account_id = 42"}); isErr || !strings.Contains(txt, `"credits":1`) {
		t.Fatalf("passthrough query: %v %s", isErr, txt)
	}
}
