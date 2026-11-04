// Command fakemcp is a minimal stand-in for the npm mcp-server-sqlite server,
// used by the end-to-end tests. It reproduces the behaviours that matter to
// the proxy: SQL is echoed to stdout as plain-text log lines (better-sqlite3
// "verbose" mode), each call accepts a single statement, failures come back
// as results with isError: true, and the server emits notifications.
package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

type request struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params struct {
		Name      string `json:"name"`
		Arguments struct {
			SQL string `json:"sql"`
		} `json:"arguments"`
	} `json:"params"`
}

func main() {
	dbPath := flag.String("db", "", "SQLite database")
	flag.Parse()
	db, err := sql.Open("sqlite", *dbPath+"?_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(1)

	out := bufio.NewWriter(os.Stdout)
	send := func(v any) {
		b, _ := json.Marshal(v)
		out.Write(append(b, '\n'))
		out.Flush()
	}
	fmt.Fprintln(out, "SQLite MCP Server running on stdio")
	out.Flush()

	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		var req request
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
			continue
		}
		if len(req.ID) == 0 {
			continue // notification
		}
		reply := func(result any) {
			send(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
		}
		text := func(s string, isErr bool) map[string]any {
			r := map[string]any{"content": []any{map[string]any{"type": "text", "text": s}}}
			if isErr {
				r["isError"] = true
			}
			return r
		}

		switch req.Method {
		case "initialize":
			reply(map[string]any{
				"protocolVersion": "2025-06-18",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "fakemcp", "version": "1"},
			})
		case "tools/list":
			schema := map[string]any{"type": "object", "properties": map[string]any{"sql": map[string]any{"type": "string"}}}
			reply(map[string]any{"tools": []any{
				map[string]any{"name": "query", "inputSchema": schema},
				map[string]any{"name": "execute", "inputSchema": schema},
			}})
		case "tools/call":
			// Server-initiated notification interleaved before the response.
			send(map[string]any{"jsonrpc": "2.0", "method": "notifications/message", "params": map[string]any{"level": "info", "data": "call"}})
			q := strings.TrimSpace(req.Params.Arguments.SQL)
			fmt.Fprintln(out, q) // verbose SQL log on stdout
			out.Flush()
			if multipleStatements(q) {
				reply(text("Database error: The supplied SQL string contains more than one statement", true))
				continue
			}
			switch req.Params.Name {
			case "query":
				rows, err := db.Query(q)
				if err != nil {
					reply(text("Database error: "+err.Error(), true))
					continue
				}
				cols, _ := rows.Columns()
				var all []map[string]any
				for rows.Next() {
					vals := make([]any, len(cols))
					ptrs := make([]any, len(cols))
					for i := range vals {
						ptrs[i] = &vals[i]
					}
					rows.Scan(ptrs...)
					m := map[string]any{}
					for i, c := range cols {
						m[c] = vals[i]
					}
					all = append(all, m)
				}
				rows.Close()
				b, _ := json.Marshal(all)
				reply(text(string(b), false))
			case "execute":
				res, err := db.Exec(q)
				if err != nil {
					reply(text("Database error: "+err.Error(), true))
					continue
				}
				n, _ := res.RowsAffected()
				id, _ := res.LastInsertId()
				reply(text(fmt.Sprintf(`{"changes":%d,"lastInsertRowid":%d}`, n, id), false))
			default:
				send(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32602, "message": "unknown tool"}})
			}
		default:
			send(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32601, "message": "Method not found"}})
		}
	}
}

// multipleStatements reports a ';' outside quotes that is followed by more SQL.
func multipleStatements(q string) bool {
	var quote rune
	for i, r := range q {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
		case r == ';':
			if strings.TrimSpace(q[i+1:]) != "" {
				return true
			}
		}
	}
	return false
}
