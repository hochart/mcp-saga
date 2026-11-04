package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"mcp-saga/internal/ast"
	"mcp-saga/internal/compensator"
	"mcp-saga/internal/store"
)

type SagaCallMeta struct {
	SagaID       string            `json:"saga_id"`
	StepIndex    int               `json:"step_index"`
	Compensation *CompensationMeta `json:"compensation,omitempty"`
}

type CompensationMeta struct {
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
}

type ToolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
	Saga      *SagaCallMeta   `json:"_saga,omitempty"`
}

// Saga control tools advertised to MCP clients (LLM hosts cannot send the
// saga/* JSON-RPC methods or the _saga extension field themselves).
const (
	toolSagaBegin  = "saga_begin"
	toolSagaCommit = "saga_commit"
	toolSagaAbort  = "saga_abort"
)

var sagaTools = []map[string]any{
	{
		"name":        toolSagaBegin,
		"description": "Start a transactional saga. Every following mutating tool call is journaled with an automatically generated compensating action until saga_commit or saga_abort. If any call fails, all prior steps are rolled back in reverse order.",
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
	},
	{
		"name":        toolSagaCommit,
		"description": "Commit the active saga and return its Merkle audit root.",
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
	},
	{
		"name":        toolSagaAbort,
		"description": "Abort the active saga and undo all of its steps in reverse order.",
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
	},
}

type SagaProxy struct {
	store       *store.SagaStore
	downstream  *DownstreamMCP
	compensator *compensator.PostgresCompensator

	mu         sync.Mutex
	activeSaga string
}

func NewSagaProxy(store *store.SagaStore, downstream *DownstreamMCP, comp *compensator.PostgresCompensator) *SagaProxy {
	return &SagaProxy{
		store:       store,
		downstream:  downstream,
		compensator: comp,
	}
}

func (p *SagaProxy) HandleRequest(ctx context.Context, raw []byte) []byte {
	var req struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      any             `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep numeric ids byte-identical when echoed back
	if err := dec.Decode(&req); err != nil {
		return encodeError(nil, -32700, "Parse error")
	}
	isNotification := req.ID == nil

	switch req.Method {
	case "saga/begin":
		sagaID, err := p.beginSaga()
		if err != nil {
			return encodeError(req.ID, -32000, err.Error())
		}
		return encodeResult(req.ID, map[string]any{"saga_id": sagaID, "status": "ACTIVE"})

	case "saga/commit":
		res, err := p.commitSaga(p.sagaIDParam(req.Params))
		if err != nil {
			return encodeError(req.ID, -32000, err.Error())
		}
		return encodeResult(req.ID, res)

	case "saga/abort":
		res, err := p.abortSaga(ctx, p.sagaIDParam(req.Params))
		if err != nil {
			return encodeError(req.ID, -32000, err.Error())
		}
		return encodeResult(req.ID, res)

	case "tools/list":
		resp, err := p.forward(ctx, raw, false)
		if err != nil {
			return encodeError(req.ID, -32000, err.Error())
		}
		return appendSagaTools(resp)

	case "tools/call":
		return p.handleToolCall(ctx, req.ID, req.Params)

	default:
		if p.downstream == nil {
			if isNotification {
				return nil
			}
			return encodeError(req.ID, -32601, "Method not found")
		}
		isNotification = isNotification || strings.HasPrefix(req.Method, "notifications/")
		resp, err := p.forward(ctx, raw, isNotification)
		if isNotification {
			return nil
		}
		if err != nil {
			return encodeError(req.ID, -32000, err.Error())
		}
		return resp
	}
}

func (p *SagaProxy) forward(ctx context.Context, raw []byte, isNotification bool) ([]byte, error) {
	if p.downstream == nil {
		return nil, fmt.Errorf("no downstream MCP server")
	}
	return p.downstream.ForwardRaw(ctx, raw, isNotification)
}

func (p *SagaProxy) sagaIDParam(params json.RawMessage) string {
	var v struct {
		SagaID string `json:"saga_id"`
	}
	_ = json.Unmarshal(params, &v)
	if v.SagaID != "" {
		return v.SagaID
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.activeSaga
}

func (p *SagaProxy) beginSaga() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeSaga != "" {
		return "", fmt.Errorf("saga %s is already active; commit or abort it first", p.activeSaga)
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	sagaID := "saga_" + hex.EncodeToString(b)
	if err := p.store.BeginSaga(sagaID, "session_default"); err != nil {
		return "", err
	}
	p.activeSaga = sagaID
	return sagaID, nil
}

func (p *SagaProxy) clearActive(sagaID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeSaga == sagaID {
		p.activeSaga = ""
	}
}

func (p *SagaProxy) commitSaga(sagaID string) (map[string]any, error) {
	if sagaID == "" {
		return nil, fmt.Errorf("no active saga")
	}
	root, err := p.store.CurrentMerkleRoot(sagaID)
	if err != nil {
		return nil, fmt.Errorf("unknown saga %s: %w", sagaID, err)
	}
	if err := p.store.FinalizeSaga(sagaID, "COMMITTED"); err != nil {
		return nil, err
	}
	p.clearActive(sagaID)
	return map[string]any{"saga_id": sagaID, "status": "COMMITTED", "merkle_root": root}, nil
}

func (p *SagaProxy) abortSaga(ctx context.Context, sagaID string) (map[string]any, error) {
	if sagaID == "" {
		return nil, fmt.Errorf("no active saga")
	}
	n, rbErr := p.executeRollback(ctx, sagaID)
	p.clearActive(sagaID)
	if rbErr != nil {
		_ = p.store.FinalizeSaga(sagaID, "ROLLBACK_FAILED")
		return nil, fmt.Errorf("rollback of saga %s stopped after %d step(s): %w", sagaID, n, rbErr)
	}
	if err := p.store.FinalizeSaga(sagaID, "ROLLED_BACK"); err != nil {
		return nil, err
	}
	return map[string]any{"saga_id": sagaID, "status": "ROLLED_BACK", "compensated_steps": n}, nil
}

// failSaga rolls a saga back after one of its steps failed.
func (p *SagaProxy) failSaga(ctx context.Context, sagaID string) string {
	n, err := p.executeRollback(ctx, sagaID)
	p.clearActive(sagaID)
	if err != nil {
		_ = p.store.FinalizeSaga(sagaID, "ROLLBACK_FAILED")
		return fmt.Sprintf("mcp-saga: step failed; rollback of saga %s FAILED after %d step(s): %v", sagaID, n, err)
	}
	_ = p.store.FinalizeSaga(sagaID, "FAILED")
	return fmt.Sprintf("mcp-saga: step failed; saga %s rolled back (%d step(s) compensated).", sagaID, n)
}

func (p *SagaProxy) handleToolCall(ctx context.Context, id any, params json.RawMessage) []byte {
	var pCall ToolCallParams
	if err := json.Unmarshal(params, &pCall); err != nil {
		return encodeError(id, -32602, "Invalid params: "+err.Error())
	}

	switch pCall.Name {
	case toolSagaBegin:
		sagaID, err := p.beginSaga()
		if err != nil {
			return encodeToolResult(id, err.Error(), true)
		}
		return encodeToolResult(id, fmt.Sprintf("Saga %s started. Subsequent tool calls are journaled until saga_commit or saga_abort.", sagaID), false)
	case toolSagaCommit:
		res, err := p.commitSaga(p.sagaIDParam(pCall.Arguments))
		if err != nil {
			return encodeToolResult(id, err.Error(), true)
		}
		return encodeToolResult(id, fmt.Sprintf("Saga %s committed. Merkle root: %s", res["saga_id"], res["merkle_root"]), false)
	case toolSagaAbort:
		res, err := p.abortSaga(ctx, p.sagaIDParam(pCall.Arguments))
		if err != nil {
			return encodeToolResult(id, err.Error(), true)
		}
		return encodeToolResult(id, fmt.Sprintf("Saga %s rolled back (%d step(s) compensated).", res["saga_id"], res["compensated_steps"]), false)
	}

	if p.downstream == nil {
		return encodeError(id, -32000, "no downstream MCP server")
	}

	// Attach the call to the active saga when the client did not use _saga.
	saga := pCall.Saga
	if saga == nil {
		p.mu.Lock()
		active := p.activeSaga
		p.mu.Unlock()
		if active != "" {
			saga = &SagaCallMeta{SagaID: active}
		}
	}
	if saga != nil && saga.StepIndex == 0 {
		next, err := p.store.NextStepIndex(saga.SagaID)
		if err != nil {
			return encodeError(id, -32000, err.Error())
		}
		saga.StepIndex = next
	}

	// Capture the pre-image before the mutation reaches the downstream server.
	var plan *compensator.CompensationPlan
	if saga != nil && saga.Compensation == nil && p.compensator != nil {
		var argMap map[string]any
		if err := json.Unmarshal(pCall.Arguments, &argMap); err == nil {
			if sqlQuery, ok := argMap["sql"].(string); ok {
				parsed := ast.InspectSQL(sqlQuery)
				if parsed.Class != ast.ClassSelect {
					var err error
					plan, err = p.compensator.GenerateCompensation(ctx, parsed)
					if err != nil {
						// Executing a mutation we cannot undo would break the saga guarantee.
						return encodeToolResult(id, fmt.Sprintf("mcp-saga: refused inside saga %s: cannot generate a compensating action (%v). Commit or abort the saga before running this statement.", saga.SagaID, err), true)
					}
				}
			}
		}
	}

	respRaw, err := p.downstream.CallTool(ctx, pCall.Name, pCall.Arguments)
	if err != nil {
		msg := err.Error()
		if saga != nil {
			msg += "\n" + p.failSaga(ctx, saga.SagaID)
		}
		return encodeError(id, -32000, msg)
	}

	var resp map[string]any
	dec := json.NewDecoder(bytes.NewReader(respRaw))
	dec.UseNumber()
	if err := dec.Decode(&resp); err != nil {
		return encodeError(id, -32000, "invalid downstream response: "+err.Error())
	}
	resp["id"] = id

	if saga == nil {
		return mustMarshal(resp)
	}

	if isFailure(resp) {
		note := p.failSaga(ctx, saga.SagaID)
		if result, ok := resp["result"].(map[string]any); ok {
			content, _ := result["content"].([]any)
			result["content"] = append(content, map[string]any{"type": "text", "text": note})
		} else if errObj, ok := resp["error"].(map[string]any); ok {
			errObj["message"] = fmt.Sprintf("%v\n%s", errObj["message"], note)
		}
		return mustMarshal(resp)
	}

	if plan != nil && saga.Compensation == nil {
		if err := p.compensator.Complete(ctx, plan); err != nil {
			note := p.failSaga(ctx, saga.SagaID)
			return encodeToolResult(id, fmt.Sprintf("mcp-saga: statement ran but its compensation could not be captured (%v).\n%s", err, note), true)
		}
		if len(plan.Statements) > 0 {
			saga.Compensation = &CompensationMeta{Tool: pCall.Name, Arguments: compensationArgs(plan.Statements)}
		}
	}

	compTool, compArgs := "", json.RawMessage(nil)
	if saga.Compensation != nil {
		compTool, compArgs = saga.Compensation.Tool, saga.Compensation.Arguments
	}
	root, err := p.store.RecordCommittedStep(saga.SagaID, saga.StepIndex, pCall.Name, pCall.Arguments, respRaw, compTool, compArgs)
	if err != nil {
		note := p.failSaga(ctx, saga.SagaID)
		return encodeToolResult(id, fmt.Sprintf("mcp-saga: failed to journal step %d (%v); the step is NOT covered by rollback.\n%s", saga.StepIndex, err, note), true)
	}

	if result, ok := resp["result"].(map[string]any); ok {
		meta, _ := result["_meta"].(map[string]any)
		if meta == nil {
			meta = map[string]any{}
		}
		meta["saga_receipt"] = map[string]any{"saga_id": saga.SagaID, "step_index": saga.StepIndex, "merkle_root": root}
		result["_meta"] = meta
	}
	return mustMarshal(resp)
}

// compensationArgs encodes one tool-argument object per statement; several
// are stored as a JSON array and replayed one call at a time.
func compensationArgs(stmts []string) json.RawMessage {
	if len(stmts) == 1 {
		out, _ := json.Marshal(map[string]string{"sql": stmts[0]})
		return out
	}
	list := make([]map[string]string, len(stmts))
	for i, s := range stmts {
		list[i] = map[string]string{"sql": s}
	}
	out, _ := json.Marshal(list)
	return out
}

func isFailure(resp map[string]any) bool {
	if _, ok := resp["error"]; ok {
		return true
	}
	if res, ok := resp["result"].(map[string]any); ok {
		if v, ok := res["isError"].(bool); ok && v {
			return true
		}
	}
	return false
}

// executeRollback replays compensations in LIFO order. It stops at the first
// failed compensation, leaving that step and earlier ones COMMITTED so the
// rollback can be retried.
func (p *SagaProxy) executeRollback(ctx context.Context, sagaID string) (int, error) {
	steps, err := p.store.GetStepsForRollback(sagaID)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, step := range steps {
		payload := strings.TrimSpace(step.CompPayload)
		if payload != "" && step.CompTool != "" {
			calls := []json.RawMessage{json.RawMessage(payload)}
			if strings.HasPrefix(payload, "[") {
				calls = nil
				if err := json.Unmarshal([]byte(payload), &calls); err != nil {
					return done, fmt.Errorf("step %d: corrupt compensation payload: %w", step.StepIndex, err)
				}
			}
			for _, args := range calls {
				resp, err := p.downstream.CallTool(ctx, step.CompTool, args)
				if err != nil {
					return done, fmt.Errorf("step %d: %w", step.StepIndex, err)
				}
				var m map[string]any
				if err := json.Unmarshal(resp, &m); err != nil || isFailure(m) {
					return done, fmt.Errorf("step %d: compensation rejected by downstream: %s", step.StepIndex, resp)
				}
			}
		}
		if err := p.store.MarkStepCompensated(sagaID, step.StepIndex); err != nil {
			return done, err
		}
		done++
	}
	return done, nil
}

func appendSagaTools(resp []byte) []byte {
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(resp))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return resp
	}
	result, ok := m["result"].(map[string]any)
	if !ok {
		return resp
	}
	if cursor, _ := result["nextCursor"].(string); cursor != "" {
		return resp // only add ours on the last page
	}
	tools, _ := result["tools"].([]any)
	for _, t := range sagaTools {
		tools = append(tools, t)
	}
	result["tools"] = tools
	return mustMarshal(m)
}

func mustMarshal(v any) []byte {
	out, _ := json.Marshal(v)
	return out
}

func encodeToolResult(id any, text string, isError bool) []byte {
	res := map[string]any{"content": []any{map[string]any{"type": "text", "text": text}}}
	if isError {
		res["isError"] = true
	}
	return encodeResult(id, res)
}

func encodeResult(id any, res any) []byte {
	return mustMarshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": res})
}

func encodeError(id any, code int, msg string) []byte {
	return mustMarshal(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": msg}})
}
