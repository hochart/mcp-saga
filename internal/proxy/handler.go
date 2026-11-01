package proxy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"

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

type SagaProxy struct {
	store       *store.SagaStore
	downstream  *DownstreamMCP
	compensator *compensator.PostgresCompensator
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

	if err := json.Unmarshal(raw, &req); err != nil {
		return encodeError(nil, -32700, "Parse error")
	}

	switch req.Method {
	case "saga/begin":
		b := make([]byte, 16)
		rand.Read(b)
		sagaID := "saga_" + hex.EncodeToString(b)
		if err := p.store.BeginSaga(sagaID, "session_default"); err != nil {
			return encodeError(req.ID, -32000, err.Error())
		}
		return encodeResult(req.ID, map[string]any{"saga_id": sagaID, "status": "ACTIVE"})

	case "saga/commit":
		var params struct {
			SagaID string `json:"saga_id"`
		}
		_ = json.Unmarshal(req.Params, &params)
		root, err := p.store.CurrentMerkleRoot(params.SagaID)
		if err != nil {
			return encodeError(req.ID, -32000, err.Error())
		}
		_ = p.store.FinalizeSaga(params.SagaID, "COMMITTED")
		return encodeResult(req.ID, map[string]any{"saga_id": params.SagaID, "status": "COMMITTED", "merkle_root": root})

	case "saga/abort":
		var params struct {
			SagaID string `json:"saga_id"`
		}
		_ = json.Unmarshal(req.Params, &params)
		_ = p.executeRollback(ctx, params.SagaID)
		_ = p.store.FinalizeSaga(params.SagaID, "ROLLED_BACK")
		return encodeResult(req.ID, map[string]any{"saga_id": params.SagaID, "status": "ROLLED_BACK"})

	case "tools/call":
		var pCall ToolCallParams
		_ = json.Unmarshal(req.Params, &pCall)

		if pCall.Saga != nil && p.compensator != nil {
			var argMap map[string]any
			if err := json.Unmarshal(pCall.Arguments, &argMap); err == nil {
				if sqlQuery, ok := argMap["sql"].(string); ok {
					parsed := ast.InspectSQL(sqlQuery)
					if plan, err := p.compensator.GenerateCompensation(ctx, parsed); err == nil && plan.CompensatingSQL != "" {
						undoPayload, _ := json.Marshal(map[string]string{"sql": plan.CompensatingSQL})
						pCall.Saga.Compensation = &CompensationMeta{
							Tool:      pCall.Name,
							Arguments: undoPayload,
						}
					}
				}
			}
		}

		respRaw, err := p.downstream.CallTool(ctx, pCall.Name, pCall.Arguments)
		if err != nil {
			if pCall.Saga != nil {
				_ = p.executeRollback(ctx, pCall.Saga.SagaID)
				_ = p.store.FinalizeSaga(pCall.Saga.SagaID, "FAILED")
			}
			return encodeError(req.ID, -32000, err.Error())
		}

		if pCall.Saga != nil && pCall.Saga.Compensation != nil {
			root, _ := p.store.RecordCommittedStep(
				pCall.Saga.SagaID,
				pCall.Saga.StepIndex,
				pCall.Name,
				pCall.Arguments,
				respRaw,
				pCall.Saga.Compensation.Tool,
				pCall.Saga.Compensation.Arguments,
			)
			var respMap map[string]any
			if err := json.Unmarshal(respRaw, &respMap); err == nil {
				respMap["_saga_receipt"] = map[string]string{"merkle_root": root}
				modified, _ := json.Marshal(respMap)
				return modified
			}
		}
		return respRaw

	default:
		return encodeError(req.ID, -32601, "Method not found")
	}
}

func (p *SagaProxy) executeRollback(ctx context.Context, sagaID string) error {
	steps, err := p.store.GetStepsForRollback(sagaID)
	if err != nil {
		return err
	}
	for _, step := range steps {
		if strings.TrimSpace(step.CompPayload) != "" && step.CompTool != "" {
			_, _ = p.downstream.CallTool(ctx, step.CompTool, json.RawMessage(step.CompPayload))
		}
		_ = p.store.MarkStepCompensated(sagaID, step.StepIndex)
	}
	return nil
}

func encodeResult(id any, res any) []byte {
	out, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": res})
	return out
}

func encodeError(id any, code int, msg string) []byte {
	out, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": msg}})
	return out
}
