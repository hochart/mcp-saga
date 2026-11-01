# mcp-saga (SEP-0142)

Deterministic Transactional Kernel and Auto-Compensating Rollback Interceptor for the Model Context Protocol (MCP).

## The Problem

Autonomous AI agents using tool calling (MCP) operate without transactional boundaries. In a multi-turn workflow across databases, cloud APIs, and file systems, a failure at step 4 of 5 leaves previous mutations stranded in production storage.

mcp-saga acts as a zero-overhead JSON-RPC proxy interceptor implementing SEP-0142 (Saga Mechanics for MCP).

## Core Architecture

- **Deterministic Pre-Image Capture:** Intercepts mutating tool operations before execution, reading target record states to synthesize deterministic, idempotency-tested reverse SQL statements.
- **SQLite WAL State Machine:** High-throughput local Write-Ahead Logging maintaining step order, intermediate parameters, and Merkle root checkpoints.
- **Automated LIFO Rollback:** If any step fails or an explicit saga/abort is triggered, compensations execute in reverse order within 0.01s, restoring original state.
- **Zero Upstream Modifications:** Downstream servers run completely unmodified.

## Benchmark Results

Verified on Apple Silicon (Darwin arm64):

- TestRawExecution_LeavesCorruptState: Confirms partial database mutation on failure.
- TestSagaGuarded_CleanRollback: Confirms clean automated state restoration in 0.01s.

## License

MIT License. See LICENSE for details.
