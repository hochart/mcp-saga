package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"

	_ "modernc.org/sqlite"
)

type StepRecord struct {
	ID          int64
	SagaID      string
	StepIndex   int
	ToolName    string
	ToolPayload string
	Result      string
	CompTool    string
	CompPayload string
	Status      string
	ChainHash   string
}

type SagaStore struct {
	db *sql.DB
	mu sync.Mutex
}

func NewSagaStore(dbPath string) (*SagaStore, error) {
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	schema := `
	CREATE TABLE IF NOT EXISTS sagas (
		saga_id TEXT PRIMARY KEY,
		session_id TEXT NOT NULL,
		status TEXT NOT NULL,
		current_merkle_root TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS saga_journal (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		saga_id TEXT NOT NULL,
		step_index INTEGER NOT NULL,
		tool_name TEXT NOT NULL,
		tool_payload TEXT NOT NULL,
		result TEXT NOT NULL,
		comp_tool TEXT NOT NULL,
		comp_payload TEXT NOT NULL,
		status TEXT NOT NULL,
		chain_hash TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(saga_id) REFERENCES sagas(saga_id),
		UNIQUE(saga_id, step_index)
	);

	CREATE INDEX IF NOT EXISTS idx_journal_saga ON saga_journal(saga_id, step_index);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}

	return &SagaStore{db: db}, nil
}

func (s *SagaStore) BeginSaga(sagaID, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(
		"INSERT INTO sagas (saga_id, session_id, status, current_merkle_root) VALUES (?, ?, 'ACTIVE', '')",
		sagaID, sessionID,
	)
	return err
}

func (s *SagaStore) RecordCommittedStep(sagaID string, stepIndex int, toolName string, toolPayload, result []byte, compTool string, compPayload []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var prevRoot string
	err := s.db.QueryRow("SELECT current_merkle_root FROM sagas WHERE saga_id = ?", sagaID).Scan(&prevRoot)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	fmt.Fprintf(h, "%d:%s:%d:%d:%s:%d:%s:%d:%s:%d:%s:%d:%s",
		len(prevRoot), prevRoot,
		stepIndex,
		len(toolName), toolName,
		len(toolPayload), string(toolPayload),
		len(result), string(result),
		len(compTool), compTool,
		len(compPayload), string(compPayload),
	)
	chainHash := hex.EncodeToString(h.Sum(nil))

	tx, err := s.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO saga_journal 
		(saga_id, step_index, tool_name, tool_payload, result, comp_tool, comp_payload, status, chain_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'COMMITTED', ?)`,
		sagaID, stepIndex, toolName, string(toolPayload), string(result), compTool, string(compPayload), chainHash,
	)
	if err != nil {
		return "", err
	}

	_, err = tx.Exec("UPDATE sagas SET current_merkle_root = ?, updated_at = CURRENT_TIMESTAMP WHERE saga_id = ?", chainHash, sagaID)
	if err != nil {
		return "", err
	}

	return chainHash, tx.Commit()
}

func (s *SagaStore) GetStepsForRollback(sagaID string) ([]StepRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows, err := s.db.Query(
		"SELECT id, saga_id, step_index, tool_name, tool_payload, result, comp_tool, comp_payload, status, chain_hash FROM saga_journal WHERE saga_id = ? AND status = 'COMMITTED' ORDER BY step_index DESC",
		sagaID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []StepRecord
	for rows.Next() {
		var r StepRecord
		if err := rows.Scan(&r.ID, &r.SagaID, &r.StepIndex, &r.ToolName, &r.ToolPayload, &r.Result, &r.CompTool, &r.CompPayload, &r.Status, &r.ChainHash); err != nil {
			return nil, err
		}
		steps = append(steps, r)
	}
	return steps, nil
}

func (s *SagaStore) MarkStepCompensated(sagaID string, stepIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("UPDATE saga_journal SET status = 'COMPENSATED' WHERE saga_id = ? AND step_index = ?", sagaID, stepIndex)
	return err
}

func (s *SagaStore) FinalizeSaga(sagaID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("UPDATE sagas SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE saga_id = ?", status, sagaID)
	return err
}

func (s *SagaStore) CurrentMerkleRoot(sagaID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var root string
	err := s.db.QueryRow("SELECT current_merkle_root FROM sagas WHERE saga_id = ?", sagaID).Scan(&root)
	return root, err
}

func (s *SagaStore) Close() error {
	return s.db.Close()
}
