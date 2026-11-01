package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"mcp-saga/internal/ast"
	"mcp-saga/internal/compensator"
	"mcp-saga/internal/store"

	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T, dbFile string) *sql.DB {
	_ = os.Remove(dbFile)
	db, err := sql.Open("sqlite", dbFile+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatalf("Failed opening test DB: %v", err)
	}

	tables := []string{
		`CREATE TABLE accounts (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			tier TEXT NOT NULL CHECK(tier IN ('free', 'pro', 'enterprise'))
		);`,
		`CREATE TABLE ledgers (
			account_id INTEGER PRIMARY KEY REFERENCES accounts(id),
			credits INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE audit_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			account_id INTEGER NOT NULL REFERENCES accounts(id),
			action TEXT NOT NULL
		);`,
	}

	for _, tbl := range tables {
		if _, err := db.Exec(tbl); err != nil {
			t.Fatalf("Failed creating table: %v", err)
		}
	}

	if _, err := db.Exec("INSERT INTO accounts (id, email, tier) VALUES (42, 'client@enterprise.corp', 'free')"); err != nil {
		t.Fatalf("Failed seeding account: %v", err)
	}

	if _, err := db.Exec("INSERT INTO ledgers (account_id, credits) VALUES (42, 100)"); err != nil {
		t.Fatalf("Failed seeding ledger: %v", err)
	}

	return db
}

func TestRawExecution_LeavesCorruptState(t *testing.T) {
	db := setupTestDB(t, "corrupt_test.db")
	defer db.Close()
	defer os.Remove("corrupt_test.db")

	if _, err := db.Exec("UPDATE accounts SET tier = 'enterprise' WHERE id = 42"); err != nil {
		t.Fatalf("Step 1 failed: %v", err)
	}

	if _, err := db.Exec("UPDATE ledgers SET credits = credits + 10000 WHERE account_id = 42"); err != nil {
		t.Fatalf("Step 2 failed: %v", err)
	}

	_, err := db.Exec("INSERT INTO audit_logs (account_id, action) VALUES (99999, 'PROMO_APPLIED')")
	if err == nil {
		t.Fatalf("Expected foreign key violation")
	}

	var tier string
	var credits int
	if err := db.QueryRow("SELECT tier FROM accounts WHERE id = 42").Scan(&tier); err != nil {
		t.Fatalf("Query accounts failed: %v", err)
	}
	if err := db.QueryRow("SELECT credits FROM ledgers WHERE account_id = 42").Scan(&credits); err != nil {
		t.Fatalf("Query ledgers failed: %v", err)
	}

	if tier != "enterprise" || credits != 10100 {
		t.Fatalf("Expected corrupted state (tier=enterprise, credits=10100), got tier=%s, credits=%d", tier, credits)
	}
}

func TestSagaGuarded_CleanRollback(t *testing.T) {
	targetDB := setupTestDB(t, "clean_test.db")
	defer targetDB.Close()
	defer os.Remove("clean_test.db")
	defer os.Remove("bench_wal.db")

	walStore, err := store.NewSagaStore("bench_wal.db")
	if err != nil {
		t.Fatalf("WAL error: %v", err)
	}
	defer walStore.Close()

	comp := compensator.NewPostgresCompensator(targetDB)
	ctx := context.Background()

	sagaID := fmt.Sprintf("saga_%d", time.Now().UnixNano())
	_ = walStore.BeginSaga(sagaID, "bench_session")

	s1 := "UPDATE accounts SET tier = 'enterprise' WHERE id = 42"
	m1 := ast.InspectSQL(s1)
	p1, err := comp.GenerateCompensation(ctx, m1)
	if err != nil {
		t.Fatalf("Step 1 pre-image failed: %v", err)
	}
	if _, err := targetDB.Exec(s1); err != nil {
		t.Fatalf("Step 1 execution failed: %v", err)
	}
	c1JSON, _ := json.Marshal(map[string]string{"sql": p1.CompensatingSQL})
	_, _ = walStore.RecordCommittedStep(sagaID, 1, "execute_sql", []byte(s1), []byte("OK"), "execute_sql", c1JSON)

	s2 := "UPDATE ledgers SET credits = credits + 10000 WHERE account_id = 42"
	m2 := ast.InspectSQL(s2)
	p2, err := comp.GenerateCompensation(ctx, m2)
	if err != nil {
		t.Fatalf("Step 2 pre-image failed: %v", err)
	}
	if _, err := targetDB.Exec(s2); err != nil {
		t.Fatalf("Step 2 execution failed: %v", err)
	}
	c2JSON, _ := json.Marshal(map[string]string{"sql": p2.CompensatingSQL})
	_, _ = walStore.RecordCommittedStep(sagaID, 2, "execute_sql", []byte(s2), []byte("OK"), "execute_sql", c2JSON)

	_, err = targetDB.Exec("INSERT INTO audit_logs (account_id, action) VALUES (99999, 'PROMO_APPLIED')")
	if err != nil {
		steps, err := walStore.GetStepsForRollback(sagaID)
		if err != nil {
			t.Fatalf("Failed fetching rollback steps: %v", err)
		}
		for _, step := range steps {
			var args map[string]string
			if err := json.Unmarshal([]byte(step.CompPayload), &args); err == nil && args["sql"] != "" {
				if _, err := targetDB.Exec(args["sql"]); err != nil {
					t.Fatalf("Compensation SQL failed [%s]: %v", args["sql"], err)
				}
			}
			_ = walStore.MarkStepCompensated(sagaID, step.StepIndex)
		}
		_ = walStore.FinalizeSaga(sagaID, "ROLLED_BACK")
	}

	var tier string
	var credits int
	if err := targetDB.QueryRow("SELECT tier FROM accounts WHERE id = 42").Scan(&tier); err != nil {
		t.Fatalf("Query accounts failed: %v", err)
	}
	if err := targetDB.QueryRow("SELECT credits FROM ledgers WHERE account_id = 42").Scan(&credits); err != nil {
		t.Fatalf("Query ledgers failed: %v", err)
	}

	if tier != "free" || credits != 100 {
		t.Fatalf("Rollback failed! Expected tier=free, credits=100. Got tier=%s, credits=%d", tier, credits)
	}
}
