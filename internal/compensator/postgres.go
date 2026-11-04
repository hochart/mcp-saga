package compensator

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"mcp-saga/internal/ast"
)

type DBQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// CompensationPlan holds the inverse of one mutation.
//
// Statements holds one SQL statement per entry so it can be replayed through
// downstream tools that only accept a single statement per call.
// CompensatingSQL is the same statements joined by newlines.
type CompensationPlan struct {
	CompensatingSQL string
	Statements      []string
	RowsAffected    int

	// INSERT compensation is only known after the insert ran: Complete
	// diffs the table's rowids against this baseline.
	insertTable    string
	insertBaseline int64
}

// NeedsCompletion reports whether Complete must be called after the
// mutation has executed successfully.
func (p *CompensationPlan) NeedsCompletion() bool { return p != nil && p.insertTable != "" }

type PostgresCompensator struct {
	db DBQueryer
}

func NewPostgresCompensator(db DBQueryer) *PostgresCompensator {
	return &PostgresCompensator{db: db}
}

func (c *PostgresCompensator) GenerateCompensation(ctx context.Context, m ast.ParsedMutation) (*CompensationPlan, error) {
	if m.Class == ast.ClassSelect {
		return &CompensationPlan{}, nil
	}
	if !m.IsReversible || m.Table == "" {
		return nil, fmt.Errorf("mutation is irreversible or lacks table/predicate: %s", m.RawQuery)
	}

	switch m.Class {
	case ast.ClassInsert:
		var maxRowID sql.NullInt64
		if err := c.queryRow(ctx, fmt.Sprintf("SELECT MAX(rowid) FROM %s", m.Table), &maxRowID); err != nil {
			return nil, fmt.Errorf("failed insert baseline snapshot: %w", err)
		}
		return &CompensationPlan{insertTable: m.Table, insertBaseline: maxRowID.Int64}, nil
	case ast.ClassUpdate, ast.ClassDelete:
		if m.WhereClause == "" {
			return nil, fmt.Errorf("mutation is irreversible or lacks table/predicate: %s", m.RawQuery)
		}
	default:
		return nil, fmt.Errorf("unsupported mutation class %s: %s", m.Class, m.RawQuery)
	}

	plan, err := c.sqlitePreImage(ctx, m)
	if err != nil {
		// Not SQLite (no rowid / quote()): fall back to driver-formatted literals.
		plan, err = c.genericPreImage(ctx, m)
	}
	if err != nil {
		return nil, err
	}
	plan.CompensatingSQL = strings.Join(plan.Statements, "\n")
	return plan, nil
}

// Complete finishes an INSERT plan once the insert has run, producing a
// DELETE for exactly the rows that were added.
func (c *PostgresCompensator) Complete(ctx context.Context, p *CompensationPlan) error {
	if !p.NeedsCompletion() {
		return nil
	}
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("SELECT rowid FROM %s WHERE rowid > %d ORDER BY rowid", p.insertTable, p.insertBaseline))
	if err != nil {
		return fmt.Errorf("failed insert post-image snapshot: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, fmt.Sprint(id))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	p.RowsAffected = len(ids)
	p.Statements = nil
	if len(ids) > 0 {
		p.Statements = []string{fmt.Sprintf("DELETE FROM %s WHERE rowid IN (%s);", p.insertTable, strings.Join(ids, ", "))}
	}
	p.CompensatingSQL = strings.Join(p.Statements, "\n")
	return nil
}

// sqlitePreImage snapshots matching rows keyed by rowid, using SQLite's
// quote() so every value round-trips exactly (text, blobs, reals, NULL).
// Keying by rowid keeps the restore correct when the predicate matches
// several rows or references columns the mutation changes.
func (c *PostgresCompensator) sqlitePreImage(ctx context.Context, m ast.ParsedMutation) (*CompensationPlan, error) {
	cols, err := c.columns(ctx, m.Table)
	if err != nil {
		return nil, err
	}
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = fmt.Sprintf("quote(%s)", quoteIdent(col))
	}
	q := fmt.Sprintf("SELECT rowid, %s FROM %s WHERE %s", strings.Join(quoted, ", "), m.Table, m.WhereClause)
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	identCols := make([]string, len(cols))
	for i, col := range cols {
		identCols[i] = quoteIdent(col)
	}

	plan := &CompensationPlan{}
	for rows.Next() {
		var rowID int64
		lits := make([]string, len(cols))
		dest := []any{&rowID}
		for i := range lits {
			dest = append(dest, &lits[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		plan.RowsAffected++
		if m.Class == ast.ClassUpdate {
			sets := make([]string, len(cols))
			for i := range cols {
				sets[i] = fmt.Sprintf("%s = %s", identCols[i], lits[i])
			}
			plan.Statements = append(plan.Statements, fmt.Sprintf("UPDATE %s SET %s WHERE rowid = %d;", m.Table, strings.Join(sets, ", "), rowID))
		} else {
			plan.Statements = append(plan.Statements, fmt.Sprintf("INSERT INTO %s (rowid, %s) VALUES (%d, %s);", m.Table, strings.Join(identCols, ", "), rowID, strings.Join(lits, ", ")))
		}
	}
	return plan, rows.Err()
}

func (c *PostgresCompensator) genericPreImage(ctx context.Context, m ast.ParsedMutation) (*CompensationPlan, error) {
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s WHERE %s", m.Table, m.WhereClause))
	if err != nil {
		return nil, fmt.Errorf("failed pre-image snapshot: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	plan := &CompensationPlan{}
	for rows.Next() {
		plan.RowsAffected++
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		lits := make([]string, len(values))
		for i, v := range values {
			lits[i] = formatSQLLiteral(v)
		}
		if m.Class == ast.ClassUpdate {
			sets := make([]string, len(cols))
			for i, col := range cols {
				sets[i] = fmt.Sprintf("%s = %s", col, lits[i])
			}
			plan.Statements = append(plan.Statements, fmt.Sprintf("UPDATE %s SET %s WHERE %s;", m.Table, strings.Join(sets, ", "), m.WhereClause))
		} else {
			plan.Statements = append(plan.Statements, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", m.Table, strings.Join(cols, ", "), strings.Join(lits, ", ")))
		}
	}
	return plan, rows.Err()
}

func (c *PostgresCompensator) columns(ctx context.Context, table string) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rows.Columns()
}

func (c *PostgresCompensator) queryRow(ctx context.Context, q string, dest ...any) error {
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	return rows.Scan(dest...)
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func formatSQLLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case []byte:
		return "X'" + hex.EncodeToString(val) + "'"
	case time.Time:
		return "'" + val.Format(time.RFC3339Nano) + "'"
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("%v", val)
	}
}
