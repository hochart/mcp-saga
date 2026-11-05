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
	// diffs the table's rowids against this baseline. preRowIDs is only
	// captured when the statement supplies the key itself, since an
	// explicit key can land below insertBaseline.
	insertTable    string
	insertBaseline int64
	insertCount    int64
	preRowIDs      map[int64]struct{}
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
		return c.insertBaseline(ctx, m)
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

// insertBaseline records what Complete needs to identify the rows an INSERT
// adds. Auto-assigned rowids always exceed the current maximum, so the
// maximum alone suffices; an explicitly supplied key can fill a gap below
// it, so for those statements the existing rowid set is captured too.
func (c *PostgresCompensator) insertBaseline(ctx context.Context, m ast.ParsedMutation) (*CompensationPlan, error) {
	var count int64
	var maxRowID sql.NullInt64
	if err := c.queryRow(ctx, fmt.Sprintf("SELECT COUNT(*), MAX(rowid) FROM %s", m.Table), &count, &maxRowID); err != nil {
		return nil, fmt.Errorf("failed insert baseline snapshot: %w", err)
	}
	plan := &CompensationPlan{insertTable: m.Table, insertBaseline: maxRowID.Int64, insertCount: count}
	if c.keySuppliedExplicitly(ctx, m) {
		ids, err := c.rowIDs(ctx, fmt.Sprintf("SELECT rowid FROM %s", m.Table))
		if err != nil {
			return nil, fmt.Errorf("failed insert baseline snapshot: %w", err)
		}
		plan.preRowIDs = make(map[int64]struct{}, len(ids))
		for _, id := range ids {
			plan.preRowIDs[id] = struct{}{}
		}
	}
	return plan, nil
}

// keySuppliedExplicitly reports whether an INSERT may set the rowid itself:
// it has no column list, or its list names rowid or the table's INTEGER
// PRIMARY KEY (the rowid alias). When unsure it answers true, which only
// costs a fuller snapshot.
func (c *PostgresCompensator) keySuppliedExplicitly(ctx context.Context, m ast.ParsedMutation) bool {
	if m.InsertDefaultValues {
		return false
	}
	if m.InsertColumns == nil {
		return true
	}
	keyNames := map[string]bool{"rowid": true, "oid": true, "_rowid_": true}
	alias, ok := c.rowIDAlias(ctx, m.Table)
	if !ok {
		return true
	}
	if alias != "" {
		keyNames[strings.ToLower(alias)] = true
	}
	for _, col := range m.InsertColumns {
		if keyNames[strings.ToLower(col)] {
			return true
		}
	}
	return false
}

// rowIDAlias returns the column aliasing rowid (a sole INTEGER PRIMARY KEY),
// "" if there is none, and ok=false if the schema could not be read.
func (c *PostgresCompensator) rowIDAlias(ctx context.Context, table string) (alias string, ok bool) {
	rows, err := c.db.QueryContext(ctx, "SELECT name, type, pk FROM pragma_table_info(?)", table)
	if err != nil {
		return "", false
	}
	defer rows.Close()
	var pkCols []string
	var pkType string
	n := 0
	for rows.Next() {
		var name, typ string
		var pk int
		if err := rows.Scan(&name, &typ, &pk); err != nil {
			return "", false
		}
		n++
		if pk > 0 {
			pkCols = append(pkCols, name)
			pkType = typ
		}
	}
	if rows.Err() != nil || n == 0 {
		return "", false
	}
	if len(pkCols) == 1 && strings.EqualFold(strings.TrimSpace(pkType), "INTEGER") {
		return pkCols[0], true
	}
	return "", true
}

// Complete finishes an INSERT plan once the insert has run, producing a
// DELETE for exactly the rows that were added. It fails rather than record
// a wrong compensation when the rows it identifies do not account for the
// table's change in row count (e.g. a trigger or another writer).
func (c *PostgresCompensator) Complete(ctx context.Context, p *CompensationPlan) error {
	if !p.NeedsCompletion() {
		return nil
	}
	var added []int64
	if p.preRowIDs != nil {
		all, err := c.rowIDs(ctx, fmt.Sprintf("SELECT rowid FROM %s ORDER BY rowid", p.insertTable))
		if err != nil {
			return fmt.Errorf("failed insert post-image snapshot: %w", err)
		}
		for _, id := range all {
			if _, existed := p.preRowIDs[id]; !existed {
				added = append(added, id)
			}
		}
	} else {
		var err error
		added, err = c.rowIDs(ctx, fmt.Sprintf("SELECT rowid FROM %s WHERE rowid > %d ORDER BY rowid", p.insertTable, p.insertBaseline))
		if err != nil {
			return fmt.Errorf("failed insert post-image snapshot: %w", err)
		}
	}

	var count int64
	if err := c.queryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", p.insertTable), &count); err != nil {
		return fmt.Errorf("failed insert post-image snapshot: %w", err)
	}
	if grew := count - p.insertCount; grew != int64(len(added)) {
		return fmt.Errorf("table %s grew by %d row(s) but %d new rowid(s) were found; refusing to record an inexact compensation", p.insertTable, grew, len(added))
	}

	p.RowsAffected = len(added)
	p.Statements = nil
	if len(added) > 0 {
		ids := make([]string, len(added))
		for i, id := range added {
			ids[i] = fmt.Sprint(id)
		}
		p.Statements = []string{fmt.Sprintf("DELETE FROM %s WHERE rowid IN (%s);", p.insertTable, strings.Join(ids, ", "))}
	}
	p.CompensatingSQL = strings.Join(p.Statements, "\n")
	p.preRowIDs = nil
	return nil
}

func (c *PostgresCompensator) rowIDs(ctx context.Context, q string) ([]int64, error) {
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
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
