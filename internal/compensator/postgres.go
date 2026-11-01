package compensator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"mcp-saga/internal/ast"
)

type DBQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type CompensationPlan struct {
	CompensatingSQL string
	RowsAffected    int
}

type PostgresCompensator struct {
	db DBQueryer
}

func NewPostgresCompensator(db DBQueryer) *PostgresCompensator {
	return &PostgresCompensator{db: db}
}

func (c *PostgresCompensator) GenerateCompensation(ctx context.Context, m ast.ParsedMutation) (*CompensationPlan, error) {
	if m.Class == ast.ClassSelect {
		return &CompensationPlan{CompensatingSQL: "", RowsAffected: 0}, nil
	}
	if !m.IsReversible || m.Table == "" || m.WhereClause == "" {
		return nil, fmt.Errorf("mutation is irreversible or lacks table/predicate: %s", m.RawQuery)
	}

	preQuery := fmt.Sprintf("SELECT * FROM %s WHERE %s", m.Table, m.WhereClause)
	rows, err := c.db.QueryContext(ctx, preQuery)
	if err != nil {
		return nil, fmt.Errorf("failed pre-image snapshot: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var rollbackStatements []string
	rowCount := 0

	for rows.Next() {
		rowCount++
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		if m.Class == ast.ClassUpdate {
			var setClauses []string
			for i, col := range cols {
				setClauses = append(setClauses, fmt.Sprintf("%s = %s", col, formatSQLLiteral(values[i])))
			}
			stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s;", m.Table, strings.Join(setClauses, ", "), m.WhereClause)
			rollbackStatements = append(rollbackStatements, stmt)
		} else if m.Class == ast.ClassDelete {
			var litVals []string
			for _, val := range values {
				litVals = append(litVals, formatSQLLiteral(val))
			}
			stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", m.Table, strings.Join(cols, ", "), strings.Join(litVals, ", "))
			rollbackStatements = append(rollbackStatements, stmt)
		}
	}

	return &CompensationPlan{
		CompensatingSQL: strings.Join(rollbackStatements, "\n"),
		RowsAffected:    rowCount,
	}, nil
}

func formatSQLLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(val), "'", "''") + "'"
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
