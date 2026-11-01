package ast

import (
	"strings"
	"unicode"
)

type SQLMutationClass string

const (
	ClassSelect  SQLMutationClass = "SELECT"
	ClassInsert  SQLMutationClass = "INSERT"
	ClassUpdate  SQLMutationClass = "UPDATE"
	ClassDelete  SQLMutationClass = "DELETE"
	ClassDDL     SQLMutationClass = "DDL"
	ClassUnknown SQLMutationClass = "UNKNOWN"
)

type ParsedMutation struct {
	Class        SQLMutationClass
	Table        string
	WhereClause  string
	RawQuery     string
	IsReversible bool
}

func InspectSQL(query string) ParsedMutation {
	clean := stripSQLComments(query)
	clean = strings.TrimSpace(clean)
	upper := strings.ToUpper(clean)

	res := ParsedMutation{
		RawQuery: query,
		Class:    ClassUnknown,
	}

	if strings.HasPrefix(upper, "SELECT") {
		res.Class = ClassSelect
		res.IsReversible = true
		return res
	}

	for _, ddl := range []string{"DROP", "ALTER", "TRUNCATE", "CREATE", "RENAME"} {
		if strings.HasPrefix(upper, ddl) {
			res.Class = ClassDDL
			res.IsReversible = false
			return res
		}
	}

	if strings.HasPrefix(upper, "UPDATE") {
		res.Class = ClassUpdate
		parts := strings.Fields(clean)
		if len(parts) >= 2 {
			res.Table = strings.Trim(parts[1], `";`)
		}
		if idx := strings.Index(upper, " WHERE "); idx != -1 {
			res.WhereClause = strings.TrimSpace(clean[idx+7:])
			res.WhereClause = strings.TrimSuffix(res.WhereClause, ";")
			res.IsReversible = true
		} else {
			res.IsReversible = false
		}
		return res
	}

	if strings.HasPrefix(upper, "DELETE") {
		res.Class = ClassDelete
		parts := strings.Fields(clean)
		if len(parts) >= 3 && strings.ToUpper(parts[1]) == "FROM" {
			res.Table = strings.Trim(parts[2], `";`)
		}
		if idx := strings.Index(upper, " WHERE "); idx != -1 {
			res.WhereClause = strings.TrimSpace(clean[idx+7:])
			res.WhereClause = strings.TrimSuffix(res.WhereClause, ";")
			res.IsReversible = true
		} else {
			res.IsReversible = false
		}
		return res
	}

	return res
}

func stripSQLComments(query string) string {
	lines := strings.Split(query, "\n")
	var out []string
	for _, l := range lines {
		trim := strings.TrimLeftFunc(l, unicode.IsSpace)
		if !strings.HasPrefix(trim, "--") {
			out = append(out, l)
		}
	}
	return strings.Join(out, " ")
}
