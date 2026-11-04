package ast

import (
	"regexp"
	"strings"
	"unicode"
)

var (
	whereRe  = regexp.MustCompile(`(?i)\sWHERE\s`)
	insertRe = regexp.MustCompile(`(?is)^INSERT\s+(?:OR\s+(\w+)\s+)?INTO\s+([^\s(;]+)`)
)

// extractWhere returns the predicate after the first WHERE keyword.
func extractWhere(clean string) string {
	loc := whereRe.FindStringIndex(clean)
	if loc == nil {
		return ""
	}
	w := strings.TrimSpace(clean[loc[1]:])
	return strings.TrimSpace(strings.TrimSuffix(w, ";"))
}

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
		res.WhereClause = extractWhere(clean)
		res.IsReversible = res.WhereClause != ""
		return res
	}

	if strings.HasPrefix(upper, "DELETE") {
		res.Class = ClassDelete
		parts := strings.Fields(clean)
		if len(parts) >= 3 && strings.ToUpper(parts[1]) == "FROM" {
			res.Table = strings.Trim(parts[2], `";`)
		}
		res.WhereClause = extractWhere(clean)
		res.IsReversible = res.WhereClause != ""
		return res
	}

	if strings.HasPrefix(upper, "INSERT") {
		res.Class = ClassInsert
		if m := insertRe.FindStringSubmatch(clean); m != nil {
			res.Table = strings.Trim(m[2], `"`)
			// OR REPLACE overwrites existing rows, which a rowid diff cannot undo.
			res.IsReversible = !strings.EqualFold(m[1], "REPLACE")
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
