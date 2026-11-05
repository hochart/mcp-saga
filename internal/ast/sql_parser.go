package ast

import (
	"regexp"
	"strings"
	"unicode"
)

var (
	whereRe  = regexp.MustCompile(`(?i)\sWHERE\s`)
	insertRe = regexp.MustCompile(`(?is)^INSERT\s+(?:OR\s+(\w+)\s+)?INTO\s+([^\s(;]+)`)
	// An upsert may update an existing row instead of inserting one.
	upsertRe = regexp.MustCompile(`(?is)\bON\s+CONFLICT\b.*\bDO\s+UPDATE\b`)
)

// insertColumns returns the explicit column list following the table name of
// an INSERT, or nil when the statement has none (every column is supplied).
func insertColumns(rest string) []string {
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "(") {
		return nil
	}
	end := strings.IndexByte(rest, ')')
	if end < 0 {
		return nil
	}
	var cols []string
	for _, c := range strings.Split(rest[1:end], ",") {
		c = strings.Trim(strings.TrimSpace(c), "\"`[]")
		if c != "" {
			cols = append(cols, c)
		}
	}
	return cols
}

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

	// InsertColumns is the explicit column list of an INSERT; nil means the
	// statement supplies every column (no list, or INSERT ... SELECT *).
	InsertColumns []string
	// InsertDefaultValues is set for INSERT ... DEFAULT VALUES.
	InsertDefaultValues bool
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
		if loc := insertRe.FindStringSubmatchIndex(clean); loc != nil {
			orAction := ""
			if loc[2] >= 0 {
				orAction = clean[loc[2]:loc[3]]
			}
			res.Table = strings.Trim(clean[loc[4]:loc[5]], `"`)
			rest := clean[loc[1]:]
			res.InsertColumns = insertColumns(rest)
			res.InsertDefaultValues = res.InsertColumns == nil &&
				strings.HasPrefix(strings.ToUpper(strings.TrimSpace(rest)), "DEFAULT VALUES")
			// OR REPLACE and upserts can overwrite existing rows, which
			// deleting the newly added rowids cannot undo.
			res.IsReversible = !strings.EqualFold(orAction, "REPLACE") && !upsertRe.MatchString(clean)
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
