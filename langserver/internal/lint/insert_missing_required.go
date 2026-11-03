package lint

import (
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var InsertMissingRequired = &Analyzer{
	Name:           "insert-missing-required",
	Doc:            "reports INSERT column lists, also in MERGE, that leave out a REQUIRED column without a default value, which BigQuery rejects only at run time",
	DefaultEnabled: true,
	Run:            runInsertMissingRequired,
}

// The column lists are read from the AST, since the resolved
// insert_column_list of a statement without one holds every column.
func runInsertMissingRequired(pass *Pass) {
	lists := make(map[offsets][]*googlesql.ASTColumnList)
	for _, s := range file.ListAstNode[*googlesql.ASTInsertStatement](pass.File.Node) {
		path, err := s.GetTargetPathForNonNested()
		if err != nil || path == nil {
			continue
		}
		if cl, err := s.ColumnList(); err == nil && cl != nil {
			at := astOffsets(path)
			lists[at] = append(lists[at], cl)
		}
	}
	for _, s := range file.ListAstNode[*googlesql.ASTMergeStatement](pass.File.Node) {
		path, err := s.TargetPath()
		if err != nil || path == nil {
			continue
		}
		whens, err := s.WhenClauses()
		if err != nil || whens == nil {
			continue
		}
		at := astOffsets(path)
		for _, c := range children(whens) {
			when, ok := c.(*googlesql.ASTMergeWhenClause)
			if !ok {
				continue
			}
			action, err := when.Action()
			if err != nil || action == nil {
				continue
			}
			if cl, err := action.InsertColumnList(); err == nil && cl != nil {
				lists[at] = append(lists[at], cl)
			}
		}
	}

	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		var target dmlTarget
		var ok bool
		switch stmt := stmt.(type) {
		case *googlesql.ResolvedInsertStmt:
			target, ok = lookupDMLTarget(pass, stmt.TableScan)
		case *googlesql.ResolvedMergeStmt:
			target, ok = lookupDMLTarget(pass, stmt.TableScan)
		}
		if !ok {
			continue
		}
		for _, cl := range lists[target.offsets] {
			checkMissingRequired(pass, target, cl)
		}
	}
}

func checkMissingRequired(pass *Pass, target dmlTarget, cl *googlesql.ASTColumnList) {
	listed := make(map[string]bool)
	for _, c := range children(cl) {
		id, ok := c.(*googlesql.ASTIdentifier)
		if !ok {
			return
		}
		name, err := id.GetAsString()
		if err != nil {
			return
		}
		listed[strings.ToLower(name)] = true
	}
	for _, f := range target.schema {
		if !missingRequired(f, listed) {
			continue
		}
		at := astOffsets(cl)
		r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
		if !ok {
			return
		}
		pass.Report(Diagnostic{
			Range:    r,
			Message:  fmt.Sprintf("the column list leaves out `%s`, which is REQUIRED in `%s` and has no default value; BigQuery fails the statement at run time once it writes a row", f.Name, target.name),
			Severity: SeverityWarning,
		})
	}
}

func missingRequired(f *bq.FieldSchema, listed map[string]bool) bool {
	return f.Required && f.DefaultValueExpression == "" && !listed[strings.ToLower(f.Name)]
}
