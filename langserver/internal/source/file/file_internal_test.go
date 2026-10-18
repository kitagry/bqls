package file

import (
	"errors"
	"strings"
	"testing"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func TestParseZetaSQLError_ConvertsColumnToByteOffset(t *testing.T) {
	// GoogleSQL reports 1-based columns counted in code points, with tabs
	// expanded to the next multiple of 8. The expected Character is the byte
	// offset of "X" within its line.
	tests := map[string]struct {
		src    string
		errMsg string
		want   lsp.Position
	}{
		"ascii only": {
			src:    "SELECT X",
			errMsg: "msg [at 1:8]",
			want:   lsp.Position{Line: 0, Character: 7},
		},
		"multibyte character before the error": {
			src:    "SELECT 'あ', X",
			errMsg: "msg [at 1:13]",
			want:   lsp.Position{Line: 0, Character: 14},
		},
		"surrogate pair character before the error": {
			src:    "SELECT '😀', X",
			errMsg: "msg [at 1:13]",
			want:   lsp.Position{Line: 0, Character: 15},
		},
		"tab at the beginning of the line": {
			src:    "\tX",
			errMsg: "msg [at 1:9]",
			want:   lsp.Position{Line: 0, Character: 1},
		},
		"tab in the middle of the line": {
			src:    "SELECT\t1, X",
			errMsg: "msg [at 1:12]",
			want:   lsp.Position{Line: 0, Character: 10},
		},
		"consecutive tabs": {
			src:    "SELECT 1,\t\tX",
			errMsg: "msg [at 1:25]",
			want:   lsp.Position{Line: 0, Character: 11},
		},
		"tab and multibyte on the second line": {
			src:    "SELECT 1;\n\t'あ' || X",
			errMsg: "msg [at 2:16]",
			want:   lsp.Position{Line: 1, Character: 10},
		},
		"crlf line endings": {
			src:    "SELECT 1,\r\n\tX",
			errMsg: "msg [at 2:9]",
			want:   lsp.Position{Line: 1, Character: 1},
		},
		"end of script": {
			src:    "SELECT 'あ' +",
			errMsg: "msg [at 1:13]",
			want:   lsp.Position{Line: 0, Character: 14},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := parseZetaSQLError(tt.src, errors.New(tt.errMsg))
			if diff := cmp.Diff(Error{Msg: "msg", Position: tt.want}, got); diff != "" {
				t.Errorf("parseZetaSQLError(%q, %q) diff (-want +got):\n%s", tt.src, tt.errMsg, diff)
			}
		})
	}
}

func TestFixFunctions_KeepSourceWhenErrorIsOutOfRange(t *testing.T) {
	const src = "SELECT x"
	tests := map[string]struct {
		parsedErr Error
	}{
		"position beyond the end of the line": {
			parsedErr: Error{Position: lsp.Position{Line: 0, Character: 100}, TermLength: 1, IncompleteColumnName: "x"},
		},
		"position beyond the last line": {
			parsedErr: Error{Position: lsp.Position{Line: 5, Character: 0}, TermLength: 1, IncompleteColumnName: "x"},
		},
	}

	fixes := map[string]func(src string, parsedErr Error) (string, []FixOffset){
		"fixDeclarationError": func(src string, parsedErr Error) (string, []FixOffset) {
			return fixDeclarationError(src, parsedErr, "0")
		},
		"fixErrorToLiteral": fixErrorToLiteral,
		"fixErrorForWhereStatement": func(src string, parsedErr Error) (string, []FixOffset) {
			return fixErrorForWhereStatement(src, nil, parsedErr)
		},
		"fixSelectListMustNotBeEmptyStatement": func(src string, parsedErr Error) (string, []FixOffset) {
			fixed, _, fo := fixSelectListMustNotBeEmptyStatement(src, parsedErr)
			return fixed, fo
		},
		"fixOnlyWithClauseSyntaxError": func(src string, parsedErr Error) (string, []FixOffset) {
			fixed, _, fo := fixOnlyWithClauseSyntaxError(src, parsedErr)
			return fixed, fo
		},
		"fixUnexpectedEndOfScriptWithDeletion": func(src string, parsedErr Error) (string, []FixOffset) {
			fixed, _, fo := fixUnexpectedEndOfScriptWithDeletion(src+" GROUP BY", parsedErr, []string{"GROUP BY"})
			return strings.TrimSuffix(fixed, " GROUP BY"), fo
		},
		"fixUnexpectedEndOfScriptWithCondition": func(src string, parsedErr Error) (string, []FixOffset) {
			fixed, _, fo := fixUnexpectedEndOfScriptWithCondition(src+" WHERE", parsedErr, []string{"WHERE"})
			return strings.TrimSuffix(fixed, " WHERE"), fo
		},
	}

	for fixName, fix := range fixes {
		for name, tt := range tests {
			t.Run(fixName+"/"+name, func(t *testing.T) {
				gotSrc, gotOffsets := fix(src, tt.parsedErr)
				if gotSrc != src || len(gotOffsets) != 0 {
					t.Errorf("expected source to be kept as %q without fix offsets, got %q, %v", src, gotSrc, gotOffsets)
				}
			})
		}
	}
}

func TestFixFunctions_KeepSourceWhenTermOverflowsSource(t *testing.T) {
	const src = "SELECT x"
	parsedErr := Error{Position: lsp.Position{Line: 0, Character: 7}, TermLength: 11, IncompleteColumnName: "my_variable"}

	fixes := map[string]func(src string, parsedErr Error) (string, []FixOffset){
		"fixDeclarationError": func(src string, parsedErr Error) (string, []FixOffset) {
			return fixDeclarationError(src, parsedErr, "0")
		},
		"fixErrorToLiteral": fixErrorToLiteral,
		"fixErrorForWhereStatement": func(src string, parsedErr Error) (string, []FixOffset) {
			return fixErrorForWhereStatement(src, nil, parsedErr)
		},
	}

	for name, fix := range fixes {
		t.Run(name, func(t *testing.T) {
			gotSrc, gotOffsets := fix(src, parsedErr)
			if gotSrc != src || len(gotOffsets) != 0 {
				t.Errorf("expected source to be kept as %q without fix offsets, got %q, %v", src, gotSrc, gotOffsets)
			}
		})
	}
}

// addInformationToNotExistInStructErrorWithoutAST checks the text-based fallback used for path expressions.
func addInformationToNotExistInStructErrorWithoutAST(src string, parsedErr Error) Error {
	return addInformationToNotExistInStructError(src, nil, parsedErr)
}

func TestAddInformationFunctions_KeepErrorWhenOutOfRange(t *testing.T) {
	const src = "SELECT x"
	tests := map[string]struct {
		add       func(src string, parsedErr Error) Error
		parsedErr Error
	}{
		"addInformationToNotExistInStructError": {
			add:       addInformationToNotExistInStructErrorWithoutAST,
			parsedErr: Error{Msg: "Field name too_long_name does not exist in STRUCT", Position: lsp.Position{Line: 0, Character: 7}},
		},
		"addInformationToNotFoundInsideTableError": {
			add:       addInformationToNotFoundInsideTableError,
			parsedErr: Error{Msg: "Name too_long_name not found inside t", Position: lsp.Position{Line: 0, Character: 7}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := tt.add(src, tt.parsedErr)
			if diff := cmp.Diff(tt.parsedErr, got); diff != "" {
				t.Errorf("expected error to be unchanged (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAddInformationFunctions_StopAtAnyWhitespace(t *testing.T) {
	tests := map[string]struct {
		add       func(src string, parsedErr Error) Error
		src       string
		parsedErr Error
		want      string
	}{
		"addInformationToNotExistInStructError after tab": {
			add:       addInformationToNotExistInStructErrorWithoutAST,
			src:       "SELECT 'a',\ts.zz, id FROM t",
			parsedErr: Error{Msg: "Field name zz does not exist in STRUCT<id INT64>", Position: lsp.Position{Line: 0, Character: 14}},
			want:      "s.zz",
		},
		"addInformationToNotExistInStructError after newline": {
			add:       addInformationToNotExistInStructErrorWithoutAST,
			src:       "SELECT id,\n  'a',\ns.zz FROM t",
			parsedErr: Error{Msg: "Field name zz does not exist in STRUCT<id INT64>", Position: lsp.Position{Line: 2, Character: 2}},
			want:      "s.zz",
		},
		"addInformationToNotFoundInsideTableError after tab": {
			add:       addInformationToNotFoundInsideTableError,
			src:       "SELECT 'a',\ts.zz, id FROM t",
			parsedErr: Error{Msg: "Name zz not found inside s", Position: lsp.Position{Line: 0, Character: 14}},
			want:      "s.zz",
		},
		"addInformationToNotFoundInsideTableError after newline": {
			add:       addInformationToNotFoundInsideTableError,
			src:       "SELECT id,\n  'a',\ns.zz FROM t",
			parsedErr: Error{Msg: "Name zz not found inside s", Position: lsp.Position{Line: 2, Character: 2}},
			want:      "s.zz",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := tt.add(tt.src, tt.parsedErr)
			if got.IncompleteColumnName != tt.want {
				t.Errorf("IncompleteColumnName = %q, want %q", got.IncompleteColumnName, tt.want)
			}
		})
	}
}
func TestParseZetaSQLError_MalformedLocation(t *testing.T) {
	tests := map[string]struct {
		errMsg string
		want   Error
	}{
		"no closing bracket": {
			errMsg: "msg [at ",
			want:   Error{Msg: "msg [at "},
		},
		"no column": {
			errMsg: "msg [at 1]",
			want:   Error{Msg: "msg [at 1]"},
		},
		"non numeric location": {
			errMsg: "msg [at x:y]",
			want:   Error{Msg: "msg [at x:y]"},
		},
		"several locations": {
			errMsg: "msg [at 1:8]; inner [at 2:3]",
			want:   Error{Msg: "msg", Position: lsp.Position{Line: 0, Character: 7}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := parseZetaSQLError("SELECT X\nSELECT 1", errors.New(tt.errMsg))
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("parseZetaSQLError(%q) diff (-want +got):\n%s", tt.errMsg, diff)
			}
		})
	}
}

func TestAddInformationFunctions_NameAtEndOfMessage(t *testing.T) {
	const src = "SELECT t.zz FROM t"
	tests := map[string]struct {
		add       func(src string, parsedErr Error) Error
		parsedErr Error
	}{
		"addInformationToNotExistInStructError": {
			add:       addInformationToNotExistInStructErrorWithoutAST,
			parsedErr: Error{Msg: "Field name zz", Position: lsp.Position{Line: 0, Character: 9}},
		},
		"addInformationToNotFoundInsideTableError": {
			add:       addInformationToNotFoundInsideTableError,
			parsedErr: Error{Msg: "Name zz", Position: lsp.Position{Line: 0, Character: 9}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := tt.add(src, tt.parsedErr)
			want := tt.parsedErr
			want.TermLength = 2
			want.IncompleteColumnName = "t.zz"
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFixDot_KeepsTrailingNewline(t *testing.T) {
	tests := map[string]struct {
		src  string
		want string
	}{
		"lf": {
			src:  "SELECT t. FROM t\n",
			want: "SELECT true FROM t\n",
		},
		"crlf": {
			src:  "SELECT * FROM t WHERE 1=1\r\n\n",
			want: "SELECT * FROM t WHERE 1=1\r\n\n",
		},
		"no trailing newline": {
			src:  "SELECT t. FROM t",
			want: "SELECT true FROM t",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, _, _ := fixDot(tt.src)
			if got != tt.want {
				t.Errorf("fixDot(%q) = %q, want %q", tt.src, got, tt.want)
			}
		})
	}
}

func TestFixDot_WordBeforeSemicolon(t *testing.T) {
	got, errs, _ := fixDot("SELECT r.;")
	if want := "SELECT true;"; got != want {
		t.Errorf("fixDot() = %q, want %q", got, want)
	}
	if len(errs) != 1 || errs[0].IncompleteColumnName != "r." {
		t.Errorf("fixDot() errors = %+v, want one error for %q", errs, "r.")
	}
}

func TestFixDot_HyphenatedTablePath(t *testing.T) {
	tests := map[string]struct {
		src            string
		want           string
		wantIncomplete string
	}{
		"after FROM": {
			src:            "SELECT long FROM my-project.dataset.",
			want:           "SELECT long FROM true",
			wantIncomplete: "my-project.dataset.",
		},
		"after JOIN": {
			src:            "SELECT * FROM t join my-project.dataset. USING (id)",
			want:           "SELECT * FROM t join true USING (id)",
			wantIncomplete: "my-project.dataset.",
		},
		"minus in select list is not a table path": {
			src:            "SELECT 1-t. FROM t",
			want:           "SELECT 1-true FROM t",
			wantIncomplete: "t.",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, errs, _ := fixDot(tt.src)
			if got != tt.want {
				t.Errorf("fixDot(%q) = %q, want %q", tt.src, got, tt.want)
			}
			if len(errs) != 1 || errs[0].IncompleteColumnName != tt.wantIncomplete {
				t.Errorf("fixDot() errors = %+v, want one error for %q", errs, tt.wantIncomplete)
			}
		})
	}
}

func TestFixIncompletePipeOperator(t *testing.T) {
	tests := map[string]struct {
		src            string
		errPosition    lsp.Position
		wantSrc        string
		wantFixOffsets []FixOffset
	}{
		"pipe operator at the end": {
			src:            "FROM t |> ",
			errPosition:    lsp.Position{Line: 0, Character: 9},
			wantSrc:        "FROM t  ",
			wantFixOffsets: []FixOffset{{Offset: 9, Length: -2, edits: []sourceEdit{{start: 7, oldLen: 2}}}},
		},
		"pipe operator with keywords at the end": {
			src:            "FROM t |> ORDER BY",
			errPosition:    lsp.Position{Line: 0, Character: 18},
			wantSrc:        "FROM t ",
			wantFixOffsets: []FixOffset{{Offset: 18, Length: -11, edits: []sourceEdit{{start: 7, oldLen: 11}}}},
		},
		"pipe operator followed by a semicolon": {
			src:            "FROM t |> WHERE; SELECT 1",
			errPosition:    lsp.Position{Line: 0, Character: 15},
			wantSrc:        "FROM t ; SELECT 1",
			wantFixOffsets: []FixOffset{{Offset: 15, Length: -8, edits: []sourceEdit{{start: 7, oldLen: 8}}}},
		},
		"pipe operator followed by a comment": {
			src:            "FROM t |> SELECT -- c",
			errPosition:    lsp.Position{Line: 0, Character: 21},
			wantSrc:        "FROM t  -- c",
			wantFixOffsets: []FixOffset{{Offset: 16, Length: -9, edits: []sourceEdit{{start: 7, oldLen: 9}}}},
		},
		"pipe operator followed by a semicolon on the next line": {
			src:            "FROM t |> SELECT\n;\nSELECT 1",
			errPosition:    lsp.Position{Line: 1, Character: 0},
			wantSrc:        "FROM t \n;\nSELECT 1",
			wantFixOffsets: []FixOffset{{Offset: 16, Length: -9, edits: []sourceEdit{{start: 7, oldLen: 9}}}},
		},
		"pipe operator followed by another pipe operator": {
			src:            "FROM t |> WHERE |> SELECT x",
			errPosition:    lsp.Position{Line: 0, Character: 16},
			wantSrc:        "FROM t  |> SELECT x",
			wantFixOffsets: []FixOffset{{Offset: 15, Length: -8, edits: []sourceEdit{{start: 7, oldLen: 8}}}},
		},
		"pipe operator in a subquery": {
			src:            "SELECT * FROM (FROM t |> SELECT)",
			errPosition:    lsp.Position{Line: 0, Character: 31},
			wantSrc:        "SELECT * FROM (FROM t )",
			wantFixOffsets: []FixOffset{{Offset: 31, Length: -9, edits: []sourceEdit{{start: 22, oldLen: 9}}}},
		},
		"lower case keywords": {
			src:            "FROM t |> left outer join",
			errPosition:    lsp.Position{Line: 0, Character: 25},
			wantSrc:        "FROM t ",
			wantFixOffsets: []FixOffset{{Offset: 25, Length: -18, edits: []sourceEdit{{start: 7, oldLen: 18}}}},
		},
		"identifier after pipe operator": {
			src:         "SELECT x FROM t WHERE x |> y",
			errPosition: lsp.Position{Line: 0, Character: 27},
			wantSrc:     "SELECT x FROM t WHERE x |> y",
		},
		"pipe operator in a line comment": {
			src:         "SELECT x FROM t WHERE -- a |> b",
			errPosition: lsp.Position{Line: 0, Character: 31},
			wantSrc:     "SELECT x FROM t WHERE -- a |> b",
		},
		"pipe operator in a block comment": {
			src:         "SELECT x FROM t WHERE /* a |> b */",
			errPosition: lsp.Position{Line: 0, Character: 34},
			wantSrc:     "SELECT x FROM t WHERE /* a |> b */",
		},
		"pipe operator in a string": {
			src:         "SELECT x FROM t WHERE x = '|> SELECT'",
			errPosition: lsp.Position{Line: 0, Character: 37},
			wantSrc:     "SELECT x FROM t WHERE x = '|> SELECT'",
		},
		"keyword of the following statement": {
			src:         "FROM t |> SELECT x\nSELECT",
			errPosition: lsp.Position{Line: 1, Character: 0},
			wantSrc:     "FROM t |> SELECT x\nSELECT",
		},
		"keyword on the next line": {
			src:         "FROM t |> SELECT\nSELECT",
			errPosition: lsp.Position{Line: 1, Character: 0},
			wantSrc:     "FROM t |> SELECT\nSELECT",
		},
		"error before the pipe operator": {
			src:         "SELECT FROM t |> SELECT",
			errPosition: lsp.Position{Line: 0, Character: 7},
			wantSrc:     "SELECT FROM t |> SELECT",
		},
		"position beyond the end of the line": {
			src:         "FROM t |> SELECT",
			errPosition: lsp.Position{Line: 0, Character: 100},
			wantSrc:     "FROM t |> SELECT",
		},
		"position beyond the last line": {
			src:         "FROM t |> SELECT",
			errPosition: lsp.Position{Line: 5, Character: 0},
			wantSrc:     "FROM t |> SELECT",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			gotSrc, gotFixOffsets := fixIncompletePipeOperator(tt.src, Error{Position: tt.errPosition})
			if gotSrc != tt.wantSrc {
				t.Errorf("fixed source = %q, want %q", gotSrc, tt.wantSrc)
			}
			if diff := cmp.Diff(tt.wantFixOffsets, gotFixOffsets, cmp.AllowUnexported(FixOffset{}, sourceEdit{})); diff != "" {
				t.Errorf("fix offsets diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewLanguageOptions_PipeFeatures(t *testing.T) {
	tests := map[string]struct {
		feature googlesql.LanguageFeature
		want    bool
	}{
		"pipes":                            {feature: googlesql.LanguageFeatureFeaturePipes, want: true},
		"pipe WITH":                        {feature: googlesql.LanguageFeatureFeaturePipeWith, want: true},
		"pipe named windows":               {feature: googlesql.LanguageFeatureFeaturePipeNamedWindows, want: true},
		"pipe STATIC_DESCRIBE":             {feature: googlesql.LanguageFeatureFeaturePipeStaticDescribe, want: false},
		"pipe ASSERT":                      {feature: googlesql.LanguageFeatureFeaturePipeAssert, want: false},
		"pipe LOG":                         {feature: googlesql.LanguageFeatureFeaturePipeLog, want: false},
		"pipe IF":                          {feature: googlesql.LanguageFeatureFeaturePipeIf, want: false},
		"pipe FORK":                        {feature: googlesql.LanguageFeatureFeaturePipeFork, want: false},
		"pipe TEE":                         {feature: googlesql.LanguageFeatureFeaturePipeTee, want: false},
		"pipe EXPORT DATA":                 {feature: googlesql.LanguageFeatureFeaturePipeExportData, want: false},
		"pipe CREATE TABLE":                {feature: googlesql.LanguageFeatureFeaturePipeCreateTable, want: false},
		"pipe INSERT":                      {feature: googlesql.LanguageFeatureFeaturePipeInsert, want: false},
		"pipe RECURSIVE UNION":             {feature: googlesql.LanguageFeatureFeaturePipeRecursiveUnion, want: false},
		"pipe DESCRIBE":                    {feature: googlesql.LanguageFeatureFeaturePipeDescribe, want: false},
		"pipe CALL with INPUT TABLE":       {feature: googlesql.LanguageFeatureFeaturePipeCallInputTable, want: false},
		"pipe AGGREGATE with differential": {feature: googlesql.LanguageFeatureFeaturePipeAggregateWithDifferentialPrivacy, want: false},
	}

	langOpt, err := newLanguageOptions()
	if err != nil {
		t.Fatalf("newLanguageOptions: %v", err)
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got, err := langOpt.LanguageFeatureEnabled(tt.feature)
			if err != nil {
				t.Fatalf("LanguageFeatureEnabled: %v", err)
			}
			if got != tt.want {
				t.Errorf("LanguageFeatureEnabled(%v) = %v, want %v", tt.feature, got, tt.want)
			}
		})
	}
}

func TestErrorToSrc(t *testing.T) {
	src := "SELECT aaa + bb + UNKNOWN(),\nUNKNOWN2()"
	pass1 := "SELECT 1 + bb + UNKNOWN(),\nUNKNOWN2()"
	pass2 := "SELECT 1 + 'xyz' + UNKNOWN(),\nUNKNOWN2()"
	edits := []sourceEdit{{start: 7, oldLen: len("aaa"), newLen: len("1")}, {start: 11, oldLen: len("bb"), newLen: len("'xyz'")}}

	tests := map[string]struct {
		errSrc string
		edits  []sourceEdit
		err    Error
		want   Error
	}{
		"after every rewrite on the same line": {
			errSrc: pass2,
			edits:  edits,
			err:    Error{Position: lsp.Position{Line: 0, Character: strings.Index(pass2, "UNKNOWN")}},
			want:   Error{Position: lsp.Position{Line: 0, Character: strings.Index(src, "UNKNOWN")}},
		},
		"recorded before a later rewrite": {
			errSrc: pass1,
			edits:  edits[:1],
			err:    Error{Position: lsp.Position{Line: 0, Character: strings.Index(pass1, "bb")}, TermLength: 2},
			want:   Error{Position: lsp.Position{Line: 0, Character: strings.Index(src, "bb")}, TermLength: 2},
		},
		"term is the rewritten text": {
			errSrc: pass2,
			edits:  edits,
			err:    Error{Position: lsp.Position{Line: 0, Character: strings.Index(pass2, "'xyz'")}, TermLength: len("'xyz'")},
			want:   Error{Position: lsp.Position{Line: 0, Character: strings.Index(src, "bb")}, TermLength: len("bb")},
		},
		"on a later line": {
			errSrc: pass2,
			edits:  edits,
			err:    Error{Position: lsp.Position{Line: 1, Character: 0}, TermLength: len("UNKNOWN2")},
			want:   Error{Position: lsp.Position{Line: 1, Character: 0}, TermLength: len("UNKNOWN2")},
		},
		"out of the source": {
			errSrc: pass2,
			edits:  edits,
			err:    Error{Position: lsp.Position{Line: 5, Character: 3}},
			want:   Error{Position: lsp.Position{Line: 5, Character: 3}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := errorToSrc(src, tt.errSrc, tt.edits, tt.err)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("errorToSrc diff (-want, +got)\n%s", diff)
			}
		})
	}
}

func TestErrorToSrc_InsertedText(t *testing.T) {
	src := "SELECT FROM t"
	fixed := "SELECT 1 FROM t"
	edits := []sourceEdit{{start: 7, newLen: len("1 ")}}

	tests := map[string]struct {
		err  Error
		want Error
	}{
		"inside inserted text": {
			err:  Error{Position: lsp.Position{Character: strings.Index(fixed, "1")}},
			want: Error{Position: lsp.Position{Character: strings.Index(src, "FROM")}},
		},
		"after inserted text": {
			err:  Error{Position: lsp.Position{Character: strings.Index(fixed, "t")}, TermLength: 1},
			want: Error{Position: lsp.Position{Character: strings.Index(src, "t")}, TermLength: 1},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := errorToSrc(src, fixed, edits, tt.err)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("errorToSrc diff (-want, +got)\n%s", diff)
			}
		})
	}
}

func TestErrorToSrc_DeletedText(t *testing.T) {
	src := "SELECT * FROM t |> WHERE x |> ORDER BY"
	fixed := "SELECT * FROM t |> WHERE x"
	edits := []sourceEdit{{start: len(fixed), oldLen: len(" |> ORDER BY")}}

	tests := map[string]struct {
		err  Error
		want Error
	}{
		"term right before the deletion": {
			err:  Error{Position: lsp.Position{Character: strings.Index(fixed, "x")}, TermLength: 1},
			want: Error{Position: lsp.Position{Character: strings.Index(src, "x")}, TermLength: 1},
		},
		"position at the deletion": {
			err:  Error{Position: lsp.Position{Character: len(fixed)}},
			want: Error{Position: lsp.Position{Character: len(src)}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := errorToSrc(src, fixed, edits, tt.err)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("errorToSrc diff (-want, +got)\n%s", diff)
			}
		})
	}
}

func TestFixDot_ReportsPositionsInSource(t *testing.T) {
	_, errs, _ := fixDot("SELECT a.\nFROM t WHERE bb.")

	want := []lsp.Position{{Line: 0, Character: 7}, {Line: 1, Character: 13}}
	got := make([]lsp.Position, len(errs))
	for i, e := range errs {
		got[i] = e.Position
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("fixDot error positions diff (-want, +got)\n%s", diff)
	}
}

func TestFixUnexpectedEndOfScriptWithDeletion_KeepsLeadingWhitespace(t *testing.T) {
	src := "\n\n  SELECT * FROM t GROUP BY"
	errPos, _ := byteOffsetToPosition(src, len(src))

	gotSrc, _, gotFixOffsets := fixUnexpectedEndOfScriptWithDeletion(src, Error{Position: errPos}, []string{"GROUP BY"})

	wantSrc := "\n\n  SELECT * FROM t"
	if gotSrc != wantSrc {
		t.Errorf("fixed source = %q, want %q", gotSrc, wantSrc)
	}
	wantEdits := []sourceEdit{{start: len(wantSrc), oldLen: len(" GROUP BY")}}
	if diff := cmp.Diff(wantEdits, editsOf(gotFixOffsets), cmp.AllowUnexported(sourceEdit{})); diff != "" {
		t.Errorf("edits diff (-want, +got)\n%s", diff)
	}
}

func TestFixUnexpectedEndOfScript_TrailingComment(t *testing.T) {
	tests := map[string]struct {
		src     string
		fix     func(src string, parsedErr Error) (string, Error, []FixOffset)
		wantSrc string
	}{
		"condition before a line comment": {
			src:     "SELECT x FROM t WHERE -- a b",
			fix:     fixUnexpectedEndOfScript,
			wantSrc: "SELECT x FROM t WHERE  1=1-- a b",
		},
		"condition before a block comment and a line comment": {
			src:     "SELECT x FROM t WHERE /* a */\n# b",
			fix:     fixUnexpectedEndOfScript,
			wantSrc: "SELECT x FROM t WHERE  1=1/* a */\n# b",
		},
		"deletion before a line comment": {
			src:     "SELECT x FROM t GROUP BY -- GROUP",
			fix:     fixUnexpectedEndOfScript,
			wantSrc: "SELECT x FROM t-- GROUP",
		},
		"SELECT 1 before a line comment": {
			src:     "WITH t AS (SELECT 1) -- c",
			fix:     fixOnlyWithClauseSyntaxError,
			wantSrc: "WITH t AS (SELECT 1)  SELECT 1 -- c",
		},
		"comment marker inside a string is not a comment": {
			src:     "SELECT '--' WHERE",
			fix:     fixUnexpectedEndOfScript,
			wantSrc: "SELECT '--' WHERE 1=1",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			errPos, _ := byteOffsetToPosition(tt.src, len(tt.src))
			gotSrc, _, _ := tt.fix(tt.src, Error{Position: errPos})
			if gotSrc != tt.wantSrc {
				t.Errorf("fixed source = %q, want %q", gotSrc, tt.wantSrc)
			}
		})
	}
}

func TestParsedFile_MapsOffsetsThroughShrinkingReplacement(t *testing.T) {
	// fixDeclarationError replaces the declared variable with a shorter dummy value.
	p := ParsedFile{
		Src:      "SELECT yyyyyy + 1 FROM t",
		FixedSrc: "SELECT 1 + 1 FROM t",
		edits:    []sourceEdit{{start: 7, oldLen: len("yyyyyy"), newLen: len("1")}},
	}

	t.Run("range starting at the replaced term", func(t *testing.T) {
		got, ok := p.OffsetsToLspRange(7, 12)
		want := lsp.Range{Start: lsp.Position{Character: 7}, End: lsp.Position{Character: 17}}
		if !ok {
			t.Fatal("OffsetsToLspRange returned not ok")
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("OffsetsToLspRange diff (-want, +got)\n%s", diff)
		}
	})

	tests := map[string]struct {
		pos  lsp.Position
		want int
	}{
		"start of the replaced term": {pos: lsp.Position{Character: 7}, want: 7},
		"end of the replaced term":   {pos: lsp.Position{Character: 13}, want: 8},
		"after the replaced term":    {pos: lsp.Position{Character: 17}, want: 12},
		"before the replaced term":   {pos: lsp.Position{Character: 6}, want: 6},
	}
	for name, tt := range tests {
		t.Run("TermOffset/"+name, func(t *testing.T) {
			if got := p.TermOffset(tt.pos); got != tt.want {
				t.Errorf("TermOffset(%+v) = %d, want %d", tt.pos, got, tt.want)
			}
		})
	}
}

func TestParsedFile_MapsOffsetsAtInsertionAndDeletion(t *testing.T) {
	tests := map[string]struct {
		p         ParsedFile
		pos       lsp.Position
		wantTerm  int
		fixedOff  [2]int
		wantRange lsp.Range
	}{
		"cursor at an insertion stays before the inserted text": {
			p: ParsedFile{
				Src:      "SELECT * FROM t WHERE ",
				FixedSrc: "SELECT * FROM t WHERE  1=1",
				edits:    []sourceEdit{{start: 22, newLen: len(" 1=1")}},
			},
			pos:       lsp.Position{Character: 22},
			wantTerm:  22,
			fixedOff:  [2]int{0, 26},
			wantRange: lsp.Range{End: lsp.Position{Character: 22}},
		},
		"cursor in deleted text maps to the deletion": {
			p: ParsedFile{
				Src:      "SELECT 1 FROM t GROUP BY",
				FixedSrc: "SELECT 1 FROM t",
				edits:    []sourceEdit{{start: 15, oldLen: len(" GROUP BY")}},
			},
			pos:       lsp.Position{Character: 24},
			wantTerm:  15,
			fixedOff:  [2]int{14, 15},
			wantRange: lsp.Range{Start: lsp.Position{Character: 14}, End: lsp.Position{Character: 15}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.p.TermOffset(tt.pos); got != tt.wantTerm {
				t.Errorf("TermOffset(%+v) = %d, want %d", tt.pos, got, tt.wantTerm)
			}
			got, ok := tt.p.OffsetsToLspRange(tt.fixedOff[0], tt.fixedOff[1])
			if !ok {
				t.Fatal("OffsetsToLspRange returned not ok")
			}
			if diff := cmp.Diff(tt.wantRange, got); diff != "" {
				t.Errorf("OffsetsToLspRange diff (-want, +got)\n%s", diff)
			}
		})
	}
}

func TestInsideInsertedText(t *testing.T) {
	// "SELECT 1 + -- c" became "SELECT 1 +  SELECT 1 -- c" after its first rewrite.
	text := "SELECT 1 +  SELECT 1 -- c"
	edits := []sourceEdit{{start: 11, newLen: len(" SELECT 1 ")}}

	tests := map[string]struct {
		offset int
		want   bool
	}{
		"before the insertion":                     {offset: 9, want: false},
		"inside the insertion":                     {offset: 12, want: true},
		"end of script after the trailing comment": {offset: len(text), want: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := insideInsertedText(text, edits, tt.offset); got != tt.want {
				t.Errorf("insideInsertedText(%d) = %v, want %v", tt.offset, got, tt.want)
			}
		})
	}

	t.Run("replacement is not insertion", func(t *testing.T) {
		if insideInsertedText("SELECT 1 + 1", []sourceEdit{{start: 7, oldLen: 6, newLen: 1}}, 7) {
			t.Error("a term the user wrote was treated as inserted text")
		}
	})
}

func TestFixUnexpectedEndOfScript_IncompleteClauseAndSetOperation(t *testing.T) {
	tests := map[string]struct {
		src     string
		wantSrc string
	}{
		"ORDER without BY": {
			src:     "SELECT 1 FROM t ORDER",
			wantSrc: "SELECT 1 FROM t",
		},
		"GROUP without BY": {
			src:     "SELECT 1 FROM t GROUP",
			wantSrc: "SELECT 1 FROM t",
		},
		"lowercase group by": {
			src:     "select * from t group by",
			wantSrc: "select * from t",
		},
		"lowercase order by across lines": {
			src:     "select * from t order\n  by",
			wantSrc: "select * from t",
		},
		"lowercase where": {
			src:     "select * from t where",
			wantSrc: "select * from t where 1=1",
		},
		"UNION ALL": {
			src:     "SELECT 1 UNION ALL",
			wantSrc: "SELECT 1",
		},
		"lowercase union distinct": {
			src:     "SELECT 1 union distinct",
			wantSrc: "SELECT 1",
		},
		"UNION without ALL or DISTINCT": {
			src:     "SELECT 1 UNION",
			wantSrc: "SELECT 1",
		},
		"INTERSECT DISTINCT": {
			src:     "SELECT 1 INTERSECT DISTINCT",
			wantSrc: "SELECT 1",
		},
		"EXCEPT DISTINCT before a line comment": {
			src:     "SELECT 1 EXCEPT DISTINCT -- c",
			wantSrc: "SELECT 1-- c",
		},
		"keyword as the end of another word": {
			src:     "SELECT factor",
			wantSrc: "SELECT factor",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			errPos, _ := byteOffsetToPosition(tt.src, len(tt.src))
			gotSrc, _, _ := fixUnexpectedEndOfScript(tt.src, Error{Position: errPos})
			if gotSrc != tt.wantSrc {
				t.Errorf("fixed source = %q, want %q", gotSrc, tt.wantSrc)
			}
		})
	}
}
