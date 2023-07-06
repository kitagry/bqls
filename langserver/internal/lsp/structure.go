// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.package langserver
// Source:
// https://github.com/sourcegraph/go-lsp/blob/219e11d77f5d414b4fe5ba7e532b277fca34c1b4/structures.go
package lsp

import (
	"encoding/json"
	"fmt"
)

type Position struct {
	/**
	 * Line position in a document (zero-based).
	 */
	Line int `json:"line"`

	/**
	 * Character offset on a line in a document (zero-based).
	 */
	Character int `json:"character"`
}

func (p Position) String() string {
	return fmt.Sprintf("%d:%d", p.Line, p.Character)
}

type Range struct {
	/**
	 * The range's start position.
	 */
	Start Position `json:"start"`

	/**
	 * The range's end position.
	 */
	End Position `json:"end"`
}

func (r Range) String() string {
	return fmt.Sprintf("%s-%s", r.Start, r.End)
}

type Location struct {
	URI   DocumentURI `json:"uri"`
	Range Range       `json:"range"`
}

type Diagnostic struct {
	/**
	 * The range at which the message applies.
	 */
	Range Range `json:"range"`

	/**
	 * The diagnostic's severity. Can be omitted. If omitted it is up to the
	 * client to interpret diagnostics as error, warning, info or hint.
	 */
	Severity DiagnosticSeverity `json:"severity,omitempty"`

	/**
	 * The diagnostic's code. Can be omitted.
	 */
	Code string `json:"code,omitempty"`

	/**
	 * A human-readable string describing the source of this
	 * diagnostic, e.g. 'typescript' or 'super lint'.
	 */
	Source string `json:"source,omitempty"`

	/**
	 * The diagnostic's message.
	 */
	Message string `json:"message"`
}

type DiagnosticSeverity int

const (
	Error       DiagnosticSeverity = 1
	Warning                        = 2
	Information                    = 3
	Hint                           = 4
)

type Command struct {
	/**
	 * Title of the command, like `save`.
	 */
	Title string `json:"title"`
	/**
	 * The identifier of the actual command handler.
	 */
	Command string `json:"command"`
	/**
	 * Arguments that the command handler should be
	 * invoked with.
	 */
	Arguments []interface{} `json:"arguments"`
}

type TextEdit struct {
	/**
	 * The range of the text document to be manipulated. To insert
	 * text into a document create a range where start === end.
	 */
	Range Range `json:"range"`

	/**
	 * The string to be inserted. For delete operations use an
	 * empty string.
	 */
	NewText string `json:"newText"`
}

type WorkspaceEdit struct {
	/**
	 * Holds changes to existing resources.
	 */
	Changes map[string][]TextEdit `json:"changes"`
}

type TextDocumentIdentifier struct {
	/**
	 * The text document's URI.
	 */
	URI DocumentURI `json:"uri"`
}

type TextDocumentItem struct {
	/**
	 * The text document's URI.
	 */
	URI DocumentURI `json:"uri"`

	/**
	 * The text document's language identifier.
	 */
	LanguageID string `json:"languageId"`

	/**
	 * The version number of this document (it will strictly increase after each
	 * change, including undo/redo).
	 */
	Version int `json:"version"`

	/**
	 * The content of the opened text document.
	 */
	Text string `json:"text"`
}

type VersionedTextDocumentIdentifier struct {
	TextDocumentIdentifier
	/**
	 * The version number of this document.
	 */
	Version int `json:"version"`
}

type TextDocumentPositionParams struct {
	/**
	 * The text document.
	 */
	TextDocument TextDocumentIdentifier `json:"textDocument"`

	/**
	 * The position inside the text document.
	 */
	Position Position `json:"position"`
}

type ProgressToken string

type WorkDoneProgressParams struct {
	/**
	 * An optional token that a server can use to report work done progress.
	 */
	WorkDoneToken ProgressToken `json:"workDoneToken,omitempty"`
}

type TextDocumentPrepareRenameParams struct {
	TextDocumentPositionParams
	WorkDoneProgressParams
}

type ProgressParams[T wdp] struct {
	/**
	 * An optional token that a server can use to report work done progress.
	 */
	Token ProgressToken `json:"token"`

	Value *T `json:"value,omitempty"`
}

type workDoneProgress struct {
	Kind        string  `json:"kind"`
	Title       string  `json:"title,omitempty"`
	Cancellable bool    `json:"cancellable,omitempty"`
	Message     string  `json:"message,omitempty"`
	Percentage  float64 `json:"percentage,omitempty"`
}

type wdp interface {
	IsWorkDoneProgress()
}

type WorkDoneProgressBegin struct {
	Title       string  `json:"title"`
	Cancellable bool    `json:"cancellable,omitempty"`
	Message     string  `json:"message,omitempty"`
	Percentage  float64 `json:"percentage,omitempty"`
}

func (w WorkDoneProgressBegin) IsWorkDoneProgress() {}

func (w WorkDoneProgressBegin) MarshalJSON() ([]byte, error) {
	wdp := workDoneProgress{
		Kind:        "begin",
		Title:       w.Title,
		Cancellable: w.Cancellable,
		Message:     w.Message,
		Percentage:  w.Percentage,
	}

	return json.Marshal(wdp)
}

type WorkDoneProgressReport struct {
	Cancellable bool    `json:"cancellable,omitempty"`
	Message     string  `json:"message,omitempty"`
	Percentage  float64 `json:"percentage,omitempty"`
}

func (w WorkDoneProgressReport) IsWorkDoneProgress() {}

func (w WorkDoneProgressReport) MarshalJSON() ([]byte, error) {
	wdp := workDoneProgress{
		Kind:        "report",
		Cancellable: w.Cancellable,
		Message:     w.Message,
		Percentage:  w.Percentage,
	}

	return json.Marshal(wdp)
}

type WorkDoneProgressEnd struct {
	Message string `json:"message,omitempty"`
}

func (w WorkDoneProgressEnd) IsWorkDoneProgress() {}

func (w WorkDoneProgressEnd) MarshalJSON() ([]byte, error) {
	wdp := workDoneProgress{
		Kind:    "end",
		Message: w.Message,
	}

	return json.Marshal(wdp)
}
