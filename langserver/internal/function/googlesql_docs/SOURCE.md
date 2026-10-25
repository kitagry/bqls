# Source of the Markdown files in this directory

These files are verbatim copies of the function reference documents from
[google/googlesql](https://github.com/google/googlesql) (`docs/`), at commit
`36dd14aa0657ea299725504bc0f938732f58f380` — the revision used by
`github.com/goccy/go-googlesql` v0.4.0.

They are licensed under the Apache License 2.0; see `LICENSE` in this directory.
Copyright Google LLC.

`gen_builtin_function.go` reads them to generate `builtin_function.go`, so that
code generation needs no network access. To update, replace the files with the
ones from a newer googlesql commit and update the commit hash above.
