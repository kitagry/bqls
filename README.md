# bqls

BigQuery language server

https://github.com/user-attachments/assets/3e980a26-bc9b-4c2b-8c5b-1e9582a6a644

## Installation

### Prerequisites

- Go 1.25 or later

### Install from releases

Download the latest binary from [GitHub Releases](https://github.com/kitagry/bqls/releases) and place it in your PATH.

### Build from source

```bash
go install
```

## Settings

### Login to use BigQuery API

```bash
gcloud auth login
gcloud auth application-default login
```

### Neovim

You can use [bqls.nvim](https://github.com/kitagry/bqls.nvim) to integrate BigQuery with Neovim.

You can specify your BigQuery Project ID and location in the configuration. If not specified, the plugin will use `gcloud config get project` for the Project ID and `US` as the default location.

```lua
require("lspconfig").bqls.setup({
  settings = {
    project_id = "YOUR_PROJECT_ID",
    location = "YOUR_LOCATION",
  },
})
```

You can change project_id with `workspace/didChangeConfiguration`.

```lua
vim.lsp.buf_notify(0, "workspace/didChangeConfiguration", { settings = { project_id = "ANOTHER_PROJECT_ID", location = "ANOTHER_LOCATION" } })
```

`bqls` also supports fetching virtual document previews (table/job info) asynchronously so the
client isn't blocked while BigQuery is queried; see [`bqls/publishVirtualTextDocument`](./docs/api_reference.md)
in the API reference. This is opt-in via `supports_async_virtual_text_document` in
`initializationOptions`, so bqls.nvim keeps working unmodified until it adopts the new flow.

### VSCode

The official VSCode extension lives in [editors/vscode](./editors/vscode) in this repository.

You can also use the community extension [bqls-vscode](https://github.com/yokomotod/bqls-vscode).

## Lint

bqls reports lint warnings and information (diagnostic source `bqls-lint`, code = rule name) for queries that analyze without errors.

| Rule | Default | Description |
| --- | --- | --- |
| `null-comparison` | on | `x = CAST(NULL AS T)` / `!=` always yields NULL; quick fix rewrites it to `IS [NOT] NULL`. |
| `unused-cte` | on | A `WITH` entry that is never referenced (a recursive CTE's self reference does not count). Reported as information; quick fix removes the entry. |
| `partition-filter` | on | A time- or range-partitioned table that is read without any filter on its partition column (`_PARTITIONTIME` / `_PARTITIONDATE` for ingestion-time partitioning), so the query may scan every partition. A filter anywhere in the statement on the column or a value derived from it (through CTEs, subqueries, and `UNION`) counts. Statements bqls cannot trace (e.g. recursive CTEs, table functions), view definitions, DML targets, wildcard tables, unreferenced CTEs, and reads that consume no column (e.g. `SELECT COUNT(*)`, unless the table requires a partition filter) are skipped. |

Bare `x = NULL` / `x != NULL` is rejected by BigQuery itself ("Operands of = cannot be literal NULL"); bqls offers an `IS [NOT] NULL` quick fix on that error.

Rules can be toggled with the `lint` setting (also via `workspace/didChangeConfiguration`):

```lua
settings = {
  project_id = "YOUR_PROJECT_ID",
  lint = { rules = { ["null-comparison"] = false } },
}
```

To silence a single occurrence, add a `bqls:ignore` comment. A comment on its own line applies to the next line; a trailing comment applies to its own line. Omitting rule names ignores every rule.

```sql
-- bqls:ignore null-comparison
SELECT * FROM t WHERE x = CAST(NULL AS INT64)
SELECT * FROM t WHERE y = CAST(NULL AS INT64) -- bqls:ignore
```

### CLI

`bqls lint` runs the same analysis and lint rules as the language server, without an editor. Table schemas are fetched with Application Default Credentials; no query is run.

```console
$ bqls lint [flags] <file|dir|->...
$ bqls lint --project my-project queries/
queries/report.sql:3:6: info: CTE `tmp` is defined but never used (unused-cte)
queries/report.sql:8:7: error: Unrecognized name: y
```

Directories are searched recursively for `*.sql` files (skipping directories whose name starts with `.`, such as `.git`), and `-` reads from stdin. Flags must come before files. Run `bqls lint --help` for the full list of rules.

| Flag | Description |
| --- | --- |
| `--project` | BigQuery project ID. Defaults to `gcloud config get project`. |
| `--location` | BigQuery location. Defaults to `US`. |
| `--enable <rule>` / `--disable <rule>` | Turn a rule on or off. Repeatable; the last one wins. |
| `--format text\|json\|github` | `text` (default) prints `path:line:col: severity: message (rule)`. `json` prints an array of `{file, line, column, endLine, endColumn, severity, rule, message}`. `github` prints [workflow commands](https://docs.github.com/en/actions/reference/workflow-commands-for-github-actions) that show up as annotations on pull requests. |

Lines and columns are 1-based and the end position is exclusive. Columns are currently off on lines containing non-ASCII text ([#375](https://github.com/kitagry/bqls/issues/375)). Analysis errors are reported with severity `error` and no rule. With `--format github`, file paths are made relative to `$GITHUB_WORKSPACE` so annotations attach to the diff.

Exit codes: `0` nothing found, `1` any lint finding or analysis error, `2` usage or runtime error (bad flags, unreadable file, authentication/permission/network failure while fetching table metadata). An unreadable file does not stop the others from being linted; their findings are still printed and the exit code is `2`.

GitHub Actions example. The service account only needs to read table metadata (e.g. `roles/bigquery.metadataViewer`). Pass `--project` explicitly, since the fallback to `gcloud config get project` only works where gcloud is installed and configured. Instead of `go install`, you can download a binary from [GitHub Releases](https://github.com/kitagry/bqls/releases) (the `lint` subcommand needs a release that includes it).

```yaml
jobs:
  lint:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-go@v7
        with:
          go-version: stable
      - uses: google-github-actions/auth@v3
        with:
          workload_identity_provider: ${{ secrets.WORKLOAD_IDENTITY_PROVIDER }}
          service_account: ${{ secrets.SERVICE_ACCOUNT }}
      - run: go install github.com/kitagry/bqls@latest
      - run: bqls lint --project my-project --format github .
```

## Save Result

In order to save for spreadsheet, you should enable Google Drive API.

1. Enable [Google Drive API](https://console.cloud.google.com/marketplace/product/google/drive.googleapis.com) and [Google Sheets API](https://console.cloud.google.com/marketplace/product/google/sheets.googleapis.com)
2. `gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/drive`

## Supported Protocol

- textDocument/formatting
- textDocument/hover
- textDocument/completion
- textDocument/definition
- textDocument/codeAction
- [workspace/executeCommand](./docs/api_reference.md#workspaceexecutecommand)
    - [bqls.executeQuery](./docs/api_reference.md#bqlsexecutequery)
    - [bqls.cancelQuery](./docs/api_reference.md#bqlscancelquery)
    - [bqls.listDatasets](./docs/api_reference.md#bqlslistdatasets)
    - [bqls.listTables](./docs/api_reference.md#bqlslisttables)
    - [bqls.listJobHistories](./docs/api_reference.md#bqlslistjobhistories)
    - [bqls.saveResult](./docs/api_reference.md#bqlssaveresult)
- workspace/didChangeConfiguration
- [bqls/virtualTextDocument](./docs/api_reference.md#bqlsvirtualtextdocument)
