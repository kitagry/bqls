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

Settings omitted from the notification keep their current values, so you can send only what changed. A `lint` setting, when sent, replaces the whole rule set; `lint = vim.empty_dict()` or `lint = vim.NIL` restores the defaults (a plain `{}` is encoded as a JSON array and rejected).

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
| `in-null` | on | `x IN (NULL)` / `x NOT IN (NULL)` whose list holds only NULLs (including `CAST(NULL AS T)` and `SAFE_CAST(NULL AS T)`) always yields NULL. No quick fix, since `IS [NOT] NULL` is not equivalent. |
| `not-in-nullable` | on | `x NOT IN (SELECT col ...)` where `col` is a `NULLABLE` column of a table: once the subquery returns a NULL, `NOT IN` yields NULL (never TRUE) for every row, so a `WHERE` keeps none. Rewrite it with `NOT EXISTS`, or filter the subquery with `col IS NOT NULL`. Unlike `in-null`, which reports a literal list of only NULLs, this rule depends on the table schema, so it needs table metadata. To avoid false positives, the column is followed only while it is passed through unchanged (column references, aliases, joins, `GROUP BY` keys, CTEs and derived tables); not reported: a computed value (`IFNULL`, `COALESCE`, aggregates, arithmetic, struct fields), `UNION` and other set operations, `UNNEST`, `REQUIRED` columns, views and other non-table sources, and a column that any `WHERE` / `HAVING` / `QUALIFY` / `JOIN ... ON` condition in the statement mentions (e.g. `IS NOT NULL`, `=`, `>`). No quick fix. |
| `unused-cte` | on | A `WITH` entry that is never referenced (a recursive CTE's self reference does not count). Reported as information; quick fix removes the entry. |
| `unused-declare` | on | A `DECLARE` variable whose name appears nowhere else in the script. Any identifier with the same name counts as a reference, including a `SET` target, so a variable that is only assigned is not reported. Reported as information; quick fix removes the declaration (or just that name from `DECLARE x, y ...` when another variable stays). |
| `partition-filter` | on | A time- or range-partitioned table that is read without any filter on its partition column (`_PARTITIONTIME` / `_PARTITIONDATE` for ingestion-time partitioning), so the query may scan every partition. A filter anywhere in the statement on the column or a value derived from it (through CTEs, subqueries, and `UNION`) counts. Statements bqls cannot trace (e.g. recursive CTEs, table functions), view definitions, DML targets, wildcard tables, unreferenced CTEs, and reads that consume no column (e.g. `SELECT COUNT(*)`, unless the table requires a partition filter) are skipped. |
| `partition-filter-not-prunable` | off | A filter on a partition column written in a form the [BigQuery documentation](https://cloud.google.com/bigquery/docs/querying-partitioned-tables) lists as unable to prune partitions: an `OR` with a branch that does not filter the partition column, a comparison with a scalar subquery, a comparison with another column of the same table (`ts >= ts2`), arithmetic on the column (`ts + INTERVAL 1 DAY > ...`), `EXTRACT` of a part other than `YEAR`, `FORMAT_DATE` / `FORMAT_DATETIME` / `FORMAT_TIMESTAMP` with a literal format other than `%F`, `%Y-%m-%d` or `%Y%m%d`, and any function over an integer-range partitioning column. Reported only when no other term on the partition column (joined by `AND`) might prune, so `dt >= '2020-01-01' AND EXTRACT(MONTH FROM dt) = 3` is fine. Not reported: forms the documentation says do prune (e.g. `DATE(ts)`, `TIMESTAMP_TRUNC`, `DATE_ADD`, `CURRENT_TIMESTAMP()`, query parameters), forms it does not mention (e.g. `CAST(dt AS STRING)`, `dt IN (SELECT ...)`, join conditions against another table), filters on values computed from the column in a CTE or subquery (plain aliases are followed), and whatever `partition-filter` skips (including the case with no filter at all). Off by default because the documentation, not the optimizer, is the source of these patterns, and bqls cannot confirm pruning per table. No quick fix. |
| `order-by-in-subquery` | on | An `ORDER BY` without `LIMIT` in a subquery, CTE or derived table whose order cannot reach the result (BigQuery does not preserve row order), so it only costs a sort. Not reported when the order may matter: `LIMIT`, `ARRAY(SELECT ...)`, scalar subqueries, set operations, or a consumer that is limited, aggregated (e.g. `ARRAY_AGG`) or windowed. DML, `CREATE TABLE AS SELECT` and pipe `ORDER BY` are skipped. Quick fix removes the clause. |
| `unused-join` | on | A `LEFT JOIN` (or `RIGHT JOIN`) whose optional side contributes no column outside its own `ON` / `USING` condition, so it only adds cost — or duplicates rows when several rows match, which is rarely intended. A column counts as used when any expression reads it or a `SELECT` list outputs it (including `SELECT *`), even in a subquery whose result is not read further. `INNER`, `CROSS`, comma and `FULL` joins are not reported, since they filter, multiply or add rows; neither are `UNNEST` joins. Reported as information on the `JOIN` keyword. No quick fix. |
| `wildcard-table-suffix` | on | A wildcard table (`` `p.d.events_*` ``) that is queried without any filter on `_TABLE_SUFFIX`, so it scans every matching table. Filters are judged as in `partition-filter`: a `WHERE` / `JOIN ... ON` condition that mentions `_TABLE_SUFFIX` or a value derived from it (through CTEs, subqueries, and `UNION`) counts, even on one side of an `OR`. Unlike `partition-filter`, `SELECT COUNT(*)` is still reported. Statements bqls cannot trace, view definitions, and unreferenced CTEs are skipped. No quick fix. |
| `wildcard-table-suffix-not-constant` | off | A filter on `_TABLE_SUFFIX` that is not a constant expression, which the [BigQuery documentation](https://cloud.google.com/bigquery/docs/querying-wildcard-tables) says does not limit the tables a wildcard table query scans: a comparison with a scalar subquery, a comparison with another column of the same table, or an `OR` with a branch that does not filter `_TABLE_SUFFIX`. Judged as in `partition-filter-not-prunable`: reported only when no other term on `_TABLE_SUFFIX` (joined by `AND`) might limit the scan, so `_TABLE_SUFFIX BETWEEN '40' AND '60' AND _TABLE_SUFFIX = (SELECT ...)` is fine. Not reported: functions over `_TABLE_SUFFIX` (e.g. `PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)`), `_TABLE_SUFFIX IN (SELECT ...)`, join conditions against another table, values computed from `_TABLE_SUFFIX` in a CTE or subquery (plain aliases are followed), JavaScript UDFs, and whatever `wildcard-table-suffix` skips (including the case with no filter at all). Off by default for the same reason as `partition-filter-not-prunable`. No quick fix. |
| `cross-join` | on | An inner join with no join condition (a comma join `FROM a, b` or `JOIN b ON TRUE`), which returns every combination of rows from both sides. A join counts as conditioned when a `WHERE` / `HAVING` / `JOIN ... ON` term mentions both sides, or each side is compared with another table (`FROM a, b, c WHERE a.id = c.a_id AND b.id = c.b_id`); column flow is followed through CTEs and subqueries. Not reported: an explicit `CROSS JOIN`, `UNNEST`, outer and lateral joins, a side that yields at most one row (an aggregate without `GROUP BY`, `SELECT` without `FROM`, `LIMIT 1`, also through CTEs), pipe `\|> JOIN`, statements bqls cannot trace, view definitions, and unreferenced CTEs. Write `CROSS JOIN` to mark an intended product. No quick fix. |
| `union-column-order` | on | A set operation input (`UNION ALL`, `UNION DISTINCT`, `INTERSECT`, `EXCEPT`) whose columns are the first input's columns in a different order. Set operations match columns by position, so when the types agree the values are silently swapped. Names are the explicit aliases or the implicit alias of a column path (`t.a` is `a`); a column without a name (`SELECT x + 1`) is left out of the comparison, and inputs whose names merely differ are not reported. Skipped: `CORRESPONDING` / `BY NAME`, inputs with `*` or `SELECT AS STRUCT`, pipe `\|> UNION ALL`, inputs that read exactly the same tables (`SELECT src, dst FROM edges UNION ALL SELECT dst, src FROM edges` is how a relation is made symmetric), and unreferenced CTEs. Reported on the select list of the reordered input. No quick fix. |
| `insert-column-order` | on | An `INSERT ... SELECT` whose query lists the target columns in a different order. `INSERT` matches the query's columns to the target columns by position, not by name, so when the types agree the values are silently swapped. The targets are the column list when the statement has one (`INSERT INTO t (a, b) SELECT b, a ...`), and otherwise every column of the table in schema order, which the statement does not show (`INSERT INTO t SELECT b, a ...` where `t` has `a, b`). Names are compared as in `union-column-order`: explicit aliases or the implicit alias of a column path, case-insensitively; a column without a name is left out, and queries whose names merely differ from the targets are not reported. Skipped: queries with `*` or pipe syntax, a set operation matching `BY NAME` / `CORRESPONDING`, `INSERT ... VALUES`, and queries that read the target table itself (`INSERT INTO edges SELECT dst, src FROM edges` is how a relation is made symmetric). Reported on the select list. No quick fix. |
| `left-join-as-inner` | on | A `LEFT JOIN` (or `RIGHT JOIN`) whose `WHERE` clause filters the optional side with a condition that rejects NULL, so the unmatched rows the outer join adds are dropped again and it behaves as an `INNER JOIN` (e.g. `a LEFT JOIN b ON a.id = b.id WHERE b.status = 'active'`). A top-level `AND` term counts when it is certainly NULL or FALSE for such a row: `=`, `!=`, `<`, `<=`, `>`, `>=`, `LIKE`, `IN (...)` / `BETWEEN` on the tested value, `NOT` of those, `IS NOT NULL`, `IS TRUE`, `IS FALSE`, or a bare boolean column, where the column may be wrapped in `CAST`, struct field access or arithmetic. Not reported: `IS NULL`, `COALESCE` / `IFNULL` / `IF` and other functions, `OR`, `IS [NOT] DISTINCT FROM`, subqueries, conditions in `ON` or `HAVING` or an outer query, `FULL` and `UNNEST` joins, and a `USING` column (it is the preserved side's). Move the condition into the `ON` clause to keep the unmatched rows, or write `INNER JOIN`. Reported on the `JOIN` keyword. No quick fix. |
| `bytes-processed-threshold` | off | The dry-run estimate of the whole file exceeds `max_bytes` (default `1TiB`). `max_bytes` takes a byte count or a size with a unit: `B`, `KiB`, `MiB`, `GiB`, `TiB`, `PiB`, `EiB` (powers of 1024) or `KB` … `EB` (powers of 1000). Single-letter units such as `100G` are rejected as ambiguous. Needs a dry-run result: the language server dry-runs a file when it is saved, and `bqls lint` only with `--dry-run`. Without one, or after the file is edited since the dry-run, the rule reports nothing. Reported on the first line of the first statement, so a `-- bqls:ignore bytes-processed-threshold` comment must be on the line directly above that line (or trailing on it). |
| `select-star` | off | A bare `*` or `t.*` that reads at least `min_columns` (default `50`) columns of a table in the same `SELECT`'s `FROM`, since BigQuery bills every column read (`LIMIT` does not help). Only columns the statement actually consumes count, so `SELECT a FROM (SELECT * FROM t)` or `EXISTS (SELECT * ...)` is not reported. Skipped: `* EXCEPT (...)` / `* REPLACE (...)` (the columns were chosen deliberately), a `*` over a CTE or subquery (the `*` inside it that reads the table is reported instead), DML, `CREATE TABLE AS SELECT`, `EXPORT DATA`, view definitions, and statements bqls cannot trace. When a dry-run result is available, its bytes estimate for the whole file is added to the message. Off by default because whether reading every column is wasteful depends on the query's intent, and a sensible `min_columns` differs per team. |
| `variable-column-shadow` | on | A comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`) of a column with itself where one side is written as the bare name of a `DECLARE` variable in scope, e.g. `DECLARE user_id INT64; SELECT * FROM t WHERE user_id = user_id` (or `t.user_id = user_id`). A column takes precedence over a variable of the same name, so the variable is never read and the condition is always true (or always false) for non-NULL rows. Qualifying the column does not help; rename the variable. Other uses of a name shared by a variable and a column (e.g. `GROUP BY user_id`) are not reported, since the column is likely intended there; neither are `IF` / `WHILE` conditions. No quick fix. |

Bare `x = NULL` / `x != NULL` is rejected by BigQuery itself ("Operands of = cannot be literal NULL"); bqls offers an `IS [NOT] NULL` quick fix on that error.

Rules can be toggled with the `lint` setting (also via `workspace/didChangeConfiguration`):

```lua
settings = {
  project_id = "YOUR_PROJECT_ID",
  lint = { rules = { ["null-comparison"] = false } },
}
```

### Config file

To share lint settings between your editor and CI, put a `.bqls.yaml` in your repository. Both the language server and `bqls lint` look for it from the SQL file's directory up to the repository root (the directory containing `.git`); the nearest one wins.

```yaml
lint:
  rules:
    unused-cte: false
    bytes-processed-threshold:   # a rule can also take an object
      enabled: true
      max_bytes: 100GiB          # rule-specific options
```

Each rule is either `true` / `false` or an object with an optional `enabled` and the rule's options; the editor's `lint` setting takes the same form. Unknown rules and options are errors (`bqls lint` exits with `2`; the language server logs the error and ignores the file). The language server rereads the file when it changes.

Precedence: `--enable` / `--disable` flags or the editor's `lint` setting > `.bqls.yaml` > rule defaults. Rule options are merged key by key, so a setting only needs the options it changes.

To silence a single occurrence, add a `bqls:ignore` comment. A comment on its own line applies to the next line; a trailing comment applies to its own line. Omitting rule names ignores every rule.

```sql
-- bqls:ignore null-comparison
SELECT * FROM t WHERE x = CAST(NULL AS INT64)
SELECT * FROM t WHERE y = CAST(NULL AS INT64) -- bqls:ignore
```

### CLI

`bqls lint` runs the same analysis and lint rules as the language server, without an editor. Table schemas are fetched with Application Default Credentials; no query is run unless `--dry-run` is given.

```console
$ bqls lint [flags] <file|dir|->...
$ bqls lint --project my-project queries/
queries/report.sql:3:6: info: CTE `tmp` is defined but never used (unused-cte)
queries/report.sql:8:7: error: Unrecognized name: y
```

Directories are searched recursively for `*.sql` files (skipping directories whose name starts with `.`, such as `.git`), and `-` reads from stdin. Flags must come before files. Run `bqls lint --help` for the full list of rules. Each file uses the nearest [`.bqls.yaml`](#config-file) (stdin uses the one found from the working directory).

| Flag | Description |
| --- | --- |
| `--project` | BigQuery project ID. Defaults to `gcloud config get project`. |
| `--location` | BigQuery location. Defaults to `US`. |
| `--enable <rule>` / `--disable <rule>` | Turn a rule on or off. Repeatable; the last one wins. Takes precedence over the config file. |
| `--config <path>` | Use this config file for every input instead of looking up `.bqls.yaml`. |
| `--fix` | Apply the quick fixes (the first one of each finding) and write the files back, then report the findings that are left. With `-`, the fixed SQL is printed to stdout and the findings go to stderr. A fix that overlaps another fix in the same file is skipped (noted on stderr) and its finding is reported; running `--fix` again applies it. |
| `--dry-run` | Dry-run each file that analyzes without errors (after `--fix`, if given), so that rules using the estimate such as `bytes-processed-threshold` can report. This creates a dry-run query job per file, which is free but needs `bigquery.jobs.create` (e.g. `roles/bigquery.jobUser`). A failed dry-run is printed to stderr and makes the exit code `2`; the other findings are still printed. |
| `--format text\|json\|github` | `text` (default) prints `path:line:col: severity: message (rule)`. `json` prints an array of `{file, line, column, endLine, endColumn, severity, rule, message}`. `github` prints [workflow commands](https://docs.github.com/en/actions/reference/workflow-commands-for-github-actions) that show up as annotations on pull requests. |

Lines and columns are 1-based and the end position is exclusive. Columns are currently off on lines containing non-ASCII text ([#375](https://github.com/kitagry/bqls/issues/375)). Analysis errors are reported with severity `error` and no rule. With `--format github`, file paths are made relative to `$GITHUB_WORKSPACE` so annotations attach to the diff.

Exit codes: `0` nothing found, `1` any lint finding or analysis error (with `--fix`, any that is left after fixing), `2` usage or runtime error (bad flags, invalid config file, unreadable or unwritable file, authentication/permission/network failure while fetching table metadata, failed `--dry-run`). An unreadable file does not stop the others from being linted; their findings are still printed and the exit code is `2`.

GitHub Actions example. The service account only needs to read table metadata (e.g. `roles/bigquery.metadataViewer`), plus `roles/bigquery.jobUser` with `--dry-run`. Pass `--project` explicitly, since the fallback to `gcloud config get project` only works where gcloud is installed and configured. Instead of `go install`, you can download a binary from [GitHub Releases](https://github.com/kitagry/bqls/releases) (the `lint` subcommand needs a release that includes it).

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
