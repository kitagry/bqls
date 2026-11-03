# README demo

`demo.gif` in the top-level README is rendered from [`demo.html`](./demo.html), a scripted mock of Neovim with [bqls.nvim](https://github.com/kitagry/bqls.nvim), followed by a terminal running `bqls lint`. It is not a screen capture: the page draws every frame from a timestamp, so the recording needs no BigQuery credentials, costs nothing, and comes out the same every time.

The scenes are:

1. Completion of tables and columns
2. Hover on a table (metadata and schema)
3. A lint warning (`null-comparison`) fixed with its quick fix
4. The dry-run cost estimate shown on save
5. `Execute Query` and the job result in a split
6. `bqls lint` in a terminal, as in CI: the findings of schema-aware rules (`not-in-nullable`, `union-column-order`, `left-join-as-inner`) and the exit code, `--fix` with its summary of the fixed files and the resulting `git diff`, a clean rerun, and `--dry-run`, which lets `bytes-processed-threshold` (set in `.bqls.yaml`) flag the query that would scan 91.18 MiB

## Regenerating

Requirements: Node.js, [ffmpeg](https://ffmpeg.org/), and network access to Google Fonts (JetBrains Mono and Inter).

```sh
cd docs/demo
npm ci
npx playwright install chromium   # or set CHROME_PATH, see below
npm run gif                       # writes demo.gif
npm run mp4                       # optional: demo.mp4 (git-ignored)
```

To use an installed Chrome or Chromium instead of the one Playwright downloads (for example on NixOS, where that one does not run), set `CHROME_PATH`:

```sh
CHROME_PATH=$(command -v google-chrome-stable) npm run gif
```

On NixOS, ffmpeg can come from a temporary shell:

```sh
CHROME_PATH=$(command -v google-chrome-stable) nix --extra-experimental-features 'nix-command flakes' shell nixpkgs#ffmpeg -c npm run gif
```

To preview while editing, open `demo.html` in a browser; it plays in a loop.

## Editing the demo

The timeline is the list of steps at the bottom of the `<script>` in `demo.html` (`type`, `keys`, `wait`, `scene`, and `command` for the terminal). The texts shown on screen must match what bqls actually returns, so update them together with the code they come from:

| On screen | Source |
| --- | --- |
| Table and column completion items | `langserver/internal/source/completion/table_path.go`, `completion.go` |
| Table hover | `buildBigQueryTableMetadataMarkedString` and `createBigQuerySchemaMarkdownTable` in `langserver/internal/source/` |
| Lint message and quick fix | `langserver/internal/lint/null_comparison.go`, `langserver/code_action.go` |
| Code action list | `langserver/execute_command.go` (`Execute Query`) and Neovim's `vim.ui.select` prompt |
| Dry-run message | `langserver/diagnostic.go` (`This query will process ... when run.`) |
| Job document and result table | `buildBigQueryJobMarkedString` in `langserver/internal/source/project.go`, and `convert_data_to_markdown` in bqls.nvim |
| `bqls lint` output and exit codes | `writeText` and `run` in `langserver/lintcli/lintcli.go`; the messages come from `not_in_nullable.go`, `union_column_order.go`, `left_join_as_inner.go`, and `bytes_processed_threshold.go` in `langserver/internal/lint/` |
| `--fix` summary | `fixSummary` and `run` in `langserver/lintcli/lintcli.go` |
| `git diff` after `--fix` | The quick fixes of the same three rules, applied by `applyFixes` in `langserver/lintcli/lintcli.go` |
| `.bqls.yaml` | `ParseConfigFile` in `langserver/internal/lint/config_file.go` |

The table metadata, the 4.88 MiB estimate, and the result rows are real values for `bigquery-public-data.samples.shakespeare`. The job ID, timestamps, and slot milliseconds are made up, and `my-project` stands in for the billing project.

The terminal output was captured from `bqls lint` run on the three files below, whose tables are in `bigquery-public-data.austin_bikeshare` and `san_francisco_bikeshare`; the `index` hashes in the diff come from that run, and the 91.18 MiB is the dry-run estimate of `walk_up_trips.sql` after `--fix`. To check it again after changing a rule's message or fix, recreate the files in a new git repository and run the same commands:

```sql
-- queries/idle_stations.sql
SELECT name
FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations`
WHERE station_id NOT IN (
  SELECT start_station_id
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
)

-- queries/stations.sql
SELECT name, number_of_docks AS capacity, council_district AS region_id
FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations`
UNION ALL
SELECT name, region_id, capacity
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info`

-- queries/walk_up_trips.sql
SELECT s.name, t.trip_id
FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations` AS s
LEFT JOIN `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS t
  ON t.start_station_id = s.station_id
WHERE t.subscriber_type = 'Walk Up'
```
