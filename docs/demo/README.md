# README demo

`demo.gif` in the top-level README is rendered from [`demo.html`](./demo.html), a scripted mock of Neovim with [bqls.nvim](https://github.com/kitagry/bqls.nvim). It is not a screen capture: the page draws every frame from a timestamp, so the recording needs no BigQuery credentials, costs nothing, and comes out the same every time.

The scenes are:

1. Completion of tables and columns
2. Hover on a table (metadata and schema)
3. A lint warning (`null-comparison`) fixed with its quick fix
4. The dry-run cost estimate shown on save
5. `Execute Query` and the job result in a split

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

The timeline is the list of steps at the bottom of the `<script>` in `demo.html` (`type`, `keys`, `wait`, `scene`). The texts shown on screen must match what bqls actually returns, so update them together with the code they come from:

| On screen | Source |
| --- | --- |
| Table and column completion items | `langserver/internal/source/completion/table_path.go`, `completion.go` |
| Table hover | `buildBigQueryTableMetadataMarkedString` and `createBigQuerySchemaMarkdownTable` in `langserver/internal/source/` |
| Lint message and quick fix | `langserver/internal/lint/null_comparison.go`, `langserver/code_action.go` |
| Code action list | `langserver/execute_command.go` (`Execute Query`) and Neovim's `vim.ui.select` prompt |
| Dry-run message | `langserver/diagnostic.go` (`This query will process ... when run.`) |
| Job document and result table | `buildBigQueryJobMarkedString` in `langserver/internal/source/project.go`, and `convert_data_to_markdown` in bqls.nvim |

The table metadata, the 4.88 MiB estimate, and the result rows are real values for `bigquery-public-data.samples.shakespeare`. The job ID, timestamps, and slot milliseconds are made up, and `my-project` stands in for the billing project.
