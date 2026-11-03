# bqls for VSCode

Official VSCode extension for [bqls](https://github.com/kitagry/bqls), the BigQuery language server.

## Features

- Hover, completion, definition, formatting, and code actions for `.sql` / `.bq` files, powered by `bqls`.
- View query results and table/job information through the `bqls://` virtual document scheme.
- Running "Execute Query" or picking a job from "List Job Histories" opens the result automatically; "Save Result to Spreadsheet" opens the created sheet in your browser.

## Requirements

On macOS and Linux, the extension downloads a matching `bqls` release from
GitHub automatically the first time it activates (cached under the
extension's global storage), so no manual install is required. If `bqls` is
already on your `PATH` (e.g. via `go install`), that one is used instead.

Windows is not yet supported by the automatic download. Install `bqls`
manually and point `bqls.path` at it:

```bash
go install github.com/kitagry/bqls@latest
```

### bqls version

The automatic download installs bqls **v0.8.3**. A `bqls` found on your
`PATH` (or set via `bqls.path`) takes precedence and is used as-is, so make
sure it is recent enough for the features you use:

| Feature                                                      | Minimum bqls version |
| ------------------------------------------------------------ | -------------------- |
| Execute Query, job histories, Save Result, Datasets explorer | v0.6.0               |
| "Add Project…" in the Datasets explorer                      | v0.7.1               |
| Cancelling a running query from the progress notification    | v0.7.2               |
| `bqls.lint.rules` (enable/disable rules by name)             | v0.8.0               |
| `bqls.lint.rules` object form with rule options              | v0.8.1               |
| `bytes-processed-threshold` lint rule                        | v0.8.2               |

You also need to authenticate with Google Cloud:

```bash
gcloud auth login
gcloud auth application-default login
```

## Settings

| Setting             | Description                                                                                                                                  | Default |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `bqls.path`         | Path to the `bqls` executable.                                                                                                               | `bqls`  |
| `bqls.projectId`    | BigQuery project ID. If empty, `bqls` falls back to `gcloud config get project`.                                                             | `""`    |
| `bqls.location`     | BigQuery location (e.g. `US`, `asia-northeast1`). If empty, `bqls` uses `US`.                                                                | `""`    |
| `bqls.lint.rules`   | Enable or disable lint rules by name, e.g. `{"null-comparison": false}`. See [Lint](https://github.com/kitagry/bqls/blob/main/docs/lint.md). | `{}`    |
| `bqls.trace.server` | Trace communication between VSCode and the language server.                                                                                  | `off`   |

## Development

```bash
npm install
npm run watch   # rebuild on change
npm run test    # run unit tests
npm run lint
```

Press `F5` in VSCode (opened at `editors/vscode`) to launch an Extension Development Host.
