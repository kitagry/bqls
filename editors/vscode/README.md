# bqls for VSCode

Official VSCode extension for [bqls](https://github.com/kitagry/bqls), the BigQuery language server.

## Features

- Hover, completion, definition, formatting, and code actions for `.sql` / `.bq` files, powered by `bqls`.
- View query results and table/job information through the `bqls://` virtual document scheme.

## Requirements

Install the `bqls` binary and make sure it's on your `PATH` (or set `bqls.path`).

```bash
go install github.com/kitagry/bqls@latest
```

You also need to authenticate with Google Cloud:

```bash
gcloud auth login
gcloud auth application-default login
```

## Settings

| Setting | Description | Default |
| --- | --- | --- |
| `bqls.path` | Path to the `bqls` executable. | `bqls` |
| `bqls.projectId` | BigQuery project ID. If empty, `bqls` falls back to `gcloud config get project`. | `""` |
| `bqls.location` | BigQuery location (e.g. `US`, `asia-northeast1`). If empty, `bqls` uses `US`. | `""` |
| `bqls.trace.server` | Trace communication between VSCode and the language server. | `off` |

## Development

```bash
npm install
npm run watch   # rebuild on change
npm run test    # run unit tests
npm run lint
```

Press `F5` in VSCode (opened at `editors/vscode`) to launch an Extension Development Host.
