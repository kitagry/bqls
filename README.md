# bqls

BigQuery language server

https://github.com/user-attachments/assets/3e980a26-bc9b-4c2b-8c5b-1e9582a6a644

## Settings

### Login to use BigQuery API

```bash
gcloud auth login
gcloud auth application-default login
```

### Neovim

You can use [bqls.nvim](https://github.com/kitagry/bqls.nvim).

You can set Bigquery Project ID. If you don't specify it, `gcloud config get project` will be used.

```lua
require("lspconfig").bqls.setup({
  settings = {
    project_id = "YOUR_PROJECT_ID",
  },
})
```

You can change project_id with `workspace/didChangeConfiguration`.

```lua
vim.lsp.get_clients({ name = "bqls" })[1].settings = { project_id="ANOTHER_PROJECT_ID" }
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
    - [bqls.listDatasets](./docs/api_reference.md#bqlslistdatasets)
    - [bqls.listTables](./docs/api_reference.md#bqlslisttables)
    - [bqls.listJobHistories](./docs/api_reference.md#bqlslistjobhistories)
    - [bqls.saveResult](./docs/api_reference.md#bqlssaveresult)
- workspace/didChangeConfiguration
- [bqls/virtualTextDocument](./docs/api_reference.md#bqlsvirtualtextdocument)
