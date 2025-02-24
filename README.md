# bqls

BigQuery language server

## Settings

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

## Supported Protocol

- textDocument/formatting
- textDocument/hover
- textDocument/completion
- textDocument/definition
- textDocument/codeAction
- [workspace/executeCommand](./docs/api_reference.md#workspaceexecutecommand)
    - [executeQuery](./docs/api_reference.md#executequery)
    - [listDatasets](./docs/api_reference.md#listdatasets)
    - [listTables](./docs/api_reference.md#listtables)
    - [listJobHistories](./docs/api_reference.md#listjobhistories)
    - [saveResult](./docs/api_reference.md#saveResult)
- workspace/didChangeConfiguration
- [bqls/virtualTextDocument](./docs/api_reference.md#bqlsvirtualtextdocument)
