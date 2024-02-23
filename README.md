# bqls

BigQuery language server

## Some Protocols

### `workspace/executeCommand`

#### `listDatasets`

list up all datasets in the project.

Request:

```json
{
    "command": "listDatasets",
    "arguments": ["YOUR_PROJECT_ID"]
}
```

Response:

```json
{
    "datasets": ["dataset1", "dataset2", "dataset3"]
}
```

#### `listTables`

list up all tables in the dataset.

Request:

```json
{
    "command": "listTables",
    "arguments": ["YOUR_PROJECT_ID", "YOUR_DATASET_ID"]
}
```

Response:

```json
{
    "tables": ["table1", "table2", "table3"]
}
```

#### `listJobHistories`

list up job histories in the project.

Arguments:

* `--all-user`: list up all jobs in the project. When this flag is not set, list up only jobs submitted by the user.

Request:

```json
{
    "command": "listJobHistories",
}
```

Response:

```json
{
    "jobs": [
        {
            "textDocument": { "uri": "bqls://..."},
            "id": "job_id",
            "owner": "user@example.com",
            "summary": "job summary"
        },
    ]
}
```


## Custom API

### `bqls/virtualTextDocument`

Requests a virtual text document from the LSP, which is a read only document that can be displayed in the client.
`bqls` will encode all virtual files under custom schema `bqls:`, so clients should route all requests for the `bqls:` schema back to the `bqls/virtualTextDocument`.
I used [deno language server protocol](https://docs.deno.com/runtime/manual/advanced/language_server/overview) below as reference.

For example, bqls can provide a virtual text document for a table information.
Currently, `bqls://` schema supported the following path:

* table: `bqls://project/${project}/dataset/${dataset}/table/${table}`
* job: `bqls://project/${project}/job/${job}`

Requests:

```ts
interface VirtualTextDocumentParams {
    textDocument: TextDocumentIdentifier;
}
```

Response:

```ts
interface VirtualTextDocument {
    contents: MarkedString[];
    result: QueryResult;
}

interface QueryResult {
    columns: string[];
    rows: any[][];
}
```
