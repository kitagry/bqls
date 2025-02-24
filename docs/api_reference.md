# Some Protocols

## `workspace/executeCommand`

### `executeQuery`

Execute a query and return the virtual text document url.

Request:

```json
{
    "command": "executeQuery",
    "arguments": ["YOUR_DOCUMENT_URI"]
}
```

Response:

```json
{
    "textDocument": {
        "uri": "bqls://project/${project}/job/${job}"
    }
}
```

You can get the result of the query by requesting the `bqls/virtualTextDocument`.

### `listDatasets`

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

### `listTables`

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

### `listJobHistories`

list up job histories in the project.

Arguments:

* `--all-user`: list up all jobs in the project. When this flag is not set, list up only jobs submitted by the user.
* `--page-size`: job histories size. default is 100.

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

### `saveResult`

save bqls/virtualTextDocument result to file.

arguments:

* 1st: target virtualTextDocument to save from.
* 2nd: target file to save. (currently, only csv file is supported)

Request:

```json
{
    "command": "saveResult",
    "arguments": [
        "bqls://project/${project}/job/${job}",
        "file://path/to/target.csv"
    ],
}
```

Response:

```json
null
```


# Custom API

## `bqls/virtualTextDocument`

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
