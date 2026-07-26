# Some Protocols

## `workspace/executeCommand`

### `bqls.executeQuery`

Execute a query and return the virtual text document url.

Request:

```json
{
    "command": "bqls.executeQuery",
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

### `bqls.listDatasets`

list up all datasets in the project.

Request:

```json
{
    "command": "bqls.listDatasets",
    "arguments": ["YOUR_PROJECT_ID"]
}
```

Response:

```json
{
    "datasets": ["dataset1", "dataset2", "dataset3"]
}
```

### `bqls.listTables`

list up all tables in the dataset.

Request:

```json
{
    "command": "bqls.listTables",
    "arguments": ["YOUR_PROJECT_ID", "YOUR_DATASET_ID"]
}
```

Response:

```json
{
    "tables": ["table1", "table2", "table3"]
}
```

### `bqls.listJobHistories`

list up job histories in the project.

Arguments:

* `--all-user`: list up all jobs in the project. When this flag is not set, list up only jobs submitted by the user.
* `--page-size`: job histories size. default is 100.

Request:

```json
{
    "command": "bqls.listJobHistories",
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
        }
    ]
}
```

### `bqls.saveResult`

save bqls/virtualTextDocument result to file.

arguments:

* 1st: target virtualTextDocument to save from.
* 2nd: target file to save. (currently, only csv file and spreadsheet is supported)
    * csv: `file://path/to/target.csv`
    * spreadsheet:
        * `sheet://new`: create new spreadsheet and save result
        * `https://docs.google.com/spreadsheets/d/asdf_asdfasdf/edit?gid=123`: overwrite result to existing spreadsheet

Request:

```json
{
    "command": "bqls.saveResult",
    "arguments": [
        "bqls://project/${project}/job/${job}",
        "file://path/to/target.csv"
    ]
}
```

Response:

```json
{
    "url": "https://docs.google.com/spreadsheets/d/1...."
}
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
    // The table's schema as structured data, letting clients render a
    // collapsible tree instead of parsing the Markdown table already in
    // contents. Only set for table (not job) virtual documents.
    schema?: FieldSchema[];
    // Set when the client declared `supports_async_virtual_text_document`
    // (see below) and the real result will follow via
    // `bqls/publishVirtualTextDocument` instead of being included here.
    pending?: boolean;
}

interface QueryResult {
    columns: string[];
    data: any[][];
    // Additional column type information (name/type/repeated/required/fields)
    // used to render nested RECORD/REPEATED values. Omitted when empty.
    schema?: FieldSchema[];
}
```

### Async virtual text document fetching

Fetching a virtual text document can be slow (it calls out to BigQuery), which blocks the
connection while the request is in flight. Clients that don't want to block can opt in to an
async flow:

1. Send `supports_async_virtual_text_document: true` in the `initialize` request's
   `initializationOptions`.
2. `bqls/virtualTextDocument` then responds immediately with `{ pending: true }` instead of the
   full result.
3. Once details (table/job metadata) are ready, `bqls` pushes them via a
   `bqls/publishVirtualTextDocument` notification with `kind: "details"`. Once the preview (query
   result rows) is ready — which is usually slower, since it may need to wait for a job to finish
   or scan a large table — a second notification with `kind: "preview"` follows. These are plain
   notifications, not a request/response — no reply is expected.
4. Sending another `bqls/virtualTextDocument` request for the same uri cancels any still-running
   fetch for that uri; only the latest request's notifications are published.

Clients that omit the flag (the default) keep getting the original fully-synchronous response,
so this is fully backwards compatible.

## `bqls/publishVirtualTextDocument`

A notification (not a request) pushed by the server once part of an async
`bqls/virtualTextDocument` fetch completes or fails. Only sent to clients that opted in via
`supports_async_virtual_text_document`. Sent twice per request — once for `kind: "details"` and
once for `kind: "preview"` — so a client can render details as soon as they're available instead
of waiting for the (usually slower) preview too.

```ts
interface PublishVirtualTextDocumentParams {
    textDocument: TextDocumentIdentifier;
    kind: "details" | "preview";
    // Set when kind is "details".
    contents?: MarkedString[];
    // Set alongside contents when kind is "details", for table virtual
    // documents; see VirtualTextDocument.schema.
    schema?: FieldSchema[];
    // Set when kind is "preview".
    result?: QueryResult;
    // Set instead of contents/result if the fetch failed.
    error?: string;
}
```
