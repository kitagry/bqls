export type MarkedString = string | { language: string; value: string };

export interface FieldSchema {
  name: string;
  type: string;
  repeated?: boolean;
  required?: boolean;
  description?: string;
  fields?: FieldSchema[];
}

export interface QueryResult {
  columns: string[] | null;
  data: unknown[][] | null;
  // schema is omitted by the server when empty; older servers may not send it at all.
  schema?: FieldSchema[];
}

export interface VirtualTextDocumentResult {
  contents: MarkedString[] | null;
  result?: QueryResult;
  // The table's schema as structured data, letting the Details tab render a
  // collapsible tree instead of parsing the Markdown table already in
  // contents. Only set for table (not job) virtual documents.
  schema?: FieldSchema[];
  // Set by servers that support supports_async_virtual_text_document: the
  // real contents/result will follow via bqls/publishVirtualTextDocument.
  pending?: boolean;
}

// Pushed by the server once part of an async bqls/virtualTextDocument fetch
// completes (or fails) for the given kind. textDocument.uri correlates it
// back to the uri the client originally requested. Sent twice per request:
// once for "details" (usually fast) and once for "preview" (usually slower,
// e.g. waiting for a job or scanning a large table).
export interface PublishVirtualTextDocumentParams {
  textDocument: { uri: string };
  kind: "details" | "preview";
  contents?: MarkedString[];
  // Set alongside contents when kind is "details", for table virtual
  // documents; see VirtualTextDocumentResult.schema.
  schema?: FieldSchema[];
  result?: QueryResult;
  error?: string;
}
