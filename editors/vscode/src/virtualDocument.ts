export type MarkedString = string | { language: string; value: string };

export interface FieldSchema {
  name: string;
  type: string;
  repeated?: boolean;
  required?: boolean;
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
}
