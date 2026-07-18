export type MarkedString = string | { language: string; value: string };

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
}

export interface VirtualTextDocumentResult {
  contents: MarkedString[];
  result?: QueryResult;
}

export function markedStringToString(m: MarkedString): string {
  if (typeof m === "string") {
    return m;
  }
  return "```" + m.language + "\n" + m.value + "\n```";
}

export function queryResultToMarkdownTable(result: QueryResult): string {
  const header = `| ${result.columns.join(" | ")} |`;
  const separator = `| ${result.columns.map(() => "---").join(" | ")} |`;
  const rows = result.rows.map((row) => `| ${row.map(String).join(" | ")} |`);
  return [header, separator, ...rows].join("\n");
}

export function renderVirtualTextDocument(
  doc: VirtualTextDocumentResult,
): string {
  const parts = doc.contents.map(markedStringToString);
  if (doc.result) {
    parts.push(queryResultToMarkdownTable(doc.result));
  }
  return parts.join("\n\n");
}
