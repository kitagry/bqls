import { FieldSchema, MarkedString, QueryResult } from "./virtualDocument";

export function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function withoutRepeated(schema: FieldSchema): FieldSchema {
  return { ...schema, repeated: false };
}

export function renderCellValue(value: unknown, schema?: FieldSchema): string {
  if (value === null || value === undefined) {
    return '<span class="bqls-null">null</span>';
  }

  if (schema?.repeated) {
    const items = value as unknown[];
    const itemSchema = withoutRepeated(schema);
    const inner = items
      .map((item) => `<li>${renderCellValue(item, itemSchema)}</li>`)
      .join("");
    return `<details class="bqls-nested"><summary>[${items.length}]</summary><ul>${inner}</ul></details>`;
  }

  if (schema?.fields && schema.fields.length > 0) {
    const record = value as unknown[];
    const rows = schema.fields
      .map(
        (f, i) =>
          `<tr><th>${escapeHtml(f.name)}</th><td>${renderCellValue(record[i], f)}</td></tr>`,
      )
      .join("");
    return `<details class="bqls-nested"><summary>{…}</summary><table class="bqls-record">${rows}</table></details>`;
  }

  return escapeHtml(String(value));
}

function renderInline(text: string): string {
  return escapeHtml(text).replace(
    /\[([^\]]+)\]\(([^)]+)\)/g,
    (_match, label: string, url: string) => `<a href="${url}">${label}</a>`,
  );
}

function renderMarkdownText(text: string): string {
  const parts: string[] = [];
  let listItems: string[] = [];

  const flushList = () => {
    if (listItems.length > 0) {
      parts.push(`<ul>${listItems.join("")}</ul>`);
      listItems = [];
    }
  };

  for (const line of text.split("\n")) {
    const headingMatch = /^(#{1,6})\s+(.*)$/.exec(line);
    const listMatch = /^\s*[-*]\s+(.*)$/.exec(line);
    if (headingMatch) {
      flushList();
      const level = headingMatch[1].length;
      parts.push(`<h${level}>${renderInline(headingMatch[2])}</h${level}>`);
    } else if (listMatch) {
      listItems.push(`<li>${renderInline(listMatch[1])}</li>`);
    } else if (line.trim() === "") {
      flushList();
    } else {
      flushList();
      parts.push(`<p>${renderInline(line)}</p>`);
    }
  }
  flushList();
  return parts.join("");
}

// Contents is only loosely Markdown, so skip a full parser and convert just
// headings/bullet lists/links. Any other language is rendered as a code block.
export function renderMarkedString(m: MarkedString): string {
  if (typeof m === "string") {
    return renderMarkdownText(m);
  }
  if (m.language === "markdown") {
    return renderMarkdownText(m.value);
  }
  return `<pre><code>${escapeHtml(m.value)}</code></pre>`;
}

const PAGE_STYLE = `
  body { font-family: var(--vscode-font-family); padding: 0 1rem; }
  .bqls-tabs { display: flex; gap: 0.5rem; border-bottom: 1px solid var(--vscode-panel-border); margin-bottom: 1rem; }
  .bqls-tab { background: none; border: none; padding: 0.5rem 0.75rem; cursor: pointer; color: var(--vscode-foreground); }
  .bqls-tab.active { border-bottom: 2px solid var(--vscode-focusBorder); font-weight: bold; }
  .bqls-tabpanel { display: none; }
  .bqls-tabpanel.active { display: block; }
  table.bqls-result, table.bqls-record { border-collapse: collapse; }
  table.bqls-result th, table.bqls-result td, table.bqls-record th, table.bqls-record td {
    border: 1px solid var(--vscode-panel-border); padding: 0.25rem 0.5rem;
  }
  .bqls-null { opacity: 0.6; font-style: italic; }
`;

const PAGE_SCRIPT = `
  document.querySelectorAll(".bqls-tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      const name = tab.getAttribute("data-tab");
      document.querySelectorAll(".bqls-tab").forEach((t) => t.classList.toggle("active", t === tab));
      document.querySelectorAll(".bqls-tabpanel").forEach((p) => {
        p.classList.toggle("active", p.getAttribute("data-tabpanel") === name);
      });
    });
  });
`;

export function renderPage(params: {
  detailsHtml: string;
  previewHtml: string | null;
}): string {
  const hasPreview = params.previewHtml !== null;
  const defaultTab = hasPreview ? "preview" : "details";

  const tabs = [
    hasPreview
      ? `<button class="bqls-tab active" data-tab="preview">Preview</button>`
      : "",
    `<button class="bqls-tab${hasPreview ? "" : " active"}" data-tab="details">Details</button>`,
  ].join("");

  const panels = [
    hasPreview
      ? `<div class="bqls-tabpanel active" data-tabpanel="preview">${params.previewHtml}</div>`
      : "",
    `<div class="bqls-tabpanel${hasPreview ? "" : " active"}" data-tabpanel="details">${params.detailsHtml}</div>`,
  ].join("");

  return `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>${PAGE_STYLE}</style>
</head>
<body data-default-tab="${defaultTab}">
<div class="bqls-tabs">${tabs}</div>
<div class="bqls-tabpanels">${panels}</div>
<script>${PAGE_SCRIPT}</script>
</body>
</html>`;
}

export function virtualDocumentTitle(uriString: string): string {
  const tableMatch = /dataset\/([^/]+)\/table\/([^/]+)/.exec(uriString);
  if (tableMatch) {
    return `${tableMatch[1]}.${tableMatch[2]}`;
  }
  const jobMatch = /job\/([^/]+)/.exec(uriString);
  if (jobMatch) {
    return `Job ${jobMatch[1]}`;
  }
  return "bqls";
}

export function renderQueryResultTable(result: QueryResult): string {
  const columns = result.columns ?? [];
  const data = result.data ?? [];
  const schema = result.schema ?? [];

  const header = columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("");
  const rows = data
    .map((row) => {
      const cells = row
        .map((cell, i) => `<td>${renderCellValue(cell, schema[i])}</td>`)
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");

  return `<table class="bqls-result"><thead><tr>${header}</tr></thead><tbody>${rows}</tbody></table>`;
}
