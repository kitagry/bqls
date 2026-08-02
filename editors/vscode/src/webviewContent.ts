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

  // The extension sends these once each part of the async
  // bqls/virtualTextDocument fetch completes, so the panel that opened in a
  // "Loading..." state can be filled in without a full webview reload.
  // Details and preview arrive as separate messages (details first, since
  // it's usually much faster than preview) so each tab can update as soon
  // as its own data is ready instead of waiting for both.
  window.addEventListener("message", (event) => {
    const message = event.data;
    const detailsPanel = document.querySelector('[data-tabpanel="details"]');
    const previewPanel = document.querySelector('[data-tabpanel="preview"]');
    if (message.type === "error") {
      if (detailsPanel) {
        detailsPanel.innerHTML = '<p class="bqls-error"></p>';
        detailsPanel.querySelector(".bqls-error").textContent = message.message;
      }
      return;
    }
    if (message.type === "details" && detailsPanel) {
      detailsPanel.innerHTML = message.detailsHtml;
      return;
    }
    if (message.type === "preview" && previewPanel) {
      previewPanel.innerHTML = message.previewHtml;
      return;
    }
  });
`;

export function renderPage(params: {
	detailsHtml: string;
	previewHtml: string | null;
}): string {
	const hasPreview = params.previewHtml !== null;
	const defaultTab = "details";

	const tabs = [
		`<button class="bqls-tab active" data-tab="details">Details</button>`,
		hasPreview
			? `<button class="bqls-tab" data-tab="preview">Preview</button>`
			: "",
	].join("");

	const panels = [
		`<div class="bqls-tabpanel active" data-tabpanel="details">${params.detailsHtml}</div>`,
		hasPreview
			? `<div class="bqls-tabpanel" data-tabpanel="preview">${params.previewHtml}</div>`
			: "",
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

export function buildDetailsHtml(
	contents: MarkedString[] | null | undefined,
	schema?: FieldSchema[],
): string {
	const parts = (contents ?? []).map(renderMarkedString);
	if (schema && schema.length > 0) {
		parts.push(renderSchemaTable(schema));
	}
	return parts.join("\n");
}

// Renders a table schema (name/type/repeated/required/fields) as a
// Name/Type/Mode table, mirroring the BigQuery console's schema tab.
// Nested RECORD fields are indented with non-breaking spaces (matching
// createBigQuerySchemaMarkdownTable on the server, for Hover).
export function renderSchemaTable(schema: FieldSchema[]): string {
	const rows = renderSchemaTableRows(schema, 0);
	return `<table class="bqls-result"><thead><tr><th>Name</th><th>Type</th><th>Mode</th><th>Description</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function renderSchemaTableRows(schema: FieldSchema[], depth: number): string {
	const indent = "&nbsp;&nbsp;".repeat(depth);
	return schema
		.map((f) => {
			const mode = f.repeated
				? "REPEATED"
				: f.required
					? "REQUIRED"
					: "NULLABLE";
			const description = f.description ? escapeHtml(f.description) : "";
			const row = `<tr><td>${indent}${escapeHtml(f.name)}</td><td>${escapeHtml(f.type)}</td><td>${mode}</td><td>${description}</td></tr>`;
			const nested = f.fields ? renderSchemaTableRows(f.fields, depth + 1) : "";
			return row + nested;
		})
		.join("");
}

export function buildPreviewHtml(
	result: QueryResult | null | undefined,
): string {
	return result?.columns
		? renderQueryResultTable(result)
		: '<p class="bqls-empty">No query result to preview.</p>';
}

// Shared between the initial synchronous render and the async
// bqls/publishVirtualTextDocument notification handler, so both paths
// build the same details/preview HTML from the same inputs.
export function buildVirtualDocumentHtml(
	contents: MarkedString[] | null | undefined,
	result: QueryResult | null | undefined,
	schema?: FieldSchema[],
): { detailsHtml: string; previewHtml: string } {
	return {
		detailsHtml: buildDetailsHtml(contents, schema),
		previewHtml: buildPreviewHtml(result),
	};
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

	const header = columns
		.map((c, i) => {
			const description = schema[i]?.description;
			const title = description ? ` title="${escapeHtml(description)}"` : "";
			return `<th${title}>${escapeHtml(c)}</th>`;
		})
		.join("");
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
