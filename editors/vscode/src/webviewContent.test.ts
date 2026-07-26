import { describe, expect, it } from "vitest";
import {
  buildDetailsHtml,
  buildPreviewHtml,
  buildVirtualDocumentHtml,
  escapeHtml,
  renderCellValue,
  renderMarkedString,
  renderPage,
  renderQueryResultTable,
  renderSchemaTable,
  virtualDocumentTitle,
} from "./webviewContent";

describe("escapeHtml", () => {
  it("escapes html special characters", () => {
    expect(escapeHtml(`<b>a & "b" 'c'</b>`)).toBe(
      "&lt;b&gt;a &amp; &quot;b&quot; &#39;c&#39;&lt;/b&gt;",
    );
  });
});

describe("renderCellValue", () => {
  it("renders null as a null marker", () => {
    expect(renderCellValue(null)).toBe('<span class="bqls-null">null</span>');
  });

  it("renders a primitive value escaped", () => {
    expect(renderCellValue("<script>")).toBe("&lt;script&gt;");
  });

  it("renders a repeated (array) value as a collapsible list", () => {
    const html = renderCellValue(["a", "b"], {
      name: "tags",
      type: "STRING",
      repeated: true,
    });
    expect(html).toBe(
      '<details class="bqls-nested"><summary>[2]</summary><ul><li>a</li><li>b</li></ul></details>',
    );
  });

  it("renders a record value as a collapsible key-value table", () => {
    const html = renderCellValue(["Tokyo", "100-0001"], {
      name: "address",
      type: "RECORD",
      fields: [
        { name: "city", type: "STRING" },
        { name: "zip", type: "STRING" },
      ],
    });
    expect(html).toBe(
      '<details class="bqls-nested"><summary>{…}</summary>' +
        '<table class="bqls-record">' +
        "<tr><th>city</th><td>Tokyo</td></tr>" +
        "<tr><th>zip</th><td>100-0001</td></tr>" +
        "</table></details>",
    );
  });

  it("renders repeated record fields recursively", () => {
    const html = renderCellValue(
      [
        ["Tokyo"],
        ["Osaka"],
      ],
      {
        name: "addresses",
        type: "RECORD",
        repeated: true,
        fields: [{ name: "city", type: "STRING" }],
      },
    );
    expect(html).toBe(
      '<details class="bqls-nested"><summary>[2]</summary><ul>' +
        '<li><details class="bqls-nested"><summary>{…}</summary>' +
        '<table class="bqls-record"><tr><th>city</th><td>Tokyo</td></tr></table></details></li>' +
        '<li><details class="bqls-nested"><summary>{…}</summary>' +
        '<table class="bqls-record"><tr><th>city</th><td>Osaka</td></tr></table></details></li>' +
        "</ul></details>",
    );
  });
});

describe("renderMarkedString", () => {
  it("renders headings", () => {
    expect(renderMarkedString("## Job info")).toBe("<h2>Job info</h2>");
  });

  it("renders a bullet list", () => {
    expect(renderMarkedString("* Created: today\n* Ended: never")).toBe(
      "<ul><li>Created: today</li><li>Ended: never</li></ul>",
    );
  });

  it("renders a markdown link", () => {
    expect(renderMarkedString("[Query URL](https://example.com)")).toBe(
      '<p><a href="https://example.com">Query URL</a></p>',
    );
  });

  it("renders a plain paragraph", () => {
    expect(renderMarkedString("hello world")).toBe("<p>hello world</p>");
  });

  it("renders non-markdown languages as an escaped code block", () => {
    expect(renderMarkedString({ language: "yaml", value: "- name: id" })).toBe(
      "<pre><code>- name: id</code></pre>",
    );
  });

  it("escapes html in plain text", () => {
    expect(renderMarkedString("<script>alert(1)</script>")).toBe(
      "<p>&lt;script&gt;alert(1)&lt;/script&gt;</p>",
    );
  });
});

describe("renderPage", () => {
  it("includes tab buttons and both panels' content", () => {
    const html = renderPage({
      detailsHtml: "<p>details</p>",
      previewHtml: "<table>preview</table>",
    });
    expect(html).toContain("<p>details</p>");
    expect(html).toContain("<table>preview</table>");
    expect(html).toContain('data-tab="details"');
    expect(html).toContain('data-tab="preview"');
  });

  it("omits the preview tab when there is no query result", () => {
    const html = renderPage({
      detailsHtml: "<p>details</p>",
      previewHtml: null,
    });
    expect(html).toContain("<p>details</p>");
    expect(html).not.toContain('data-tab="preview"');
  });

  it("includes a script that listens for postMessage updates from the extension", () => {
    const html = renderPage({
      detailsHtml: "Loading...",
      previewHtml: "Loading...",
    });
    expect(html).toContain('addEventListener("message"');
    expect(html).toContain('data-tabpanel="details"');
    expect(html).toContain('data-tabpanel="preview"');
  });

  it("includes a script that handles separate details/preview message updates", () => {
    const html = renderPage({
      detailsHtml: "Loading...",
      previewHtml: "Loading...",
    });
    expect(html).toContain('message.type === "details"');
    expect(html).toContain('message.type === "preview"');
  });

  it("puts the Details tab before the Preview tab and makes Details active by default", () => {
    const html = renderPage({
      detailsHtml: "<p>details</p>",
      previewHtml: "<table>preview</table>",
    });
    const detailsTabIndex = html.indexOf('data-tab="details"');
    const previewTabIndex = html.indexOf('data-tab="preview"');
    expect(detailsTabIndex).toBeGreaterThan(-1);
    expect(previewTabIndex).toBeGreaterThan(-1);
    expect(detailsTabIndex).toBeLessThan(previewTabIndex);
    expect(html).toContain(
      '<button class="bqls-tab active" data-tab="details">',
    );
    expect(html).toContain(
      '<div class="bqls-tabpanel active" data-tabpanel="details">',
    );
  });
});

describe("buildVirtualDocumentHtml", () => {
  it("renders contents and a query result table", () => {
    const html = buildVirtualDocumentHtml(
      ["## Job info"],
      { columns: ["id"], data: [[1]] },
    );
    expect(html.detailsHtml).toBe("<h2>Job info</h2>");
    expect(html.previewHtml).toBe(
      '<table class="bqls-result"><thead><tr><th>id</th></tr></thead>' +
        "<tbody><tr><td>1</td></tr></tbody></table>",
    );
  });

  it("shows a placeholder preview when there is no query result (DDL/DML jobs)", () => {
    const html = buildVirtualDocumentHtml(["## Job info"], undefined);
    expect(html.detailsHtml).toBe("<h2>Job info</h2>");
    expect(html.previewHtml).toContain("No query result");
  });

  it("treats missing contents as empty", () => {
    const html = buildVirtualDocumentHtml(undefined, undefined);
    expect(html.detailsHtml).toBe("");
  });
});

describe("buildDetailsHtml", () => {
  it("renders contents", () => {
    expect(buildDetailsHtml(["## Job info"])).toBe("<h2>Job info</h2>");
  });

  it("treats missing contents as empty", () => {
    expect(buildDetailsHtml(undefined)).toBe("");
    expect(buildDetailsHtml(null)).toBe("");
  });

  it("appends a schema table when schema is provided", () => {
    const html = buildDetailsHtml(["## Job info"], [
      { name: "id", type: "INTEGER" },
    ]);
    expect(html).toBe(
      "<h2>Job info</h2>\n" +
        '<table class="bqls-result"><thead><tr><th>Name</th><th>Type</th><th>Mode</th><th>Description</th></tr></thead>' +
        "<tbody><tr><td>id</td><td>INTEGER</td><td>NULLABLE</td><td></td></tr></tbody></table>",
    );
  });

  it("omits the schema table when schema is empty", () => {
    expect(buildDetailsHtml(["## Job info"], [])).toBe("<h2>Job info</h2>");
  });
});

describe("renderSchemaTable", () => {
  it("renders a flat schema as a Name/Type/Mode/Description table", () => {
    const html = renderSchemaTable([
      { name: "id", type: "INTEGER", required: true, description: "primary key" },
      { name: "name", type: "STRING" },
    ]);
    expect(html).toBe(
      '<table class="bqls-result"><thead><tr><th>Name</th><th>Type</th><th>Mode</th><th>Description</th></tr></thead>' +
        "<tbody>" +
        "<tr><td>id</td><td>INTEGER</td><td>REQUIRED</td><td>primary key</td></tr>" +
        "<tr><td>name</td><td>STRING</td><td>NULLABLE</td><td></td></tr>" +
        "</tbody></table>",
    );
  });

  it("renders a repeated field's mode as REPEATED", () => {
    const html = renderSchemaTable([
      { name: "tags", type: "STRING", repeated: true },
    ]);
    expect(html).toContain("<td>REPEATED</td>");
  });

  it("indents nested RECORD fields", () => {
    const html = renderSchemaTable([
      {
        name: "address",
        type: "RECORD",
        fields: [
          { name: "city", type: "STRING" },
          { name: "zip", type: "STRING" },
        ],
      },
    ]);
    expect(html).toBe(
      '<table class="bqls-result"><thead><tr><th>Name</th><th>Type</th><th>Mode</th><th>Description</th></tr></thead>' +
        "<tbody>" +
        "<tr><td>address</td><td>RECORD</td><td>NULLABLE</td><td></td></tr>" +
        "<tr><td>&nbsp;&nbsp;city</td><td>STRING</td><td>NULLABLE</td><td></td></tr>" +
        "<tr><td>&nbsp;&nbsp;zip</td><td>STRING</td><td>NULLABLE</td><td></td></tr>" +
        "</tbody></table>",
    );
  });
});

describe("buildPreviewHtml", () => {
  it("renders a query result table", () => {
    expect(buildPreviewHtml({ columns: ["id"], data: [[1]] })).toBe(
      '<table class="bqls-result"><thead><tr><th>id</th></tr></thead>' +
        "<tbody><tr><td>1</td></tr></tbody></table>",
    );
  });

  it("shows a placeholder when there is no query result", () => {
    expect(buildPreviewHtml(undefined)).toContain("No query result");
    expect(buildPreviewHtml(null)).toContain("No query result");
  });
});

describe("virtualDocumentTitle", () => {
  it("builds a title for a table uri", () => {
    expect(
      virtualDocumentTitle("bqls://project/p/dataset/d/table/t"),
    ).toBe("d.t");
  });

  it("builds a title for a job uri", () => {
    expect(
      virtualDocumentTitle("bqls://project/p/job/abc123/location/US"),
    ).toBe("Job abc123");
  });

  it("falls back to a generic title for an unrecognized uri", () => {
    expect(virtualDocumentTitle("bqls://something/else")).toBe("bqls");
  });
});

describe("renderQueryResultTable", () => {
  it("renders columns and data as an html table", () => {
    const html = renderQueryResultTable({
      columns: ["id", "name"],
      data: [[1, "alice"]],
    });
    expect(html).toBe(
      '<table class="bqls-result"><thead><tr><th>id</th><th>name</th></tr></thead>' +
        "<tbody><tr><td>1</td><td>alice</td></tr></tbody></table>",
    );
  });

  it("uses schema to render nested cells", () => {
    const html = renderQueryResultTable({
      columns: ["tags"],
      data: [[["a", "b"]]],
      schema: [{ name: "tags", type: "STRING", repeated: true }],
    });
    expect(html).toBe(
      '<table class="bqls-result"><thead><tr><th>tags</th></tr></thead>' +
        "<tbody><tr><td>" +
        '<details class="bqls-nested"><summary>[2]</summary><ul><li>a</li><li>b</li></ul></details>' +
        "</td></tr></tbody></table>",
    );
  });

  it("adds a title attribute with the column description when available", () => {
    const html = renderQueryResultTable({
      columns: ["id"],
      data: [[1]],
      schema: [{ name: "id", type: "INTEGER", description: "primary key" }],
    });
    expect(html).toBe(
      '<table class="bqls-result"><thead><tr><th title="primary key">id</th></tr></thead>' +
        "<tbody><tr><td>1</td></tr></tbody></table>",
    );
  });

  it("omits the title attribute when there is no description", () => {
    const html = renderQueryResultTable({
      columns: ["id"],
      data: [[1]],
      schema: [{ name: "id", type: "INTEGER" }],
    });
    expect(html).toBe(
      '<table class="bqls-result"><thead><tr><th>id</th></tr></thead>' +
        "<tbody><tr><td>1</td></tr></tbody></table>",
    );
  });
});
