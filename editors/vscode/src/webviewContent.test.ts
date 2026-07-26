import { describe, expect, it } from "vitest";
import {
  escapeHtml,
  renderCellValue,
  renderMarkedString,
  renderPage,
  renderQueryResultTable,
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
});
