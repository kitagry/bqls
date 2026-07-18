import { describe, expect, it } from "vitest";
import {
  markedStringToString,
  queryResultToMarkdownTable,
  renderVirtualTextDocument,
} from "./virtualDocument";

describe("markedStringToString", () => {
  it("returns plain strings as-is", () => {
    expect(markedStringToString("hello")).toBe("hello");
  });

  it("renders a code-marked string as a fenced code block", () => {
    expect(
      markedStringToString({ language: "sql", value: "SELECT 1" }),
    ).toBe("```sql\nSELECT 1\n```");
  });
});

describe("queryResultToMarkdownTable", () => {
  it("renders columns and rows as a markdown table", () => {
    const table = queryResultToMarkdownTable({
      columns: ["id", "name"],
      rows: [
        [1, "alice"],
        [2, "bob"],
      ],
    });
    expect(table).toBe(
      [
        "| id | name |",
        "| --- | --- |",
        "| 1 | alice |",
        "| 2 | bob |",
      ].join("\n"),
    );
  });

  it("renders header only when there are no rows", () => {
    const table = queryResultToMarkdownTable({ columns: ["id"], rows: [] });
    expect(table).toBe(["| id |", "| --- |"].join("\n"));
  });
});

describe("renderVirtualTextDocument", () => {
  it("joins contents without a result", () => {
    const text = renderVirtualTextDocument({ contents: ["a", "b"] });
    expect(text).toBe("a\n\nb");
  });

  it("appends the query result table when present", () => {
    const text = renderVirtualTextDocument({
      contents: ["summary"],
      result: { columns: ["id"], rows: [[1]] },
    });
    expect(text).toBe(
      ["summary", ["| id |", "| --- |", "| 1 |"].join("\n")].join("\n\n"),
    );
  });
});
