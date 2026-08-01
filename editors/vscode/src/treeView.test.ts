import { describe, expect, it } from "vitest";
import {
  datasetNodes,
  describeTreeItem,
  listDatasetsArguments,
  listTablesArguments,
  rootNodes,
  searchResultQuickPickItems,
  searchTablesArguments,
  tableNodes,
  tableVirtualDocumentUri,
} from "./treeView";

describe("tableVirtualDocumentUri", () => {
  it("matches the server's NewTableVirtualTextDocumentURI format", () => {
    expect(tableVirtualDocumentUri("my-project", "my_dataset", "my_table")).toBe(
      "bqls://project/my-project/dataset/my_dataset/table/my_table",
    );
  });
});

describe("rootNodes", () => {
  it("returns a message node prompting configuration when projectId is empty", () => {
    expect(rootNodes("")).toEqual([
      { kind: "message", text: expect.stringContaining("bqls.projectId") },
    ]);
  });

  it("returns a single project node when projectId is set", () => {
    expect(rootNodes("my-project")).toEqual([
      { kind: "project", projectId: "my-project" },
    ]);
  });
});

describe("listDatasetsArguments", () => {
  it("wraps the projectId in a single-element array", () => {
    expect(listDatasetsArguments("my-project")).toEqual(["my-project"]);
  });
});

describe("datasetNodes", () => {
  it("maps dataset ids to dataset nodes under the given project", () => {
    expect(
      datasetNodes("my-project", { datasets: ["a", "b"] }),
    ).toEqual([
      { kind: "dataset", projectId: "my-project", datasetId: "a" },
      { kind: "dataset", projectId: "my-project", datasetId: "b" },
    ]);
  });

  it("returns an empty array when there are no datasets", () => {
    expect(datasetNodes("my-project", { datasets: [] })).toEqual([]);
  });
});

describe("listTablesArguments", () => {
  it("returns [projectId, datasetId]", () => {
    expect(listTablesArguments("my-project", "my_dataset")).toEqual([
      "my-project",
      "my_dataset",
    ]);
  });
});

describe("tableNodes", () => {
  it("maps table ids to table nodes under the given project/dataset", () => {
    expect(
      tableNodes("my-project", "my_dataset", { tables: ["t1", "t2"] }),
    ).toEqual([
      { kind: "table", projectId: "my-project", datasetId: "my_dataset", tableId: "t1" },
      { kind: "table", projectId: "my-project", datasetId: "my_dataset", tableId: "t2" },
    ]);
  });

  it("returns an empty array when there are no tables", () => {
    expect(tableNodes("my-project", "my_dataset", { tables: [] })).toEqual([]);
  });
});

describe("searchTablesArguments", () => {
  it("returns [query, projectId]", () => {
    expect(searchTablesArguments("users", "my-project")).toEqual([
      "users",
      "my-project",
    ]);
  });
});

describe("searchResultQuickPickItems", () => {
  it("builds a dotted label and passes through the description", () => {
    expect(
      searchResultQuickPickItems({
        tables: [
          {
            projectId: "my-project",
            datasetId: "my_dataset",
            tableId: "my_table",
            description: "user records",
          },
        ],
      }),
    ).toEqual([
      {
        label: "my-project.my_dataset.my_table",
        description: "user records",
        uri: "bqls://project/my-project/dataset/my_dataset/table/my_table",
      },
    ]);
  });

  it("falls back to an empty description when omitted", () => {
    expect(
      searchResultQuickPickItems({
        tables: [
          { projectId: "p", datasetId: "d", tableId: "t" },
        ],
      }),
    ).toEqual([
      { label: "p.d.t", description: "", uri: "bqls://project/p/dataset/d/table/t" },
    ]);
  });

  it("returns an empty array when there are no matches", () => {
    expect(searchResultQuickPickItems({ tables: [] })).toEqual([]);
  });
});

describe("describeTreeItem", () => {
  it("describes a message node as non-collapsible with an info icon", () => {
    expect(describeTreeItem({ kind: "message", text: "hello" })).toEqual({
      label: "hello",
      collapsible: "none",
      icon: "info",
    });
  });

  it("describes a project node as collapsible with a database icon", () => {
    expect(describeTreeItem({ kind: "project", projectId: "my-project" })).toEqual({
      label: "my-project",
      collapsible: "collapsed",
      icon: "database",
    });
  });

  it("describes a dataset node as collapsible with a database icon", () => {
    expect(
      describeTreeItem({ kind: "dataset", projectId: "p", datasetId: "my_dataset" }),
    ).toEqual({
      label: "my_dataset",
      collapsible: "collapsed",
      icon: "database",
    });
  });

  it("describes a table node as non-collapsible with a table icon", () => {
    expect(
      describeTreeItem({
        kind: "table",
        projectId: "p",
        datasetId: "d",
        tableId: "my_table",
      }),
    ).toEqual({
      label: "my_table",
      collapsible: "none",
      icon: "table",
    });
  });
});
