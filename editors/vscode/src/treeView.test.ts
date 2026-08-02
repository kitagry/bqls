import { describe, expect, it } from "vitest";
import {
	addProjectId,
	addableProjectQuickPickItems,
	datasetNodes,
	describeTreeItem,
	listDatasetsArguments,
	listProjectsArguments,
	listTablesArguments,
	removeProjectId,
	rootNodes,
	searchResultQuickPickItems,
	searchTablesArguments,
	tableNodes,
	tableVirtualDocumentUri,
} from "./treeView";

describe("tableVirtualDocumentUri", () => {
	it("matches the server's NewTableVirtualTextDocumentURI format", () => {
		expect(
			tableVirtualDocumentUri("my-project", "my_dataset", "my_table"),
		).toBe("bqls://project/my-project/dataset/my_dataset/table/my_table");
	});
});

describe("rootNodes", () => {
	it("returns a message node prompting the user to add a project when the list is empty", () => {
		expect(rootNodes([])).toEqual([
			{ kind: "message", text: expect.stringContaining("project") },
		]);
	});

	it("returns one project node per project id, in order", () => {
		expect(rootNodes(["my-project", "other-project"])).toEqual([
			{ kind: "project", projectId: "my-project" },
			{ kind: "project", projectId: "other-project" },
		]);
	});

	it("de-duplicates project ids", () => {
		expect(rootNodes(["my-project", "my-project"])).toEqual([
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
		expect(datasetNodes("my-project", { datasets: ["a", "b"] })).toEqual([
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
			{
				kind: "table",
				projectId: "my-project",
				datasetId: "my_dataset",
				tableId: "t1",
			},
			{
				kind: "table",
				projectId: "my-project",
				datasetId: "my_dataset",
				tableId: "t2",
			},
		]);
	});

	it("returns an empty array when there are no tables", () => {
		expect(tableNodes("my-project", "my_dataset", { tables: [] })).toEqual([]);
	});
});

describe("searchTablesArguments", () => {
	it("returns [query, ...projectIds]", () => {
		expect(
			searchTablesArguments("users", ["my-project", "other-project"]),
		).toEqual(["users", "my-project", "other-project"]);
	});

	it("returns just [query] when there are no project ids", () => {
		expect(searchTablesArguments("users", [])).toEqual(["users"]);
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
				tables: [{ projectId: "p", datasetId: "d", tableId: "t" }],
			}),
		).toEqual([
			{
				label: "p.d.t",
				description: "",
				uri: "bqls://project/p/dataset/d/table/t",
			},
		]);
	});

	it("returns an empty array when there are no matches", () => {
		expect(searchResultQuickPickItems({ tables: [] })).toEqual([]);
	});
});

describe("listProjectsArguments", () => {
	it("takes no arguments", () => {
		expect(listProjectsArguments()).toEqual([]);
	});
});

describe("addableProjectQuickPickItems", () => {
	it("maps projects to quick pick items keyed by projectId", () => {
		expect(
			addableProjectQuickPickItems(
				{
					projects: [
						{ projectId: "my-project", name: "My Project" },
						{ projectId: "other-project", name: "Other Project" },
					],
				},
				[],
			),
		).toEqual([
			{
				label: "my-project",
				description: "My Project",
				projectId: "my-project",
			},
			{
				label: "other-project",
				description: "Other Project",
				projectId: "other-project",
			},
		]);
	});

	it("excludes projects already in the explorer", () => {
		expect(
			addableProjectQuickPickItems(
				{
					projects: [
						{ projectId: "my-project", name: "My Project" },
						{ projectId: "other-project", name: "Other Project" },
					],
				},
				["my-project"],
			),
		).toEqual([
			{
				label: "other-project",
				description: "Other Project",
				projectId: "other-project",
			},
		]);
	});
});

describe("addProjectId", () => {
	it("appends a new project id", () => {
		expect(addProjectId(["a"], "b")).toEqual(["a", "b"]);
	});

	it("leaves the list unchanged when the project id is already present", () => {
		expect(addProjectId(["a", "b"], "b")).toEqual(["a", "b"]);
	});
});

describe("removeProjectId", () => {
	it("removes the matching project id", () => {
		expect(removeProjectId(["a", "b"], "a")).toEqual(["b"]);
	});

	it("leaves the list unchanged when the project id is absent", () => {
		expect(removeProjectId(["a", "b"], "c")).toEqual(["a", "b"]);
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

	it("describes a project node as collapsible with a database icon and a removable contextValue", () => {
		expect(
			describeTreeItem({ kind: "project", projectId: "my-project" }),
		).toEqual({
			label: "my-project",
			collapsible: "collapsed",
			icon: "database",
			contextValue: "bqlsProject",
		});
	});

	it("describes a dataset node as collapsible with a database icon", () => {
		expect(
			describeTreeItem({
				kind: "dataset",
				projectId: "p",
				datasetId: "my_dataset",
			}),
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
