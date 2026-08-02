export type BqlsTreeNode =
	| { kind: "message"; text: string }
	| { kind: "project"; projectId: string }
	| { kind: "dataset"; projectId: string; datasetId: string }
	| { kind: "table"; projectId: string; datasetId: string; tableId: string };

export interface ListDatasetsResult {
	datasets: string[];
}

export interface ListTablesResult {
	tables: string[];
}

export interface TableSearchResult {
	projectId: string;
	datasetId: string;
	tableId: string;
	description?: string;
}

export interface SearchTablesResult {
	tables: TableSearchResult[];
}

export interface ProjectInfo {
	projectId: string;
	name: string;
}

export interface ListProjectsResult {
	projects: ProjectInfo[];
}

export const COMMAND_LIST_DATASETS = "bqls.listDatasets";
export const COMMAND_LIST_TABLES = "bqls.listTables";
export const COMMAND_SEARCH_TABLES = "bqls.searchTables";
export const COMMAND_LIST_PROJECTS = "bqls.listProjects";

export function tableVirtualDocumentUri(
	projectId: string,
	datasetId: string,
	tableId: string,
): string {
	return `bqls://project/${projectId}/dataset/${datasetId}/table/${tableId}`;
}

// projectIds is the explicit list the user has added to the explorer
// (bqls.projectIds), independent of the single bqls.projectId setting used
// as the LSP server's default project. When it's empty, show a message node
// prompting the user to add one via the "+" button instead of a project node.
export function rootNodes(projectIds: string[]): BqlsTreeNode[] {
	const uniqueProjectIds = Array.from(new Set(projectIds));
	if (uniqueProjectIds.length === 0) {
		return [
			{
				kind: "message",
				text: 'Click "+" above to add a BigQuery project to the Datasets explorer.',
			},
		];
	}
	return uniqueProjectIds.map((projectId) => ({ kind: "project", projectId }));
}

export function listDatasetsArguments(projectId: string): unknown[] {
	return [projectId];
}

export function datasetNodes(
	projectId: string,
	result: ListDatasetsResult,
): BqlsTreeNode[] {
	return result.datasets.map((datasetId) => ({
		kind: "dataset",
		projectId,
		datasetId,
	}));
}

export function listTablesArguments(
	projectId: string,
	datasetId: string,
): unknown[] {
	return [projectId, datasetId];
}

export function tableNodes(
	projectId: string,
	datasetId: string,
	result: ListTablesResult,
): BqlsTreeNode[] {
	return result.tables.map((tableId) => ({
		kind: "table",
		projectId,
		datasetId,
		tableId,
	}));
}

export function searchTablesArguments(
	query: string,
	projectIds: string[],
): unknown[] {
	return [query, ...projectIds];
}

export interface SearchTablesQuickPickItem {
	label: string;
	description: string;
	uri: string;
}

export function searchResultQuickPickItems(
	result: SearchTablesResult,
): SearchTablesQuickPickItem[] {
	return result.tables.map((t) => ({
		label: `${t.projectId}.${t.datasetId}.${t.tableId}`,
		description: t.description ?? "",
		uri: tableVirtualDocumentUri(t.projectId, t.datasetId, t.tableId),
	}));
}

export function listProjectsArguments(): unknown[] {
	return [];
}

export interface ProjectQuickPickItem {
	label: string;
	description: string;
	projectId: string;
}

export function addableProjectQuickPickItems(
	result: ListProjectsResult,
	existingProjectIds: string[],
): ProjectQuickPickItem[] {
	const existing = new Set(existingProjectIds);
	return result.projects
		.filter((p) => !existing.has(p.projectId))
		.map((p) => ({
			label: p.projectId,
			description: p.name,
			projectId: p.projectId,
		}));
}

export function addProjectId(
	projectIds: string[],
	projectId: string,
): string[] {
	if (projectIds.includes(projectId)) {
		return projectIds;
	}
	return [...projectIds, projectId];
}

export function removeProjectId(
	projectIds: string[],
	projectId: string,
): string[] {
	return projectIds.filter((id) => id !== projectId);
}

export interface TreeItemDescriptor {
	label: string;
	collapsible: "none" | "collapsed";
	icon: "info" | "database" | "table";
	contextValue?: "bqlsProject";
}

export function describeTreeItem(node: BqlsTreeNode): TreeItemDescriptor {
	switch (node.kind) {
		case "message":
			return { label: node.text, collapsible: "none", icon: "info" };
		case "project":
			return {
				label: node.projectId,
				collapsible: "collapsed",
				icon: "database",
				contextValue: "bqlsProject",
			};
		case "dataset":
			return {
				label: node.datasetId,
				collapsible: "collapsed",
				icon: "database",
			};
		case "table":
			return { label: node.tableId, collapsible: "none", icon: "table" };
	}
}
