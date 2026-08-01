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

export const COMMAND_LIST_DATASETS = "bqls.listDatasets";
export const COMMAND_LIST_TABLES = "bqls.listTables";
export const COMMAND_SEARCH_TABLES = "bqls.searchTables";

export function tableVirtualDocumentUri(
  projectId: string,
  datasetId: string,
  tableId: string,
): string {
  return `bqls://project/${projectId}/dataset/${datasetId}/table/${tableId}`;
}

// The BigQuery API requires a concrete project id, so when bqls.projectId
// is unset, just show a message node prompting the user to configure it.
export function rootNodes(projectId: string): BqlsTreeNode[] {
  if (!projectId) {
    return [
      {
        kind: "message",
        text: 'Set "bqls.projectId" in Settings to browse BigQuery datasets.',
      },
    ];
  }
  return [{ kind: "project", projectId }];
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
  projectId: string,
): unknown[] {
  return [query, projectId];
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

export interface TreeItemDescriptor {
  label: string;
  collapsible: "none" | "collapsed";
  icon: "info" | "database" | "table";
}

export function describeTreeItem(node: BqlsTreeNode): TreeItemDescriptor {
  switch (node.kind) {
    case "message":
      return { label: node.text, collapsible: "none", icon: "info" };
    case "project":
      return { label: node.projectId, collapsible: "collapsed", icon: "database" };
    case "dataset":
      return { label: node.datasetId, collapsible: "collapsed", icon: "database" };
    case "table":
      return { label: node.tableId, collapsible: "none", icon: "table" };
  }
}
