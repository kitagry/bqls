import * as crypto from "node:crypto";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import { createWriteStream } from "node:fs";
import * as https from "node:https";
import { promisify } from "node:util";
import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from "vscode-languageclient/node";
import { InstallerDeps, resolveBqlsCommand } from "./bqlsInstaller";
import {
  PublishVirtualTextDocumentParams,
  VirtualTextDocumentResult,
} from "./virtualDocument";
import {
  isExternalUrl,
  jobHistoryQuickPickItems,
  ListJobHistoryResult,
} from "./commandResultHandler";
import {
  buildDetailsHtml,
  buildPreviewHtml,
  buildVirtualDocumentHtml,
  renderPage,
  virtualDocumentTitle,
} from "./webviewContent";
import {
  BqlsTreeNode,
  COMMAND_LIST_DATASETS,
  COMMAND_LIST_PROJECTS,
  COMMAND_LIST_TABLES,
  COMMAND_SEARCH_TABLES,
  ListDatasetsResult,
  ListProjectsResult,
  ListTablesResult,
  SearchTablesResult,
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

const VIRTUAL_SCHEME = "bqls";

const COMMAND_EXECUTE_QUERY = "bqls.executeQuery";
const COMMAND_LIST_JOB_HISTORIES = "bqls.listJobHistories";
const COMMAND_SAVE_RESULT = "bqls.saveResult";

const COMMAND_OPEN_TABLE_FROM_TREE = "bqls.openTableFromTree";
const COMMAND_REFRESH_DATASET_EXPLORER = "bqls.refreshDatasetExplorer";
const COMMAND_SEARCH_TABLES_IN_EXPLORER = "bqls.searchTablesInExplorer";
const COMMAND_ADD_PROJECT_TO_EXPLORER = "bqls.addProjectToExplorer";
const COMMAND_REMOVE_PROJECT_FROM_EXPLORER = "bqls.removeProjectFromExplorer";

let client: LanguageClient | undefined;

// Tracks the WebviewPanel open for each uri so repeated opens reveal the
// existing panel instead of creating a duplicate.
const webviewPanels = new Map<string, vscode.WebviewPanel>();

// Whatever route opens a bqls:// document (Execute Query, List Job
// Histories, Go to Definition), render it in the same BigQuery
// console-style tabbed webview. The panel opens immediately in a loading
// state; if the server supports async fetching it responds with
// `pending: true` and the real content arrives later via
// bqls/publishVirtualTextDocument (see the onNotification handler in
// activate). Older servers just return the full result synchronously.
async function openVirtualDocument(uriString: string): Promise<void> {
  if (!client) {
    return;
  }

  const existing = webviewPanels.get(uriString);
  if (existing) {
    existing.reveal();
    return;
  }

  const panel = vscode.window.createWebviewPanel(
    "bqlsVirtualDocument",
    virtualDocumentTitle(uriString),
    vscode.ViewColumn.Active,
    // retainContextWhenHidden: without this, VSCode tears down the webview's
    // DOM state whenever the panel is backgrounded (e.g. the user switches
    // tabs). Since the async fetch's postMessage updates only touch the live
    // DOM (not panel.webview.html), a backgrounded-then-reopened panel would
    // otherwise reload from the original "Loading..." html and get stuck
    // there even though the data already arrived.
    { enableScripts: true, retainContextWhenHidden: true },
  );
  panel.webview.html = renderPage({
    detailsHtml: "Loading...",
    previewHtml: "Loading...",
  });
  webviewPanels.set(uriString, panel);
  panel.onDidDispose(() => {
    webviewPanels.delete(uriString);
  });

  const result = await client.sendRequest<VirtualTextDocumentResult>(
    "bqls/virtualTextDocument",
    { textDocument: { uri: uriString } },
  );

  if (!result.pending) {
    const { detailsHtml, previewHtml } = buildVirtualDocumentHtml(
      result.contents,
      result.result,
      result.schema,
    );
    panel.webview.html = renderPage({ detailsHtml, previewHtml });
  }
  // If result.pending is true, wait for the bqls/publishVirtualTextDocument
  // notification to fill in the panel via postMessage.
}

// Executing a Code Action (Command) that bqls returns doesn't show anything
// in the editor by itself, so handle each command's result on the VSCode
// side to open or notify.
async function handleCommandResult(
  command: string,
  result: unknown,
): Promise<void> {
  switch (command) {
    case COMMAND_EXECUTE_QUERY: {
      const uri = (result as { textDocument?: { uri?: string } })
        ?.textDocument?.uri;
      if (uri) {
        await openVirtualDocument(uri);
      }
      break;
    }
    case COMMAND_LIST_JOB_HISTORIES: {
      const items = jobHistoryQuickPickItems(result as ListJobHistoryResult);
      const picked = await vscode.window.showQuickPick(items, {
        placeHolder: "Select a job to view",
      });
      if (picked) {
        await openVirtualDocument(picked.uri);
      }
      break;
    }
    case COMMAND_SAVE_RESULT: {
      const url = (result as { url?: string })?.url;
      if (url) {
        if (isExternalUrl(url)) {
          await vscode.env.openExternal(vscode.Uri.parse(url));
        } else {
          void vscode.window.showInformationMessage(`Saved result to ${url}`);
        }
      }
      break;
    }
  }
}

// When a Go to Definition target is a bqls:// document, VSCode's standard
// jump would open it as a plain text editor and bypass the webview, so
// intercept it here instead.
async function handleDefinitionResult(
  result: vscode.Definition | vscode.DefinitionLink[] | null | undefined,
): Promise<vscode.Definition | vscode.DefinitionLink[] | null | undefined> {
  if (!result) {
    return result;
  }

  const locations = Array.isArray(result) ? result : [result];
  const otherLocations: (vscode.Location | vscode.DefinitionLink)[] = [];
  const virtualUris: string[] = [];

  for (const loc of locations) {
    const uri = "targetUri" in loc ? loc.targetUri : loc.uri;
    if (uri.scheme === VIRTUAL_SCHEME) {
      virtualUris.push(uri.toString());
    } else {
      otherLocations.push(loc);
    }
  }

  for (const uriString of virtualUris) {
    await openVirtualDocument(uriString);
  }

  if (virtualUris.length === 0) {
    return result;
  }
  if (otherLocations.length === 0) {
    return null;
  }
  return otherLocations as vscode.Definition | vscode.DefinitionLink[];
}

const execFileAsync = promisify(execFile);

// GitHub release asset downloads respond with a redirect to
// objects.githubusercontent.com; https.get does not follow redirects itself.
function httpGetFollowingRedirects(
  url: string,
  redirectsLeft = 5,
): Promise<NodeJS.ReadableStream> {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        const status = res.statusCode ?? 0;
        if (status >= 300 && status < 400 && res.headers.location) {
          res.resume();
          if (redirectsLeft === 0) {
            reject(new Error(`too many redirects for ${url}`));
            return;
          }
          resolve(httpGetFollowingRedirects(res.headers.location, redirectsLeft - 1));
          return;
        }
        if (status !== 200) {
          res.resume();
          reject(new Error(`request to ${url} failed with status ${status}`));
          return;
        }
        resolve(res);
      })
      .on("error", reject);
  });
}

async function downloadText(url: string): Promise<string> {
  const stream = await httpGetFollowingRedirects(url);
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function downloadFile(url: string, destPath: string): Promise<void> {
  const stream = await httpGetFollowingRedirects(url);
  await new Promise<void>((resolve, reject) => {
    const fileStream = createWriteStream(destPath);
    stream.on("error", reject);
    fileStream.on("error", reject);
    fileStream.on("finish", resolve);
    stream.pipe(fileStream);
  });
}

function createNodeInstallerDeps(): InstallerDeps {
  return {
    fileExists: async (path) => {
      try {
        await fs.access(path);
        return true;
      } catch {
        return false;
      }
    },
    mkdir: async (path) => {
      await fs.mkdir(path, { recursive: true });
    },
    downloadText,
    downloadFile: async (url, destPath) => {
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: "Downloading bqls...",
        },
        () => downloadFile(url, destPath),
      );
    },
    extractTarGz: async (archivePath, destDir) => {
      await execFileAsync("tar", ["-xzf", archivePath, "-C", destDir]);
    },
    chmodExecutable: async (path) => {
      await fs.chmod(path, 0o755);
    },
    sha256File: async (path) => {
      const data = await fs.readFile(path);
      return crypto.createHash("sha256").update(data).digest("hex");
    },
    checkOnPath: async (command) => {
      try {
        await execFileAsync(command, ["--version"]);
        return true;
      } catch {
        return false;
      }
    },
  };
}

function buildSettings(): {
  project_id: string;
  location: string;
  supports_async_virtual_text_document: boolean;
} {
  const config = vscode.workspace.getConfiguration("bqls");
  return {
    project_id: config.get<string>("projectId") ?? "",
    location: config.get<string>("location") ?? "",
    supports_async_virtual_text_document: true,
  };
}

// The list of projects shown in the Datasets explorer (bqls.projectIds) is
// deliberately independent of bqls.projectId: the latter is the LSP
// server's single default project (used for query execution, hover, etc.),
// while the former is a user-managed list of projects to browse. Falling
// back from one to the other would make "remove" ambiguous (removing the
// last explorer project could make it reappear via bqls.projectId).
function getExplorerProjectIds(): string[] {
  return vscode.workspace.getConfiguration("bqls").get<string[]>("projectIds") ?? [];
}

// Mirrors bqls.nvim's sidebar (lua/bqls/sidebar.lua): a lazily-loaded
// project -> dataset -> table tree, backed by the same
// bqls.listDatasets/bqls.listTables workspace/executeCommand commands the
// nvim sidebar uses. We call workspace/executeCommand directly instead of
// going through vscode.commands.executeCommand, since the latter requires
// the command to be advertised in the server's initialize capabilities
// (which bqls.searchTables currently is not).
class BqlsTreeDataProvider implements vscode.TreeDataProvider<BqlsTreeNode> {
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<
    BqlsTreeNode | undefined
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  refresh(): void {
    this._onDidChangeTreeData.fire(undefined);
  }

  getTreeItem(node: BqlsTreeNode): vscode.TreeItem {
    const descriptor = describeTreeItem(node);
    const item = new vscode.TreeItem(
      descriptor.label,
      descriptor.collapsible === "collapsed"
        ? vscode.TreeItemCollapsibleState.Collapsed
        : vscode.TreeItemCollapsibleState.None,
    );
    item.iconPath = new vscode.ThemeIcon(descriptor.icon);
    if (descriptor.contextValue) {
      item.contextValue = descriptor.contextValue;
    }
    if (node.kind === "table") {
      item.command = {
        command: COMMAND_OPEN_TABLE_FROM_TREE,
        title: "Open",
        arguments: [
          tableVirtualDocumentUri(node.projectId, node.datasetId, node.tableId),
        ],
      };
    } else if (node.kind === "message") {
      item.command = {
        command: COMMAND_ADD_PROJECT_TO_EXPLORER,
        title: "Add Project",
      };
    }
    return item;
  }

  async getChildren(node?: BqlsTreeNode): Promise<BqlsTreeNode[]> {
    if (!client) {
      return [];
    }
    if (!node) {
      return rootNodes(getExplorerProjectIds());
    }
    try {
      switch (node.kind) {
        case "project": {
          const result = await client.sendRequest<ListDatasetsResult>(
            "workspace/executeCommand",
            {
              command: COMMAND_LIST_DATASETS,
              arguments: listDatasetsArguments(node.projectId),
            },
          );
          return datasetNodes(node.projectId, result);
        }
        case "dataset": {
          const result = await client.sendRequest<ListTablesResult>(
            "workspace/executeCommand",
            {
              command: COMMAND_LIST_TABLES,
              arguments: listTablesArguments(node.projectId, node.datasetId),
            },
          );
          return tableNodes(node.projectId, node.datasetId, result);
        }
        default:
          return [];
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      void vscode.window.showErrorMessage(`bqls: ${message}`);
      return [];
    }
  }
}

async function setExplorerProjectIds(projectIds: string[]): Promise<void> {
  await vscode.workspace
    .getConfiguration("bqls")
    .update("projectIds", projectIds, vscode.ConfigurationTarget.Global);
}

async function addProjectToExplorer(): Promise<void> {
  if (!client) {
    return;
  }

  let projectId: string | undefined;
  try {
    const result = await client.sendRequest<ListProjectsResult>(
      "workspace/executeCommand",
      { command: COMMAND_LIST_PROJECTS, arguments: listProjectsArguments() },
    );
    const items = addableProjectQuickPickItems(result, getExplorerProjectIds());
    if (items.length === 0) {
      void vscode.window.showInformationMessage(
        "bqls: no more accessible BigQuery projects to add",
      );
      return;
    }
    const picked = await vscode.window.showQuickPick(items, {
      placeHolder: "Select a BigQuery project to add",
    });
    projectId = picked?.projectId;
  } catch (err) {
    // Listing projects requires resourcemanager.projects.list; fall back to
    // manual entry so the explorer still works without that permission.
    const message = err instanceof Error ? err.message : String(err);
    void vscode.window.showWarningMessage(
      `bqls: failed to list BigQuery projects (${message}). Enter a project id manually.`,
    );
    projectId = await vscode.window.showInputBox({ prompt: "BigQuery project ID" });
  }

  if (!projectId) {
    return;
  }
  await setExplorerProjectIds(addProjectId(getExplorerProjectIds(), projectId));
}

async function removeProjectFromExplorer(node?: BqlsTreeNode): Promise<void> {
  if (!node || node.kind !== "project") {
    return;
  }
  await setExplorerProjectIds(removeProjectId(getExplorerProjectIds(), node.projectId));
}

async function searchTablesInExplorer(): Promise<void> {
  if (!client) {
    return;
  }
  const projectIds = getExplorerProjectIds();
  if (projectIds.length === 0) {
    void vscode.window.showWarningMessage(
      'Add a BigQuery project to the Datasets explorer (click "+") before searching tables.',
    );
    return;
  }

  const query = await vscode.window.showInputBox({ prompt: "Search tables" });
  if (!query) {
    return;
  }

  try {
    const result = await client.sendRequest<SearchTablesResult>(
      "workspace/executeCommand",
      {
        command: COMMAND_SEARCH_TABLES,
        arguments: searchTablesArguments(query, projectIds),
      },
    );
    const items = searchResultQuickPickItems(result);
    if (items.length === 0) {
      void vscode.window.showInformationMessage("bqls: no tables found");
      return;
    }

    const picked = await vscode.window.showQuickPick(items, {
      placeHolder: "Search Tables",
    });
    if (picked) {
      await openVirtualDocument(picked.uri);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    void vscode.window.showErrorMessage(`bqls: ${message}`);
  }
}

export async function activate(
  context: vscode.ExtensionContext,
): Promise<void> {
  const config = vscode.workspace.getConfiguration("bqls");
  const configuredPath = config.get<string>("path") ?? "bqls";
  const { command, error } = await resolveBqlsCommand(
    configuredPath,
    context.globalStorageUri.fsPath,
    process.platform,
    process.arch,
    createNodeInstallerDeps(),
  );
  if (error) {
    void vscode.window.showWarningMessage(error);
  }

  const serverOptions: ServerOptions = {
    command,
    transport: TransportKind.stdio,
  };

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "sql" }],
    initializationOptions: buildSettings(),
    middleware: {
      executeCommand: async (command, args, next) => {
        const result = await next(command, args);
        await handleCommandResult(command, result);
        return result;
      },
      provideDefinition: async (document, position, token, next) => {
        const result = await next(document, position, token);
        return handleDefinitionResult(result);
      },
    },
  };

  client = new LanguageClient("bqls", "bqls", serverOptions, clientOptions);

  const treeDataProvider = new BqlsTreeDataProvider();
  context.subscriptions.push(
    vscode.window.createTreeView("bqlsDatasetExplorer", { treeDataProvider }),
  );
  context.subscriptions.push(
    vscode.commands.registerCommand(COMMAND_REFRESH_DATASET_EXPLORER, () =>
      treeDataProvider.refresh(),
    ),
    vscode.commands.registerCommand(COMMAND_OPEN_TABLE_FROM_TREE, (uri: string) =>
      openVirtualDocument(uri),
    ),
    vscode.commands.registerCommand(
      COMMAND_SEARCH_TABLES_IN_EXPLORER,
      searchTablesInExplorer,
    ),
    vscode.commands.registerCommand(
      COMMAND_ADD_PROJECT_TO_EXPLORER,
      addProjectToExplorer,
    ),
    vscode.commands.registerCommand(
      COMMAND_REMOVE_PROJECT_FROM_EXPLORER,
      removeProjectFromExplorer,
    ),
  );

  context.subscriptions.push(
    client.onNotification(
      "bqls/publishVirtualTextDocument",
      (params: PublishVirtualTextDocumentParams) => {
        const panel = webviewPanels.get(params.textDocument.uri);
        if (!panel) {
          return; // panel was closed before the fetch completed
        }
        if (params.error) {
          void panel.webview.postMessage({
            type: "error",
            message: params.error,
          });
          return;
        }
        if (params.kind === "details") {
          void panel.webview.postMessage({
            type: "details",
            detailsHtml: buildDetailsHtml(params.contents, params.schema),
          });
          return;
        }
        if (params.kind === "preview") {
          void panel.webview.postMessage({
            type: "preview",
            previewHtml: buildPreviewHtml(params.result),
          });
        }
      },
    ),
  );

  context.subscriptions.push(
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (!client) {
        return;
      }
      if (
        event.affectsConfiguration("bqls.projectId") ||
        event.affectsConfiguration("bqls.location")
      ) {
        client.sendNotification("workspace/didChangeConfiguration", {
          settings: buildSettings(),
        });
      }
      // The tree's root nodes reflect bqls.projectIds, so re-render it too.
      if (event.affectsConfiguration("bqls.projectIds")) {
        treeDataProvider.refresh();
      }
    }),
  );

  await client.start();
}

export async function deactivate(): Promise<void> {
  if (client) {
    await client.stop();
    client = undefined;
  }
}
