import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from "vscode-languageclient/node";
import { VirtualTextDocumentResult } from "./virtualDocument";
import {
  isExternalUrl,
  jobHistoryQuickPickItems,
  ListJobHistoryResult,
} from "./commandResultHandler";
import {
  renderMarkedString,
  renderPage,
  renderQueryResultTable,
  virtualDocumentTitle,
} from "./webviewContent";

const VIRTUAL_SCHEME = "bqls";

const COMMAND_EXECUTE_QUERY = "bqls.executeQuery";
const COMMAND_LIST_JOB_HISTORIES = "bqls.listJobHistories";
const COMMAND_SAVE_RESULT = "bqls.saveResult";

let client: LanguageClient | undefined;

// Tracks the WebviewPanel open for each uri so repeated opens reveal the
// existing panel instead of creating a duplicate.
const webviewPanels = new Map<string, vscode.WebviewPanel>();

// Whatever route opens a bqls:// document (Execute Query, List Job
// Histories, Go to Definition), render it in the same BigQuery
// console-style tabbed webview.
async function openVirtualDocument(uriString: string): Promise<void> {
  if (!client) {
    return;
  }

  const existing = webviewPanels.get(uriString);
  if (existing) {
    existing.reveal();
    return;
  }

  const result = await client.sendRequest<VirtualTextDocumentResult>(
    "bqls/virtualTextDocument",
    { textDocument: { uri: uriString } },
  );

  const panel = vscode.window.createWebviewPanel(
    "bqlsVirtualDocument",
    virtualDocumentTitle(uriString),
    vscode.ViewColumn.Active,
    { enableScripts: true },
  );

  const detailsHtml = (result.contents ?? [])
    .map(renderMarkedString)
    .join("\n");
  const previewHtml = result.result?.columns
    ? renderQueryResultTable(result.result)
    : null;
  panel.webview.html = renderPage({ detailsHtml, previewHtml });

  webviewPanels.set(uriString, panel);
  panel.onDidDispose(() => {
    webviewPanels.delete(uriString);
  });
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

function buildSettings(): { project_id: string; location: string } {
  const config = vscode.workspace.getConfiguration("bqls");
  return {
    project_id: config.get<string>("projectId") ?? "",
    location: config.get<string>("location") ?? "",
  };
}

export async function activate(
  context: vscode.ExtensionContext,
): Promise<void> {
  const config = vscode.workspace.getConfiguration("bqls");
  const command = config.get<string>("path") ?? "bqls";

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
