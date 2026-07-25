import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from "vscode-languageclient/node";
import {
  VirtualTextDocumentResult,
  renderVirtualTextDocument,
} from "./virtualDocument";
import {
  isExternalUrl,
  jobHistoryQuickPickItems,
  ListJobHistoryResult,
} from "./commandResultHandler";

const VIRTUAL_SCHEME = "bqls";

const COMMAND_EXECUTE_QUERY = "bqls.executeQuery";
const COMMAND_LIST_JOB_HISTORIES = "bqls.listJobHistories";
const COMMAND_SAVE_RESULT = "bqls.saveResult";

let client: LanguageClient | undefined;

async function openVirtualDocument(uri: string): Promise<void> {
  const doc = await vscode.workspace.openTextDocument(vscode.Uri.parse(uri));
  await vscode.window.showTextDocument(doc, { preview: false });
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
    },
  };

  client = new LanguageClient("bqls", "bqls", serverOptions, clientOptions);

  context.subscriptions.push(
    vscode.workspace.registerTextDocumentContentProvider(VIRTUAL_SCHEME, {
      provideTextDocumentContent: async (uri) => {
        if (!client) {
          return "";
        }
        const result = await client.sendRequest<VirtualTextDocumentResult>(
          "bqls/virtualTextDocument",
          { textDocument: { uri: uri.toString() } },
        );
        return renderVirtualTextDocument(result);
      },
    }),
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
