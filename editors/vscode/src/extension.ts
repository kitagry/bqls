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

const VIRTUAL_SCHEME = "bqls";

let client: LanguageClient | undefined;

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
