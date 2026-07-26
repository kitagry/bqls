import {
  BQLS_VERSION,
  assetFileName,
  checksumFileName,
  findSha256,
  releaseAssetUrl,
  resolvePlatformTarget,
} from "./bqlsRelease";

export interface InstallerDeps {
  fileExists(path: string): Promise<boolean>;
  mkdir(path: string): Promise<void>;
  downloadText(url: string): Promise<string>;
  downloadFile(url: string, destPath: string): Promise<void>;
  extractTarGz(archivePath: string, destDir: string): Promise<void>;
  chmodExecutable(path: string): Promise<void>;
  sha256File(path: string): Promise<string>;
  checkOnPath(command: string): Promise<boolean>;
}

export interface ResolveBqlsCommandResult {
  command: string;
  error?: string;
}

const DEFAULT_PATH = "bqls";

function manualInstallFallback(reason: string): ResolveBqlsCommandResult {
  return {
    command: DEFAULT_PATH,
    error: `${reason} Please install bqls manually and set bqls.path.`,
  };
}

export async function resolveBqlsCommand(
  configuredPath: string,
  storageDir: string,
  platform: string,
  arch: string,
  deps: InstallerDeps,
): Promise<ResolveBqlsCommandResult> {
  if (configuredPath && configuredPath !== DEFAULT_PATH) {
    return { command: configuredPath };
  }

  if (await deps.checkOnPath(DEFAULT_PATH)) {
    return { command: DEFAULT_PATH };
  }

  const target = resolvePlatformTarget(platform, arch);
  if (!target) {
    return manualInstallFallback(
      "Windows is not yet supported for automatic bqls installation.",
    );
  }

  const installDir = `${storageDir}/bqls-${BQLS_VERSION}`;
  const cachedCommand = `${installDir}/bqls`;
  if (await deps.fileExists(cachedCommand)) {
    return { command: cachedCommand };
  }

  const fileName = assetFileName(BQLS_VERSION, target);

  try {
    const checksumText = await deps.downloadText(
      releaseAssetUrl(BQLS_VERSION, checksumFileName(BQLS_VERSION)),
    );
    const expectedSha = findSha256(checksumText, fileName);
    if (!expectedSha) {
      throw new Error(`no checksum found for ${fileName}`);
    }

    await deps.mkdir(installDir);
    const archivePath = `${installDir}/${fileName}`;
    await deps.downloadFile(
      releaseAssetUrl(BQLS_VERSION, fileName),
      archivePath,
    );

    const actualSha = await deps.sha256File(archivePath);
    if (actualSha !== expectedSha) {
      throw new Error(
        `checksum mismatch for ${fileName}: expected ${expectedSha}, got ${actualSha}`,
      );
    }

    await deps.extractTarGz(archivePath, installDir);
    await deps.chmodExecutable(cachedCommand);

    return { command: cachedCommand };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return manualInstallFallback(`Failed to download bqls automatically: ${message}.`);
  }
}
