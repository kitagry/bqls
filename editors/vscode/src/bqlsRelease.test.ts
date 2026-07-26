import { describe, expect, it } from "vitest";
import {
  BQLS_VERSION,
  assetFileName,
  checksumFileName,
  findSha256,
  releaseAssetUrl,
  resolvePlatformTarget,
} from "./bqlsRelease";

describe("resolvePlatformTarget", () => {
  it("maps darwin/arm64", () => {
    expect(resolvePlatformTarget("darwin", "arm64")).toEqual({
      os: "darwin",
      arch: "arm64",
    });
  });

  it("maps darwin/x64 to amd64", () => {
    expect(resolvePlatformTarget("darwin", "x64")).toEqual({
      os: "darwin",
      arch: "amd64",
    });
  });

  it("maps linux/x64 to amd64", () => {
    expect(resolvePlatformTarget("linux", "x64")).toEqual({
      os: "linux",
      arch: "amd64",
    });
  });

  it("maps linux/arm64", () => {
    expect(resolvePlatformTarget("linux", "arm64")).toEqual({
      os: "linux",
      arch: "arm64",
    });
  });

  it("returns undefined for win32", () => {
    expect(resolvePlatformTarget("win32", "x64")).toBeUndefined();
  });

  it("returns undefined for unsupported arch", () => {
    expect(resolvePlatformTarget("darwin", "ia32")).toBeUndefined();
  });
});

describe("assetFileName", () => {
  it("builds the goreleaser archive name", () => {
    expect(
      assetFileName("0.6.0", { os: "darwin", arch: "arm64" }),
    ).toBe("bqls_0.6.0_darwin_arm64.tar.gz");
  });
});

describe("checksumFileName", () => {
  it("builds the goreleaser checksums file name", () => {
    expect(checksumFileName("0.6.0")).toBe("bqls_0.6.0_checksums.txt");
  });
});

describe("releaseAssetUrl", () => {
  it("builds the GitHub release download URL", () => {
    expect(releaseAssetUrl("0.6.0", "bqls_0.6.0_darwin_arm64.tar.gz")).toBe(
      "https://github.com/kitagry/bqls/releases/download/v0.6.0/bqls_0.6.0_darwin_arm64.tar.gz",
    );
  });
});

describe("findSha256", () => {
  const checksums = [
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  bqls_0.6.0_darwin_amd64.tar.gz",
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb  bqls_0.6.0_darwin_arm64.tar.gz",
    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc  bqls_0.6.0_linux_amd64.tar.gz",
  ].join("\n");

  it("finds the sha256 for a matching file name", () => {
    expect(findSha256(checksums, "bqls_0.6.0_darwin_arm64.tar.gz")).toBe(
      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
  });

  it("returns undefined when the file name is not present", () => {
    expect(
      findSha256(checksums, "bqls_0.6.0_windows_amd64.zip"),
    ).toBeUndefined();
  });
});

describe("BQLS_VERSION", () => {
  it("is a non-empty version string", () => {
    expect(BQLS_VERSION).toMatch(/^\d+\.\d+\.\d+$/);
  });
});
