import { describe, expect, it, vi } from "vitest";
import { InstallerDeps, resolveBqlsCommand } from "./bqlsInstaller";
import { BQLS_VERSION } from "./bqlsRelease";

function makeDeps(overrides: Partial<InstallerDeps> = {}): InstallerDeps {
	return {
		fileExists: vi.fn().mockResolvedValue(false),
		mkdir: vi.fn().mockResolvedValue(undefined),
		downloadText: vi.fn().mockResolvedValue(""),
		downloadFile: vi.fn().mockResolvedValue(undefined),
		extractTarGz: vi.fn().mockResolvedValue(undefined),
		chmodExecutable: vi.fn().mockResolvedValue(undefined),
		sha256File: vi.fn().mockResolvedValue(""),
		checkOnPath: vi.fn().mockResolvedValue(false),
		...overrides,
	};
}

const STORAGE_DIR = "/storage";

describe("resolveBqlsCommand", () => {
	it("trusts an explicitly configured path without touching deps", async () => {
		const deps = makeDeps();

		const result = await resolveBqlsCommand(
			"/custom/bqls",
			STORAGE_DIR,
			"darwin",
			"arm64",
			deps,
		);

		expect(result).toEqual({ command: "/custom/bqls" });
		expect(deps.checkOnPath).not.toHaveBeenCalled();
	});

	it("treats an empty configured path as unset and resolves automatically", async () => {
		const deps = makeDeps({ checkOnPath: vi.fn().mockResolvedValue(true) });

		const result = await resolveBqlsCommand(
			"",
			STORAGE_DIR,
			"darwin",
			"arm64",
			deps,
		);

		expect(result).toEqual({ command: "bqls" });
		expect(deps.checkOnPath).toHaveBeenCalledWith("bqls");
	});

	it("uses bqls from PATH when already installed", async () => {
		const deps = makeDeps({ checkOnPath: vi.fn().mockResolvedValue(true) });

		const result = await resolveBqlsCommand(
			"bqls",
			STORAGE_DIR,
			"darwin",
			"arm64",
			deps,
		);

		expect(result).toEqual({ command: "bqls" });
		expect(deps.downloadFile).not.toHaveBeenCalled();
	});

	it("falls back with an error message on unsupported platforms", async () => {
		const deps = makeDeps();

		const result = await resolveBqlsCommand(
			"bqls",
			STORAGE_DIR,
			"win32",
			"x64",
			deps,
		);

		expect(result.command).toBe("bqls");
		expect(result.error).toMatch(/windows/i);
		expect(deps.downloadFile).not.toHaveBeenCalled();
	});

	it("reuses an already-cached binary without downloading again", async () => {
		const deps = makeDeps({ fileExists: vi.fn().mockResolvedValue(true) });

		const result = await resolveBqlsCommand(
			"bqls",
			STORAGE_DIR,
			"darwin",
			"arm64",
			deps,
		);

		expect(result).toEqual({
			command: `${STORAGE_DIR}/bqls-${BQLS_VERSION}/bqls`,
		});
		expect(deps.downloadFile).not.toHaveBeenCalled();
	});

	it("downloads, verifies checksum, and extracts when nothing is cached", async () => {
		const sha = "a".repeat(64);
		const deps = makeDeps({
			downloadText: vi
				.fn()
				.mockResolvedValue(`${sha}  bqls_${BQLS_VERSION}_darwin_arm64.tar.gz`),
			sha256File: vi.fn().mockResolvedValue(sha),
		});

		const result = await resolveBqlsCommand(
			"bqls",
			STORAGE_DIR,
			"darwin",
			"arm64",
			deps,
		);

		expect(deps.mkdir).toHaveBeenCalled();
		expect(deps.downloadFile).toHaveBeenCalledWith(
			`https://github.com/kitagry/bqls/releases/download/v${BQLS_VERSION}/bqls_${BQLS_VERSION}_darwin_arm64.tar.gz`,
			expect.any(String),
		);
		expect(deps.extractTarGz).toHaveBeenCalled();
		expect(deps.chmodExecutable).toHaveBeenCalledWith(
			`${STORAGE_DIR}/bqls-${BQLS_VERSION}/bqls`,
		);
		expect(result).toEqual({
			command: `${STORAGE_DIR}/bqls-${BQLS_VERSION}/bqls`,
		});
	});

	it("falls back with an error when the checksum does not match", async () => {
		const deps = makeDeps({
			downloadText: vi
				.fn()
				.mockResolvedValue(
					`${"a".repeat(64)}  bqls_${BQLS_VERSION}_darwin_arm64.tar.gz`,
				),
			sha256File: vi.fn().mockResolvedValue("b".repeat(64)),
		});

		const result = await resolveBqlsCommand(
			"bqls",
			STORAGE_DIR,
			"darwin",
			"arm64",
			deps,
		);

		expect(result.command).toBe("bqls");
		expect(result.error).toMatch(/checksum/i);
		expect(deps.extractTarGz).not.toHaveBeenCalled();
	});

	it("falls back with an error when the download fails", async () => {
		const deps = makeDeps({
			downloadText: vi
				.fn()
				.mockResolvedValue(
					`${"a".repeat(64)}  bqls_${BQLS_VERSION}_darwin_arm64.tar.gz`,
				),
			downloadFile: vi.fn().mockRejectedValue(new Error("network error")),
		});

		const result = await resolveBqlsCommand(
			"bqls",
			STORAGE_DIR,
			"darwin",
			"arm64",
			deps,
		);

		expect(result.command).toBe("bqls");
		expect(result.error).toMatch(/network error/);
	});
});
