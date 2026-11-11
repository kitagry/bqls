export interface PlatformTarget {
	os: "darwin" | "linux";
	arch: "amd64" | "arm64";
}

const OS_MAP: Record<string, PlatformTarget["os"]> = {
	darwin: "darwin",
	linux: "linux",
};

const ARCH_MAP: Record<string, PlatformTarget["arch"]> = {
	x64: "amd64",
	arm64: "arm64",
};

// bqls only ships goreleaser builds for darwin/linux (see .goreleaser.yaml);
// win32 and other platforms return undefined so callers fall back to manual
// install.
export function resolvePlatformTarget(
	platform: string,
	arch: string,
): PlatformTarget | undefined {
	const os = OS_MAP[platform];
	const mappedArch = ARCH_MAP[arch];
	if (!os || !mappedArch) {
		return undefined;
	}
	return { os, arch: mappedArch };
}

export function assetFileName(version: string, target: PlatformTarget): string {
	return `bqls_${version}_${target.os}_${target.arch}.tar.gz`;
}

export function checksumFileName(version: string): string {
	return `bqls_${version}_checksums.txt`;
}

export function releaseAssetUrl(version: string, fileName: string): string {
	return `https://github.com/kitagry/bqls/releases/download/v${version}/${fileName}`;
}

// goreleaser's checksums.txt uses `sha256(space)(space)filename` lines.
export function findSha256(
	checksumText: string,
	fileName: string,
): string | undefined {
	for (const line of checksumText.split("\n")) {
		const [sha, name] = line.trim().split(/\s+/);
		if (name === fileName) {
			return sha;
		}
	}
	return undefined;
}
