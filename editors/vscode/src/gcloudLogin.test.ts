import { describe, expect, it } from "vitest";
import {
	CLOUD_SDK_INSTALL_URL,
	handleAuthenticationRequired,
	type LoginDeps,
} from "./gcloudLogin";

const params = {
	message:
		"bqls could not authenticate to BigQuery. Run `gcloud auth login` and `gcloud auth application-default login`, then bqls picks up the new credentials.",
	error: "credentials: could not find default credentials.",
};

// fakeDeps stands in for the VSCode window, the integrated terminal, and the
// language server; each choice is what the user picks on that message.
function fakeDeps(options: {
	warningChoice?: string;
	errorChoice?: string;
	gcloudAvailable?: boolean;
	loginExitCode?: number | undefined;
}) {
	const calls = {
		warnings: [] as { message: string; items: string[] }[],
		errors: [] as { message: string; items: string[] }[],
		infos: [] as string[],
		logins: 0,
		reloads: 0,
		openedUrls: [] as string[],
	};
	const deps: LoginDeps = {
		showWarningMessage: async (message, ...items) => {
			calls.warnings.push({ message, items });
			return options.warningChoice;
		},
		showErrorMessage: async (message, ...items) => {
			calls.errors.push({ message, items });
			return options.errorChoice;
		},
		showInformationMessage: async (message) => {
			calls.infos.push(message);
			return undefined;
		},
		isGcloudAvailable: async () => options.gcloudAvailable ?? true,
		runLogin: async () => {
			calls.logins++;
			return options.loginExitCode;
		},
		reloadCredentials: async () => {
			calls.reloads++;
		},
		openExternal: async (url) => {
			calls.openedUrls.push(url);
		},
	};
	return { deps, calls };
}

describe("handleAuthenticationRequired", () => {
	it("shows the guidance with a Login button", async () => {
		const { deps, calls } = fakeDeps({});

		await handleAuthenticationRequired(params, deps);

		expect(calls.warnings).toEqual([
			{
				message: `bqls: ${params.message} (${params.error})`,
				items: ["Login"],
			},
		]);
		expect(calls.logins).toBe(0);
	});

	it("logs in and reloads the credentials when Login is chosen", async () => {
		const { deps, calls } = fakeDeps({
			warningChoice: "Login",
			loginExitCode: 0,
		});

		await handleAuthenticationRequired(params, deps);

		expect(calls.logins).toBe(1);
		expect(calls.reloads).toBe(1);
		expect(calls.infos).toHaveLength(1);
	});

	it("does not reload the credentials when the login fails", async () => {
		const { deps, calls } = fakeDeps({
			warningChoice: "Login",
			loginExitCode: 1,
		});

		await handleAuthenticationRequired(params, deps);

		expect(calls.reloads).toBe(0);
		expect(calls.errors).toHaveLength(1);
		expect(calls.errors[0].message).toContain("exit code 1");
	});

	it("does not reload the credentials when the login is aborted", async () => {
		const { deps, calls } = fakeDeps({
			warningChoice: "Login",
			loginExitCode: undefined,
		});

		await handleAuthenticationRequired(params, deps);

		expect(calls.reloads).toBe(0);
		expect(calls.errors).toHaveLength(1);
	});

	it("offers to install the Cloud SDK when gcloud is not found", async () => {
		const { deps, calls } = fakeDeps({
			warningChoice: "Login",
			gcloudAvailable: false,
			errorChoice: "Install Google Cloud SDK",
		});

		await handleAuthenticationRequired(params, deps);

		expect(calls.logins).toBe(0);
		expect(calls.errors).toEqual([
			{
				message: expect.stringContaining("gcloud"),
				items: ["Install Google Cloud SDK"],
			},
		]);
		expect(calls.openedUrls).toEqual([CLOUD_SDK_INSTALL_URL]);
	});
});
