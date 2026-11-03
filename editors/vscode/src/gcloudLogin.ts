// Sent by bqls when BigQuery rejects the credentials, to clients that declare
// supports_authentication_required_notification.
export const NOTIFICATION_AUTHENTICATION_REQUIRED =
	"bqls/authenticationRequired";
export const COMMAND_RELOAD_CREDENTIALS = "bqls.reloadCredentials";
export const CLOUD_SDK_INSTALL_URL =
	"https://cloud.google.com/sdk/docs/install";

export interface AuthenticationRequiredParams {
	message: string;
	error: string;
}

// LoginDeps abstracts the VSCode window, the integrated terminal, and the
// language server so the flow can be tested without them.
export interface LoginDeps {
	showWarningMessage(
		message: string,
		...items: string[]
	): Promise<string | undefined>;
	showErrorMessage(
		message: string,
		...items: string[]
	): Promise<string | undefined>;
	showInformationMessage(message: string): Promise<string | undefined>;
	isGcloudAvailable(): Promise<boolean>;
	// Runs `gcloud auth application-default login` and resolves with its exit
	// code, or undefined if it was terminated without one.
	runLogin(): Promise<number | undefined>;
	reloadCredentials(): Promise<void>;
	openExternal(url: string): Promise<void>;
}

const LOGIN = "Login";
const INSTALL_CLOUD_SDK = "Install Google Cloud SDK";

export async function handleAuthenticationRequired(
	params: AuthenticationRequiredParams,
	deps: LoginDeps,
): Promise<void> {
	const choice = await deps.showWarningMessage(
		`bqls: ${params.message} (${params.error})`,
		LOGIN,
	);
	if (choice !== LOGIN) {
		return;
	}

	if (!(await deps.isGcloudAvailable())) {
		const install = await deps.showErrorMessage(
			"bqls: gcloud was not found. Install the Google Cloud SDK, then run `gcloud auth application-default login`.",
			INSTALL_CLOUD_SDK,
		);
		if (install === INSTALL_CLOUD_SDK) {
			await deps.openExternal(CLOUD_SDK_INSTALL_URL);
		}
		return;
	}

	const exitCode = await deps.runLogin();
	if (exitCode !== 0) {
		const reason =
			exitCode === undefined
				? "was aborted"
				: `failed with exit code ${exitCode}`;
		await deps.showErrorMessage(
			`bqls: gcloud auth application-default login ${reason}.`,
		);
		return;
	}
	await deps.reloadCredentials();
	await deps.showInformationMessage(
		"bqls: Logged in. BigQuery credentials have been reloaded.",
	);
}
