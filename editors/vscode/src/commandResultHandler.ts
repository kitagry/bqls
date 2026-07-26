export interface JobHistory {
  textDocument: { uri: string };
  id: string;
  owner: string;
  summary: string;
}

export interface ListJobHistoryResult {
  jobs: JobHistory[];
}

export interface JobHistoryQuickPickItem {
  label: string;
  description: string;
  uri: string;
}

export function isExternalUrl(url: string): boolean {
  return url.startsWith("http://") || url.startsWith("https://");
}

export function jobHistoryQuickPickItems(
  result: ListJobHistoryResult,
): JobHistoryQuickPickItem[] {
  return result.jobs.map((job) => ({
    label: job.summary,
    description: job.owner,
    uri: job.textDocument.uri,
  }));
}
