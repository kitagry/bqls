import { describe, expect, it } from "vitest";
import { isExternalUrl, jobHistoryQuickPickItems } from "./commandResultHandler";

describe("isExternalUrl", () => {
  it("returns true for an http url", () => {
    expect(isExternalUrl("http://example.com/sheet")).toBe(true);
  });

  it("returns true for an https url", () => {
    expect(isExternalUrl("https://docs.google.com/spreadsheets/d/abc")).toBe(
      true,
    );
  });

  it("returns false for a local file path", () => {
    expect(isExternalUrl("/Users/foo/Downloads/1.csv")).toBe(false);
  });
});

describe("jobHistoryQuickPickItems", () => {
  it("maps jobs to quick pick items keyed by job uri", () => {
    const items = jobHistoryQuickPickItems({
      jobs: [
        {
          textDocument: { uri: "bqls://project/job1" },
          id: "job1",
          owner: "alice@example.com",
          summary: "SELECT 1",
        },
        {
          textDocument: { uri: "bqls://project/job2" },
          id: "job2",
          owner: "bob@example.com",
          summary: "SELECT 2",
        },
      ],
    });

    expect(items).toEqual([
      {
        label: "SELECT 1",
        description: "alice@example.com",
        uri: "bqls://project/job1",
      },
      {
        label: "SELECT 2",
        description: "bob@example.com",
        uri: "bqls://project/job2",
      },
    ]);
  });

  it("returns an empty array when there are no jobs", () => {
    expect(jobHistoryQuickPickItems({ jobs: [] })).toEqual([]);
  });
});
