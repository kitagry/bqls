package source_test

import (
	"context"
	"slices"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
)

var bytesThresholdConfig = lint.Config{
	Rules: map[string]bool{"bytes-processed-threshold": true},
}

// dryRunJob is a dry-run job estimating more than the default 1 TiB threshold.
func dryRunJob(ctrl *gomock.Controller) bigquery.BigqueryJob {
	job := mock_bigquery.NewMockBigqueryJob(ctrl)
	job.EXPECT().LastStatus().Return(&bq.JobStatus{
		State:      bq.Done,
		Statistics: &bq.JobStatistics{TotalBytesProcessed: 2 << 40},
	}).AnyTimes()
	return job
}

func lintRules(d source.Diagnostics) []string {
	rules := make([]string, len(d.Lint))
	for i, l := range d.Lint {
		rules[i] = l.Rule
	}
	return rules
}

func TestProject_Diagnose_UsesDryRunResultOfTheCurrentText(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().Run(gomock.Any(), "SELECT 1", true).Return(dryRunJob(ctrl), nil)
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	if err := p.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}

	if _, err := p.Dryrun(t.Context(), uri); err != nil {
		t.Fatalf("Dryrun() error = %v", err)
	}
	got, _ := p.Diagnose(t.Context(), uri, bytesThresholdConfig)

	if rules := lintRules(got); !slices.Equal(rules, []string{"bytes-processed-threshold"}) {
		t.Errorf("Diagnose() lint rules = %v, want [bytes-processed-threshold] from the dry-run of the current text", rules)
	}
}

func TestProject_Diagnose_IgnoresDryRunResultAfterTextChanges(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().Run(gomock.Any(), "SELECT 1", true).Return(dryRunJob(ctrl), nil)
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	if err := p.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Dryrun(t.Context(), uri); err != nil {
		t.Fatalf("Dryrun() error = %v", err)
	}

	if err := p.UpdateFile(uri, "SELECT 2", 2); err != nil {
		t.Fatal(err)
	}
	got, _ := p.Diagnose(t.Context(), uri, bytesThresholdConfig)

	if len(got.Lint) != 0 {
		t.Errorf("Diagnose() lint = %+v, want none since the dry-run ran on older text", got.Lint)
	}
}

func TestProject_Diagnose_IgnoresDryRunResultOfTextEditedDuringDryRun(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	bqClient.EXPECT().Run(gomock.Any(), "SELECT 1", true).
		DoAndReturn(func(context.Context, string, bool) (bigquery.BigqueryJob, error) {
			if err := p.UpdateFile(uri, "SELECT 2", 2); err != nil {
				t.Fatal(err)
			}
			return dryRunJob(ctrl), nil
		})
	if err := p.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}

	if _, err := p.Dryrun(t.Context(), uri); err != nil {
		t.Fatalf("Dryrun() error = %v", err)
	}
	got, _ := p.Diagnose(t.Context(), uri, bytesThresholdConfig)

	if len(got.Lint) != 0 {
		t.Errorf("Diagnose() lint = %+v, want none since the text changed while the dry-run ran", got.Lint)
	}
}

func TestProject_Diagnose_RecomputesAfterNewDryRunResult(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().Run(gomock.Any(), "SELECT 1", true).Return(dryRunJob(ctrl), nil)
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	if err := p.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}
	if before, _ := p.Diagnose(t.Context(), uri, bytesThresholdConfig); len(before.Lint) != 0 {
		t.Fatalf("Diagnose() before the dry-run lint = %+v, want none", before.Lint)
	}

	if _, err := p.Dryrun(t.Context(), uri); err != nil {
		t.Fatalf("Dryrun() error = %v", err)
	}
	got, _ := p.Diagnose(t.Context(), uri, bytesThresholdConfig)

	if rules := lintRules(got); !slices.Equal(rules, []string{"bytes-processed-threshold"}) {
		t.Errorf("Diagnose() lint rules = %v, want [bytes-processed-threshold] once the dry-run finished", rules)
	}
}

func TestProject_Diagnose_ForgetsDryRunResultWhenFileIsClosed(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().Run(gomock.Any(), "SELECT 1", true).Return(dryRunJob(ctrl), nil)
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	if err := p.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Dryrun(t.Context(), uri); err != nil {
		t.Fatalf("Dryrun() error = %v", err)
	}

	p.DeleteFile(uri)
	if err := p.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}
	got, _ := p.Diagnose(t.Context(), uri, bytesThresholdConfig)

	if len(got.Lint) != 0 {
		t.Errorf("Diagnose() lint = %+v, want none since the dry-run result belonged to the closed file", got.Lint)
	}
}
