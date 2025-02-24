// Code generated by MockGen. DO NOT EDIT.
// Source: ./langserver/internal/bigquery/bigquery.go

// Package mock_bigquery is a generated GoMock package.
package mock_bigquery

import (
	context "context"
	reflect "reflect"

	bigquery "cloud.google.com/go/bigquery"
	gomock "github.com/golang/mock/gomock"
	bigquery0 "github.com/kitagry/bqls/langserver/internal/bigquery"
	cloudresourcemanager "google.golang.org/api/cloudresourcemanager/v1"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockClient) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockClient)(nil).Close))
}

// GetDefaultProject mocks base method.
func (m *MockClient) GetDefaultProject() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDefaultProject")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetDefaultProject indicates an expected call of GetDefaultProject.
func (mr *MockClientMockRecorder) GetDefaultProject() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDefaultProject", reflect.TypeOf((*MockClient)(nil).GetDefaultProject))
}

// GetTable mocks base method.
func (m *MockClient) GetTable(projectID, datasetID, tableID string) *bigquery.Table {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTable", projectID, datasetID, tableID)
	ret0, _ := ret[0].(*bigquery.Table)
	return ret0
}

// GetTable indicates an expected call of GetTable.
func (mr *MockClientMockRecorder) GetTable(projectID, datasetID, tableID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTable", reflect.TypeOf((*MockClient)(nil).GetTable), projectID, datasetID, tableID)
}

// GetTableMetadata mocks base method.
func (m *MockClient) GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTableMetadata", ctx, projectID, datasetID, tableID)
	ret0, _ := ret[0].(*bigquery.TableMetadata)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTableMetadata indicates an expected call of GetTableMetadata.
func (mr *MockClientMockRecorder) GetTableMetadata(ctx, projectID, datasetID, tableID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTableMetadata", reflect.TypeOf((*MockClient)(nil).GetTableMetadata), ctx, projectID, datasetID, tableID)
}

// GetTableRecord mocks base method.
func (m *MockClient) GetTableRecord(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.RowIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTableRecord", ctx, projectID, datasetID, tableID)
	ret0, _ := ret[0].(*bigquery.RowIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTableRecord indicates an expected call of GetTableRecord.
func (mr *MockClientMockRecorder) GetTableRecord(ctx, projectID, datasetID, tableID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTableRecord", reflect.TypeOf((*MockClient)(nil).GetTableRecord), ctx, projectID, datasetID, tableID)
}

// JobFromProject mocks base method.
func (m *MockClient) JobFromProject(ctx context.Context, projectID, id string) (bigquery0.BigqueryJob, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "JobFromProject", ctx, projectID, id)
	ret0, _ := ret[0].(bigquery0.BigqueryJob)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// JobFromProject indicates an expected call of JobFromProject.
func (mr *MockClientMockRecorder) JobFromProject(ctx, projectID, id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "JobFromProject", reflect.TypeOf((*MockClient)(nil).JobFromProject), ctx, projectID, id)
}

// Jobs mocks base method.
func (m *MockClient) Jobs(ctx context.Context) *bigquery.JobIterator {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Jobs", ctx)
	ret0, _ := ret[0].(*bigquery.JobIterator)
	return ret0
}

// Jobs indicates an expected call of Jobs.
func (mr *MockClientMockRecorder) Jobs(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Jobs", reflect.TypeOf((*MockClient)(nil).Jobs), ctx)
}

// ListDatasets mocks base method.
func (m *MockClient) ListDatasets(ctx context.Context, projectID string) ([]*bigquery.Dataset, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListDatasets", ctx, projectID)
	ret0, _ := ret[0].([]*bigquery.Dataset)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListDatasets indicates an expected call of ListDatasets.
func (mr *MockClientMockRecorder) ListDatasets(ctx, projectID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListDatasets", reflect.TypeOf((*MockClient)(nil).ListDatasets), ctx, projectID)
}

// ListProjects mocks base method.
func (m *MockClient) ListProjects(ctx context.Context) ([]*cloudresourcemanager.Project, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListProjects", ctx)
	ret0, _ := ret[0].([]*cloudresourcemanager.Project)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListProjects indicates an expected call of ListProjects.
func (mr *MockClientMockRecorder) ListProjects(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListProjects", reflect.TypeOf((*MockClient)(nil).ListProjects), ctx)
}

// ListTables mocks base method.
func (m *MockClient) ListTables(ctx context.Context, projectID, datasetID string) ([]*bigquery.Table, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListTables", ctx, projectID, datasetID)
	ret0, _ := ret[0].([]*bigquery.Table)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTables indicates an expected call of ListTables.
func (mr *MockClientMockRecorder) ListTables(ctx, projectID, datasetID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTables", reflect.TypeOf((*MockClient)(nil).ListTables), ctx, projectID, datasetID)
}

// Query mocks base method.
func (m *MockClient) Query(query string) *bigquery.Query {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Query", query)
	ret0, _ := ret[0].(*bigquery.Query)
	return ret0
}

// Query indicates an expected call of Query.
func (mr *MockClientMockRecorder) Query(query interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Query", reflect.TypeOf((*MockClient)(nil).Query), query)
}

// Run mocks base method.
func (m *MockClient) Run(ctx context.Context, q string, dryrun bool) (bigquery0.BigqueryJob, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run", ctx, q, dryrun)
	ret0, _ := ret[0].(bigquery0.BigqueryJob)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Run indicates an expected call of Run.
func (mr *MockClientMockRecorder) Run(ctx, q, dryrun interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockClient)(nil).Run), ctx, q, dryrun)
}

// MockBigqueryJob is a mock of BigqueryJob interface.
type MockBigqueryJob struct {
	ctrl     *gomock.Controller
	recorder *MockBigqueryJobMockRecorder
}

// MockBigqueryJobMockRecorder is the mock recorder for MockBigqueryJob.
type MockBigqueryJobMockRecorder struct {
	mock *MockBigqueryJob
}

// NewMockBigqueryJob creates a new mock instance.
func NewMockBigqueryJob(ctrl *gomock.Controller) *MockBigqueryJob {
	mock := &MockBigqueryJob{ctrl: ctrl}
	mock.recorder = &MockBigqueryJobMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBigqueryJob) EXPECT() *MockBigqueryJobMockRecorder {
	return m.recorder
}

// Config mocks base method.
func (m *MockBigqueryJob) Config() (bigquery.JobConfig, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Config")
	ret0, _ := ret[0].(bigquery.JobConfig)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Config indicates an expected call of Config.
func (mr *MockBigqueryJobMockRecorder) Config() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Config", reflect.TypeOf((*MockBigqueryJob)(nil).Config))
}

// ID mocks base method.
func (m *MockBigqueryJob) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockBigqueryJobMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockBigqueryJob)(nil).ID))
}

// LastStatus mocks base method.
func (m *MockBigqueryJob) LastStatus() *bigquery.JobStatus {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LastStatus")
	ret0, _ := ret[0].(*bigquery.JobStatus)
	return ret0
}

// LastStatus indicates an expected call of LastStatus.
func (mr *MockBigqueryJobMockRecorder) LastStatus() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LastStatus", reflect.TypeOf((*MockBigqueryJob)(nil).LastStatus))
}

// ProjectID mocks base method.
func (m *MockBigqueryJob) ProjectID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProjectID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ProjectID indicates an expected call of ProjectID.
func (mr *MockBigqueryJobMockRecorder) ProjectID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProjectID", reflect.TypeOf((*MockBigqueryJob)(nil).ProjectID))
}

// Read mocks base method.
func (m *MockBigqueryJob) Read(arg0 context.Context) (*bigquery.RowIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", arg0)
	ret0, _ := ret[0].(*bigquery.RowIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockBigqueryJobMockRecorder) Read(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockBigqueryJob)(nil).Read), arg0)
}
