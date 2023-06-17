// Code generated by MockGen. DO NOT EDIT.
// Source: ./langserver/internal/bigquery/bigquery.go

// Package mock_bigquery is a generated GoMock package.
package mock_bigquery

import (
	context "context"
	reflect "reflect"

	bigquery "cloud.google.com/go/bigquery"
	gomock "github.com/golang/mock/gomock"
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

// ListDatasets mocks base method.
func (m *MockClient) ListDatasets(ctx context.Context, projectID string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListDatasets", ctx, projectID)
	ret0, _ := ret[0].([]string)
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
func (m *MockClient) ListTables(ctx context.Context, projectID, datasetID string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListTables", ctx, projectID, datasetID)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTables indicates an expected call of ListTables.
func (mr *MockClientMockRecorder) ListTables(ctx, projectID, datasetID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTables", reflect.TypeOf((*MockClient)(nil).ListTables), ctx, projectID, datasetID)
}
