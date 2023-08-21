all: mockgen gogen

.PHONY: mockgen gogen
mockgen:
	go run github.com/golang/mock/mockgen -source=./langserver/internal/bigquery/bigquery.go -destination=./langserver/internal/bigquery/mock_bigquery/mock_bigquery.go

gogen:
	go generate ./langserver/internal/function
