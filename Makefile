all: mockgen gogen

.PHONY: mockgen gogen
mockgen:
	go tool mockgen -source=./langserver/internal/bigquery/bigquery.go -destination=./langserver/internal/bigquery/mock_bigquery/mock_bigquery.go

gogen:
	go generate ./langserver/internal/function
