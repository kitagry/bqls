package file

import (
	"strings"

	"github.com/goccy/go-googlesql"
)

type systemVariableType func(tf *googlesql.TypeFactory) (googlesql.Googlesql_TypeNode, error)

// bigQuerySystemVariables lists the documented BigQuery system variables.
// https://cloud.google.com/bigquery/docs/reference/system-variables
//
// Dotted variables are registered per name path, as BigQuery documents them,
// rather than as one STRUCT such as @@error.
var bigQuerySystemVariables = map[string]systemVariableType{
	"current_job_id":              stringType,
	"dataset_id":                  stringType,
	"dataset_project_id":          stringType,
	"error.formatted_stack_trace": stringType,
	"error.message":               stringType,
	"error.stack_trace":           stackTraceType,
	"error.statement_text":        stringType,
	"last_job_id":                 stringType,
	"location":                    stringType,
	"project_id":                  stringType,
	"query_label":                 stringType,
	"reservation":                 stringType,
	"row_count":                   int64Type,
	"script.bytes_billed":         int64Type,
	"script.bytes_processed":      int64Type,
	"script.creation_time":        timestampType,
	"script.job_id":               stringType,
	"script.num_child_jobs":       int64Type,
	"script.slot_ms":              int64Type,
	"session_id":                  stringType,
	"time_zone":                   stringType,
}

func addBigQuerySystemVariables(opts *googlesql.AnalyzerOptions, tf *googlesql.TypeFactory) error {
	for name, typ := range bigQuerySystemVariables {
		t, err := typ(tf)
		if err != nil {
			return err
		}
		if err := opts.AddSystemVariable(strings.Split(name, "."), t); err != nil {
			return err
		}
	}
	return nil
}

func stringType(tf *googlesql.TypeFactory) (googlesql.Googlesql_TypeNode, error) {
	return tf.GetString()
}

func int64Type(tf *googlesql.TypeFactory) (googlesql.Googlesql_TypeNode, error) {
	return tf.GetInt64()
}

func timestampType(tf *googlesql.TypeFactory) (googlesql.Googlesql_TypeNode, error) {
	return tf.GetTimestamp()
}

// stackTraceType is ARRAY<STRUCT<line INT64, column INT64, filename STRING, location STRING>>.
func stackTraceType(tf *googlesql.TypeFactory) (googlesql.Googlesql_TypeNode, error) {
	int64T, err := tf.GetInt64()
	if err != nil {
		return nil, err
	}
	stringT, err := tf.GetString()
	if err != nil {
		return nil, err
	}
	frame, err := tf.MakeStructType2([]*googlesql.StructField{
		{Name: "line", Type_: int64T},
		{Name: "column", Type_: int64T},
		{Name: "filename", Type_: stringT},
		{Name: "location", Type_: stringT},
	})
	if err != nil {
		return nil, err
	}
	return tf.MakeArrayType2(frame)
}
