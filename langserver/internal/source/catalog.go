package source

import (
	"context"
	"errors"
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
)

const (
	catalogName = "bqls"
)

type Catalog struct {
	catalog  *types.SimpleCatalog
	bqClient bigquery.Client
}

func NewCatalog(bqClient bigquery.Client) types.Catalog {
	return &Catalog{
		catalog:  types.NewSimpleCatalog(catalogName),
		bqClient: bqClient,
	}
}

func (c *Catalog) FullName() string {
	return c.catalog.FullName()
}

func (c *Catalog) FindTable(path []string) (types.Table, error) {
	table, err := c.catalog.FindTable(path)
	if err == nil {
		return table, nil
	}

	addErr := c.addTable(path)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to find table: %w", err), fmt.Errorf("failed to add table: %w", addErr))
	}
	return c.catalog.FindTable(path)
}

func (c *Catalog) addTable(path []string) error {
	tableSep := strings.Split(strings.Join(path, "."), ".")
	var schema bq.Schema
	var err error
	if len(tableSep) == 3 {
		schema, err = c.bqClient.GetSchema(context.Background(), tableSep[0], tableSep[1], tableSep[2])
	} else if len(tableSep) == 2 {
		schema, err = c.bqClient.GetSchema(context.Background(), c.bqClient.GetDefaultProject(), tableSep[0], tableSep[1])
	} else {
		return fmt.Errorf("unknown table: %s", strings.Join(path, "."))
	}
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	tableName := strings.Join(path, ".")

	columns := make([]types.Column, len(schema))
	for i, field := range schema {
		typ, err := bigqueryTypeToZetaSQLType(field.Type, field.Repeated, field.Schema)
		if err != nil {
			return fmt.Errorf("failed to convert type(%s): %w", field.Name, err)
		}
		columns[i] = types.NewSimpleColumn(tableName, field.Name, typ)
	}

	table := types.NewSimpleTable(tableName, columns)
	c.catalog.AddTable(table)
	return nil
}

func bigqueryTypeToZetaSQLType(typ bq.FieldType, isRepeated bool, schema bq.Schema) (types.Type, error) {
	result, err := literalBigqueryTypeToZetaSQLType(typ, schema)
	if err != nil {
		return nil, err
	}

	if isRepeated {
		return types.ArrayTypeFromKind(result.Kind()), nil
	}

	return result, nil
}

func literalBigqueryTypeToZetaSQLType(typ bq.FieldType, schema bq.Schema) (types.Type, error) {
	switch typ {
	case bq.StringFieldType:
		return types.StringType(), nil
	case bq.IntegerFieldType:
		return types.Int64Type(), nil
	case bq.FloatFieldType:
		return types.FloatType(), nil
	case bq.BooleanFieldType:
		return types.BoolType(), nil
	case bq.TimestampFieldType:
		return types.TimestampType(), nil
	case bq.DateFieldType:
		return types.DateType(), nil
	case bq.TimeFieldType:
		return types.TimeType(), nil
	case bq.DateTimeFieldType:
		return types.DatetimeType(), nil
	case bq.NumericFieldType:
		return types.NumericType(), nil
	case bq.BytesFieldType:
		return types.BytesType(), nil
	case bq.GeographyFieldType:
		return types.GeographyType(), nil
	case bq.BigNumericFieldType:
		return types.BigNumericType(), nil
	case bq.RecordFieldType:
		fields := make([]*types.StructField, len(schema))
		for _, field := range schema {
			typ, err := bigqueryTypeToZetaSQLType(field.Type, field.Repeated, field.Schema)
			if err != nil {
				return nil, fmt.Errorf("failed to convert type(%s): %w", field.Name, err)
			}
			fields = append(fields, types.NewStructField(field.Name, typ))
		}
		st, err := types.NewStructType(fields)
		if err != nil {
			return nil, fmt.Errorf("failed to create StructType: %w", err)
		}
		return st, nil
	case bq.IntervalFieldType:
		return types.IntervalType(), nil
	case bq.JSONFieldType:
		return types.JsonType(), nil
	default:
		return nil, fmt.Errorf("unsupported type: %v", typ)
	}
}

func (c *Catalog) FindModel(path []string) (types.Model, error) { return c.catalog.FindModel(path) }

func (c *Catalog) FindConnection(path []string) (types.Connection, error) {
	return c.catalog.FindConnection(path)
}

func (c *Catalog) FindFunction(path []string) (*types.Function, error) {
	return c.catalog.FindFunction(path)
}

func (c *Catalog) FindTableValuedFunction(path []string) (types.TableValuedFunction, error) {
	return c.catalog.FindTableValuedFunction(path)
}

func (c *Catalog) FindProcedure(path []string) (*types.Procedure, error) {
	return c.catalog.FindProcedure(path)
}
func (c *Catalog) FindType(path []string) (types.Type, error) { return c.catalog.FindType(path) }

func (c *Catalog) FindConstant(path []string) (types.Constant, int, error) {
	return c.catalog.FindConstant(path)
}

func (c *Catalog) FindConversion(from, to types.Type) (types.Conversion, error) {
	return c.catalog.FindConversion(from, to)
}

func (c *Catalog) ExtendedTypeSuperTypes(typ types.Type) (*types.TypeListView, error) {
	return c.catalog.ExtendedTypeSuperTypes(typ)
}

func (c *Catalog) SuggestTable(mistypedPath []string) string {
	return c.catalog.SuggestTable(mistypedPath)
}

func (c *Catalog) SuggestModel(mistypedPath []string) string {
	return c.catalog.SuggestModel(mistypedPath)
}

func (c *Catalog) SuggestFunction(mistypedPath []string) string {
	return c.catalog.SuggestFunction(mistypedPath)
}

func (c *Catalog) SuggestTableValuedFunction(mistypedPath []string) string {
	return c.catalog.SuggestTableValuedFunction(mistypedPath)
}

func (c *Catalog) SuggestConstant(mistypedPath []string) string {
	return c.catalog.SuggestConstant(mistypedPath)
}
