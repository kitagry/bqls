package file

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	bq "cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
)

const (
	catalogName = "bqls"
)

type Catalog struct {
	catalog      *types.SimpleCatalog
	bqClient     bigquery.Client
	tableMetaMap map[string]*bq.TableMetadata
	mu           *sync.Mutex
}

var _ types.Catalog = (*Catalog)(nil)

func NewCatalog(bqClient bigquery.Client) *Catalog {
	catalog := types.NewSimpleCatalog(catalogName)
	catalog.AddZetaSQLBuiltinFunctions(nil)
	return &Catalog{
		catalog:      catalog,
		bqClient:     bqClient,
		tableMetaMap: make(map[string]*bq.TableMetadata),
		mu:           &sync.Mutex{},
	}
}

func (c *Catalog) Clone() *Catalog {
	catalog := types.NewSimpleCatalog(catalogName)
	catalog.AddZetaSQLBuiltinFunctions(nil)
	return &Catalog{
		catalog:      catalog,
		bqClient:     c.bqClient,
		tableMetaMap: make(map[string]*bq.TableMetadata),
		mu:           &sync.Mutex{},
	}
}

func (c *Catalog) FullName() string {
	return c.catalog.FullName()
}

func (c *Catalog) FindTable(path []string) (types.Table, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	projectID, datasetID, tableID, err := c.pathToProjectTable(path)
	if err != nil {
		return nil, err
	}

	formattedPath := []string{fmt.Sprintf("%s.%s.%s", projectID, datasetID, tableID)}

	table, err := c.catalog.FindTable(formattedPath)
	if err == nil {
		return table, nil
	}
	errs := []error{fmt.Errorf("failed to find table: %w", err)}

	err = c.addTable(path)
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to add table: %w", err))
		return nil, errors.Join(errs...)
	}
	return c.catalog.FindTable(formattedPath)
}

func (c *Catalog) addTable(path []string) error {
	projectID, datasetID, tableID, err := c.pathToProjectTable(path)
	if err != nil {
		return err
	}

	metadata, err := c.bqClient.GetTableMetadata(context.Background(), projectID, datasetID, tableID)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	tableName := fmt.Sprintf("%s.%s.%s", projectID, datasetID, tableID)

	schema := metadata.Schema
	columns := make([]types.Column, len(schema))
	for i, field := range schema {
		typ, err := bigqueryTypeToZetaSQLType(field.Type, field.Repeated, field.Schema)
		if err != nil {
			return fmt.Errorf("failed to convert type(%s): %w", field.Name, err)
		}
		columns[i] = types.NewSimpleColumn(tableName, field.Name, typ)
	}

	if metadata.TimePartitioning != nil {
		columns = append(columns, types.NewSimpleColumn(tableName, "_PARTITIONTIME", types.TimestampType()))
	}

	if isWildCardTable(path) {
		columns = append(columns, types.NewSimpleColumn(tableName, "_TABLE_SUFFIX", types.StringType()))
	}

	table := types.NewSimpleTable(tableName, columns)
	c.catalog.AddTable(table)
	c.tableMetaMap[tableName] = metadata
	return nil
}

func (c *Catalog) pathToProjectTable(path []string) (projectID, datasetID, tableID string, err error) {
	tableSep := strings.Split(strings.Join(path, "."), ".")
	if len(tableSep) == 3 {
		return tableSep[0], tableSep[1], tableSep[2], nil
	} else if len(tableSep) == 2 {
		return c.bqClient.GetDefaultProject(), tableSep[0], tableSep[1], nil
	}
	return "", "", "", fmt.Errorf(`unknown table "%s"`, strings.Join(tableSep, "."))
}

func bigqueryTypeToZetaSQLType(typ bq.FieldType, isRepeated bool, schema bq.Schema) (types.Type, error) {
	result, err := literalBigqueryTypeToZetaSQLType(typ, schema)
	if err != nil {
		return nil, err
	}

	if isRepeated {
		result, err = types.NewArrayType(result)
		if err != nil {
			return nil, fmt.Errorf("Failed to create array type: %w", err)
		}
		return result, nil
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
		for i, field := range schema {
			typ, err := bigqueryTypeToZetaSQLType(field.Type, field.Repeated, field.Schema)
			if err != nil {
				return nil, fmt.Errorf("failed to convert type(%s): %w", field.Name, err)
			}
			fields[i] = types.NewStructField(field.Name, typ)
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

func isWildCardTable(path []string) bool {
	if len(path) == 0 {
		return false
	}

	return strings.HasSuffix(path[len(path)-1], "*")
}

func (c *Catalog) FindModel(path []string) (types.Model, error) { return c.catalog.FindModel(path) }

func (c *Catalog) FindConnection(path []string) (types.Connection, error) {
	return c.catalog.FindConnection(path)
}

func (c *Catalog) AddFunctionWithName(name string, fn *types.Function) {
	c.catalog.AddFunctionWithName(name, fn)
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
