package file

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
)

const (
	catalogName = "bqls"
)

type Catalog struct {
	catalog      *googlesql.SimpleCatalog
	typeFactory  *googlesql.TypeFactory
	langOpts     *googlesql.LanguageOptions
	bqClient     bigquery.Client
	tableMetaMap map[string]*bq.TableMetadata
	mu           *sync.Mutex
	// subCatalogs tracks nested sub-catalogs for non-backtick 3-part path resolution.
	subCatalogs map[string]*googlesql.SimpleCatalog // key: "project" or "project.dataset"
}

func NewCatalog(bqClient bigquery.Client) *Catalog {
	tf, err := googlesql.NewTypeFactory()
	if err != nil {
		panic(fmt.Sprintf("failed to create TypeFactory: %v", err))
	}
	langOpts, err := newLanguageOptions()
	if err != nil {
		panic(fmt.Sprintf("failed to create LanguageOptions: %v", err))
	}
	cat, err := newSimpleCatalogWithBuiltins(tf, langOpts)
	if err != nil {
		panic(fmt.Sprintf("failed to create catalog: %v", err))
	}
	return &Catalog{
		catalog:      cat,
		typeFactory:  tf,
		langOpts:     langOpts,
		bqClient:     bqClient,
		tableMetaMap: make(map[string]*bq.TableMetadata),
		mu:           &sync.Mutex{},
		subCatalogs:  make(map[string]*googlesql.SimpleCatalog),
	}
}

func newSimpleCatalogWithBuiltins(tf *googlesql.TypeFactory, langOpts *googlesql.LanguageOptions) (*googlesql.SimpleCatalog, error) {
	cat, err := googlesql.NewSimpleCatalog(catalogName, tf)
	if err != nil {
		return nil, err
	}
	bfo := &googlesql.BuiltinFunctionOptions{LanguageOptions: langOpts}
	if err := cat.AddBuiltinFunctionsAndTypes(bfo); err != nil {
		return nil, err
	}
	return cat, nil
}

func (c *Catalog) Clone() *Catalog {
	cat, err := newSimpleCatalogWithBuiltins(c.typeFactory, c.langOpts)
	if err != nil {
		panic(fmt.Sprintf("failed to clone catalog: %v", err))
	}
	return &Catalog{
		catalog:      cat,
		typeFactory:  c.typeFactory,
		langOpts:     c.langOpts,
		bqClient:     c.bqClient,
		tableMetaMap: make(map[string]*bq.TableMetadata),
		mu:           &sync.Mutex{},
		subCatalogs:  make(map[string]*googlesql.SimpleCatalog),
	}
}

// CatalogNode returns the underlying SimpleCatalog which implements CatalogNode.
func (c *Catalog) CatalogNode() *googlesql.SimpleCatalog {
	return c.catalog
}

// TypeFactory returns the TypeFactory for creating types.
func (c *Catalog) TypeFactory() *googlesql.TypeFactory {
	return c.typeFactory
}

func (c *Catalog) AddFunctionWithName(name string, fn *googlesql.Function) {
	_ = c.catalog.AddFunction2(name, fn)
}

// EnsureTable loads a table from BigQuery if not already in the catalog.
func (c *Catalog) EnsureTable(path []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	projectID, datasetID, tableID, err := c.pathToProjectTable(path)
	if err != nil {
		return err
	}
	tableName := fmt.Sprintf("%s.%s.%s", projectID, datasetID, tableID)
	if _, ok := c.tableMetaMap[tableName]; ok {
		return nil // already loaded
	}
	return c.addTable(path)
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

	table, err := googlesql.NewSimpleTable(tableName, 0)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	schema := metadata.Schema
	for _, field := range schema {
		typ, err := bigqueryTypeToGooglesqlType(c.typeFactory, field.Type, field.Repeated, field.Schema)
		if err != nil {
			return fmt.Errorf("failed to convert type(%s): %w", field.Name, err)
		}
		col, err := googlesql.NewSimpleColumn(tableName, field.Name, typ, false, true)
		if err != nil {
			return fmt.Errorf("failed to create column(%s): %w", field.Name, err)
		}
		if err := table.AddColumn2(col, false); err != nil {
			return fmt.Errorf("failed to add column(%s): %w", field.Name, err)
		}
	}

	if metadata.TimePartitioning != nil {
		tsType, err := c.typeFactory.GetTimestamp()
		if err == nil {
			col, err := googlesql.NewSimpleColumn(tableName, "_PARTITIONTIME", tsType, false, true)
			if err == nil {
				_ = table.AddColumn2(col, false)
			}
		}
	}

	if isWildCardTable(path) {
		strType, err := c.typeFactory.GetString()
		if err == nil {
			col, err := googlesql.NewSimpleColumn(tableName, "_TABLE_SUFFIX", strType, false, true)
			if err == nil {
				_ = table.AddColumn2(col, false)
			}
		}
	}

	if err := c.catalog.AddTable(table); err != nil {
		return fmt.Errorf("failed to add table to catalog: %w", err)
	}

	// Also register in nested sub-catalogs for non-backtick 3-part path resolution.
	// project.dataset.table → root["project"]["dataset"]["table"]
	projectCat, err := c.getOrCreateSubCatalog(c.catalog, projectID)
	if err == nil {
		datasetKey := projectID + "." + datasetID
		datasetCat, err := c.getOrCreateSubCatalog(projectCat, datasetKey)
		if err == nil {
			_ = datasetCat.AddTable2(tableID, table)
		}
	}

	c.tableMetaMap[tableName] = metadata
	return nil
}

func (c *Catalog) getOrCreateSubCatalog(parent *googlesql.SimpleCatalog, key string) (*googlesql.SimpleCatalog, error) {
	if sub, ok := c.subCatalogs[key]; ok {
		return sub, nil
	}
	// Extract the local name (last segment of the key).
	name := key
	if idx := strings.LastIndex(key, "."); idx >= 0 {
		name = key[idx+1:]
	}
	sub, err := parent.MakeOwnedSimpleCatalog(name)
	if err != nil {
		return nil, err
	}
	c.subCatalogs[key] = sub
	return sub, nil
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

func bigqueryTypeToGooglesqlType(tf *googlesql.TypeFactory, typ bq.FieldType, isRepeated bool, schema bq.Schema) (googlesql.Googlesql_TypeNode, error) {
	result, err := literalBigqueryTypeToGooglesqlType(tf, typ, schema)
	if err != nil {
		return nil, err
	}

	if isRepeated {
		result, err = tf.MakeArrayType2(result)
		if err != nil {
			return nil, fmt.Errorf("failed to create array type: %w", err)
		}
		return result, nil
	}

	return result, nil
}

func literalBigqueryTypeToGooglesqlType(tf *googlesql.TypeFactory, typ bq.FieldType, schema bq.Schema) (googlesql.Googlesql_TypeNode, error) {
	switch typ {
	case bq.StringFieldType:
		return tf.GetString()
	case bq.IntegerFieldType:
		return tf.GetInt64()
	case bq.FloatFieldType:
		return tf.GetDouble()
	case bq.BooleanFieldType:
		return tf.GetBool()
	case bq.TimestampFieldType:
		return tf.GetTimestamp()
	case bq.DateFieldType:
		return tf.GetDate()
	case bq.TimeFieldType:
		return tf.GetTime()
	case bq.DateTimeFieldType:
		return tf.GetDatetime()
	case bq.NumericFieldType:
		return tf.GetNumeric()
	case bq.BytesFieldType:
		return tf.GetBytes()
	case bq.GeographyFieldType:
		return tf.GetGeography()
	case bq.BigNumericFieldType:
		return tf.GetBignumeric()
	case bq.RecordFieldType:
		fields := make([]*googlesql.StructField, len(schema))
		for i, field := range schema {
			fieldTyp, err := bigqueryTypeToGooglesqlType(tf, field.Type, field.Repeated, field.Schema)
			if err != nil {
				return nil, fmt.Errorf("failed to convert type(%s): %w", field.Name, err)
			}
			fields[i] = &googlesql.StructField{Name: field.Name, Type_: fieldTyp}
		}
		return tf.MakeStructType2(fields)
	case bq.IntervalFieldType:
		return tf.GetInterval()
	case bq.JSONFieldType:
		return tf.GetJson()
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

// PreloadTablesFromAST walks the AST to find all table path expressions and pre-loads them.
func (c *Catalog) PreloadTablesFromAST(node *googlesql.ASTScript) {
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		tpe, ok := n.(*googlesql.ASTTablePathExpression)
		if !ok {
			return nil
		}
		name, ok := CreateTableNameFromTablePathExpressionNode(tpe)
		if !ok {
			return nil
		}
		path := strings.Split(name, ".")
		_ = c.EnsureTable(path)
		return nil
	})
}

// FindTableMetadata returns the BigQuery metadata for a table path (e.g., "project.dataset.table").
func (c *Catalog) FindTableMetadata(path string) (*bq.TableMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	splitNode := strings.Split(path, ".")
	var tableName string
	switch len(splitNode) {
	case 3:
		tableName = path
	case 2:
		tableName = fmt.Sprintf("%s.%s.%s", c.bqClient.GetDefaultProject(), splitNode[0], splitNode[1])
	default:
		return nil, fmt.Errorf("invalid path: %s", path)
	}

	if meta, ok := c.tableMetaMap[tableName]; ok {
		return meta, nil
	}
	return nil, errors.New("table not found in catalog")
}
