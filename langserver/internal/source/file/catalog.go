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
	catalog     *googlesql.SimpleCatalog
	typeFactory *googlesql.TypeFactory
	langOpts    *googlesql.LanguageOptions
	bqClient    bigquery.Client
	// tableMetaMap, tables and subCatalogs are keyed by lowercased names,
	// because SimpleCatalog matches names case-insensitively and registering
	// a name that differs only in case traps.
	tableMetaMap map[string]*bq.TableMetadata
	tables       map[string]*googlesql.SimpleTable // key: "project.dataset.table"
	// aliases holds lowercased "dataset.table" names already registered, because
	// registering one twice traps in SimpleCatalog, which matches names
	// case-insensitively.
	aliases map[string]aliasState
	// loadErrs keeps why a table could not be registered, because the analyzer
	// only reports "Table not found", which hides auth or network problems.
	loadErrs map[string]error // key: lowercased "project.dataset.table"
	mu       *sync.Mutex
	// subCatalogs tracks nested sub-catalogs for non-backtick path resolution.
	subCatalogs map[string]*googlesql.SimpleCatalog // key: "project", "project.dataset" or a default-project "dataset"
}

// aliasState tracks the two registrations of a "dataset.table" alias
// separately, so that a retry after a partial failure does not register the
// succeeded one again.
type aliasState struct {
	backticked bool
	unquoted   bool
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
		tables:       make(map[string]*googlesql.SimpleTable),
		aliases:      make(map[string]aliasState),
		loadErrs:     make(map[string]error),
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
		tables:       make(map[string]*googlesql.SimpleTable),
		aliases:      make(map[string]aliasState),
		loadErrs:     make(map[string]error),
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

	projectID, datasetID, tableID, defaulted, err := c.pathToProjectTable(path)
	if err != nil {
		return err
	}
	key := strings.ToLower(fmt.Sprintf("%s.%s.%s", projectID, datasetID, tableID))
	// A different casing of a loaded table resolves to the same catalog entry,
	// so it is reused even though BigQuery may hold it as a separate table.
	table, ok := c.tables[key]
	if !ok {
		table, err = c.addTable(path)
		if err != nil {
			c.loadErrs[key] = err
			return err
		}
	}
	// Checked even when the table is already loaded, because it may have been
	// loaded through its 3-part name first.
	if defaulted && projectID != "" {
		if err := c.addDatasetQualifiedAlias(table, datasetID, tableID); err != nil {
			c.loadErrs[key] = err
			return err
		}
	}
	delete(c.loadErrs, key)
	return nil
}

// TableLoadError returns why the table referenced by name, as written in the
// query, could not be registered, unless the cause is about the query itself,
// e.g. the table does not exist.
func (c *Catalog) TableLoadError(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	path := strings.Split(strings.ReplaceAll(name, "`", ""), ".")
	projectID, datasetID, tableID, _, err := c.pathToProjectTable(path)
	if err != nil {
		return nil
	}
	err = c.loadErrs[strings.ToLower(fmt.Sprintf("%s.%s.%s", projectID, datasetID, tableID))]
	if err == nil || !bigquery.IsRuntimeFailure(err) {
		return nil
	}
	return err
}

// addDatasetQualifiedAlias makes the table resolvable as "dataset.table" for
// the default project. The same table object is registered so that its name
// stays "project.dataset.table", which hover, completion and lint rely on.
func (c *Catalog) addDatasetQualifiedAlias(table *googlesql.SimpleTable, datasetID, tableID string) error {
	key := strings.ToLower(datasetID + "." + tableID)
	state := c.aliases[key]

	// Backticked `dataset.table` is looked up as a single name.
	if !state.backticked {
		if err := c.catalog.AddTable2(datasetID+"."+tableID, table); err != nil {
			return fmt.Errorf("failed to add table as %s.%s: %w", datasetID, tableID, err)
		}
		state.backticked = true
		c.aliases[key] = state
	}

	// Unquoted dataset.table → root["dataset"]["table"]. The cache key is shared
	// with a project of the same name, because the root cannot hold two
	// sub-catalogs with one name.
	if !state.unquoted {
		datasetCat, err := c.getOrCreateSubCatalog(c.catalog, datasetID)
		if err != nil {
			return fmt.Errorf("failed to create sub-catalog(%s): %w", datasetID, err)
		}
		if err := datasetCat.AddTable2(tableID, table); err != nil {
			return fmt.Errorf("failed to add table to sub-catalog(%s): %w", datasetID, err)
		}
		state.unquoted = true
		c.aliases[key] = state
	}
	return nil
}

func (c *Catalog) addTable(path []string) (*googlesql.SimpleTable, error) {
	projectID, datasetID, tableID, _, err := c.pathToProjectTable(path)
	if err != nil {
		return nil, err
	}

	metadata, err := c.bqClient.GetTableMetadata(context.Background(), projectID, datasetID, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to load table metadata: %w", err)
	}

	tableName := fmt.Sprintf("%s.%s.%s", projectID, datasetID, tableID)

	table, err := googlesql.NewSimpleTable(tableName, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	schema := metadata.Schema
	for _, field := range schema {
		typ, err := bigqueryTypeToGooglesqlType(c.typeFactory, field.Type, field.Repeated, field.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to convert type(%s): %w", field.Name, err)
		}
		if err := addColumn(table, tableName, field.Name, typ, false, true); err != nil {
			return nil, err
		}
	}

	// Partition pseudo columns exist only on ingestion-time partitioned tables;
	// column-partitioned tables filter on their partitioning column instead.
	if metadata.TimePartitioning != nil && metadata.TimePartitioning.Field == "" {
		// Pseudo so that it stays out of SELECT *, but writable because BigQuery
		// accepts INSERT INTO t (_PARTITIONTIME, ...) on ingestion-time tables.
		tsType, err := c.typeFactory.GetTimestamp()
		if err != nil {
			return nil, fmt.Errorf("failed to get TIMESTAMP type: %w", err)
		}
		if err := addColumn(table, tableName, "_PARTITIONTIME", tsType, true, true); err != nil {
			return nil, err
		}
		// A pseudo column, as in BigQuery, so that it stays out of SELECT * and
		// cannot be written.
		if HasPartitionDate(metadata) {
			dateType, err := c.typeFactory.GetDate()
			if err != nil {
				return nil, fmt.Errorf("failed to get DATE type: %w", err)
			}
			if err := addColumn(table, tableName, "_PARTITIONDATE", dateType, true, false); err != nil {
				return nil, err
			}
		}
	}

	if isWildCardTable(path) {
		strType, err := c.typeFactory.GetString()
		if err != nil {
			return nil, fmt.Errorf("failed to get STRING type: %w", err)
		}
		// A pseudo column, as in BigQuery, so that it stays out of SELECT * and
		// cannot be written.
		if err := addColumn(table, tableName, "_TABLE_SUFFIX", strType, true, false); err != nil {
			return nil, err
		}
	}

	// Registered in nested sub-catalogs for non-backtick 3-part path resolution:
	// project.dataset.table → root["project"]["dataset"]["table"]. Done before
	// the root registration so that a failure leaves nothing behind and a retry
	// hits the same error instead of a duplicate-name one.
	projectCat, err := c.getOrCreateSubCatalog(c.catalog, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create sub-catalog(%s): %w", projectID, err)
	}
	datasetKey := projectID + "." + datasetID
	datasetCat, err := c.getOrCreateSubCatalog(projectCat, datasetKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create sub-catalog(%s): %w", datasetKey, err)
	}
	if err := datasetCat.AddTable2(tableID, table); err != nil {
		return nil, fmt.Errorf("failed to add table to sub-catalog(%s): %w", datasetKey, err)
	}

	if err := c.catalog.AddTable(table); err != nil {
		return nil, fmt.Errorf("failed to add table to catalog: %w", err)
	}

	key := strings.ToLower(tableName)
	c.tableMetaMap[key] = metadata
	c.tables[key] = table
	return table, nil
}

func addColumn(table *googlesql.SimpleTable, tableName, name string, typ googlesql.Googlesql_TypeNode, isPseudoColumn, isWritableColumn bool) error {
	col, err := googlesql.NewSimpleColumn(tableName, name, typ, isPseudoColumn, isWritableColumn)
	if err != nil {
		return fmt.Errorf("failed to create column(%s): %w", name, err)
	}
	if err := table.AddColumn2(col, false); err != nil {
		return fmt.Errorf("failed to add column(%s): %w", name, err)
	}
	return nil
}

func (c *Catalog) getOrCreateSubCatalog(parent *googlesql.SimpleCatalog, key string) (*googlesql.SimpleCatalog, error) {
	if sub, ok := c.subCatalogs[strings.ToLower(key)]; ok {
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
	c.subCatalogs[strings.ToLower(key)] = sub
	return sub, nil
}

// pathToProjectTable also reports whether the project was filled in from the
// default project.
func (c *Catalog) pathToProjectTable(path []string) (projectID, datasetID, tableID string, defaulted bool, err error) {
	tableSep := strings.Split(strings.Join(path, "."), ".")
	if len(tableSep) == 3 {
		return tableSep[0], tableSep[1], tableSep[2], false, nil
	} else if len(tableSep) == 2 {
		return c.bqClient.GetDefaultProject(), tableSep[0], tableSep[1], true, nil
	}
	return "", "", "", false, fmt.Errorf(`unknown table "%s"`, strings.Join(tableSep, "."))
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

// HasPartitionDate reports whether BigQuery exposes the _PARTITIONDATE pseudo
// column, which exists only on daily ingestion-time partitioned tables.
func HasPartitionDate(metadata *bq.TableMetadata) bool {
	tp := metadata.TimePartitioning
	return tp != nil && tp.Field == "" && (tp.Type == "" || tp.Type == bq.DayPartitioningType)
}

func isWildCardTable(path []string) bool {
	if len(path) == 0 {
		return false
	}
	return strings.HasSuffix(path[len(path)-1], "*")
}

// PreloadTablesFromAST walks the AST to find all table path expressions and
// DML target tables, and pre-loads them. It returns the errors that
// TableLoadError reports, so that the caller can log them.
func (c *Catalog) PreloadTablesFromAST(node *googlesql.ASTScript) error {
	var errs []error
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		var name string
		var ok bool
		if tpe, isTPE := n.(*googlesql.ASTTablePathExpression); isTPE {
			name, ok = CreateTableNameFromTablePathExpressionNode(tpe)
		} else if pe := dmlTargetPath(n); pe != nil {
			name, ok = joinPathExpression(pe)
		}
		if !ok {
			return nil
		}
		// Split after joining so that a backticked `p.d.t` becomes three parts.
		path := strings.Split(name, ".")
		if err := c.EnsureTable(path); err != nil && c.TableLoadError(name) != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
		return nil
	})
	return errors.Join(errs...)
}

// dmlTargetPath returns the target table of a top-level DML statement, or nil.
func dmlTargetPath(n googlesql.ASTNode) *googlesql.ASTPathExpression {
	var target googlesql.ASTNode
	var err error
	switch stmt := n.(type) {
	case *googlesql.ASTInsertStatement:
		target, err = stmt.TargetPath()
	case *googlesql.ASTUpdateStatement:
		target, err = stmt.TargetPath()
	case *googlesql.ASTDeleteStatement:
		target, err = stmt.TargetPath()
	case *googlesql.ASTMergeStatement:
		target, err = stmt.TargetPath()
	default:
		return nil
	}
	if err != nil {
		return nil
	}
	// Checked after the switch because Walk visits every node.
	if isNestedDMLStatement(n) {
		return nil
	}
	pe, _ := target.(*googlesql.ASTPathExpression)
	return pe
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

	if meta, ok := c.tableMetaMap[strings.ToLower(tableName)]; ok {
		return meta, nil
	}
	return nil, errors.New("table not found in catalog")
}
