package file

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	names        *tableNames
	// loadErrs keeps why a table could not be registered, because the analyzer
	// only reports "Table not found", which hides auth or network problems.
	loadErrs map[string]error // key: lowercased "project.dataset.table"
	mu       *sync.Mutex
	// scriptTables holds the tables created by the statements analyzed so far,
	// in creation order. They are registered in their own catalog layered over
	// the base catalog, rebuilt on every creation, because CREATE OR REPLACE
	// replaces a table and SimpleCatalog cannot remove one.
	scriptTables []scriptTable
	// scriptNames keeps the layer's catalogs referenced from Go for as long as
	// scriptNode uses them.
	scriptNames *tableNames
	scriptNode  googlesql.CatalogNode
}

type scriptTable struct {
	key   string
	paths [][]string
	table *googlesql.SimpleTable
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
		names:        newTableNames(cat),
		loadErrs:     make(map[string]error),
		mu:           &sync.Mutex{},
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
		names:        newTableNames(cat),
		loadErrs:     make(map[string]error),
		mu:           &sync.Mutex{},
	}
}

// CatalogNode returns the underlying SimpleCatalog which implements CatalogNode.
func (c *Catalog) CatalogNode() *googlesql.SimpleCatalog {
	return c.catalog
}

// node returns the catalog that statements are analyzed against: the tables
// created by the script so far over the base catalog.
func (c *Catalog) node() googlesql.CatalogNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.scriptNode == nil {
		return c.catalog
	}
	return c.scriptNode
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

	ref, err := c.parseTablePath(path)
	if err != nil {
		return err
	}
	key := ref.key()
	// A different casing of a loaded table resolves to the same catalog entry,
	// so it is reused even though BigQuery may hold it as a separate table.
	table, ok := c.tables[key]
	if !ok {
		table, err = c.addTable(ref, isWildCardTable(path))
		if err != nil {
			c.loadErrs[key] = err
			return err
		}
	}
	// Registered even when the table is already loaded, because it may have
	// been loaded through another name, e.g. its 3-part name.
	if err := c.names.registerAllSplits(ref.path(), table); err != nil {
		c.loadErrs[key] = err
		return err
	}
	if ref.defaulted && ref.project != "" {
		if err := c.names.registerAllSplits(ref.rest, table); err != nil {
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

	ref, err := c.parseTablePath([]string{strings.ReplaceAll(name, "`", "")})
	if err != nil {
		return nil
	}
	err = c.loadErrs[ref.key()]
	if err == nil || !bigquery.IsRuntimeFailure(err) {
		return nil
	}
	return err
}

// addScriptTable makes a table created by a statement of the script
// resolvable by the later statements, replacing a table of the same name.
func (c *Catalog) addScriptTable(namePath []string, columnNames []string, columnTypes []googlesql.Googlesql_TypeNode) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	paths := c.scriptTablePaths(namePath)
	tableName := strings.Join(paths[0], ".")
	table, err := googlesql.NewSimpleTable(tableName, 0)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	for i, name := range columnNames {
		if err := addColumn(table, tableName, name, columnTypes[i], false, true); err != nil {
			return err
		}
	}

	key := strings.ToLower(tableName)
	tables := slices.DeleteFunc(slices.Clone(c.scriptTables), func(t scriptTable) bool { return t.key == key })
	tables = append(tables, scriptTable{key: key, paths: paths, table: table})

	root, err := googlesql.NewSimpleCatalog("", c.typeFactory)
	if err != nil {
		return err
	}
	names := newTableNames(root)
	for _, t := range tables {
		for _, path := range t.paths {
			if err := names.registerAllSplits(path, t.table); err != nil {
				return err
			}
		}
	}
	node, err := googlesql.NewMultiCatalogCreate("", []googlesql.CatalogNode{root, c.catalog})
	if err != nil {
		return err
	}
	c.scriptTables, c.scriptNames, c.scriptNode = tables, names, node
	return nil
}

// hasTable reports whether the table is loaded from BigQuery or created by the
// script.
func (c *Catalog) hasTable(namePath []string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := strings.ToLower(strings.Join(c.scriptTablePaths(namePath)[0], "."))
	if _, ok := c.tables[key]; ok {
		return true
	}
	return slices.ContainsFunc(c.scriptTables, func(t scriptTable) bool { return t.key == key })
}

// scriptTablePaths returns the paths a created table resolves by, the full
// path first. A temporary table is named by a single identifier.
func (c *Catalog) scriptTablePaths(namePath []string) [][]string {
	ref, err := c.parseTablePath(namePath)
	if err != nil || ref.informationSchema {
		return [][]string{namePath}
	}
	paths := [][]string{ref.path()}
	// Also checked when the project is written, because a table created as
	// project.dataset.table is then referred to as dataset.table.
	if ref.project != "" && ref.project == c.bqClient.GetDefaultProject() {
		paths = append(paths, ref.rest)
	}
	return paths
}

// tableNames registers tables in a catalog under their paths.
type tableNames struct {
	root *googlesql.SimpleCatalog
	// registered holds the identifier splits a table is already registered
	// under, because registering one twice traps in SimpleCatalog, which
	// matches names case-insensitively.
	registered map[string]bool // key: splitKey
	// subCatalogs tracks nested sub-catalogs for non-backtick path resolution.
	subCatalogs map[string]*googlesql.SimpleCatalog // key: splitKey of the catalog path
}

func newTableNames(root *googlesql.SimpleCatalog) *tableNames {
	return &tableNames{
		root:        root,
		registered:  make(map[string]bool),
		subCatalogs: make(map[string]*googlesql.SimpleCatalog),
	}
}

// registerAllSplits makes the table resolvable however its path is quoted,
// e.g. p.d.t, `p.d.t`, `p.d`.t and p.`d.t`, because the analyzer looks a
// table up by the identifiers as written. The same table object is registered
// so that its name stays the full path, which hover, completion and lint rely
// on.
func (n *tableNames) registerAllSplits(path []string, table *googlesql.SimpleTable) error {
	for mask := 0; mask < 1<<(len(path)-1); mask++ {
		// Bit i joins path[i] and path[i+1] into one identifier.
		groups := []string{path[0]}
		for i, part := range path[1:] {
			if mask&(1<<i) != 0 {
				groups[len(groups)-1] += "." + part
			} else {
				groups = append(groups, part)
			}
		}
		key := splitKey(groups)
		if n.registered[key] {
			continue
		}
		parent := n.root
		for i, name := range groups[:len(groups)-1] {
			sub, err := n.getOrCreateSubCatalog(parent, groups[:i+1], name)
			if err != nil {
				return fmt.Errorf("failed to create sub-catalog(%s): %w", name, err)
			}
			parent = sub
		}
		name := groups[len(groups)-1]
		if err := parent.AddTable2(name, table); err != nil {
			return fmt.Errorf("failed to add table as %s: %w", name, err)
		}
		n.registered[key] = true
	}
	return nil
}

// splitKey joins identifiers with NUL, which no identifier contains, so that
// `a.b`.c and a.`b.c` get different keys.
func splitKey(groups []string) string {
	return strings.ToLower(strings.Join(groups, "\x00"))
}

func (c *Catalog) addTable(ref tableRef, isWildCard bool) (*googlesql.SimpleTable, error) {
	metadata, err := fetchTableMetadata(context.Background(), c.bqClient, ref)
	if err != nil {
		return nil, fmt.Errorf("failed to load table metadata: %w", err)
	}

	tableName := strings.Join(ref.path(), ".")

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

	if isWildCard {
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

	key := ref.key()
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

// getOrCreateSubCatalog returns the sub-catalog named name under parent,
// which is reached by catalogPath. A project and a default-project dataset of
// the same name share one, because the root cannot hold two sub-catalogs with
// one name.
func (n *tableNames) getOrCreateSubCatalog(parent *googlesql.SimpleCatalog, catalogPath []string, name string) (*googlesql.SimpleCatalog, error) {
	key := splitKey(catalogPath)
	if sub, ok := n.subCatalogs[key]; ok {
		return sub, nil
	}
	sub, err := parent.MakeOwnedSimpleCatalog(name)
	if err != nil {
		return nil, err
	}
	n.subCatalogs[key] = sub
	return sub, nil
}

// tableRef is a table path with the project filled in.
type tableRef struct {
	project string
	// rest is the path after the project: dataset and table, or for
	// INFORMATION_SCHEMA views an optional region or dataset qualifier,
	// INFORMATION_SCHEMA and the view.
	rest []string
	// defaulted reports whether project was filled in from the default project.
	defaulted         bool
	informationSchema bool
}

func (r tableRef) path() []string {
	return append([]string{r.project}, r.rest...)
}

func (r tableRef) key() string {
	return strings.ToLower(strings.Join(r.path(), "."))
}

// parseTablePath splits path at every dot, so that however the path is quoted
// it resolves to the same table. defaultProject is called only when the
// project is omitted.
func parseTablePath(path []string, defaultProject func() string) (tableRef, error) {
	parts := strings.Split(strings.Join(path, "."), ".")
	// INFORMATION_SCHEMA views are [[project.]region-or-dataset.]INFORMATION_SCHEMA.view.
	if len(parts) >= 2 && strings.EqualFold(parts[len(parts)-2], "INFORMATION_SCHEMA") {
		switch len(parts) {
		case 2, 3:
			return tableRef{project: defaultProject(), rest: parts, defaulted: true, informationSchema: true}, nil
		case 4:
			return tableRef{project: parts[0], rest: parts[1:], informationSchema: true}, nil
		}
	}
	switch len(parts) {
	case 3:
		return tableRef{project: parts[0], rest: parts[1:]}, nil
	case 2:
		return tableRef{project: defaultProject(), rest: parts, defaulted: true}, nil
	}
	return tableRef{}, fmt.Errorf(`unknown table "%s"`, strings.Join(parts, "."))
}

func (c *Catalog) parseTablePath(path []string) (tableRef, error) {
	return parseTablePath(path, c.bqClient.GetDefaultProject)
}

// fetchTableMetadata asks INFORMATION_SCHEMA views for their schema by a dry
// run, because they have no table metadata.
func fetchTableMetadata(ctx context.Context, bqClient bigquery.Client, ref tableRef) (*bq.TableMetadata, error) {
	if !ref.informationSchema {
		return bqClient.GetTableMetadata(ctx, ref.project, ref.rest[0], ref.rest[1])
	}
	path := ref.rest
	if ref.project != "" {
		path = ref.path()
	}
	schema, err := bqClient.DryRunSchema(ctx, "SELECT * FROM `"+strings.Join(path, "`.`")+"`")
	if err != nil {
		return nil, err
	}
	return &bq.TableMetadata{Schema: schema}, nil
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
		if err := c.EnsureTable([]string{name}); err != nil && c.TableLoadError(name) != nil {
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

	ref, err := c.parseTablePath([]string{path})
	if err != nil {
		return nil, fmt.Errorf("invalid path: %s", path)
	}

	if meta, ok := c.tableMetaMap[ref.key()]; ok {
		return meta, nil
	}
	return nil, errors.New("table not found in catalog")
}
