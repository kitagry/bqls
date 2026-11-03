# Lint

bqls reports lint findings for queries that analyze without errors. The language server publishes them as diagnostics (source `bqls-lint`, code = rule name), and [`bqls lint`](#cli) runs the same rules from the command line, e.g. in CI.

- [Rules](#rules)
- [Configuration](#configuration)
  - [Editor settings](#editor-settings)
  - [Config file](#config-file)
  - [Ignoring a finding](#ignoring-a-finding)
- [CLI](#cli)

## Rules

Findings are reported as warnings, except those marked *information* below. A quick fix is offered as an LSP code action and applied by `bqls lint --fix`.

| Rule | Default | Severity | Quick fix | Options | Reports |
| --- | --- | --- | --- | --- | --- |
| **Correctness** | | | | | |
| [`null-comparison`](#null-comparison) | on | warning | yes | | `= NULL` / `!= NULL` comparisons, which always yield NULL |
| [`in-null`](#in-null) | on | warning | yes | | `IN (NULL)` / `NOT IN (NULL)` lists of only NULLs, which always yield NULL, and `NOT IN` lists with a NULL, which never yield TRUE |
| [`not-in-nullable`](#not-in-nullable) | on | warning | yes | | `x NOT IN (subquery)` over a `NULLABLE` table column, which yields no rows once that column holds a NULL |
| [`cross-join`](#cross-join) | on | warning | yes | | Joins without a join condition, which return the Cartesian product of both sides |
| [`union-column-order`](#union-column-order) | on | warning | yes | | Set operation inputs whose columns are the first input's columns in a different order |
| [`insert-column-order`](#insert-column-order) | on | warning | yes | | `INSERT ... SELECT` queries whose columns are the target columns in a different order |
| [`insert-null-into-required`](#insert-null-into-required) | off | warning | | | `INSERT` / `UPDATE` / `MERGE` values that may be NULL written to a `REQUIRED` column, which BigQuery rejects only at run time |
| [`left-join-as-inner`](#left-join-as-inner) | on | warning | yes | | `LEFT` / `RIGHT JOIN`s whose `WHERE` clause rejects the NULL rows of the optional side |
| [`variable-column-shadow`](#variable-column-shadow) | on | warning | yes | | A column compared with itself because its bare name was meant as a same-named `DECLARE` variable |
| [`join-key-implicit-cast`](#join-key-implicit-cast) | on | warning | | | `ON` / `USING` join keys of different types, so one side is implicitly cast (e.g. `INT64` to `FLOAT64`) |
| [`self-comparison`](#self-comparison) | on | warning | | | Comparisons whose two sides are the same column, e.g. `a.id = a.id` meant as `a.id = b.id` |
| [`missing-order-by`](#missing-order-by) | off | warning | | | `ROW_NUMBER()` filters, `ARRAY_AGG` / `STRING_AGG` with `LIMIT`, and indexed `ARRAY_AGG` results without `ORDER BY`, which pick arbitrary rows |
| [`timestamp-string-literal`](#timestamp-string-literal) | off | warning | yes | `time_zone` | String literals without a time zone compared with a `TIMESTAMP`, which are read as UTC |
| [`nondeterministic-cte`](#nondeterministic-cte) | on | warning | | | `RAND()` / `GENERATE_UUID()` in a CTE that is evaluated more than once, so each reference sees different values |
| [`window-default-frame`](#window-default-frame) | on | warning | yes | | `LAST_VALUE` / `NTH_VALUE` whose window has `ORDER BY` but no frame, so the default frame ends at the current row |
| **Cost** | | | | | |
| [`partition-filter`](#partition-filter) | on | warning | | | Partitioned tables read without a filter on the partition column |
| [`partition-filter-not-prunable`](#partition-filter-not-prunable) | off | warning | | | Partition column filters written in a form BigQuery documents as unable to prune partitions |
| [`wildcard-table-suffix`](#wildcard-table-suffix) | on | warning | | | Wildcard table queries that never filter on `_TABLE_SUFFIX` |
| [`wildcard-table-suffix-not-constant`](#wildcard-table-suffix-not-constant) | off | warning | | | `_TABLE_SUFFIX` filters that BigQuery documents as unable to limit the tables scanned |
| [`order-by-in-subquery`](#order-by-in-subquery) | on | warning | yes | | `ORDER BY` in a subquery, CTE or derived table whose order cannot reach the result |
| [`select-star`](#select-star) | off | warning | yes | `min_columns` | `SELECT *` that reads at least `min_columns` columns of a table |
| [`bytes-processed-threshold`](#bytes-processed-threshold) | off | warning | | `max_bytes` | Files whose dry-run estimate exceeds `max_bytes` |
| **Unused code** | | | | | |
| [`unused-cte`](#unused-cte) | on | information | yes | | `WITH` entries that are never referenced |
| [`unused-declare`](#unused-declare) | on | information | yes | | `DECLARE` variables that are never referenced |
| [`unused-temp-function`](#unused-temp-function) | on | information | yes | | `CREATE TEMP FUNCTION` definitions that are never called |
| [`unused-temp-function-argument`](#unused-temp-function-argument) | on | information | | | Arguments of SQL temp functions that the body never references |
| [`unused-join`](#unused-join) | on | information | | | `LEFT` / `RIGHT JOIN`s whose optional side is never used outside the join condition |

Bare `x = NULL` / `x != NULL` is rejected by BigQuery itself ("Operands of = cannot be literal NULL"); bqls offers an `IS [NOT] NULL` quick fix on that error.

### Correctness

#### `null-comparison`

`x = CAST(NULL AS T)` / `!=` always yields NULL. The quick fix rewrites it to `IS [NOT] NULL`.

#### `in-null`

`x IN (NULL)` / `x NOT IN (NULL)` whose list holds only NULLs (including `CAST(NULL AS T)` and `SAFE_CAST(NULL AS T)`) always yields NULL. Since no one means that, the quick fix rewrites it to the NULL check `x IS [NOT] NULL`.

`x NOT IN (1, NULL)` is also reported: a NULL in a `NOT IN` list makes it NULL or FALSE, never TRUE, so a `WHERE` keeps no rows. The quick fix removes the NULLs (`x NOT IN (1)`). `x IN (1, NULL)` is not reported, since in a filter it acts as `x IN (1)`.

There is no quick fix when a comment would be removed or moved by the rewrite.

#### `not-in-nullable`

`x NOT IN (SELECT col ...)` where `col` is a `NULLABLE` column of a table: once the subquery returns a NULL, `NOT IN` yields NULL (never TRUE) for every row, so a `WHERE` keeps none. Rewrite it with `NOT EXISTS`, or filter the subquery with `col IS NOT NULL`.

The quick fix adds `col IS NOT NULL` to the subquery's `WHERE` (with `AND`, parenthesizing a top-level `OR`), which drops only the NULLs. It is not offered when the subquery has `LIMIT`, `QUALIFY`, pipe operators or `ROLLUP` / `CUBE` / `GROUPING SETS`, is a set operation, or selects the column through anything but a column path (e.g. `*`). `NOT EXISTS` is not offered as a fix, since it also keeps the rows whose `x` is NULL, which `NOT IN` drops.

Unlike `in-null`, which reports a literal list of only NULLs, this rule depends on the table schema, so it needs table metadata. To avoid false positives, the column is followed only while it is passed through unchanged (column references, aliases, joins, `GROUP BY` keys, CTEs and derived tables).

Not reported: a computed value (`IFNULL`, `COALESCE`, aggregates, arithmetic, struct fields), `UNION` and other set operations, `UNNEST`, `REQUIRED` columns, views and other non-table sources, and a column that any `WHERE` / `HAVING` / `QUALIFY` / `JOIN ... ON` condition in the statement mentions (e.g. `IS NOT NULL`, `=`, `>`).

#### `cross-join`

An inner join with no join condition (a comma join `FROM a, b` or `JOIN b ON TRUE`), which returns every combination of rows from both sides. A join counts as conditioned when a `WHERE` / `HAVING` / `JOIN ... ON` term mentions both sides, or each side is compared with another table (`FROM a, b, c WHERE a.id = c.a_id AND b.id = c.b_id`); column flow is followed through CTEs and subqueries.

Not reported: an explicit `CROSS JOIN`, `UNNEST`, outer and lateral joins, a side that yields at most one row (an aggregate without `GROUP BY`, `SELECT` without `FROM`, `LIMIT 1`, also through CTEs), pipe `|> JOIN`, statements bqls cannot trace, view definitions, and unreferenced CTEs. Write `CROSS JOIN` to mark an intended product.

The quick fix does that: it replaces the comma, or `[INNER] JOIN` together with `ON TRUE`, with `CROSS JOIN`, which returns the same rows. It is not offered when the `ON` clause is anything but a literal `TRUE` (e.g. a `DECLARE`d variable), a comment is in the way, the join has a hint (`INNER HASH JOIN`), or the join is part of consecutive `ON` clauses (`a JOIN b JOIN c ON ... ON ...`), where dropping one `ON` would regroup the joins.

#### `union-column-order`

A set operation input (`UNION ALL`, `UNION DISTINCT`, `INTERSECT`, `EXCEPT`) whose columns are the first input's columns in a different order. Set operations match columns by position, so when the types agree the values are silently swapped. Names are the explicit aliases or the implicit alias of a column path (`t.a` is `a`); a column without a name (`SELECT x + 1`) is left out of the comparison, and inputs whose names merely differ are not reported. Reported on the select list of the reordered input.

The quick fix moves the select columns of the reordered input, each with its alias, into the first input's order; columns without a name stay where they are. It is not offered when the input is not a plain `SELECT` (e.g. a nested set operation), its own `GROUP BY` or `ORDER BY` may refer to columns by position (`GROUP BY 1`; an `ORDER BY 1` after the whole set operation refers to the first input's columns and does not prevent the fix), the names involved are not unique, or the select list contains a comment.

Skipped: `CORRESPONDING` / `BY NAME`, inputs with `*` or `SELECT AS STRUCT`, pipe `|> UNION ALL`, inputs that read exactly the same tables (`SELECT src, dst FROM edges UNION ALL SELECT dst, src FROM edges` is how a relation is made symmetric), and unreferenced CTEs.

#### `insert-column-order`

An `INSERT ... SELECT` whose query lists the target columns in a different order. `INSERT` matches the query's columns to the target columns by position, not by name, so when the types agree the values are silently swapped. The targets are the column list when the statement has one (`INSERT INTO t (a, b) SELECT b, a ...`), and otherwise every column of the table in schema order, which the statement does not show (`INSERT INTO t SELECT b, a ...` where `t` has columns `a, b`). Names are compared as in `union-column-order`, and queries whose names merely differ from the targets are not reported. Reported on the select list.

The quick fix makes the column list follow the query's names: without a column list it adds one after the table (`INSERT INTO t (b, a) SELECT b, a ...`), quoting names that need it, and an existing column list is reordered in place. A column without a name (`x + 1`) keeps its current target. It is not offered when the names involved are not unique or the column list contains a comment.

Skipped: queries with `*` or pipe syntax, a set operation matching `BY NAME` / `CORRESPONDING`, `INSERT ... VALUES`, and queries that read the target table itself (`INSERT INTO edges SELECT dst, src FROM edges` is how a relation is made symmetric).

#### `insert-null-into-required`

A value that may be NULL written to a `REQUIRED` column of the target table by `INSERT` (with or without a column list, `VALUES` or a query), `UPDATE ... SET`, or the `INSERT` / `UPDATE SET` of a `MERGE`. BigQuery checks the mode only while writing rows, so such a statement passes analysis and dry run but fails at run time with `Required field ... cannot be null` once a row holds a NULL there. `REQUIRED` fields of a `STRUCT` written as `STRUCT(...)` are checked too (`Required field s.f cannot be null`), as is a `REQUIRED` `RECORD` column written as NULL. Reported on the value, or on the select list item of `INSERT ... SELECT`; on the target table when the query has `*` or pipe syntax.

A value counts as possibly NULL only when it is:

- a `NULL` literal, also `CAST(NULL AS T)`,
- a `NULLABLE` column of a regular table, followed while it is passed through unchanged (column references, aliases, casts, joins, `GROUP BY` keys, CTEs and derived tables) and no `WHERE` / `HAVING` / `QUALIFY` / `JOIN ... ON` condition in the statement, nor the `UPDATE` `WHERE`, `MERGE ... ON` or `WHEN ... AND` condition, mentions it, or
- a column from the optional side of a `LEFT` / `RIGHT` / `FULL JOIN`, also through CTEs and derived tables, unless a condition outside that join mentions any column of that side (e.g. `WHERE b.id IS NOT NULL` or `WHERE b.status = 'active'`, which drop the unmatched rows). The join's own `ON`, the `ON` of other outer joins, and conditions inside the optional side do not count, since they keep the unmatched rows.

Not reported, to avoid false positives: computed values (`COALESCE`, `IFNULL`, `IF`, functions, aggregates, arithmetic, struct fields), `REQUIRED` source columns, set operations, `UNNEST`, views and other non-table sources, `DEFAULT`, script variables and query parameters, a whole `STRUCT` copied from a table column, `REPEATED` fields, and nested `UPDATE` targets such as `SET s.f = ...`.

This rule depends on the table schema, so it needs table metadata. It is off by default: copying from a table whose columns are all `NULLABLE` (e.g. a staging table loaded with schema auto-detection) into a `REQUIRED` table is common even when the data never holds NULLs, and such statements already fail loudly at run time. Enable it to catch these before running the statement. There is no quick fix, since any replacement value (`COALESCE(x, 0)`) or filter (`WHERE x IS NOT NULL`) changes what the statement writes.

#### `left-join-as-inner`

A `LEFT JOIN` (or `RIGHT JOIN`) whose `WHERE` clause filters the optional side with a condition that rejects NULL, so the unmatched rows the outer join adds are dropped again and it behaves as an `INNER JOIN` (e.g. `a LEFT JOIN b ON a.id = b.id WHERE b.status = 'active'`). Move the condition into the `ON` clause to keep the unmatched rows, or write `INNER JOIN`. Reported on the `JOIN` keyword.

A top-level `AND` term counts when it is certainly NULL or FALSE for such a row: `=`, `!=`, `<`, `<=`, `>`, `>=`, `LIKE`, `IN (...)` / `BETWEEN` on the tested value, `NOT` of those, `IS NOT NULL`, `IS TRUE`, `IS FALSE`, or a bare boolean column, where the column may be wrapped in `CAST`, struct field access or arithmetic.

Not reported: `IS NULL`, `COALESCE` / `IFNULL` / `IF` and other functions, `OR`, `IS [NOT] DISTINCT FROM`, subqueries, conditions in `ON` or `HAVING` or an outer query, `FULL` and `UNNEST` joins, and a `USING` column (it is the preserved side's).

The quick fix replaces `LEFT [OUTER]` / `RIGHT [OUTER]` with `INNER` (keeping a join hint such as `HASH`), which returns the same rows. It is not offered for `RIGHT JOIN ... USING`, whose `USING` columns come from the right side while `INNER JOIN`'s come from the left, or when a comment is between the keywords. If keeping the unmatched rows was the intent, move the condition into `ON` instead.

#### `variable-column-shadow`

A comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`) of a column with itself where one side is written as the bare name of a `DECLARE` variable in scope, e.g. `DECLARE user_id INT64; SELECT * FROM t WHERE user_id = user_id` (or `t.user_id = user_id`). A column takes precedence over a variable of the same name, so the variable is never read and the condition is always true (or always false) for non-NULL rows. To use the variable, rename it; qualifying the column does not make the name refer to the variable.

The quick fix keeps the current meaning and only makes it explicit: it qualifies each bare operand with the range variable of the column's table (`WHERE x = x` becomes `WHERE t.x = t.x`), which [`self-comparison`](#self-comparison) then reports. Renaming the variable is not offered, since it touches every use of the variable. The fix is offered only when the table is read directly in the `FROM` clause the comparison sees (not through a subquery, a CTE, a pipe operator that redefines the columns, or an outer query) and its range variable is a single identifier (not the implicit alias of a backquoted path such as `` `p.d.t` ``, which BigQuery and bqls name differently).

Other uses of a name shared by a variable and a column (e.g. `GROUP BY user_id`) are not reported, since the column is likely intended there; neither are `IF` / `WHILE` conditions.

#### `join-key-implicit-cast`

A join key comparison in `ON` or `USING` between columns of different types, so BigQuery implicitly casts one side, e.g. `INT64` with `FLOAT64` or `NUMERIC`, `NUMERIC` with `FLOAT64`, or `DATE` with `DATETIME` (`a.order_id = b.order_id` where one is `INT64` and the other `FLOAT64`). Mismatched key types usually point to a schema design mistake, and a cast to `FLOAT64` is lossy: it cannot represent every `INT64` or `NUMERIC` value exactly, so different keys may compare equal. Give both columns the same type, or write an explicit `CAST` to state the intent. Reported on the comparison (on the `JOIN` keyword for `USING`).

Only a top-level `AND` term of `=` or `IS NOT DISTINCT FROM` between a column of each join input counts, where the analyzer inserted the cast. Not reported: comparisons with literals, query parameters or computed values (`a.id = 1.5`, `a.id = b.id + 0`), explicit `CAST` / `SAFE_CAST` on either side, terms under `OR`, other comparisons (`<`, `!=`, ...), comma joins filtered in `WHERE`, and `MERGE ... ON`.

#### `self-comparison`

A comparison (`=`, `!=` / `<>`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`) whose two sides are references to the same column, e.g. `a JOIN b ON a.id = a.id` or `WHERE x = x`. The result does not depend on the column's value, only on whether it is NULL, so one side is almost always a typo for another column (`b.id`). Both sides must resolve to the same column, so a self join `t AS a JOIN t AS b ON a.id = b.id` is fine. For `=` the message also suggests `x IS NOT NULL`, in case a NULL check was meant; there is no quick fix, since the typo is far more likely.

Not reported: `FLOAT64` columns (`x != x` is a way to test for NaN), expressions that are not plain column references (`x = x + 0`, `s.f = s.f`), comparisons the analyzer adds itself (`JOIN ... USING`), and comparisons that `variable-column-shadow` reports (one side written as the bare name of a `DECLARE` variable in scope).

#### `missing-order-by`

An order-dependent operation without `ORDER BY`, whose result can change from run to run because BigQuery does not guarantee row order. Reported on the function name:

- `ROW_NUMBER()` whose window has no `ORDER BY` and whose value a `WHERE`, `QUALIFY`, `HAVING` or `JOIN ... ON` condition reads, e.g. `QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1`, which keeps an arbitrary row per `id`. The value is followed through aliases, subqueries and CTEs.
- `ARRAY_AGG`, `STRING_AGG` and `ARRAY_CONCAT_AGG` with `LIMIT` but no `ORDER BY`, which keep arbitrary elements.
- `ARRAY_AGG` without `ORDER BY` (or analytic `ARRAY_AGG` whose window has no `ORDER BY`) whose result is indexed with `[OFFSET(n)]`, `[ORDINAL(n)]`, `[SAFE_OFFSET(n)]` or `[SAFE_ORDINAL(n)]`.

Other numbering and navigation functions (`RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `NTH_VALUE`, ...) are not listed because BigQuery already rejects them without a window `ORDER BY`.

Not reported: `ROW_NUMBER()` that no condition reads (e.g. a surrogate id), order-insensitive aggregates (`COUNT`, `SUM`, `ANY_VALUE`, ...), `ARRAY_AGG` / `STRING_AGG` without `LIMIT` that is not indexed, and duplicates that look the same to the rest of the statement: rows whose `PARTITION BY` keys cover every column the statement uses (e.g. `PARTITION BY TO_JSON_STRING(t)`, or `SELECT id ... QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1`), and aggregate arguments that read only `GROUP BY` keys. Statements bqls cannot trace (e.g. recursive CTEs, table functions) and view definitions are skipped.

Off by default: whether the result actually changes depends on whether rows sharing a key differ, which bqls cannot see, and deduplicating rows known to be identical by key alone (e.g. after at-least-once ingestion) is a common, intentional idiom. No quick fix, since the column to order by cannot be guessed.

#### `timestamp-string-literal`

A string literal without a time zone compared with a `TIMESTAMP` (`=`, `!=`, `<`, `<=`, `>`, `>=`, `[NOT] BETWEEN`, `[NOT] IN (...)`, `IS [NOT] DISTINCT FROM`), e.g. `created_at >= '2024-01-01'`. The literal is implicitly converted to `TIMESTAMP` in the default time zone, UTC, so a local date or time shifts the boundary by the UTC offset. Add an offset or a time zone name (`'2024-01-01 00:00:00+09'`, `'2024-01-01 00:00:00 Asia/Tokyo'`, `TIMESTAMP('2024-01-01', 'Asia/Tokyo')`), or write `TIMESTAMP '2024-01-01'` to make UTC explicit. Reported on the literal.

Not reported: literals with an offset, `Z` or a zone name (including `UTC`), `TIMESTAMP '...'` and `CAST('...' AS TIMESTAMP)`, `DATE` / `DATETIME` comparisons, variables and query parameters, other uses such as function arguments, and files that `SET @@time_zone`, since it changes the zone these literals are read in.

The quick fix rewrites the literal to `TIMESTAMP('2024-01-01', 'UTC')`, which keeps the result and states the zone. When `time_zone` is set, a second quick fix uses that zone instead (`TIMESTAMP('2024-01-01', 'Asia/Tokyo')`). It changes the result, so it is never the preferred fix and `bqls lint --fix` does not apply it.

Off by default: such comparisons are common and often do mean UTC.

| Option | Default | Description |
| --- | --- | --- |
| `time_zone` | none | The time zone the literals are usually meant in, offered as a second quick fix. A [time zone name](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#time_zones) (e.g. `Asia/Tokyo`) or an offset (e.g. `+09:00`). |

#### `nondeterministic-cte`

`RAND()` or `GENERATE_UUID()` in a CTE that two or more evaluations read. BigQuery [does not materialize non-recursive CTEs](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause) and runs one once per reference, so each reference gets its own random values, e.g. `WITH s AS (SELECT * FROM t WHERE RAND() < 0.1) SELECT ... FROM s JOIN s ...` joins two different samples. Materialize the values first (a temporary table), or compute them in a single reference. Reported on the function name.

A CTE referenced once from another CTE that is itself referenced twice also runs twice. The call is reported when it decides which rows the CTE returns (`WHERE`, `JOIN ... ON`, `GROUP BY`, `ORDER BY ... LIMIT`, window ordering), or when it flows into an output column that at least two evaluations read.

Not reported: `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_DATETIME` / `CURRENT_TIME`, which BigQuery fixes for the whole statement; recursive CTEs (materialized); calls outside a CTE; references from unreferenced CTEs; a random column that only one evaluation reads (e.g. the other one only does `COUNT(*)` or `SELECT * EXCEPT (r)`); `EXISTS`; `ORDER BY RAND()` without `LIMIT`; and column flows bqls does not follow. References that read disjoint rows (`... WHERE kind = 'a' UNION ALL ... WHERE kind = 'b'`) are still reported, since bqls cannot tell the rows apart.

#### `window-default-frame`

`LAST_VALUE` or `NTH_VALUE(x, n)` (`n` > 1) whose window has `ORDER BY` but no `ROWS` / `RANGE` frame. Such a window [uses the default frame](https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#def_window_frame) `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, so `LAST_VALUE(x) OVER (PARTITION BY id ORDER BY ts)` returns `x` of the current row (or of its last peer), not of the last row of the partition, and `NTH_VALUE(x, 2)` returns NULL on the first row. Reported on the function name. A window taken from a named `WINDOW` clause is checked the same way.

Not reported: an explicit frame, also one written in a named window, and also when it is the default one written out (`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`); `FIRST_VALUE` and `NTH_VALUE(x, 1)`, which the default frame already gives the right row; `LAST_VALUE(x IGNORE NULLS)`, the usual way to carry the last non-NULL value forward; aggregates such as `SUM` and `ARRAY_AGG`, whose running results are what the default frame is for; and windows ordered only by their `PARTITION BY` columns, where every row is a peer and the frame is the whole partition.

The quick fix adds `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` after the last clause of the call's `OVER` clause, e.g. `OVER (PARTITION BY id ORDER BY ts ROWS BETWEEN ...)`. A named window gets parentheses (`OVER w` becomes `OVER (w ROWS BETWEEN ...)`), and the `WINDOW` clause itself is left alone, since other calls may use it. The fix changes the result to the one the function name suggests.

### Cost

#### `partition-filter`

A time- or range-partitioned table that is read without any filter on its partition column (`_PARTITIONTIME` / `_PARTITIONDATE` for ingestion-time partitioning), so the query may scan every partition. A filter anywhere in the statement on the column or a value derived from it (through CTEs, subqueries, and `UNION`) counts.

Skipped: statements bqls cannot trace (e.g. recursive CTEs, table functions), view definitions, DML targets, wildcard tables, unreferenced CTEs, and reads that consume no column (e.g. `SELECT COUNT(*)`, unless the table requires a partition filter).

#### `partition-filter-not-prunable`

Off by default. A filter on a partition column written in a form the [BigQuery documentation](https://cloud.google.com/bigquery/docs/querying-partitioned-tables) lists as unable to prune partitions:

- an `OR` with a branch that does not filter the partition column
- a comparison with a scalar subquery
- a comparison with another column of the same table (`ts >= ts2`)
- arithmetic on the column (`ts + INTERVAL 1 DAY > ...`)
- `EXTRACT` of a part other than `YEAR`
- `FORMAT_DATE` / `FORMAT_DATETIME` / `FORMAT_TIMESTAMP` with a literal format other than `%F`, `%Y-%m-%d` or `%Y%m%d`
- any function over an integer-range partitioning column

Reported only when no other term on the partition column (joined by `AND`) might prune, so `dt >= '2020-01-01' AND EXTRACT(MONTH FROM dt) = 3` is fine.

Not reported: forms the documentation says do prune (e.g. `DATE(ts)`, `TIMESTAMP_TRUNC`, `DATE_ADD`, `CURRENT_TIMESTAMP()`, query parameters), forms it does not mention (e.g. `CAST(dt AS STRING)`, `dt IN (SELECT ...)`, join conditions against another table), filters on values computed from the column in a CTE or subquery (plain aliases are followed), and whatever `partition-filter` skips (including the case with no filter at all).

Off by default because the documentation, not the optimizer, is the source of these patterns, and bqls cannot confirm pruning per table.

#### `wildcard-table-suffix`

A wildcard table (`` `p.d.events_*` ``) that is queried without any filter on `_TABLE_SUFFIX`, so it scans every matching table. Filters are judged as in `partition-filter`: a `WHERE` / `JOIN ... ON` condition that mentions `_TABLE_SUFFIX` or a value derived from it (through CTEs, subqueries, and `UNION`) counts, even on one side of an `OR`. Unlike `partition-filter`, `SELECT COUNT(*)` is still reported.

Skipped: statements bqls cannot trace, view definitions, and unreferenced CTEs.

#### `wildcard-table-suffix-not-constant`

Off by default. A filter on `_TABLE_SUFFIX` that is not a constant expression, which the [BigQuery documentation](https://cloud.google.com/bigquery/docs/querying-wildcard-tables) says does not limit the tables a wildcard table query scans: a comparison with a scalar subquery, a comparison with another column of the same table, or an `OR` with a branch that does not filter `_TABLE_SUFFIX`.

Judged as in `partition-filter-not-prunable`: reported only when no other term on `_TABLE_SUFFIX` (joined by `AND`) might limit the scan, so `_TABLE_SUFFIX BETWEEN '40' AND '60' AND _TABLE_SUFFIX = (SELECT ...)` is fine.

Not reported: functions over `_TABLE_SUFFIX` (e.g. `PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)`), `_TABLE_SUFFIX IN (SELECT ...)`, join conditions against another table, values computed from `_TABLE_SUFFIX` in a CTE or subquery (plain aliases are followed), JavaScript UDFs, and whatever `wildcard-table-suffix` skips (including the case with no filter at all).

Off by default for the same reason as `partition-filter-not-prunable`.

#### `order-by-in-subquery`

An `ORDER BY` without `LIMIT` in a subquery, CTE or derived table whose order cannot reach the result (BigQuery does not preserve row order), so it only costs a sort. The quick fix removes the clause.

Not reported when the order may matter: `LIMIT`, `ARRAY(SELECT ...)`, scalar subqueries, set operations, or a consumer that is limited, aggregated (e.g. `ARRAY_AGG`) or windowed. DML, `CREATE TABLE AS SELECT` and pipe `ORDER BY` are skipped.

#### `select-star`

Off by default. A bare `*` or `t.*` that reads at least `min_columns` columns of a table in the same `SELECT`'s `FROM`, since BigQuery bills every column read (`LIMIT` does not help). Only columns the statement actually consumes count, so `SELECT a FROM (SELECT * FROM t)` or `EXISTS (SELECT * ...)` is not reported. When a dry-run result is available, its bytes estimate for the whole file is added to the message.

Skipped: `* EXCEPT (...)` / `* REPLACE (...)` (the columns were chosen deliberately), a `*` over a CTE or subquery (the `*` inside it that reads the table is reported instead), DML, `CREATE TABLE AS SELECT`, `EXPORT DATA`, view definitions, and statements bqls cannot trace.

Off by default because whether reading every column is wasteful depends on the query's intent, and a sensible `min_columns` differs per team.

The quick fix expands the `*` into the columns it reads, so that the unneeded ones can be deleted: `t.*` becomes `t.a, t.b, ...`, a bare `*` over one table becomes `a, b, ...`, and a bare `*` over a join qualifies each column with its table's alias (`x.a, y.b, ...`). Pseudo-columns such as `_PARTITIONTIME` are left out like `*` does, and keywords are backquoted. The list follows the select list's layout: inline in a one-line `SELECT`, otherwise one column per line. There is no quick fix when the columns of a bare `*` are not just those of tables (`UNNEST`, subqueries, CTEs, `USING` joins), when two expanded columns share a name, when a column is named like the table's alias (the alias would win), when a table in a join has no alias and a quoted path (`` `p.d.t` ``), or when a comment is inside `t.*`.

| Option | Default | Description |
| --- | --- | --- |
| `min_columns` | `50` | The number of consumed columns at which a `*` is reported. Must be a positive integer. |

#### `bytes-processed-threshold`

Off by default. The dry-run estimate of the whole file exceeds `max_bytes`.

Needs a dry-run result: the language server dry-runs a file when it is saved, and `bqls lint` only with `--dry-run`. Without one, or after the file is edited since the dry-run, the rule reports nothing.

Reported on the first line of the first statement, so a `-- bqls:ignore bytes-processed-threshold` comment must be on the line directly above that line (or trailing on it).

| Option | Default | Description |
| --- | --- | --- |
| `max_bytes` | `1TiB` | A byte count (`1000000`) or a number with a unit (`100GiB`, `1.5TiB`): `B`, `KiB`, `MiB`, `GiB`, `TiB`, `PiB`, `EiB` (powers of 1024) or `KB` … `EB` (powers of 1000), case-insensitive. Single-letter units such as `100G` are rejected as ambiguous. Must not be negative. |

### Unused code

#### `unused-cte`

A `WITH` entry that is never referenced (a recursive CTE's self reference does not count). The quick fix removes the entry.

#### `unused-declare`

A `DECLARE` variable whose name appears nowhere else in the script. Any identifier with the same name counts as a reference, including a `SET` target, so a variable that is only assigned is not reported. The quick fix removes the declaration (or just that name from `DECLARE x, y ...` when another variable stays).

#### `unused-temp-function`

A `CREATE TEMP FUNCTION` whose name appears nowhere else in the script. As in `unused-declare`, any identifier with the same name counts as a call, and so does the name as a word of a string literal, which may be a query run by `EXECUTE IMMEDIATE`. A function that only an unused function calls counts as used until that function is removed. The quick fix removes the statement.

Not reported: a file that consists only of function definitions (it may be a library of functions for later queries of the same session), and persistent `CREATE FUNCTION dataset.f`, whose callers live outside the file.

#### `unused-temp-function-argument`

An argument of a SQL temp function that the body never references. Any identifier in the body with the argument's name counts as a reference, including a same-named column or alias. No quick fix, since removing an argument breaks every call site.

Not reported: JavaScript and other non-SQL functions, whose body bqls cannot analyze, and persistent `CREATE FUNCTION dataset.f`, which may keep an argument for its callers.

#### `unused-join`

A `LEFT JOIN` (or `RIGHT JOIN`) whose optional side contributes no column outside its own `ON` / `USING` condition, so it only adds cost — or duplicates rows when several rows match, which is rarely intended. A column counts as used when any expression reads it or a `SELECT` list outputs it (including `SELECT *`), even in a subquery whose result is not read further. Reported on the `JOIN` keyword.

`INNER`, `CROSS`, comma and `FULL` joins are not reported, since they filter, multiply or add rows; neither are `UNNEST` joins.

## Configuration

Each rule is either `true` / `false` or an object with an optional `enabled` and the rule's [options](#rules). Rules that are not listed use their default. Unknown rules and options are errors.

Precedence: `--enable` / `--disable` flags or the editor's `lint` setting > [`.bqls.yaml`](#config-file) > rule defaults. Rule options are merged key by key, so a setting only needs the options it changes.

### Editor settings

Rules can be toggled with the `lint` setting (also via `workspace/didChangeConfiguration`):

```lua
settings = {
  project_id = "YOUR_PROJECT_ID",
  lint = { rules = { ["null-comparison"] = false } },
}
```

In VSCode, use the `bqls.lint.rules` setting.

### Config file

To share lint settings between your editor and CI, put a `.bqls.yaml` in your repository. Both the language server and `bqls lint` look for it from the SQL file's directory up to the repository root (the directory containing `.git`); the nearest one wins.

```yaml
lint:
  rules:
    unused-cte: false
    bytes-processed-threshold:   # a rule can also take an object
      enabled: true
      max_bytes: 100GiB          # rule-specific options
```

The file takes the same form as the editor's `lint` setting. Unknown keys under `lint` are errors too; with an invalid file, `bqls lint` exits with `2` and the language server logs the error and ignores the file. The language server rereads the file when it changes.

### Ignoring a finding

To silence a single occurrence, add a `bqls:ignore` comment. A comment on its own line applies to the next line; a trailing comment applies to its own line, and so does a block comment followed by code (`/* bqls:ignore */ SELECT ...`). List several rules separated by spaces or commas, or omit rule names to ignore every rule. Text after `--` following the rule list is ignored, so you can leave a reason.

```sql
-- bqls:ignore null-comparison
SELECT * FROM t WHERE x = CAST(NULL AS INT64)
SELECT * FROM t WHERE y = CAST(NULL AS INT64) -- bqls:ignore
-- bqls:ignore unused-join, cross-join -- the lookup table has one row
```

The language server also offers an "Ignore *rule* on this line" code action on each finding, which adds the comment (or appends the rule to an existing `bqls:ignore` line above).

## CLI

`bqls lint` runs the same analysis and lint rules as the language server, without an editor. Table schemas are fetched with Application Default Credentials; no query is run unless `--dry-run` is given.

```console
$ bqls lint [flags] <file|dir|->...
$ bqls lint --project my-project queries/
queries/report.sql:3:6: info: CTE `tmp` is defined but never used (unused-cte)
queries/report.sql:8:7: error: Unrecognized name: y
```

Directories are searched recursively for `*.sql` files (skipping directories whose name starts with `.`, such as `.git`), and `-` reads from stdin. Flags must come before files. Run `bqls lint --help` for the full list of rules. Each file uses the nearest [`.bqls.yaml`](#config-file) (stdin uses the one found from the working directory).

| Flag | Description |
| --- | --- |
| `--project` | BigQuery project ID. Defaults to `gcloud config get project`. |
| `--location` | BigQuery location. Defaults to `US`. |
| `--enable <rule>` / `--disable <rule>` | Turn a rule on or off. Repeatable; the last one wins. Takes precedence over the config file. |
| `--config <path>` | Use this config file for every input instead of looking up `.bqls.yaml`. |
| `--fix` | Apply the quick fixes (the first one of each finding) and write the files back, then report the findings that are left. With `-`, the fixed SQL is printed to stdout and the findings go to stderr. A fix that overlaps another fix in the same file is skipped (noted on stderr) and its finding is reported; running `--fix` again applies it. |
| `--dry-run` | Dry-run each file that analyzes without errors (after `--fix`, if given), so that rules using the estimate such as `bytes-processed-threshold` can report. This creates a dry-run query job per file, which is free but needs `bigquery.jobs.create` (e.g. `roles/bigquery.jobUser`). A failed dry-run is printed to stderr and makes the exit code `2`; the other findings are still printed. |
| `--format text\|json\|github` | `text` (default) prints `path:line:col: severity: message (rule)`. `json` prints an array of `{file, line, column, endLine, endColumn, severity, rule, message}`. `github` prints [workflow commands](https://docs.github.com/en/actions/reference/workflow-commands-for-github-actions) that show up as annotations on pull requests. |

Lines and columns are 1-based and the end position is exclusive. Columns are currently off on lines containing non-ASCII text ([#375](https://github.com/kitagry/bqls/issues/375)). Analysis errors are reported with severity `error` and no rule. With `--format github`, file paths are made relative to `$GITHUB_WORKSPACE` so annotations attach to the diff.

Exit codes: `0` nothing found, `1` any lint finding or analysis error (with `--fix`, any that is left after fixing), `2` usage or runtime error (bad flags, invalid config file, unreadable or unwritable file, authentication/permission/network failure while fetching table metadata, failed `--dry-run`). An unreadable file does not stop the others from being linted; their findings are still printed and the exit code is `2`.

GitHub Actions example. The service account only needs to read table metadata (e.g. `roles/bigquery.metadataViewer`), plus `roles/bigquery.jobUser` with `--dry-run`. Pass `--project` explicitly, since the fallback to `gcloud config get project` only works where gcloud is installed and configured. Instead of `go install`, you can download a binary from [GitHub Releases](https://github.com/kitagry/bqls/releases) (the `lint` subcommand needs a release that includes it).

```yaml
jobs:
  lint:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-go@v7
        with:
          go-version: stable
      - uses: google-github-actions/auth@v3
        with:
          workload_identity_provider: ${{ secrets.WORKLOAD_IDENTITY_PROVIDER }}
          service_account: ${{ secrets.SERVICE_ACCOUNT }}
      - run: go install github.com/kitagry/bqls@latest
      - run: bqls lint --project my-project --format github .
```
