package rule

import (
	"fmt"
	"strings"

	"github.com/src-d/gitbase"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// SquashJoinsRule name.
const SquashJoinsRule = "squash_joins"

// SquashJoins finds all squashable tables and joins them in a single
// SquashedTable that will perform many operations directly over git
// objects to make the query run faster.
func SquashJoins(
	ctx *sql.Context,
	a *analyzer.Analyzer,
	n sql.Node,
) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	span, ctx := ctx.Span("gitbase.SquashJoins")
	defer span.Finish()

	a.Log("squashing joins, node of type %T", n)
	n, err := n.TransformUp(func(n sql.Node) (sql.Node, error) {
		join, ok := n.(*plan.InnerJoin)
		if !ok {
			return n, nil
		}

		return squashJoin(join)
	})
	if err != nil {
		return nil, err
	}

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		t, ok := n.(*joinedTables)
		if !ok {
			return n, nil
		}

		return buildSquashedTable(t.tables, t.filters, t.columns)
	})
}

func squashJoin(join *plan.InnerJoin) (sql.Node, error) {
	if !isJoinSquashable(join) {
		return join, nil
	}

	table, err := joinTables(join)
	if err != nil {
		return nil, err
	}

	return rearrange(join, table), nil
}

func joinTables(join *plan.InnerJoin) (sql.Table, error) {
	var tables []sql.Table
	var filters []sql.Expression
	var columns []sql.Expression
	plan.Inspect(join, func(node sql.Node) bool {
		switch node := node.(type) {
		case *joinedTables:
			tables = append(tables, node.tables...)
			columns = append(columns, node.columns...)
			filters = append(filters, node.filters...)
		case *plan.PushdownProjectionAndFiltersTable:
			table, ok := node.PushdownProjectionAndFiltersTable.(gitbase.Table)
			if ok {
				filters = append(filters, node.Filters...)
				columns = append(columns, node.Columns...)
				tables = append(tables, table)
			}
		case *plan.InnerJoin:
			filters = append(filters, exprToFilters(node.Cond)...)
		}
		return true
	})

	return &joinedTables{
		tables:  tables,
		filters: filters,
		columns: columns,
	}, nil
}

func rearrange(join *plan.InnerJoin, squashedTable sql.Table) sql.Node {
	var projections []sql.Expression
	var filters []sql.Expression
	plan.Inspect(join, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Project:
			projections = append(projections, node.Projections...)
		case *plan.Filter:
			filters = append(filters, node.Expression)
		}
		return true
	})

	var node sql.Node = squashedTable
	if len(filters) > 0 {
		node = plan.NewFilter(expression.JoinAnd(filters...), node)
	}

	if len(projections) > 0 {
		node = plan.NewProject(projections, node)
	}

	return node
}

var errInvalidIteratorChain = errors.NewKind("invalid iterator to chain with %s: %T")

func buildSquashedTable(
	tables []sql.Table,
	filters, columns []sql.Expression,
) (sql.Node, error) {
	tableNames := orderedTableNames(tables)

	var iter gitbase.ChainableIter
	var err error
	for _, t := range tableNames {
		switch t {
		case gitbase.RepositoriesTableName:
			switch iter.(type) {
			case nil:
				var f sql.Expression
				f, filters, err = filtersForTable(
					gitbase.RepositoriesTableName,
					filters,
					gitbase.RepositoriesSchema,
				)
				if err != nil {
					return nil, err
				}
				iter = gitbase.NewAllReposIter(f)
			default:
				return nil, errInvalidIteratorChain.New("repositories", iter)
			}
		case gitbase.RemotesTableName:
			switch it := iter.(type) {
			case gitbase.ReposIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RepositoriesTableName,
					gitbase.RemotesTableName,
					filters,
					append(it.Schema(), gitbase.RemotesSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewRepoRemotesIter(it, f)
			case nil:
				var f sql.Expression
				f, filters, err = filtersForTable(
					gitbase.RemotesTableName,
					filters,
					gitbase.RemotesSchema,
				)
				if err != nil {
					return nil, err
				}
				iter = gitbase.NewAllRemotesIter(f)
			default:
				return nil, errInvalidIteratorChain.New("remotes", iter)
			}
		case gitbase.ReferencesTableName:
			switch it := iter.(type) {
			case gitbase.ReposIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RepositoriesTableName,
					gitbase.ReferencesTableName,
					filters,
					append(it.Schema(), gitbase.RefsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewRepoRefsIter(it, f, false)
			case gitbase.RemotesIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RemotesTableName,
					gitbase.ReferencesTableName,
					filters,
					append(it.Schema(), gitbase.RefsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewRemoteRefsIter(it, f)
			case nil:
				var f sql.Expression
				f, filters, err = filtersForTable(
					gitbase.ReferencesTableName,
					filters,
					gitbase.RefsSchema,
				)
				if err != nil {
					return nil, err
				}
				iter = gitbase.NewAllRefsIter(f, false)
			default:
				return nil, errInvalidIteratorChain.New("refs", iter)
			}
		case gitbase.RefCommitsTableName:
			switch it := iter.(type) {
			case gitbase.ReposIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RepositoriesTableName,
					gitbase.RefCommitsTableName,
					filters,
					append(it.Schema(), gitbase.RefCommitsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewRefRefCommitsIter(gitbase.NewRepoRefsIter(it, nil, true), f)
			case gitbase.RefsIter:
				var f sql.Expression
				onlyHead := hasRefHEADFilter(filters)
				f, filters, err = filtersForJoin(
					gitbase.ReferencesTableName,
					gitbase.RefCommitsTableName,
					filters,
					append(it.Schema(), gitbase.RefCommitsSchema...),
				)
				if err != nil {
					return nil, err
				}

				if onlyHead {
					iter = gitbase.NewRefHeadRefCommitsIter(it, f)
				} else {
					iter = gitbase.NewRefRefCommitsIter(it, f)
				}
			case nil:
				var f sql.Expression
				f, filters, err = filtersForTable(
					gitbase.RefCommitsTableName,
					filters,
					gitbase.RefCommitsSchema,
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewAllRefCommitsIter(f)
			default:
				return nil, errInvalidIteratorChain.New("ref_commits", iter)
			}
		case gitbase.CommitsTableName:
			switch it := iter.(type) {
			case gitbase.ReposIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RepositoriesTableName,
					gitbase.CommitsTableName,
					filters,
					append(it.Schema(), gitbase.CommitsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewRepoCommitsIter(it, f)
			case gitbase.RefsIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.ReferencesTableName,
					gitbase.CommitsTableName,
					filters,
					append(it.Schema(), gitbase.CommitsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewRefHEADCommitsIter(it, f, false)
			case gitbase.CommitsIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RefCommitsTableName,
					gitbase.CommitsTableName,
					filters,
					append(it.Schema(), gitbase.CommitsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewRefCommitCommitsIter(it, f)
			case nil:
				var f sql.Expression
				f, filters, err = filtersForTable(
					gitbase.CommitsTableName,
					filters,
					gitbase.CommitsSchema,
				)
				if err != nil {
					return nil, err
				}
				iter = gitbase.NewAllCommitsIter(f, false)
			default:
				return nil, errInvalidIteratorChain.New("commits", iter)
			}
		case gitbase.CommitTreesTableName:
			switch it := iter.(type) {
			case gitbase.ReposIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RepositoriesTableName,
					gitbase.TreeEntriesTableName,
					filters,
					append(it.Schema(), gitbase.TreeEntriesSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewRepoTreeEntriesIter(it, f)
			case gitbase.RefsIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.ReferencesTableName,
					gitbase.CommitTreesTableName,
					filters,
					append(it.Schema(), gitbase.CommitTreesSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewCommitTreesIter(
					gitbase.NewRefHEADCommitsIter(it, nil, true),
					f,
					false,
				)
			case gitbase.RefCommitsIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RefCommitsTableName,
					gitbase.CommitTreesTableName,
					filters,
					append(it.Schema(), gitbase.CommitTreesSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewCommitTreesIter(it, f, false)
			case gitbase.CommitsIter:
				onlyMainTree := hasMainTreeFilter(filters)
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.CommitsTableName,
					gitbase.CommitTreesTableName,
					filters,
					append(it.Schema(), gitbase.CommitTreesSchema...),
				)
				if err != nil {
					return nil, err
				}

				if onlyMainTree {
					iter = gitbase.NewCommitMainTreeIter(it, f, false)
				} else {
					iter = gitbase.NewCommitTreesIter(it, f, false)
				}
			case nil:
				var f sql.Expression
				f, filters, err = filtersForTable(
					gitbase.CommitTreesTableName,
					filters,
					gitbase.CommitTreesSchema,
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewAllCommitTreesIter(f)
			default:
				return nil, errInvalidIteratorChain.New("commit_trees", iter)
			}
		case gitbase.CommitBlobsTableName:
			switch it := iter.(type) {
			case gitbase.RefsIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.ReferencesTableName,
					gitbase.CommitBlobsTableName,
					filters,
					append(it.Schema(), gitbase.CommitBlobsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewCommitBlobsIter(
					gitbase.NewRefHEADCommitsIter(it, nil, true),
					f,
				)
			case gitbase.RefCommitsIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RefCommitsTableName,
					gitbase.CommitBlobsTableName,
					filters,
					append(it.Schema(), gitbase.CommitBlobsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewCommitBlobsIter(it, f)
			case gitbase.CommitsIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.CommitsTableName,
					gitbase.CommitBlobsTableName,
					filters,
					append(it.Schema(), gitbase.CommitBlobsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewCommitBlobsIter(it, f)
			case nil:
				var f sql.Expression
				f, filters, err = filtersForTable(
					gitbase.CommitBlobsTableName,
					filters,
					gitbase.CommitBlobsSchema,
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewAllCommitBlobsIter(f)
			default:
				return nil, errInvalidIteratorChain.New("commit_blobs", iter)
			}
		case gitbase.TreeEntriesTableName:
			switch it := iter.(type) {
			case gitbase.ReposIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RepositoriesTableName,
					gitbase.TreeEntriesTableName,
					filters,
					append(it.Schema(), gitbase.TreeEntriesSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewRepoTreeEntriesIter(it, f)
			case gitbase.CommitsIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.CommitsTableName,
					gitbase.TreeEntriesTableName,
					filters,
					append(it.Schema(), gitbase.TreeEntriesSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewTreeTreeEntriesIter(
					gitbase.NewCommitMainTreeIter(it, nil, true),
					f,
					false,
				)
			case gitbase.TreesIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.CommitTreesTableName,
					gitbase.TreeEntriesTableName,
					filters,
					append(it.Schema(), gitbase.TreeEntriesSchema...),
				)

				iter = gitbase.NewTreeTreeEntriesIter(it, f, false)
			case nil:
				var f sql.Expression
				f, filters, err = filtersForTable(
					gitbase.TreeEntriesTableName,
					filters,
					gitbase.TreeEntriesSchema,
				)
				if err != nil {
					return nil, err
				}
				iter = gitbase.NewAllTreeEntriesIter(f)
			default:
				return nil, errInvalidIteratorChain.New("tree_entries", iter)
			}
		case gitbase.BlobsTableName:
			var readContent bool
			for _, e := range columns {
				if containsField(e, gitbase.BlobsTableName, "blob_content") {
					readContent = true
					break
				}
			}

			switch it := iter.(type) {
			case gitbase.ReposIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.RepositoriesTableName,
					gitbase.BlobsTableName,
					filters,
					append(it.Schema(), gitbase.BlobsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewRepoBlobsIter(it, f, readContent)
			case gitbase.FilesIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.CommitBlobsTableName,
					gitbase.BlobsTableName,
					filters,
					append(it.Schema(), gitbase.BlobsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewCommitBlobBlobsIter(it, f, readContent)
			case gitbase.TreeEntriesIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.TreeEntriesTableName,
					gitbase.BlobsTableName,
					filters,
					append(it.Schema(), gitbase.BlobsSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewTreeEntryBlobsIter(it, f, readContent)
			default:
				return nil, errInvalidIteratorChain.New("blobs", iter)
			}
		}
	}

	var originalSchema sql.Schema
	for _, t := range tables {
		originalSchema = append(originalSchema, t.Schema()...)
	}

	mapping := buildSchemaMapping(originalSchema, iter.Schema())

	var node sql.Node = newSquashedTable(iter, mapping, tableNames...)

	if len(filters) > 0 {
		f, err := fixFieldIndexes(expression.JoinAnd(filters...), iter.Schema())
		if err != nil {
			return nil, err
		}
		node = plan.NewFilter(f, node)
	}

	return node, nil
}

// buildSchemaMapping returns a mapping to convert the actual schema into the
// original schema. If both schemas are equal, nil will be returned.
func buildSchemaMapping(original, actual sql.Schema) []int {
	var result = make([]int, len(original))
	var sameSchemas = true

	for i, col := range original {
		for j, actualCol := range actual {
			if col.Source == actualCol.Source && col.Name == actualCol.Name {
				result[i] = j
				if i != j {
					sameSchemas = false
				}
				break
			}
		}
	}

	if sameSchemas {
		return nil
	}

	return result
}

var tableHierarchy = []string{
	gitbase.RepositoriesTableName,
	gitbase.RemotesTableName,
	gitbase.ReferencesTableName,
	gitbase.RefCommitsTableName,
	gitbase.CommitsTableName,
	gitbase.CommitTreesTableName,
	gitbase.TreeEntriesTableName,
	gitbase.CommitBlobsTableName,
	gitbase.BlobsTableName,
}

func orderedTableNames(tables []sql.Table) []string {
	var tableNames []string
	for _, n := range tableHierarchy {
		for _, t := range tables {
			if n == t.Name() {
				tableNames = append(tableNames, n)
				break
			}
		}
	}
	return tableNames
}

func isJoinSquashable(join *plan.InnerJoin) bool {
	return isJoinLeafSquashable(join.Left) &&
		isJoinLeafSquashable(join.Right) &&
		isJoinCondSquashable(join)
}

func isJoinLeafSquashable(node sql.Node) bool {
	var hasUnsquashableNodes, hasSquashableTables bool
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.PushdownProjectionAndFiltersTable:
			_, ok := node.PushdownProjectionAndFiltersTable.(gitbase.Table)
			if !ok {
				hasUnsquashableNodes = true
				return false
			}
			hasSquashableTables = true
		case *joinedTables:
			hasSquashableTables = true
		case *plan.InnerJoin:
			if !isJoinLeafSquashable(node.Left) ||
				!isJoinLeafSquashable(node.Right) {
				hasUnsquashableNodes = true
				return false
			}
			hasSquashableTables = true
		case *plan.Project, *plan.Filter, *plan.TableAlias, nil:
		default:
			hasUnsquashableNodes = true
			return false
		}

		return true
	})

	return !hasUnsquashableNodes && hasSquashableTables
}

func isJoinCondSquashable(join *plan.InnerJoin) bool {
	leftTables := findLeafTables(join.Left)
	rightTables := findLeafTables(join.Right)

	if len(rightTables) == 1 {
		leftTables, rightTables = rightTables, leftTables
	} else if len(leftTables) != 1 {
		return false
	}

	lt := leftTables[0]
	for _, rt := range rightTables {
		if lt == rt {
			continue
		}

		t1, t2 := orderedTablePair(lt, rt)
		if hasChainableJoinCondition(join.Cond, t1, t2) {
			return true
		}
	}
	return false
}

func orderedTablePair(t1, t2 string) (string, string) {
	idx1 := tableHierarchyIndex(t1)
	idx2 := tableHierarchyIndex(t2)
	if idx1 < idx2 {
		return t1, t2
	}
	return t2, t1
}

func tableHierarchyIndex(t string) int {
	for i, th := range tableHierarchy {
		if th == t {
			return i
		}
	}
	return -1
}

func findLeafTables(n sql.Node) []string {
	var tables []string
	plan.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *joinedTables:
			tables = orderedTableNames(n.tables)
			return false
		case *plan.PushdownProjectionAndFiltersTable:
			tables = []string{n.Name()}
			return false
		default:
			return true
		}
	})
	return tables
}

func exprToFilters(expr sql.Expression) (filters []sql.Expression) {
	if expr, ok := expr.(*expression.And); ok {
		return append(exprToFilters(expr.Left), exprToFilters(expr.Right)...)
	}

	return []sql.Expression{expr}
}

func filtersForTables(
	filters []sql.Expression,
	tables ...string,
) (tableFilters []sql.Expression, remaining []sql.Expression) {
	for _, f := range filters {
		valid := true
		expression.Inspect(f, func(e sql.Expression) bool {
			gf, ok := e.(*expression.GetField)
			if ok && !stringInSlice(tables, gf.Table()) {
				valid = false
				return false
			}

			return true
		})

		if valid {
			tableFilters = append(tableFilters, f)
		} else {
			remaining = append(remaining, f)
		}
	}

	return
}

func stringInSlice(strs []string, str string) bool {
	for _, s := range strs {
		if s == str {
			return true
		}
	}
	return false
}

type squashedTable struct {
	iter           gitbase.ChainableIter
	tables         []string
	schemaMappings []int
	schema         sql.Schema
}

func newSquashedTable(iter gitbase.ChainableIter, mapping []int, tables ...string) *squashedTable {
	return &squashedTable{iter, tables, mapping, nil}
}

var _ sql.Node = (*squashedTable)(nil)

func (t *squashedTable) Schema() sql.Schema {
	if len(t.schemaMappings) == 0 {
		return t.iter.Schema()
	}

	if t.schema == nil {
		schema := t.iter.Schema()
		t.schema = make(sql.Schema, len(schema))
		for i, j := range t.schemaMappings {
			t.schema[i] = schema[j]
		}
	}
	return t.schema
}
func (t *squashedTable) Children() []sql.Node {
	return nil
}
func (t *squashedTable) Resolved() bool {
	return true
}
func (t *squashedTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.SquashedTable")
	iter, err := gitbase.NewRowRepoIter(ctx, gitbase.NewChainableRowRepoIter(ctx, t.iter))
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(
		span,
		&schemaMapperIter{iter, t.schemaMappings},
	), nil
}
func (t *squashedTable) String() string {
	return fmt.Sprintf("SquashedTable(%s)", strings.Join(t.tables, ", "))
}
func (t *squashedTable) TransformExpressionsUp(sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}
func (t *squashedTable) TransformUp(fn sql.TransformNodeFunc) (sql.Node, error) {
	return t, nil
}

type schemaMapperIter struct {
	iter     sql.RowIter
	mappings []int
}

func (i schemaMapperIter) Next() (sql.Row, error) {
	childRow, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	if len(i.mappings) == 0 {
		return childRow, nil
	}

	var row = make(sql.Row, len(i.mappings))
	for i, j := range i.mappings {
		row[i] = childRow[j]
	}

	return row, nil
}
func (i schemaMapperIter) Close() error { return i.iter.Close() }

type joinedTables struct {
	tables  []sql.Table
	columns []sql.Expression
	filters []sql.Expression
}

var _ sql.Table = (*joinedTables)(nil)

func (t *joinedTables) Name() string {
	panic("joinedTables is a placeholder node, but Name was called")
}
func (t *joinedTables) Schema() sql.Schema {
	panic("joinedTables is a placeholder node, but Schema was called")
}
func (t *joinedTables) Children() []sql.Node {
	return nil
}
func (t *joinedTables) Resolved() bool {
	panic("joinedTables is a placeholder node, but Resolved was called")
}
func (t *joinedTables) RowIter(*sql.Context) (sql.RowIter, error) {
	panic("joinedTables is a placeholder node, but RowIter was called")
}
func (t *joinedTables) String() string {
	panic("joinedTables is a placeholder node, but String was called")
}
func (t *joinedTables) TransformExpressionsUp(sql.TransformExprFunc) (sql.Node, error) {
	panic("joinedTables is a placeholder node, but TransformExpressionsUp was called")
}
func (t *joinedTables) TransformUp(fn sql.TransformNodeFunc) (sql.Node, error) {
	return fn(t)
}

// filtersForJoin returns the filters (already joined as one expression) for
// the given tables, that is, all filters that have columns from t1 and/or t2.
// The returned filter already has the column indexes fixed using the given
// schema.
// It will return the filters and the remaining filters, that is,
// filters - returned filters.
// The two tables given MUST be sorted by hierarchy. You can't pass refs first
// and the repositories, they should be passed as repositories and refs.
func filtersForJoin(
	t1, t2 string,
	filters []sql.Expression,
	schema sql.Schema,
) (filter sql.Expression, remaining []sql.Expression, err error) {
	filters, remaining = filtersForTables(filters, t1, t2)
	filter = expression.JoinAnd(removeRedundantFilters(filters, t1, t2)...)
	filter, err = fixFieldIndexes(filter, schema)
	return
}

// filtersForTable returns the filters (already joined as one expression) for
// the given table, that is, all filters that have only references to columns
// of that table.
// It will also return the remaining filters, that is, filters - returned.
func filtersForTable(
	t string,
	filters []sql.Expression,
	schema sql.Schema,
) (filter sql.Expression, remaining []sql.Expression, err error) {
	filters, remaining = filtersForTables(filters, t)
	if len(filters) == 0 {
		return nil, remaining, nil
	}

	filter, err = fixFieldIndexes(expression.JoinAnd(filters...), schema)
	return
}

// removeRedundantFilters returns the given list of filters with all those that
// are redundant for joining t1 with t2 removed.
// t1 MUST be higher in the table hierarchy than t2.
func removeRedundantFilters(filters []sql.Expression, t1, t2 string) []sql.Expression {
	var result []sql.Expression
	for _, f := range filters {
		if !isRedundantFilter(f, t1, t2) {
			result = append(result, f)
		}
	}
	return result
}

func hasRefHEADFilter(filters []sql.Expression) bool {
	for _, f := range filters {
		ok := isEq(
			isCol(gitbase.ReferencesTableName, "commit_hash"),
			isCol(gitbase.CommitsTableName, "commit_hash"),
		)(f) || isEq(
			isCol(gitbase.ReferencesTableName, "commit_hash"),
			isCol(gitbase.RefCommitsTableName, "commit_hash"),
		)(f)
		if ok {
			return true
		}
	}
	return false
}

func hasMainTreeFilter(filters []sql.Expression) bool {
	for _, f := range filters {
		ok := isEq(
			isCol(gitbase.CommitsTableName, "tree_hash"),
			isCol(gitbase.CommitTreesTableName, "tree_hash"),
		)(f)
		if ok {
			return true
		}
	}
	return false
}

// hasChainableJoinCondition tells whether the given join condition contains
// any filter that can be used to build a chainable iterator.
// t1 with t2. t1 MUST be higher in the table hierarchy than t2.
func hasChainableJoinCondition(cond sql.Expression, t1, t2 string) bool {
	for _, f := range exprToFilters(cond) {
		if isRedundantFilter(f, t1, t2) {
			return true
		}
	}
	return false
}

// isRedundantFilter tells whether the given filter is redundant for joining
// t1 with t2. t1 MUST be higher in the table hierarchy than t2.
func isRedundantFilter(f sql.Expression, t1, t2 string) bool {
	switch true {
	case t1 == gitbase.RepositoriesTableName && t2 == gitbase.RemotesTableName:
		return isEq(
			isCol(gitbase.RepositoriesTableName, "repository_id"),
			isCol(gitbase.RemotesTableName, "repository_id"),
		)(f)
	case t1 == gitbase.RepositoriesTableName && t2 == gitbase.ReferencesTableName:
		return isEq(
			isCol(gitbase.RepositoriesTableName, "repository_id"),
			isCol(gitbase.ReferencesTableName, "repository_id"),
		)(f)
	case t1 == gitbase.RepositoriesTableName && t2 == gitbase.RefCommitsTableName:
		return isEq(
			isCol(gitbase.RepositoriesTableName, "repository_id"),
			isCol(gitbase.RefCommitsTableName, "repository_id"),
		)(f)
	case t1 == gitbase.RemotesTableName && t2 == gitbase.ReferencesTableName:
		return isEq(
			isCol(gitbase.RemotesTableName, "repository_id"),
			isCol(gitbase.ReferencesTableName, "repository_id"),
		)(f)
	case t1 == gitbase.ReferencesTableName && t2 == gitbase.CommitsTableName:
		return isEq(
			isCol(gitbase.ReferencesTableName, "commit_hash"),
			isCol(gitbase.CommitsTableName, "commit_hash"),
		)(f)
	case t1 == gitbase.ReferencesTableName && t2 == gitbase.RefCommitsTableName:
		return isEq(
			isCol(gitbase.ReferencesTableName, "ref_name"),
			isCol(gitbase.RefCommitsTableName, "ref_name"),
		)(f) ||
			isEq(
				isCol(gitbase.ReferencesTableName, "commit_hash"),
				isCol(gitbase.RefCommitsTableName, "commit_hash"),
			)(f)
	case t1 == gitbase.RefCommitsTableName && t2 == gitbase.CommitsTableName:
		return isEq(
			isCol(gitbase.RefCommitsTableName, "commit_hash"),
			isCol(gitbase.CommitsTableName, "commit_hash"),
		)(f)
	case t1 == gitbase.CommitsTableName && t2 == gitbase.TreeEntriesTableName:
		return isEq(
			isCol(gitbase.CommitsTableName, "tree_hash"),
			isCol(gitbase.TreeEntriesTableName, "tree_hash"),
		)(f)
	case t1 == gitbase.TreeEntriesTableName && t2 == gitbase.BlobsTableName:
		return isEq(
			isCol(gitbase.TreeEntriesTableName, "blob_hash"),
			isCol(gitbase.BlobsTableName, "blob_hash"),
		)(f)
	case t1 == gitbase.RepositoriesTableName && t2 == gitbase.CommitsTableName:
		return isEq(
			isCol(gitbase.RepositoriesTableName, "repository_id"),
			isCol(gitbase.CommitsTableName, "repository_id"),
		)(f)
	case t1 == gitbase.RepositoriesTableName && t2 == gitbase.TreeEntriesTableName:
		return isEq(
			isCol(gitbase.RepositoriesTableName, "repository_id"),
			isCol(gitbase.TreeEntriesTableName, "repository_id"),
		)(f)
	case t1 == gitbase.RepositoriesTableName && t2 == gitbase.BlobsTableName:
		return isEq(
			isCol(gitbase.RepositoriesTableName, "repository_id"),
			isCol(gitbase.BlobsTableName, "repository_id"),
		)(f)
	case t1 == gitbase.ReferencesTableName && t2 == gitbase.CommitTreesTableName:
		return isEq(
			isCol(gitbase.ReferencesTableName, "commit_hash"),
			isCol(gitbase.CommitTreesTableName, "commit_hash"),
		)(f)
	case t1 == gitbase.RefCommitsTableName && t2 == gitbase.CommitTreesTableName:
		return isEq(
			isCol(gitbase.RefCommitsTableName, "commit_hash"),
			isCol(gitbase.CommitTreesTableName, "commit_hash"),
		)(f)
	case t1 == gitbase.CommitsTableName && t2 == gitbase.CommitTreesTableName:
		return isEq(
			isCol(gitbase.CommitsTableName, "commit_hash"),
			isCol(gitbase.CommitTreesTableName, "commit_hash"),
		)(f) || isEq(
			isCol(gitbase.CommitsTableName, "tree_hash"),
			isCol(gitbase.CommitTreesTableName, "tree_hash"),
		)(f)
	case t1 == gitbase.CommitTreesTableName && t2 == gitbase.TreeEntriesTableName:
		return isEq(
			isCol(gitbase.CommitTreesTableName, "tree_hash"),
			isCol(gitbase.TreeEntriesTableName, "tree_hash"),
		)(f)
	case t1 == gitbase.ReferencesTableName && t2 == gitbase.CommitBlobsTableName:
		return isEq(
			isCol(gitbase.ReferencesTableName, "commit_hash"),
			isCol(gitbase.CommitBlobsTableName, "commit_hash"),
		)(f)
	case t1 == gitbase.RefCommitsTableName && t2 == gitbase.CommitBlobsTableName:
		return isEq(
			isCol(gitbase.RefCommitsTableName, "commit_hash"),
			isCol(gitbase.CommitBlobsTableName, "commit_hash"),
		)(f)
	case t1 == gitbase.CommitsTableName && t2 == gitbase.CommitBlobsTableName:
		return isEq(
			isCol(gitbase.CommitsTableName, "commit_hash"),
			isCol(gitbase.CommitBlobsTableName, "commit_hash"),
		)(f)
	case t1 == gitbase.CommitBlobsTableName && t2 == gitbase.BlobsTableName:
		return isEq(
			isCol(gitbase.CommitBlobsTableName, "blob_hash"),
			isCol(gitbase.BlobsTableName, "blob_hash"),
		)(f)
	}
	return false
}

type validator func(sql.Expression) bool

func isEq(left, right validator) validator {
	return func(e sql.Expression) bool {
		eq, ok := e.(*expression.Equals)
		if !ok {
			return false
		}

		return (left(eq.Left()) && right(eq.Right())) ||
			(right(eq.Left()) && left(eq.Right()))
	}
}

func isCol(table, name string) validator {
	return func(e sql.Expression) bool {
		gf, ok := e.(*expression.GetField)
		if !ok {
			return false
		}

		return gf.Table() == table && gf.Name() == name
	}
}

func isGte(left, right validator) validator {
	return func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.GreaterThanOrEqual:
			return left(e.Left()) && right(e.Right())
		case *expression.LessThanOrEqual:
			return left(e.Right()) && right(e.Left())
		default:
			return false
		}
	}
}

func isNum(n int64) validator {
	return func(e sql.Expression) bool {
		lit, ok := e.(*expression.Literal)
		if !ok {
			return false
		}

		result, err := lit.Eval(nil, nil)
		if err != nil {
			return false
		}

		num, ok := result.(int64)
		if !ok {
			return false
		}

		return num == n
	}
}

func containsField(e sql.Expression, table, name string) bool {
	var found bool
	expression.Inspect(e, func(e sql.Expression) bool {
		gf, ok := e.(*expression.GetField)
		if ok && gf.Table() == table && gf.Name() == name {
			found = true
			return false
		}
		return true
	})
	return found
}

func fixFieldIndexes(e sql.Expression, schema sql.Schema) (sql.Expression, error) {
	if e == nil {
		return nil, nil
	}
	return e.TransformUp(func(e sql.Expression) (sql.Expression, error) {
		gf, ok := e.(*expression.GetField)
		if !ok {
			return e, nil
		}

		for idx, col := range schema {
			if gf.Table() == col.Source && gf.Name() == col.Name {
				return expression.NewGetFieldWithTable(
					idx,
					gf.Type(),
					gf.Table(),
					gf.Name(),
					gf.IsNullable(),
				), nil
			}
		}

		return nil, analyzer.ErrColumnTableNotFound.New(gf.Table(), gf.Name())
	})
}
