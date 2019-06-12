package rule

import (
	"fmt"
	"reflect"

	"github.com/src-d/gitbase"
	errors "gopkg.in/src-d/go-errors.v1"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
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

	span, _ := ctx.Span("gitbase.SquashJoins")
	defer span.Finish()

	a.Log("squashing joins, node of type %T", n)

	projectSquashes := countProjectSquashes(n)

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

	n, err = n.TransformUp(func(n sql.Node) (sql.Node, error) {
		t, ok := n.(*joinedTables)
		if !ok {
			return n, nil
		}

		return buildSquashedTable(a, t.tables, t.filters, t.columns, t.indexes)
	})

	if err != nil {
		return nil, err
	}

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Project:
			if projectSquashes <= 0 {
				return n, nil
			}

			child, ok := n.Child.(*plan.Project)
			if !ok {
				return n, nil
			}

			squashedProject, ok := squashProjects(n, child)
			if !ok {
				return n, nil
			}

			projectSquashes--
			return squashedProject, nil
		case *plan.Filter:
			expr, err := fixFieldIndexes(n.Expression, n.Schema())
			if err != nil {
				return nil, err
			}

			return plan.NewFilter(expr, n.Child), nil
		default:
			return n, nil
		}
	})
}

func countProjectSquashes(n sql.Node) int {
	var squashableProjects int
	plan.Inspect(n, func(node sql.Node) bool {
		if project, ok := node.(*plan.Project); ok {
			if _, ok := project.Child.(*plan.InnerJoin); ok {
				squashableProjects++
			}
		}

		return true
	})

	return squashableProjects - 1
}

func squashProjects(parent, child *plan.Project) (sql.Node, bool) {
	projections := make([]sql.Expression, len(parent.Expressions()))
	schema := child.Child.Schema()

	// When squashing two projects, it's possible that the parent project has
	// a reference to a column defined in the child project.
	// For that reason, we need to gather the new columns that were defined
	// in the child project in order to replace the reference to those in the
	// parent project with the new column definition.
	var newColumns = make(map[string]sql.Expression)
	for _, e := range child.Expressions() {
		if _, ok := e.(*expression.GetField); !ok {
			var name string
			if n, ok := e.(sql.Nameable); ok {
				name = n.Name()
			} else {
				name = e.String()
			}

			newColumns[name] = e
		}
	}

	for i, e := range parent.Expressions() {
		if f, ok := e.(*expression.GetField); ok && f.Table() == "" {
			if expr, ok := newColumns[f.Name()]; ok {
				e = expr
			}
		}

		fe, err := fixFieldIndexes(e, schema)
		if err != nil {
			return nil, false
		}

		projections[i] = fe
	}

	return plan.NewProject(projections, child.Child), true
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

func joinTables(join *plan.InnerJoin) (*joinedTables, error) {
	var tables []sql.Table
	var filters []sql.Expression
	var columns []string
	var indexes = make(map[string]sql.IndexLookup)
	plan.Inspect(join, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.ResolvedTable:
			tables = append(tables, node.Table)
			if p, ok := node.Table.(sql.ProjectedTable); ok {
				columns = append(columns, p.Projection()...)
			}

			if f, ok := node.Table.(sql.FilteredTable); ok {
				filters = append(filters, f.Filters()...)
			}

			if i, ok := node.Table.(sql.IndexableTable); ok {
				indexes[node.Name()] = i.IndexLookup()
			}
		case *plan.InnerJoin:
			filters = append(filters, exprToFilters(node.Cond)...)
		case *joinedTables:
			tables = append(tables, node.tables...)
			columns = append(columns, node.columns...)
			filters = append(filters, node.filters...)
			for t, idx := range node.indexes {
				indexes[t] = idx
			}
		}

		return true
	})

	return &joinedTables{
		tables:  tables,
		filters: filters,
		columns: columns,
		indexes: indexes,
	}, nil
}

func rearrange(join *plan.InnerJoin, squashedTable *joinedTables) sql.Node {
	var projections []sql.Expression
	var filters []sql.Expression
	var parallelism int
	plan.Inspect(join, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Project:
			projections = append(projections, node.Projections...)
		case *plan.Filter:
			filters = append(filters, node.Expression)
		case *plan.Exchange:
			parallelism = node.Parallelism
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

	if parallelism > 1 {
		node = plan.NewExchange(parallelism, node)
	}

	return node
}

var errInvalidIteratorChain = errors.NewKind("invalid iterator to chain with %s: %T")

type unsquashableTable struct {
	table   sql.Table
	filters []sql.Expression
}

func buildSquashedTable(
	a *analyzer.Analyzer,
	tables []sql.Table,
	filters []sql.Expression,
	columns []string,
	indexes map[string]sql.IndexLookup,
) (sql.Node, error) {
	tableNames := orderedTableNames(tables)
	allFilters := filters

	firstTable := tableNames[0]
	var index sql.IndexLookup
	if idx, ok := indexes[firstTable]; ok {
		index = idx
	}

	tablesByName := make(map[string]sql.Table)
	for _, t := range tables {
		tablesByName[t.Name()] = t
	}

	var usedTables []string
	var unsquashable []unsquashableTable
	addUnsquashable := func(tableName string) {
		var f []sql.Expression
		f, filters = filtersForTables(filters, usedTables...)
		unsquashable = append(unsquashable, unsquashableTable{
			table:   tablesByName[tableName],
			filters: f,
		})
	}

	var iter gitbase.ChainableIter
	var squashedTables []string

	var err error
	for _, t := range tableNames {
		usedTables = append(usedTables, t)
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
				addUnsquashable(gitbase.RemotesTableName)
				continue
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

				if index == nil {
					iter = gitbase.NewAllRefsIter(f, false)
				} else {
					iter = gitbase.NewIndexRefsIter(f, index)
				}
			default:
				addUnsquashable(gitbase.ReferencesTableName)
				continue
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
				var refIt gitbase.RefsIter

				if index == nil {
					f, filters, err = transferFilters(
						filters,
						gitbase.RefCommitsTableName,
						gitbase.ReferencesTableName,
						gitbase.RefsSchema,
						"ref_name", "repository_id",
					)
					if err != nil {
						return nil, err
					}

					refIt = gitbase.NewAllRefsIter(f, true)
				}

				f, filters, err = filtersForTable(
					gitbase.RefCommitsTableName,
					filters,
					gitbase.RefCommitsSchema,
				)
				if err != nil {
					return nil, err
				}

				if index != nil {
					iter = gitbase.NewIndexRefCommitsIter(index, f)
				} else {
					iter = gitbase.NewRefRefCommitsIter(refIt, f)
				}
			default:
				addUnsquashable(gitbase.RefCommitsTableName)
				continue
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

				if index != nil {
					iter = gitbase.NewIndexCommitsIter(index, f)
				} else {
					iter = gitbase.NewAllCommitsIter(f, false)
				}
			default:
				addUnsquashable(gitbase.CommitsTableName)
				continue
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

				if index != nil {
					iter = gitbase.NewIndexCommitTreesIter(index, f)
				} else {
					iter = gitbase.NewAllCommitTreesIter(f)
				}
			default:
				addUnsquashable(gitbase.CommitTreesTableName)
				continue
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

				if index != nil {
					iter = gitbase.NewIndexCommitBlobsIter(index, f)
				} else {
					iter = gitbase.NewAllCommitBlobsIter(f)
				}
			default:
				addUnsquashable(gitbase.CommitBlobsTableName)
				continue
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
				if err != nil {
					return nil, err
				}

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

				if index != nil {
					iter = gitbase.NewIndexTreeEntriesIter(index, f)
				} else {
					iter = gitbase.NewAllTreeEntriesIter(f)
				}
			default:
				addUnsquashable(gitbase.TreeEntriesTableName)
				continue
			}
		case gitbase.BlobsTableName:
			readContent := stringInSlice(columns, "blob_content")

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
			case gitbase.BlobsIter:
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
				addUnsquashable(gitbase.BlobsTableName)
				continue
			}
		case gitbase.CommitFilesTableName:
			switch it := iter.(type) {
			case gitbase.RefsIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.ReferencesTableName,
					gitbase.CommitFilesTableName,
					filters,
					append(it.Schema(), gitbase.CommitFilesSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewCommitFilesIter(gitbase.NewRefHEADCommitsIter(it, nil, true), f)
			case gitbase.CommitsIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.CommitsTableName,
					gitbase.CommitFilesTableName,
					filters,
					append(it.Schema(), gitbase.CommitFilesSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewCommitFilesIter(it, f)
			case nil:
				var f sql.Expression
				f, filters, err = filtersForTable(
					gitbase.CommitFilesTableName,
					filters,
					gitbase.CommitFilesSchema,
				)
				if err != nil {
					return nil, err
				}

				if index != nil {
					iter = gitbase.NewIndexCommitFilesIter(index, f)
				} else {
					iter = gitbase.NewAllCommitFilesIter(f)
				}
			default:
				addUnsquashable(gitbase.CommitFilesTableName)
				continue
			}
		case gitbase.FilesTableName:
			readContent := stringInSlice(columns, "blob_content")

			switch it := iter.(type) {
			case gitbase.FilesIter:
				var f sql.Expression
				f, filters, err = filtersForJoin(
					gitbase.CommitFilesTableName,
					gitbase.FilesTableName,
					filters,
					append(it.Schema(), gitbase.FilesSchema...),
				)
				if err != nil {
					return nil, err
				}

				iter = gitbase.NewCommitFileFilesIter(it, f, readContent)
			default:
				addUnsquashable(gitbase.FilesTableName)
				continue
			}
		}

		squashedTables = append(squashedTables, t)
	}

	var originalSchema sql.Schema
	for _, t := range tables {
		originalSchema = append(originalSchema, t.Schema()...)
	}

	mapping := buildSchemaMapping(originalSchema, iter.Schema())

	var indexedTables []string
	if index != nil {
		indexedTables = []string{firstTable}
	}

	var squashMapping = mapping
	if len(unsquashable) > 0 {
		squashMapping = nil
	}

	var nonSquashedFilters []sql.Expression
	for _, t := range unsquashable {
		nonSquashedFilters = append(nonSquashedFilters, t.filters...)
	}
	nonSquashedFilters = append(nonSquashedFilters, filters...)
	squashedFilters := filterDiff(allFilters, nonSquashedFilters)

	table := gitbase.NewSquashedTable(
		iter,
		squashMapping,
		squashedFilters,
		indexedTables,
		squashedTables...,
	)
	var node sql.Node = plan.NewResolvedTable(table)

	if len(unsquashable) > 0 {
		for _, t := range unsquashable {
			var table sql.Node = plan.NewResolvedTable(t.table)
			if a.Parallelism > 1 {
				table = plan.NewExchange(a.Parallelism, table)
			}

			if len(t.filters) > 0 {
				f, err := fixFieldIndexes(
					expression.JoinAnd(t.filters...),
					append(t.table.Schema(), node.Schema()...),
				)
				if err != nil {
					return nil, err
				}

				node = plan.NewInnerJoin(
					table,
					node,
					f,
				)
			} else {
				node = plan.NewCrossJoin(table, node)
			}
		}

		node, err = projectSchema(node, originalSchema)
		if err != nil {
			return nil, err
		}
	}

	if len(filters) > 0 {
		f, err := fixFieldIndexes(expression.JoinAnd(filters...), originalSchema)
		if err != nil {
			return nil, err
		}
		node = plan.NewFilter(f, node)
	}

	return node, nil
}

var errUnsquashableFieldNotFound = errors.NewKind("unable to unsquash table, column %s.%s not found")

// projectSchema wraps the node in a Project node that has the same schema as
// the one provided.
func projectSchema(node sql.Node, schema sql.Schema) (sql.Node, error) {
	if node.Schema().Equals(schema) {
		return node, nil
	}

	var columnIndexes = make(map[string]int)
	for i, col := range node.Schema() {
		columnIndexes[fmt.Sprintf("%s.%s", col.Source, col.Name)] = i + 1
	}

	var project = make([]sql.Expression, len(schema))
	for i, col := range schema {
		idx := columnIndexes[fmt.Sprintf("%s.%s", col.Source, col.Name)]
		if idx <= 0 {
			return nil, errUnsquashableFieldNotFound.New(col.Source, col.Name)
		}

		project[i] = expression.NewGetFieldWithTable(
			idx-1,
			col.Type,
			col.Source,
			col.Name,
			col.Nullable,
		)
	}

	return plan.NewProject(project, node), nil
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
	gitbase.CommitFilesTableName,
	gitbase.FilesTableName,
}

func orderedTableNames(tables []sql.Table) []string {
	var tableNames []string
	for _, n := range tableHierarchy {
		for _, t := range tables {
			nameable, ok := t.(sql.Nameable)
			if !ok {
				continue
			}

			if n == nameable.Name() {
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
		case *plan.ResolvedTable:
			_, ok := node.Table.(gitbase.Squashable)
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
		case *plan.Project, *plan.Filter, *plan.TableAlias, *plan.Exchange, nil:
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

	var squashedTables []string
	if len(rightTables) == 1 {
		squashedTables = findSquashedTables(join.Left)
		leftTables, rightTables = rightTables, leftTables
	} else if len(leftTables) != 1 {
		return false
	} else {
		squashedTables = findSquashedTables(join.Right)
	}

	lt := leftTables[0]
	for _, rt := range rightTables {
		if lt == rt {
			continue
		}

		var cond = join.Cond
		// if the right table is squashed, we might need to rewrite some column
		// tables in order to find the condition, since natural joins deduplicate
		// columns with the same name.
		if stringInSlice(squashedTables, rt) {
			c, err := join.Cond.TransformUp(func(e sql.Expression) (sql.Expression, error) {
				gf, ok := e.(*expression.GetField)
				if ok && gf.Table() != rt && gf.Table() != lt {
					if tableHasColumn(rt, gf.Name()) {
						return expression.NewGetFieldWithTable(
							gf.Index(),
							gf.Type(),
							rt,
							gf.Name(),
							gf.IsNullable(),
						), nil
					}
				}
				return e, nil
			})
			if err == nil {
				cond = c
			}
		}

		t1, t2 := orderedTablePair(lt, rt)
		if hasChainableJoinCondition(cond, t1, t2) {
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
		case *plan.ResolvedTable:
			tables = []string{n.Name()}
			return false
		default:
			return true
		}
	})
	return tables
}

func findSquashedTables(n sql.Node) []string {
	var tables []string
	plan.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *joinedTables:
			tables = orderedTableNames(n.tables)
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

func filtersForColumns(
	filters []sql.Expression,
	table string,
	columns ...string,
) (columnFilters []sql.Expression, remaining []sql.Expression) {
	var fTable []sql.Expression
	fTable, remaining = filtersForTables(filters, table)

	for _, f := range fTable {
		valid := true
		expression.Inspect(f, func(e sql.Expression) bool {
			gf, ok := e.(*expression.GetField)
			if ok && !stringInSlice(columns, gf.Name()) {
				valid = false
				return false
			}

			return true
		})

		if valid {
			columnFilters = append(columnFilters, f)
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

type joinedTables struct {
	tables  []sql.Table
	columns []string
	filters []sql.Expression
	indexes map[string]sql.IndexLookup
}

var _ sql.Table = (*joinedTables)(nil)

func (t *joinedTables) Name() string {
	panic("joinedTables is a placeholder node, but Name was called")
}
func (t *joinedTables) Schema() sql.Schema {
	panic("joinedTables is a placeholder node, but Schema was called")
}
func (t *joinedTables) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	panic("joinedTables is a placeholder node, but Partitions was called")
}
func (t *joinedTables) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	panic("joinedTables is a placeholder node, but PartitionRows was called")
}
func (t *joinedTables) String() string {
	panic("joinedTables is a placeholder node, but String was called")
}
func (t *joinedTables) RowIter(*sql.Context) (sql.RowIter, error) {
	panic("joinedTables is a placeholder node, but RowIter was called")
}
func (t *joinedTables) Children() []sql.Node { return nil }
func (t *joinedTables) Resolved() bool       { return true }
func (t *joinedTables) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}
func (t *joinedTables) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
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
	for _, f := range removeRedundantCompoundFilters(filters, t1, t2) {
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
	filters := exprToFilters(cond)
	if hasRedundantCompoundFilter(filters, t1, t2) {
		return true
	}

	for _, f := range exprToFilters(cond) {
		if isRedundantFilter(f, t1, t2) {
			return true
		}
	}
	return false
}

var (
	isTreeHashFilter = isEq(
		isCol(gitbase.CommitFilesTableName, "tree_hash"),
		isCol(gitbase.FilesTableName, "tree_hash"),
	)

	isFilePathFilter = isEq(
		isCol(gitbase.CommitFilesTableName, "file_path"),
		isCol(gitbase.FilesTableName, "file_path"),
	)

	isBlobHashFilter = isEq(
		isCol(gitbase.CommitFilesTableName, "blob_hash"),
		isCol(gitbase.FilesTableName, "blob_hash"),
	)
)

// hasRedundantCompoindFilter returns whether there is any compound redundant
// filter in the given set of filters for joining t1 with t2. t1 MUST be higher
// in the table hierarchy than t2.
// A compound redundant filter is a set of multiple filters that are required
// for a table t1 to be joined with a table t2.
func hasRedundantCompoundFilter(filters []sql.Expression, t1, t2 string) bool {
	if t1 == gitbase.CommitFilesTableName && t2 == gitbase.FilesTableName {
		var treeHash, filePath, blobHash bool
		for _, f := range filters {
			if isFilePathFilter(f) {
				filePath = true
			} else if isBlobHashFilter(f) {
				blobHash = true
			} else if isTreeHashFilter(f) {
				treeHash = true
			}
		}

		return filePath && treeHash && blobHash
	}

	return false
}

// removeRedundantCompoundFilters removes from the given slice of filters the
// ones that correspond to compound redundant filters for joining table t1 with
// table t2. t1 must be higher in the table hierarchy than t2.
func removeRedundantCompoundFilters(
	filters []sql.Expression,
	t1, t2 string,
) []sql.Expression {
	if t1 == gitbase.CommitFilesTableName && t2 == gitbase.FilesTableName {
		var result []sql.Expression
		for _, f := range filters {
			if !isFilePathFilter(f) && !isBlobHashFilter(f) && !isTreeHashFilter(f) {
				result = append(result, f)
			}
		}

		return result
	}

	return filters
}

// isRedundantFilter tells whether the given filter is redundant for joining
// t1 with t2. t1 MUST be higher in the table hierarchy than t2.
func isRedundantFilter(f sql.Expression, t1, t2 string) bool {
	switch {
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
	case t1 == gitbase.CommitsTableName && t2 == gitbase.CommitFilesTableName:
		return isEq(
			isCol(gitbase.CommitsTableName, "commit_hash"),
			isCol(gitbase.CommitFilesTableName, "commit_hash"),
		)(f)
	case t1 == gitbase.RefCommitsTableName && t2 == gitbase.CommitFilesTableName:
		return isEq(
			isCol(gitbase.RefCommitsTableName, "commit_hash"),
			isCol(gitbase.CommitFilesTableName, "commit_hash"),
		)(f)
	case t1 == gitbase.ReferencesTableName && t2 == gitbase.CommitFilesTableName:
		return isEq(
			isCol(gitbase.ReferencesTableName, "commit_hash"),
			isCol(gitbase.CommitFilesTableName, "commit_hash"),
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

func transferFilters(
	filters []sql.Expression,
	from, to string,
	schema sql.Schema,
	columns ...string,
) (sql.Expression, []sql.Expression, error) {
	f, r := filtersForColumns(filters, from, columns...)
	fixed, err := fixFieldTable(expression.JoinAnd(f...), to, schema)
	if err != nil {
		return nil, nil, err
	}

	return fixed, r, err
}

func fixFieldTable(
	e sql.Expression,
	table string,
	schema sql.Schema,
) (sql.Expression, error) {
	if e == nil {
		return nil, nil
	}
	return e.TransformUp(func(e sql.Expression) (sql.Expression, error) {
		gf, ok := e.(*expression.GetField)
		if !ok {
			return e, nil
		}

		for idx, col := range schema {
			if gf.Name() == col.Name {
				return expression.NewGetFieldWithTable(
					idx,
					gf.Type(),
					table,
					gf.Name(),
					gf.IsNullable(),
				), nil
			}
		}

		return nil, analyzer.ErrColumnTableNotFound.New(gf.Table(), gf.Name())
	})
}

func filterDiff(a, b []sql.Expression) []sql.Expression {
	var result []sql.Expression

	for _, fa := range a {
		var found bool
		for _, fb := range b {
			if reflect.DeepEqual(fa, fb) {
				found = true
				break
			}
		}

		if !found {
			result = append(result, fa)
		}
	}

	return result
}

func tableHasColumn(t, col string) bool {
	return tableSchema(t).Contains(col, t)
}

func tableSchema(t string) sql.Schema {
	switch t {
	case gitbase.RepositoriesTableName:
		return gitbase.RepositoriesSchema
	case gitbase.ReferencesTableName:
		return gitbase.RefsSchema
	case gitbase.RemotesTableName:
		return gitbase.RemotesSchema
	case gitbase.RefCommitsTableName:
		return gitbase.RefCommitsSchema
	case gitbase.CommitsTableName:
		return gitbase.CommitsSchema
	case gitbase.CommitTreesTableName:
		return gitbase.CommitTreesSchema
	case gitbase.CommitBlobsTableName:
		return gitbase.CommitBlobsSchema
	case gitbase.CommitFilesTableName:
		return gitbase.CommitFilesSchema
	case gitbase.TreeEntriesTableName:
		return gitbase.TreeEntriesSchema
	case gitbase.BlobsTableName:
		return gitbase.BlobsSchema
	case gitbase.FilesTableName:
		return gitbase.FilesSchema
	default:
		return nil
	}
}
