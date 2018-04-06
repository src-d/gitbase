package rule

import (
	"context"
	"testing"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/src-d/gitbase"
	"github.com/src-d/gitbase/internal/function"
	"github.com/stretchr/testify/require"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestSquashJoins(t *testing.T) {
	require := require.New(t)

	tables := gitbase.NewDatabase("").Tables()

	node := plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewFilter(
			lit(2),
			plan.NewInnerJoin(
				plan.NewPushdownProjectionAndFiltersTable(
					nil, nil,
					tables[gitbase.CommitsTableName].(sql.PushdownProjectionAndFiltersTable),
				),
				plan.NewInnerJoin(
					plan.NewPushdownProjectionAndFiltersTable(
						nil, nil,
						tables[gitbase.RepositoriesTableName].(sql.PushdownProjectionAndFiltersTable),
					),
					plan.NewPushdownProjectionAndFiltersTable(
						nil, nil,
						tables[gitbase.ReferencesTableName].(sql.PushdownProjectionAndFiltersTable),
					),
					and(
						eq(
							col(0, gitbase.RepositoriesTableName, "id"),
							col(0, gitbase.ReferencesTableName, "repository_id"),
						),
						lit(4),
					),
				),
				and(
					eq(
						col(0, gitbase.ReferencesTableName, "hash"),
						col(0, gitbase.CommitsTableName, "hash"),
					),
					lit(3),
				),
			),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewFilter(
			lit(2),
			newSquashedTable(
				gitbase.NewRefHEADCommitsIter(
					gitbase.NewRepoRefsIter(
						gitbase.NewAllReposIter(
							and(
								lit(3),
								lit(4),
							),
						),
						nil,
					),
					nil,
					false,
				),
				[]int{4, 5, 6, 7, 8, 9, 10, 11, 12, 0, 1, 2, 3},
				gitbase.RepositoriesTableName,
				gitbase.ReferencesTableName,
				gitbase.CommitsTableName,
			),
		),
	)

	result, err := SquashJoins(sql.NewContext(
		context.TODO(),
		sql.NewBaseSession(),
		opentracing.NoopTracer{},
	), analyzer.New(nil), node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestSquashJoinsUnsquashable(t *testing.T) {
	require := require.New(t)

	tables := gitbase.NewDatabase("").Tables()

	node := plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewInnerJoin(
			plan.NewPushdownProjectionAndFiltersTable(
				nil, nil,
				tables[gitbase.RepositoriesTableName].(sql.PushdownProjectionAndFiltersTable),
			),
			plan.NewLimit(1, plan.NewPushdownProjectionAndFiltersTable(
				nil, nil,
				tables[gitbase.ReferencesTableName].(sql.PushdownProjectionAndFiltersTable),
			)),
			lit(4),
		),
	)

	result, err := SquashJoins(sql.NewContext(
		context.TODO(),
		sql.NewBaseSession(),
		opentracing.NoopTracer{},
	), analyzer.New(nil), node)
	require.NoError(err)
	require.Equal(node, result)
}

func TestSquashJoinsPartial(t *testing.T) {
	require := require.New(t)

	tables := gitbase.NewDatabase("").Tables()

	node := plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewInnerJoin(
			plan.NewLimit(1, plan.NewPushdownProjectionAndFiltersTable(
				nil, nil,
				tables[gitbase.CommitsTableName].(sql.PushdownProjectionAndFiltersTable),
			)),
			plan.NewInnerJoin(
				plan.NewPushdownProjectionAndFiltersTable(
					nil, nil,
					tables[gitbase.RepositoriesTableName].(sql.PushdownProjectionAndFiltersTable),
				),
				plan.NewPushdownProjectionAndFiltersTable(
					nil, nil,
					tables[gitbase.ReferencesTableName].(sql.PushdownProjectionAndFiltersTable),
				),
				and(
					eq(
						col(0, gitbase.RepositoriesTableName, "id"),
						col(0, gitbase.ReferencesTableName, "repository_id"),
					),
					lit(4),
				),
			),
			lit(3),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewInnerJoin(
			plan.NewLimit(1, plan.NewPushdownProjectionAndFiltersTable(
				nil, nil,
				tables[gitbase.CommitsTableName].(sql.PushdownProjectionAndFiltersTable),
			)),
			newSquashedTable(
				gitbase.NewRepoRefsIter(
					gitbase.NewAllReposIter(lit(4)),
					nil,
				),
				nil,
				gitbase.RepositoriesTableName,
				gitbase.ReferencesTableName,
			),
			lit(3),
		),
	)

	result, err := SquashJoins(sql.NewContext(
		context.TODO(),
		sql.NewBaseSession(),
		opentracing.NoopTracer{},
	), analyzer.New(nil), node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestSquashJoinsSchema(t *testing.T) {
	require := require.New(t)

	tables := gitbase.NewDatabase("").Tables()

	node := plan.NewInnerJoin(
		plan.NewPushdownProjectionAndFiltersTable(
			nil, nil,
			tables[gitbase.CommitsTableName].(sql.PushdownProjectionAndFiltersTable),
		),
		plan.NewInnerJoin(
			plan.NewPushdownProjectionAndFiltersTable(
				nil, nil,
				tables[gitbase.RepositoriesTableName].(sql.PushdownProjectionAndFiltersTable),
			),
			plan.NewPushdownProjectionAndFiltersTable(
				nil, nil,
				tables[gitbase.ReferencesTableName].(sql.PushdownProjectionAndFiltersTable),
			),
			and(
				eq(
					col(0, gitbase.RepositoriesTableName, "id"),
					col(0, gitbase.ReferencesTableName, "repository_id"),
				),
				lit(4),
			),
		),
		and(
			eq(
				col(0, gitbase.ReferencesTableName, "hash"),
				col(0, gitbase.CommitsTableName, "hash"),
			),
			lit(3),
		),
	)

	result, err := SquashJoins(sql.NewContext(
		context.TODO(),
		sql.NewBaseSession(),
		opentracing.NoopTracer{},
	), analyzer.New(nil), node)
	require.NoError(err)

	expected := node.Schema()
	schema := result.Schema()
	require.Len(schema, len(expected))
	for i, col := range expected {
		require.Equal(col.Source, schema[i].Source, "at index %d", i)
		require.Equal(col.Name, schema[i].Name, "at index %d", i)
	}
}

func TestBuildSquashedTable(t *testing.T) {
	tables := gitbase.NewDatabase("").Tables()
	repositories := tables[gitbase.RepositoriesTableName]
	refs := tables[gitbase.ReferencesTableName]
	remotes := tables[gitbase.RemotesTableName]
	commits := tables[gitbase.CommitsTableName]
	treeEntries := tables[gitbase.TreeEntriesTableName]
	blobs := tables[gitbase.BlobsTableName]

	remoteRefsSchema := append(gitbase.RemotesSchema, gitbase.RefsSchema...)
	refCommitsSchema := append(gitbase.RefsSchema, gitbase.CommitsSchema...)
	commitTreeEntriesSchema := append(gitbase.CommitsSchema, gitbase.TreeEntriesSchema...)
	refTreeEntriesSchema := append(gitbase.RefsSchema, gitbase.TreeEntriesSchema...)
	treeEntryBlobsSchema := append(gitbase.TreeEntriesSchema, gitbase.BlobsSchema...)
	refsBlobsSchema := append(gitbase.RefsSchema, gitbase.BlobsSchema...)
	commitBlobsSchema := append(gitbase.CommitsSchema, gitbase.BlobsSchema...)

	repoFilter := eq(
		col(0, gitbase.RepositoriesTableName, "id"),
		col(0, gitbase.RepositoriesTableName, "id"),
	)

	repoRemotesRedundantFilter := eq(
		col(0, gitbase.RepositoriesTableName, "id"),
		col(1, gitbase.RemotesTableName, "repository_id"),
	)

	repoRemotesFilter := eq(
		col(0, gitbase.RepositoriesTableName, "id"),
		col(2, gitbase.RemotesTableName, "name"),
	)

	remotesFilter := eq(
		col(1, gitbase.RemotesTableName, "repository_id"),
		col(1, gitbase.RemotesTableName, "repository_id"),
	)

	refFilter := eq(
		col(1, gitbase.ReferencesTableName, "repository_id"),
		col(1, gitbase.ReferencesTableName, "repository_id"),
	)

	remoteRefsFilter := eq(
		col(2, gitbase.RemotesTableName, "name"),
		col(8, gitbase.ReferencesTableName, "name"),
	)

	remoteRefsRedundantFilter := eq(
		col(1, gitbase.RemotesTableName, "repository_id"),
		col(7, gitbase.ReferencesTableName, "repository_id"),
	)

	repoRefsFilter := eq(
		col(0, gitbase.RepositoriesTableName, "id"),
		col(2, gitbase.ReferencesTableName, "name"),
	)

	repoRefsRedundantFilter := eq(
		col(0, gitbase.RepositoriesTableName, "id"),
		col(1, gitbase.ReferencesTableName, "repository_id"),
	)

	commitFilter := eq(
		col(4, gitbase.CommitsTableName, "hash"),
		col(4, gitbase.CommitsTableName, "hash"),
	)

	refCommitsRedundantFilter := gte(
		historyIdx(
			col(2, gitbase.ReferencesTableName, "hash"),
			col(4, gitbase.CommitsTableName, "hash"),
		),
		lit(int64(0)),
	)

	refHEADCommitsRedundantFilter := eq(
		col(2, gitbase.ReferencesTableName, "hash"),
		col(4, gitbase.CommitsTableName, "hash"),
	)

	refCommitsFilter := eq(
		col(2, gitbase.ReferencesTableName, "hash"),
		col(4, gitbase.CommitsTableName, "author_name"),
	)

	treeEntryFilter := eq(
		col(0, gitbase.TreeEntriesTableName, "entry_hash"),
		col(0, gitbase.TreeEntriesTableName, "entry_hash"),
	)

	commitTreeEntriesFilter := eq(
		col(0, gitbase.CommitsTableName, "tree_hash"),
		col(0, gitbase.TreeEntriesTableName, "entry_hash"),
	)

	commitTreeEntriesRedundantFilter := eq(
		col(0, gitbase.CommitsTableName, "tree_hash"),
		col(0, gitbase.TreeEntriesTableName, "tree_hash"),
	)

	refTreeEntriesFilter := eq(
		col(0, gitbase.ReferencesTableName, "name"),
		col(0, gitbase.TreeEntriesTableName, "name"),
	)

	refTreeEntriesRedundantFilter := commitHasTree(
		col(0, gitbase.ReferencesTableName, "hash"),
		col(0, gitbase.TreeEntriesTableName, "tree_hash"),
	)

	blobFilter := eq(
		col(0, gitbase.BlobsTableName, "hash"),
		col(0, gitbase.BlobsTableName, "hash"),
	)

	treeEntryBlobsRedundantFilter := eq(
		col(0, gitbase.TreeEntriesTableName, "entry_hash"),
		col(0, gitbase.BlobsTableName, "hash"),
	)

	treeEntryBlobsFilter := eq(
		col(0, gitbase.TreeEntriesTableName, "tree_hash"),
		col(0, gitbase.BlobsTableName, "hash"),
	)

	refBlobsRedundantFilter := commitHasBlob(
		col(0, gitbase.ReferencesTableName, "hash"),
		col(0, gitbase.BlobsTableName, "hash"),
	)

	refBlobsFilter := eq(
		col(0, gitbase.ReferencesTableName, "name"),
		col(0, gitbase.BlobsTableName, "hash"),
	)

	commitBlobsRedundantFilter := commitHasBlob(
		col(0, gitbase.CommitsTableName, "hash"),
		col(0, gitbase.BlobsTableName, "hash"),
	)

	commitBlobsFilter := eq(
		col(0, gitbase.CommitsTableName, "hash"),
		col(0, gitbase.BlobsTableName, "size"),
	)

	testCases := []struct {
		name     string
		tables   []sql.Table
		filters  []sql.Expression
		err      *errors.Kind
		expected sql.Node
	}{
		{
			"repos with remotes",
			[]sql.Table{repositories, remotes},
			[]sql.Expression{
				repoFilter,
				repoRemotesRedundantFilter,
				repoRemotesFilter,
				remotesFilter,
			},
			nil,
			newSquashedTable(
				gitbase.NewRepoRemotesIter(
					gitbase.NewAllReposIter(repoFilter),
					and(repoRemotesFilter, remotesFilter),
				),
				nil,
				gitbase.RepositoriesTableName,
				gitbase.RemotesTableName,
			),
		},
		{
			"remotes with refs",
			[]sql.Table{remotes, refs},
			[]sql.Expression{
				remotesFilter,
				remoteRefsRedundantFilter,
				remoteRefsFilter,
				refFilter,
			},
			nil,
			newSquashedTable(
				gitbase.NewRemoteRefsIter(
					gitbase.NewAllRemotesIter(
						fixIdx(t, remotesFilter, gitbase.RemotesSchema),
					),
					and(
						fixIdx(t, remoteRefsFilter, remoteRefsSchema),
						fixIdx(t, refFilter, remoteRefsSchema),
					),
				),
				nil,
				gitbase.RemotesTableName,
				gitbase.ReferencesTableName,
			),
		},
		{
			"repos with refs",
			[]sql.Table{repositories, refs},
			[]sql.Expression{
				repoFilter,
				refFilter,
				repoRefsFilter,
				repoRefsRedundantFilter,
			},
			nil,
			newSquashedTable(
				gitbase.NewRepoRefsIter(
					gitbase.NewAllReposIter(repoFilter),
					and(
						refFilter,
						repoRefsFilter,
					),
				),
				nil,
				gitbase.RepositoriesTableName,
				gitbase.ReferencesTableName,
			),
		},
		{
			"refs 1:1 commit",
			[]sql.Table{refs, commits},
			[]sql.Expression{
				commitFilter,
				refFilter,
				refCommitsFilter,
				refHEADCommitsRedundantFilter,
			},
			nil,
			newSquashedTable(
				gitbase.NewRefHEADCommitsIter(
					gitbase.NewAllRefsIter(
						fixIdx(t, refFilter, gitbase.RefsSchema),
					),
					and(
						fixIdx(t, commitFilter, refCommitsSchema),
						refCommitsFilter,
					),
					false,
				),
				nil,
				gitbase.ReferencesTableName,
				gitbase.CommitsTableName,
			),
		},
		{
			"refs with commits",
			[]sql.Table{refs, commits},
			[]sql.Expression{
				commitFilter,
				refFilter,
				refCommitsFilter,
				refCommitsRedundantFilter,
			},
			nil,
			newSquashedTable(
				gitbase.NewRefCommitsIter(
					gitbase.NewAllRefsIter(
						fixIdx(t, refFilter, gitbase.RefsSchema),
					),
					and(
						fixIdx(t, commitFilter, refCommitsSchema),
						refCommitsFilter,
					),
				),
				nil,
				gitbase.ReferencesTableName,
				gitbase.CommitsTableName,
			),
		},
		{
			"repos with commits",
			[]sql.Table{repositories, commits},
			nil,
			errInvalidIteratorChain,
			nil,
		},
		{
			"remotes with commits",
			[]sql.Table{remotes, commits},
			nil,
			errInvalidIteratorChain,
			nil,
		},
		{
			"commits with tree entries",
			[]sql.Table{commits, treeEntries},
			[]sql.Expression{
				commitFilter,
				treeEntryFilter,
				commitTreeEntriesFilter,
				commitTreeEntriesRedundantFilter,
			},
			nil,
			newSquashedTable(
				gitbase.NewCommitTreeEntriesIter(
					gitbase.NewAllCommitsIter(
						fixIdx(t, commitFilter, gitbase.CommitsSchema),
					),
					and(
						fixIdx(t, treeEntryFilter, commitTreeEntriesSchema),
						fixIdx(t, commitTreeEntriesFilter, commitTreeEntriesSchema),
					),
					false,
				),
				nil,
				gitbase.CommitsTableName,
				gitbase.TreeEntriesTableName,
			),
		},
		{
			"repos with tree entries",
			[]sql.Table{repositories, treeEntries},
			nil,
			errInvalidIteratorChain,
			nil,
		},
		{
			"refs with tree entries",
			[]sql.Table{refs, treeEntries},
			[]sql.Expression{
				refFilter,
				treeEntryFilter,
				refTreeEntriesFilter,
				refTreeEntriesRedundantFilter,
			},
			nil,
			newSquashedTable(
				gitbase.NewCommitTreeEntriesIter(
					gitbase.NewRefHEADCommitsIter(
						gitbase.NewAllRefsIter(
							fixIdx(t, refFilter, gitbase.RefsSchema),
						),
						nil,
						true,
					),
					and(
						fixIdx(t, treeEntryFilter, refTreeEntriesSchema),
						fixIdx(t, refTreeEntriesFilter, refTreeEntriesSchema),
					),
					false,
				),
				nil,
				gitbase.ReferencesTableName,
				gitbase.TreeEntriesTableName,
			),
		},
		{
			"remotes with tree entries",
			[]sql.Table{remotes, treeEntries},
			nil,
			errInvalidIteratorChain,
			nil,
		},
		{
			"tree entries with blobs",
			[]sql.Table{treeEntries, blobs},
			[]sql.Expression{
				treeEntryFilter,
				blobFilter,
				treeEntryBlobsRedundantFilter,
				treeEntryBlobsFilter,
			},
			nil,
			newSquashedTable(
				gitbase.NewTreeEntryBlobsIter(
					gitbase.NewAllTreeEntriesIter(
						fixIdx(t, treeEntryFilter, gitbase.TreeEntriesSchema),
					),
					and(
						fixIdx(t, blobFilter, treeEntryBlobsSchema),
						fixIdx(t, treeEntryBlobsFilter, treeEntryBlobsSchema),
					),
				),
				nil,
				gitbase.TreeEntriesTableName,
				gitbase.BlobsTableName,
			),
		},
		{
			"repos with blobs",
			[]sql.Table{repositories, blobs},
			nil,
			errInvalidIteratorChain,
			nil,
		},
		{
			"remotes with blobs",
			[]sql.Table{remotes, blobs},
			nil,
			errInvalidIteratorChain,
			nil,
		},
		{
			"refs with blobs",
			[]sql.Table{refs, blobs},
			[]sql.Expression{
				refFilter,
				blobFilter,
				refBlobsFilter,
				refBlobsRedundantFilter,
			},
			nil,
			newSquashedTable(
				gitbase.NewTreeEntryBlobsIter(
					gitbase.NewCommitTreeEntriesIter(
						gitbase.NewRefHEADCommitsIter(
							gitbase.NewAllRefsIter(
								fixIdx(t, refFilter, refsBlobsSchema),
							),
							nil,
							true,
						),
						nil,
						true,
					),
					and(
						fixIdx(t, blobFilter, refsBlobsSchema),
						fixIdx(t, refBlobsFilter, refsBlobsSchema),
					),
				),
				nil,
				gitbase.ReferencesTableName,
				gitbase.BlobsTableName,
			),
		},
		{
			"commits with blobs",
			[]sql.Table{commits, blobs},
			[]sql.Expression{
				commitFilter,
				blobFilter,
				commitBlobsFilter,
				commitBlobsRedundantFilter,
			},
			nil,
			newSquashedTable(
				gitbase.NewTreeEntryBlobsIter(
					gitbase.NewCommitTreeEntriesIter(
						gitbase.NewAllCommitsIter(
							fixIdx(t, commitFilter, commitBlobsSchema),
						),
						nil,
						true,
					),
					and(
						fixIdx(t, blobFilter, commitBlobsSchema),
						fixIdx(t, commitBlobsFilter, commitBlobsSchema),
					),
				),
				nil,
				gitbase.CommitsTableName,
				gitbase.BlobsTableName,
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := buildSquashedTable(tt.tables, tt.filters, nil)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}

func fixIdx(t *testing.T, e sql.Expression, schema sql.Schema) sql.Expression {
	e, err := fixFieldIndexes(e, schema)
	require.NoError(t, err)
	return e
}

func TestIsJoinLeafSquashable(t *testing.T) {
	tables := gitbase.NewDatabase("").Tables()
	t1 := plan.NewPushdownProjectionAndFiltersTable(
		nil, nil,
		tables[gitbase.RepositoriesTableName].(sql.PushdownProjectionAndFiltersTable),
	)
	t2 := plan.NewPushdownProjectionAndFiltersTable(
		nil, nil,
		tables[gitbase.ReferencesTableName].(sql.PushdownProjectionAndFiltersTable),
	)

	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"has cross join",
			plan.NewCrossJoin(t1, t2),
			false,
		},
		{
			"has a limit",
			plan.NewLimit(0, plan.NewInnerJoin(t1, t2, nil)),
			false,
		},
		{
			"has project and filter",
			plan.NewProject(
				nil,
				plan.NewFilter(
					nil,
					t1,
				),
			),
			true,
		},
		{
			"has table alias",
			plan.NewInnerJoin(
				plan.NewTableAlias("foo", t1),
				t2,
				nil,
			),
			true,
		},
		{
			"has unsquashable inner join",
			plan.NewProject(
				nil,
				plan.NewInnerJoin(
					plan.NewLimit(0, t1),
					t2,
					nil,
				),
			),
			false,
		},
		{
			"has squashable inner join",
			plan.NewProject(
				nil,
				plan.NewInnerJoin(
					t1,
					t2,
					nil,
				),
			),
			true,
		},
		{
			"has a squashable table",
			t1,
			true,
		},
		{
			"has joined tables",
			new(joinedTables),
			true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.ok, isJoinLeafSquashable(tt.node))
		})
	}
}

func TestOrderedTableNames(t *testing.T) {
	tables := gitbase.NewDatabase("foo").Tables()

	input := []sql.Table{
		tables[gitbase.BlobsTableName],
		tables[gitbase.TreeEntriesTableName],
		tables[gitbase.CommitsTableName],
		tables[gitbase.ReferencesTableName],
		tables[gitbase.RemotesTableName],
		tables[gitbase.RepositoriesTableName],
	}

	expected := []string{
		gitbase.RepositoriesTableName,
		gitbase.RemotesTableName,
		gitbase.ReferencesTableName,
		gitbase.CommitsTableName,
		gitbase.TreeEntriesTableName,
		gitbase.BlobsTableName,
	}

	require.Equal(t, expected, orderedTableNames(input))
}

func TestFiltersForJoin(t *testing.T) {
	require := require.New(t)

	filters := []sql.Expression{
		eq(
			col(0, gitbase.ReferencesTableName, "hash"),
			lit(0),
		),
		eq(
			col(0, gitbase.RemotesTableName, "name"),
			lit(1),
		),
		eq(
			col(0, gitbase.RepositoriesTableName, "id"),
			col(0, gitbase.ReferencesTableName, "repository_id"),
		),
		eq(
			col(0, gitbase.ReferencesTableName, "repository_id"),
			col(0, gitbase.RemotesTableName, "repository_id"),
		),
	}

	filter, remaining, err := filtersForJoin(
		gitbase.RemotesTableName,
		gitbase.ReferencesTableName,
		filters,
		append(gitbase.RemotesSchema, gitbase.RefsSchema...),
	)

	require.NoError(err)
	require.Equal([]sql.Expression{filters[2]}, remaining)
	require.Equal(
		expression.JoinAnd(
			eq(
				col(8, gitbase.ReferencesTableName, "hash"),
				lit(0),
			),
			eq(
				col(1, gitbase.RemotesTableName, "name"),
				lit(1),
			),
		),
		filter,
	)
}

func TestFiltersForTable(t *testing.T) {
	require := require.New(t)

	filters := []sql.Expression{
		eq(
			col(0, gitbase.ReferencesTableName, "hash"),
			lit(0),
		),
		eq(
			col(0, gitbase.ReferencesTableName, "hash"),
			lit(1),
		),
		eq(
			col(0, gitbase.RepositoriesTableName, "id"),
			col(0, gitbase.ReferencesTableName, "repository_id"),
		),
	}

	filter, remaining, err := filtersForTable(
		gitbase.ReferencesTableName,
		filters,
		gitbase.RefsSchema,
	)

	require.NoError(err)
	require.Equal(filters[2:], remaining)
	require.Equal(
		expression.NewAnd(
			eq(
				col(2, gitbase.ReferencesTableName, "hash"),
				lit(0),
			),
			eq(
				col(2, gitbase.ReferencesTableName, "hash"),
				lit(1),
			),
		),
		filter,
	)
}

func TestRemoveRedundantFilters(t *testing.T) {
	f1 := eq(
		col(0, gitbase.RepositoriesTableName, "id"),
		col(0, gitbase.ReferencesTableName, "repository_id"),
	)

	f2 := eq(
		col(0, gitbase.RepositoriesTableName, "id"),
		lit(0),
	)

	result := removeRedundantFilters(
		[]sql.Expression{f1, f2},
		gitbase.RepositoriesTableName,
		gitbase.ReferencesTableName,
	)

	require.Equal(t, []sql.Expression{f2}, result)
}

func TestIsJoinCondSquashable(t *testing.T) {
	require := require.New(t)
	tables := gitbase.NewDatabase("").Tables()
	repos := plan.NewPushdownProjectionAndFiltersTable(
		nil, nil,
		tables[gitbase.ReferencesTableName].(sql.PushdownProjectionAndFiltersTable),
	)
	refs := plan.NewPushdownProjectionAndFiltersTable(
		nil, nil,
		tables[gitbase.ReferencesTableName].(sql.PushdownProjectionAndFiltersTable),
	)
	commits := plan.NewPushdownProjectionAndFiltersTable(
		nil, nil,
		tables[gitbase.CommitsTableName].(sql.PushdownProjectionAndFiltersTable),
	)

	node := plan.NewInnerJoin(
		refs,
		commits,
		and(
			eq(
				col(0, gitbase.ReferencesTableName, "hash"),
				col(0, gitbase.CommitsTableName, "hash"),
			),
			eq(lit(0), lit(1)),
		),
	)

	require.True(isJoinCondSquashable(node))

	node = plan.NewInnerJoin(
		refs,
		commits,
		and(
			eq(
				col(0, gitbase.ReferencesTableName, "hash"),
				col(0, gitbase.CommitsTableName, "message"),
			),
			eq(lit(0), lit(1)),
		),
	)

	require.False(isJoinCondSquashable(node))

	node = plan.NewInnerJoin(
		&joinedTables{
			tables: []sql.Table{
				refs,
				repos,
			},
		},
		commits,
		and(
			eq(
				col(0, gitbase.ReferencesTableName, "hash"),
				col(0, gitbase.CommitsTableName, "hash"),
			),
			eq(lit(0), lit(1)),
		),
	)

	require.True(isJoinCondSquashable(node))
}

func TestIsRedundantFilter(t *testing.T) {
	testCases := []struct {
		t1, t2   string
		filter   sql.Expression
		expected bool
	}{
		{
			gitbase.RepositoriesTableName,
			gitbase.RemotesTableName,
			eq(
				col(0, gitbase.RepositoriesTableName, "id"),
				col(0, gitbase.RemotesTableName, "repository_id"),
			),
			true,
		},
		{
			gitbase.RepositoriesTableName,
			gitbase.RemotesTableName,
			eq(
				col(0, gitbase.RemotesTableName, "repository_id"),
				col(0, gitbase.RepositoriesTableName, "id"),
			),
			true,
		},
		{
			gitbase.RepositoriesTableName,
			gitbase.ReferencesTableName,
			eq(
				col(0, gitbase.RepositoriesTableName, "id"),
				col(0, gitbase.ReferencesTableName, "repository_id"),
			),
			true,
		},
		{
			gitbase.RepositoriesTableName,
			gitbase.ReferencesTableName,
			eq(
				col(0, gitbase.ReferencesTableName, "repository_id"),
				col(0, gitbase.RepositoriesTableName, "id"),
			),
			true,
		},
		{
			gitbase.RemotesTableName,
			gitbase.ReferencesTableName,
			eq(
				col(0, gitbase.RemotesTableName, "repository_id"),
				col(0, gitbase.ReferencesTableName, "repository_id"),
			),
			true,
		},
		{
			gitbase.RemotesTableName,
			gitbase.ReferencesTableName,
			eq(
				col(0, gitbase.ReferencesTableName, "repository_id"),
				col(0, gitbase.RemotesTableName, "repository_id"),
			),
			true,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.CommitsTableName,
			eq(
				col(0, gitbase.ReferencesTableName, "hash"),
				col(0, gitbase.CommitsTableName, "hash"),
			),
			true,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.CommitsTableName,
			eq(
				col(0, gitbase.CommitsTableName, "hash"),
				col(0, gitbase.ReferencesTableName, "hash"),
			),
			true,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.CommitsTableName,
			gte(
				historyIdx(
					col(0, gitbase.ReferencesTableName, "hash"),
					col(0, gitbase.CommitsTableName, "hash"),
				),
				lit(int64(0)),
			),
			true,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.CommitsTableName,
			lte(
				lit(int64(0)),
				historyIdx(
					col(0, gitbase.ReferencesTableName, "hash"),
					col(0, gitbase.CommitsTableName, "hash"),
				),
			),
			true,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.CommitsTableName,
			gte(
				historyIdx(
					col(0, gitbase.ReferencesTableName, "hash"),
					col(0, gitbase.CommitsTableName, "hash"),
				),
				lit(1),
			),
			false,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.TreeEntriesTableName,
			commitHasTree(
				col(0, gitbase.ReferencesTableName, "hash"),
				col(0, gitbase.TreeEntriesTableName, "tree_hash"),
			),
			true,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.TreeEntriesTableName,
			commitHasBlob(
				col(0, gitbase.ReferencesTableName, "hash"),
				col(0, gitbase.TreeEntriesTableName, "entry_hash"),
			),
			true,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.BlobsTableName,
			commitHasBlob(
				col(0, gitbase.ReferencesTableName, "hash"),
				col(0, gitbase.BlobsTableName, "hash"),
			),
			true,
		},
		{
			gitbase.CommitsTableName,
			gitbase.TreeEntriesTableName,
			commitHasTree(
				col(0, gitbase.CommitsTableName, "hash"),
				col(0, gitbase.TreeEntriesTableName, "tree_hash"),
			),
			true,
		},
		{
			gitbase.CommitsTableName,
			gitbase.TreeEntriesTableName,
			commitHasBlob(
				col(0, gitbase.CommitsTableName, "hash"),
				col(0, gitbase.TreeEntriesTableName, "entry_hash"),
			),
			true,
		},
		{
			gitbase.CommitsTableName,
			gitbase.TreeEntriesTableName,
			eq(
				col(0, gitbase.CommitsTableName, "tree_hash"),
				col(0, gitbase.TreeEntriesTableName, "tree_hash"),
			),
			true,
		},
		{
			gitbase.CommitsTableName,
			gitbase.TreeEntriesTableName,
			eq(
				col(0, gitbase.TreeEntriesTableName, "tree_hash"),
				col(0, gitbase.CommitsTableName, "tree_hash"),
			),
			true,
		},
		{
			gitbase.CommitsTableName,
			gitbase.BlobsTableName,
			commitHasBlob(
				col(0, gitbase.CommitsTableName, "hash"),
				col(0, gitbase.BlobsTableName, "hash"),
			),
			true,
		},
		{
			gitbase.TreeEntriesTableName,
			gitbase.BlobsTableName,
			eq(
				col(0, gitbase.TreeEntriesTableName, "entry_hash"),
				col(0, gitbase.BlobsTableName, "hash"),
			),
			true,
		},
		{
			gitbase.TreeEntriesTableName,
			gitbase.BlobsTableName,
			eq(
				col(0, gitbase.BlobsTableName, "hash"),
				col(0, gitbase.TreeEntriesTableName, "entry_hash"),
			),
			true,
		},
		{
			gitbase.TreeEntriesTableName,
			gitbase.BlobsTableName,
			eq(
				col(0, gitbase.TreeEntriesTableName, "tree_hash"),
				col(0, gitbase.BlobsTableName, "hash"),
			),
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.filter.String(), func(t *testing.T) {
			require := require.New(t)
			require.Equal(
				tt.expected,
				isRedundantFilter(tt.filter, tt.t1, tt.t2),
			)
		})
	}
}

func eq(left, right sql.Expression) sql.Expression {
	return expression.NewEquals(left, right)
}

func col(idx int, table, name string) sql.Expression {
	return expression.NewGetFieldWithTable(idx, sql.Int64, table, name, false)
}

func commitHasTree(left, right sql.Expression) sql.Expression {
	return function.NewCommitHasTree(left, right)
}

func commitHasBlob(left, right sql.Expression) sql.Expression {
	return function.NewCommitHasBlob(left, right)
}

func historyIdx(left, right sql.Expression) sql.Expression {
	return function.NewHistoryIdx(left, right)
}

func lit(v interface{}) sql.Expression {
	return expression.NewLiteral(v, sql.Int64)
}

func gte(left, right sql.Expression) sql.Expression {
	return expression.NewGreaterThanOrEqual(left, right)
}

func lte(left, right sql.Expression) sql.Expression {
	return expression.NewLessThanOrEqual(left, right)
}

func and(exprs ...sql.Expression) sql.Expression {
	return expression.JoinAnd(exprs...)
}
