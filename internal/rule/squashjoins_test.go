package rule

import (
	"fmt"
	"testing"

	"github.com/src-d/gitbase"
	"github.com/stretchr/testify/require"
	errors "gopkg.in/src-d/go-errors.v1"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/parse"
	"github.com/src-d/go-mysql-server/sql/plan"
)

func TestAnalyzeSquashJoinsExchange(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(
		gitbase.NewDatabase("foo", gitbase.NewRepositoryPool(0)),
	)
	a := analyzer.NewBuilder(catalog).
		WithParallelism(2).
		AddPostAnalyzeRule(SquashJoinsRule, SquashJoins).
		Build()
	a.Batches[len(a.Batches)-1].Rules = a.Batches[len(a.Batches)-1].Rules[1:]
	ctx := sql.NewEmptyContext()

	node, err := parse.Parse(ctx, `SELECT * FROM ref_commits NATURAL JOIN commits`)
	require.NoError(err)

	result, err := a.Analyze(ctx, node)
	require.NoError(err)

	exchange, ok := result.(*plan.Exchange)
	require.True(ok)
	require.Equal(2, exchange.Parallelism)

	project, ok := exchange.Child.(*plan.Project)
	require.True(ok)

	project, ok = project.Child.(*plan.Project)
	require.True(ok)

	rt, ok := project.Child.(*plan.ResolvedTable)
	require.True(ok)

	_, ok = rt.Table.(*gitbase.SquashedTable)
	require.True(ok)
}

func TestAnalyzeSquashNaturalJoins(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(
		gitbase.NewDatabase("foo", gitbase.NewRepositoryPool(0)),
	)
	a := analyzer.NewBuilder(catalog).
		WithParallelism(2).
		AddPostAnalyzeRule(SquashJoinsRule, SquashJoins).
		Build()
	a.Batches[len(a.Batches)-1].Rules = a.Batches[len(a.Batches)-1].Rules[1:]
	ctx := sql.NewEmptyContext()

	node, err := parse.Parse(ctx, `SELECT * FROM refs
	NATURAL JOIN commits
	NATURAL JOIN commit_files
	NATURAL JOIN files`)
	require.NoError(err)

	result, err := a.Analyze(ctx, node)
	require.NoError(err)

	exchange, ok := result.(*plan.Exchange)
	require.True(ok)
	require.Equal(2, exchange.Parallelism)

	project, ok := exchange.Child.(*plan.Project)
	require.True(ok)

	filter, ok := project.Child.(*plan.Filter)
	require.True(ok)

	rt, ok := filter.Child.(*plan.ResolvedTable)
	require.True(ok)

	_, ok = rt.Table.(*gitbase.SquashedTable)
	require.True(ok)
}

func TestSquashJoins(t *testing.T) {
	require := require.New(t)

	tables := gitbaseTables()

	node := plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewFilter(
			lit(2),
			plan.NewInnerJoin(
				plan.NewResolvedTable(
					tables[gitbase.CommitsTableName],
				),
				plan.NewInnerJoin(
					plan.NewResolvedTable(
						tables[gitbase.RepositoriesTableName],
					),
					plan.NewResolvedTable(
						tables[gitbase.ReferencesTableName],
					),
					and(
						eq(
							col(0, gitbase.RepositoriesTableName, "repository_id"),
							col(0, gitbase.ReferencesTableName, "repository_id"),
						),
						lit(4),
					),
				),
				and(
					eq(
						col(0, gitbase.ReferencesTableName, "commit_hash"),
						col(0, gitbase.CommitsTableName, "commit_hash"),
					),
					lit(3),
				),
			),
		),
	)

	var expected sql.Node = plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewFilter(
			lit(2),
			plan.NewResolvedTable(
				gitbase.NewSquashedTable(
					gitbase.NewRefHEADCommitsIter(
						gitbase.NewRepoRefsIter(
							gitbase.NewAllReposIter(
								and(
									lit(3),
									lit(4),
								),
							),
							nil,
							false,
						),
						nil,
						false,
					),
					[]int{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 0, 1, 2, 3},
					[]sql.Expression{
						eq(
							col(0, gitbase.ReferencesTableName, "commit_hash"),
							col(0, gitbase.CommitsTableName, "commit_hash"),
						),
						lit(3),
						eq(
							col(0, gitbase.RepositoriesTableName, "repository_id"),
							col(0, gitbase.ReferencesTableName, "repository_id"),
						),
						lit(4),
					},
					nil,
					gitbase.RepositoriesTableName,
					gitbase.ReferencesTableName,
					gitbase.CommitsTableName,
				),
			),
		),
	)

	result, err := SquashJoins(sql.NewEmptyContext(), analyzer.NewDefault(nil), node)
	require.NoError(err)
	expected, err = expected.TransformUp(func(n sql.Node) (sql.Node, error) {
		t, ok := n.(*plan.ResolvedTable)
		if ok {
			// precompute schema
			_ = t.Table.Schema()
			return t, nil
		}

		return n, nil
	})
	require.NoError(err)
	require.Equal(expected, result)
}

func TestSquashJoinsIndexes(t *testing.T) {
	require := require.New(t)

	tables := gitbaseTables()

	idx1, idx2 := &dummyLookup{1}, &dummyLookup{2}

	node := plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewInnerJoin(
			plan.NewResolvedTable(
				tables[gitbase.CommitsTableName].(sql.IndexableTable).WithIndexLookup(idx1),
			),
			plan.NewResolvedTable(
				tables[gitbase.CommitTreesTableName].(sql.IndexableTable).WithIndexLookup(idx2),
			),
			eq(
				col(0, gitbase.CommitsTableName, "commit_hash"),
				col(0, gitbase.CommitTreesTableName, "commit_hash"),
			),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewResolvedTable(
			gitbase.NewSquashedTable(
				gitbase.NewCommitTreesIter(
					gitbase.NewIndexCommitsIter(idx1, nil),
					nil,
					false,
				),
				nil,
				[]sql.Expression{
					eq(
						col(0, gitbase.CommitsTableName, "commit_hash"),
						col(0, gitbase.CommitTreesTableName, "commit_hash"),
					),
				},
				[]string{gitbase.CommitsTableName},
				gitbase.CommitsTableName,
				gitbase.CommitTreesTableName,
			),
		),
	)

	result, err := SquashJoins(sql.NewEmptyContext(), analyzer.NewDefault(nil), node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestSquashJoinsUnsquashable(t *testing.T) {
	require := require.New(t)

	tables := gitbaseTables()

	node := plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewInnerJoin(
			plan.NewResolvedTable(
				tables[gitbase.RepositoriesTableName],
			),
			plan.NewLimit(1, plan.NewResolvedTable(
				tables[gitbase.ReferencesTableName],
			)),
			lit(4),
		),
	)

	result, err := SquashJoins(sql.NewEmptyContext(), analyzer.NewDefault(nil), node)
	require.NoError(err)
	require.Equal(node, result)
}

func TestSquashJoinsPartial(t *testing.T) {
	require := require.New(t)

	tables := gitbaseTables()

	node := plan.NewProject(
		[]sql.Expression{lit(1)},
		plan.NewInnerJoin(
			plan.NewLimit(1, plan.NewResolvedTable(
				tables[gitbase.CommitsTableName],
			)),
			plan.NewInnerJoin(
				plan.NewResolvedTable(
					tables[gitbase.RepositoriesTableName],
				),
				plan.NewResolvedTable(
					tables[gitbase.ReferencesTableName],
				),
				and(
					eq(
						col(0, gitbase.RepositoriesTableName, "repository_id"),
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
			plan.NewLimit(1, plan.NewResolvedTable(
				tables[gitbase.CommitsTableName],
			)),
			plan.NewResolvedTable(
				gitbase.NewSquashedTable(
					gitbase.NewRepoRefsIter(
						gitbase.NewAllReposIter(lit(4)),
						nil,
						false,
					),
					nil,
					[]sql.Expression{
						eq(
							col(0, gitbase.RepositoriesTableName, "repository_id"),
							col(0, gitbase.ReferencesTableName, "repository_id"),
						),
						lit(4),
					},
					nil,
					gitbase.RepositoriesTableName,
					gitbase.ReferencesTableName,
				),
			),
			lit(3),
		),
	)

	result, err := SquashJoins(sql.NewEmptyContext(), analyzer.NewDefault(nil), node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestSquashJoinsSchema(t *testing.T) {
	require := require.New(t)

	tables := gitbaseTables()

	node := plan.NewInnerJoin(
		plan.NewResolvedTable(
			tables[gitbase.CommitsTableName],
		),
		plan.NewInnerJoin(
			plan.NewResolvedTable(
				tables[gitbase.RepositoriesTableName],
			),
			plan.NewResolvedTable(
				tables[gitbase.ReferencesTableName],
			),
			and(
				eq(
					col(0, gitbase.RepositoriesTableName, "repository_id"),
					col(0, gitbase.ReferencesTableName, "repository_id"),
				),
				lit(4),
			),
		),
		and(
			eq(
				col(0, gitbase.ReferencesTableName, "commit_hash"),
				col(0, gitbase.CommitsTableName, "commit_hash"),
			),
			lit(3),
		),
	)

	result, err := SquashJoins(sql.NewEmptyContext(), analyzer.NewDefault(nil), node)
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
	tables := gitbaseTables()
	repositories := tables[gitbase.RepositoriesTableName]
	refs := tables[gitbase.ReferencesTableName]
	refCommits := tables[gitbase.RefCommitsTableName]
	remotes := tables[gitbase.RemotesTableName]
	commits := tables[gitbase.CommitsTableName]
	treeEntries := tables[gitbase.TreeEntriesTableName]
	blobs := tables[gitbase.BlobsTableName]
	commitTrees := tables[gitbase.CommitTreesTableName]
	commitBlobs := tables[gitbase.CommitBlobsTableName]
	commitFiles := tables[gitbase.CommitFilesTableName]
	files := tables[gitbase.FilesTableName]

	repoRefCommitsSchema := append(gitbase.RepositoriesSchema, gitbase.RefCommitsSchema...)
	remoteRefsSchema := append(gitbase.RemotesSchema, gitbase.RefsSchema...)
	refCommitsSchema := append(gitbase.RefsSchema, gitbase.CommitsSchema...)
	commitTreeEntriesSchema := append(gitbase.CommitsSchema, gitbase.TreeEntriesSchema...)
	treeEntryBlobsSchema := append(gitbase.TreeEntriesSchema, gitbase.BlobsSchema...)
	repoCommitsSchema := append(gitbase.RepositoriesSchema, gitbase.CommitsSchema...)
	repoTreeEntriesSchema := append(gitbase.RepositoriesSchema, gitbase.TreeEntriesSchema...)
	repoBlobsSchema := append(gitbase.RepositoriesSchema, gitbase.BlobsSchema...)
	refsRefCommitsCommitsSchema := append(append(gitbase.RefsSchema, gitbase.RefCommitsSchema...), gitbase.CommitsSchema...)
	refCommitsCommitsSchema := append(gitbase.RefCommitsSchema, gitbase.CommitsSchema...)
	commitsCommitTreesSchema := append(gitbase.CommitsSchema, gitbase.CommitTreesSchema...)
	refsCommitTreesSchema := append(gitbase.RefsSchema, gitbase.CommitTreesSchema...)
	refCommitsCommitTreesSchema := append(gitbase.RefCommitsSchema, gitbase.CommitTreesSchema...)
	commitTreesTreeEntriesSchema := append(gitbase.CommitTreesSchema, gitbase.TreeEntriesSchema...)
	refsCommitBlobsSchema := append(gitbase.RefsSchema, gitbase.CommitBlobsSchema...)
	refCommitsCommitBlobsSchema := append(gitbase.RefCommitsSchema, gitbase.CommitBlobsSchema...)
	commitsCommitBlobsSchema := append(gitbase.CommitsSchema, gitbase.CommitBlobsSchema...)
	commitBlobsBlobsSchema := append(gitbase.CommitBlobsSchema, gitbase.BlobsSchema...)
	refsCommitFilesSchema := append(gitbase.RefsSchema, gitbase.CommitFilesSchema...)
	commitsCommitFilesSchema := append(gitbase.CommitsSchema, gitbase.CommitFilesSchema...)
	commitFilesFilesSchema := append(gitbase.CommitFilesSchema, gitbase.FilesSchema...)

	repoFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(0, gitbase.RepositoriesTableName, "repository_id"),
	)

	repoRemotesRedundantFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(1, gitbase.RemotesTableName, "repository_id"),
	)

	repoRemotesFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(2, gitbase.RemotesTableName, "remote_name"),
	)

	repoRefCommitsFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(2, gitbase.RefCommitsTableName, "commit_hash"),
	)

	repoRefCommitsRedundantFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(1, gitbase.RefCommitsTableName, "repository_id"),
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
		col(2, gitbase.RemotesTableName, "remote_name"),
		col(8, gitbase.ReferencesTableName, "ref_name"),
	)

	remoteRefsRedundantFilter := eq(
		col(1, gitbase.RemotesTableName, "repository_id"),
		col(7, gitbase.ReferencesTableName, "repository_id"),
	)

	repoRefsFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(2, gitbase.ReferencesTableName, "ref_name"),
	)

	repoRefsRedundantFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(1, gitbase.ReferencesTableName, "repository_id"),
	)

	repoCommitsFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(2, gitbase.CommitsTableName, "commit_hash"),
	)

	repoCommitsRedundantFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(1, gitbase.CommitsTableName, "repository_id"),
	)

	repoTreeEntriesFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(2, gitbase.TreeEntriesTableName, "tree_hash"),
	)

	repoTreeEntriesRedundantFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(1, gitbase.TreeEntriesTableName, "repository_id"),
	)

	repoBlobsFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(2, gitbase.BlobsTableName, "blob_hash"),
	)

	repoBlobsRedundantFilter := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(1, gitbase.BlobsTableName, "repository_id"),
	)

	refCommitsFilter := eq(
		col(0, gitbase.RefCommitsTableName, "commit_hash"),
		col(0, gitbase.RefCommitsTableName, "commit_hash"),
	)

	refsRefCommitsFilter := eq(
		col(0, gitbase.ReferencesTableName, "ref_name"),
		col(0, gitbase.RefCommitsTableName, "repository_id"),
	)

	refsRefCommitsRedundantFilter := eq(
		col(0, gitbase.ReferencesTableName, "ref_name"),
		col(0, gitbase.RefCommitsTableName, "ref_name"),
	)

	refsRefCommitsHeadRedundantFilter := eq(
		col(0, gitbase.ReferencesTableName, "commit_hash"),
		col(0, gitbase.RefCommitsTableName, "commit_hash"),
	)

	refCommitsCommitsFilter := eq(
		col(0, gitbase.RefCommitsTableName, "commit_hash"),
		col(0, gitbase.CommitsTableName, "commit_author_name"),
	)

	refCommitsCommitsRedundantFilter := eq(
		col(0, gitbase.RefCommitsTableName, "commit_hash"),
		col(0, gitbase.CommitsTableName, "commit_hash"),
	)

	commitFilter := eq(
		col(4, gitbase.CommitsTableName, "commit_hash"),
		col(4, gitbase.CommitsTableName, "commit_hash"),
	)

	refCommitsRedundantFilter := eq(
		col(0, gitbase.ReferencesTableName, "commit_hash"),
		col(0, gitbase.CommitsTableName, "commit_hash"),
	)

	refsCommitsFilter := eq(
		col(2, gitbase.ReferencesTableName, "commit_hash"),
		col(5, gitbase.CommitsTableName, "commit_author_name"),
	)

	treeEntryFilter := eq(
		col(0, gitbase.TreeEntriesTableName, "blob_hash"),
		col(0, gitbase.TreeEntriesTableName, "blob_hash"),
	)

	commitTreeEntriesFilter := eq(
		col(0, gitbase.CommitsTableName, "tree_hash"),
		col(0, gitbase.TreeEntriesTableName, "blob_hash"),
	)

	commitTreeEntriesRedundantFilter := eq(
		col(0, gitbase.CommitsTableName, "tree_hash"),
		col(0, gitbase.TreeEntriesTableName, "tree_hash"),
	)

	blobFilter := eq(
		col(0, gitbase.BlobsTableName, "blob_hash"),
		col(0, gitbase.BlobsTableName, "blob_hash"),
	)

	treeEntryBlobsRedundantFilter := eq(
		col(0, gitbase.TreeEntriesTableName, "blob_hash"),
		col(0, gitbase.BlobsTableName, "blob_hash"),
	)

	treeEntryBlobsFilter := eq(
		col(0, gitbase.TreeEntriesTableName, "tree_hash"),
		col(0, gitbase.BlobsTableName, "blob_hash"),
	)

	commitTreesFilter := eq(
		col(0, gitbase.CommitTreesTableName, "commit_hash"),
		col(0, gitbase.CommitTreesTableName, "commit_hash"),
	)

	refCommitTreesFilter := eq(
		col(0, gitbase.ReferencesTableName, "ref_name"),
		col(0, gitbase.CommitTreesTableName, "commit_hash"),
	)

	refCommitTreesRedundantFilter := eq(
		col(0, gitbase.ReferencesTableName, "commit_hash"),
		col(0, gitbase.CommitTreesTableName, "commit_hash"),
	)

	commitCommitTreesFilter := eq(
		col(0, gitbase.CommitsTableName, "commit_author_name"),
		col(0, gitbase.CommitTreesTableName, "commit_hash"),
	)

	commitCommitTreesRedundantFilter := eq(
		col(0, gitbase.CommitsTableName, "commit_hash"),
		col(0, gitbase.CommitTreesTableName, "commit_hash"),
	)

	commitCommitTreesByTreeRedundantFilter := eq(
		col(0, gitbase.CommitsTableName, "tree_hash"),
		col(0, gitbase.CommitTreesTableName, "tree_hash"),
	)

	commitTreeTreeEntriesFilter := eq(
		col(0, gitbase.CommitTreesTableName, "tree_hash"),
		col(0, gitbase.TreeEntriesTableName, "tree_entry_name"),
	)

	commitTreeTreeEntriesRedundantFilter := eq(
		col(0, gitbase.CommitTreesTableName, "tree_hash"),
		col(0, gitbase.TreeEntriesTableName, "tree_hash"),
	)

	refCommitCommitTreesFilter := eq(
		col(0, gitbase.RefCommitsTableName, "ref_name"),
		col(0, gitbase.CommitTreesTableName, "commit_hash"),
	)

	refCommitCommitTreesRedundantFilter := eq(
		col(0, gitbase.RefCommitsTableName, "commit_hash"),
		col(0, gitbase.CommitTreesTableName, "commit_hash"),
	)

	commitBlobsFilter := eq(
		col(0, gitbase.CommitBlobsTableName, "commit_hash"),
		col(0, gitbase.CommitBlobsTableName, "commit_hash"),
	)

	refCommitBlobsFilter := eq(
		col(0, gitbase.ReferencesTableName, "commit_hash"),
		col(0, gitbase.CommitBlobsTableName, "blob_hash"),
	)

	refCommitBlobsRedundantFilter := eq(
		col(0, gitbase.ReferencesTableName, "commit_hash"),
		col(0, gitbase.CommitBlobsTableName, "commit_hash"),
	)

	refCommitCommitBlobsFilter := eq(
		col(0, gitbase.RefCommitsTableName, "commit_hash"),
		col(0, gitbase.CommitBlobsTableName, "blob_hash"),
	)

	refCommitCommitBlobsRedundantFilter := eq(
		col(0, gitbase.RefCommitsTableName, "commit_hash"),
		col(0, gitbase.CommitBlobsTableName, "commit_hash"),
	)

	commitCommitBlobsFilter := eq(
		col(0, gitbase.CommitsTableName, "commit_hash"),
		col(0, gitbase.CommitBlobsTableName, "blob_hash"),
	)

	commitCommitBlobsRedundantFilter := eq(
		col(0, gitbase.CommitsTableName, "commit_hash"),
		col(0, gitbase.CommitBlobsTableName, "commit_hash"),
	)

	commitBlobBlobsFilter := eq(
		col(0, gitbase.CommitBlobsTableName, "commit_hash"),
		col(0, gitbase.BlobsTableName, "blob_hash"),
	)

	commitBlobBlobsRedundantFilter := eq(
		col(0, gitbase.CommitBlobsTableName, "blob_hash"),
		col(0, gitbase.BlobsTableName, "blob_hash"),
	)

	commitFilesFilter := eq(
		col(0, gitbase.CommitFilesTableName, "commit_hash"),
		col(0, gitbase.CommitFilesTableName, "commit_hash"),
	)

	refsCommitFilesFilter := eq(
		col(0, gitbase.ReferencesTableName, "ref_name"),
		col(0, gitbase.CommitFilesTableName, "file_path"),
	)

	refsCommitFilesRedundantFilter := eq(
		col(0, gitbase.ReferencesTableName, "commit_hash"),
		col(0, gitbase.CommitFilesTableName, "commit_hash"),
	)

	commitsCommitFilesFilter := eq(
		col(0, gitbase.CommitsTableName, "tree_hash"),
		col(0, gitbase.CommitFilesTableName, "tree_hash"),
	)

	commitsCommitFilesRedundantFilter := eq(
		col(0, gitbase.CommitsTableName, "commit_hash"),
		col(0, gitbase.CommitFilesTableName, "commit_hash"),
	)

	commitFilesFilesFilePathRedundantFilter := eq(
		col(0, gitbase.CommitFilesTableName, "file_path"),
		col(0, gitbase.FilesTableName, "file_path"),
	)

	commitFilesFilesTreeHashRedundantFilter := eq(
		col(0, gitbase.CommitFilesTableName, "tree_hash"),
		col(0, gitbase.FilesTableName, "tree_hash"),
	)

	commitFilesFilesBlobHashRedundantFilter := eq(
		col(0, gitbase.CommitFilesTableName, "blob_hash"),
		col(0, gitbase.FilesTableName, "blob_hash"),
	)

	commitFilesFilesFilter := eq(
		col(0, gitbase.CommitFilesTableName, "commit_hash"),
		col(0, gitbase.FilesTableName, "tree_hash"),
	)

	filesFilter := eq(
		col(0, gitbase.FilesTableName, "file_path"),
		col(0, gitbase.FilesTableName, "file_path"),
	)

	idx1, idx2 := &dummyLookup{1}, &dummyLookup{2}

	testCases := []struct {
		name     string
		tables   []sql.Table
		filters  []sql.Expression
		columns  []string
		indexes  map[string]sql.IndexLookup
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
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRepoRemotesIter(
					gitbase.NewAllReposIter(repoFilter),
					and(repoRemotesFilter, remotesFilter),
				),
				nil,
				[]sql.Expression{
					repoFilter,
					repoRemotesRedundantFilter,
					repoRemotesFilter,
					remotesFilter,
				},
				nil,
				gitbase.RepositoriesTableName,
				gitbase.RemotesTableName,
			)),
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
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
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
				[]sql.Expression{
					remotesFilter,
					remoteRefsRedundantFilter,
					remoteRefsFilter,
					refFilter,
				},
				nil,
				gitbase.RemotesTableName,
				gitbase.ReferencesTableName,
			)),
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
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRepoRefsIter(
					gitbase.NewAllReposIter(repoFilter),
					and(
						refFilter,
						repoRefsFilter,
					),
					false,
				),
				nil,
				[]sql.Expression{
					repoFilter,
					refFilter,
					repoRefsFilter,
					repoRefsRedundantFilter,
				},
				nil,
				gitbase.RepositoriesTableName,
				gitbase.ReferencesTableName,
			)),
		},
		{
			"refs with commits",
			[]sql.Table{refs, commits},
			[]sql.Expression{
				commitFilter,
				refFilter,
				refsCommitsFilter,
				refCommitsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRefHEADCommitsIter(
					gitbase.NewAllRefsIter(
						fixIdx(t, refFilter, gitbase.RefsSchema),
						false,
					),
					and(
						fixIdx(t, commitFilter, refCommitsSchema),
						refsCommitsFilter,
					),
					false,
				),
				nil,
				[]sql.Expression{
					commitFilter,
					refFilter,
					refsCommitsFilter,
					refCommitsRedundantFilter,
				},
				nil,
				gitbase.ReferencesTableName,
				gitbase.CommitsTableName,
			)),
		},
		{
			"remotes with commits",
			[]sql.Table{remotes, commits},
			[]sql.Expression{
				eq(
					col(0, gitbase.RemotesTableName, "repository_id"),
					col(0, gitbase.CommitsTableName, "repository_id"),
				),
			},
			nil,
			nil,
			nil,
			plan.NewProject(
				[]sql.Expression{
					colT(11, sql.Text, gitbase.RemotesTableName, "repository_id"),
					colT(12, sql.Text, gitbase.RemotesTableName, "remote_name"),
					colT(13, sql.Text, gitbase.RemotesTableName, "remote_push_url"),
					colT(14, sql.Text, gitbase.RemotesTableName, "remote_fetch_url"),
					colT(15, sql.Text, gitbase.RemotesTableName, "remote_push_refspec"),
					colT(16, sql.Text, gitbase.RemotesTableName, "remote_fetch_refspec"),
					colT(0, sql.Text, gitbase.CommitsTableName, "repository_id"),
					colT(1, sql.Text, gitbase.CommitsTableName, "commit_hash"),
					colT(2, sql.Text, gitbase.CommitsTableName, "commit_author_name"),
					colT(3, sql.Text, gitbase.CommitsTableName, "commit_author_email"),
					colT(4, sql.Timestamp, gitbase.CommitsTableName, "commit_author_when"),
					colT(5, sql.Text, gitbase.CommitsTableName, "committer_name"),
					colT(6, sql.Text, gitbase.CommitsTableName, "committer_email"),
					colT(7, sql.Timestamp, gitbase.CommitsTableName, "committer_when"),
					colT(8, sql.Text, gitbase.CommitsTableName, "commit_message"),
					colT(9, sql.Text, gitbase.CommitsTableName, "tree_hash"),
					colT(10, sql.Array(sql.Text), gitbase.CommitsTableName, "commit_parents"),
				},
				plan.NewInnerJoin(
					plan.NewExchange(2,
						plan.NewResolvedTable(commits),
					),
					plan.NewResolvedTable(gitbase.NewSquashedTable(
						gitbase.NewAllRemotesIter(nil),
						nil,
						nil,
						nil,
						gitbase.RemotesTableName,
					)),
					eq(
						col(11, gitbase.RemotesTableName, "repository_id"),
						col(0, gitbase.CommitsTableName, "repository_id"),
					),
				),
			),
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
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewTreeTreeEntriesIter(
					gitbase.NewCommitMainTreeIter(
						gitbase.NewAllCommitsIter(
							fixIdx(t, commitFilter, gitbase.CommitsSchema),
							false,
						),
						nil,
						true,
					),
					and(
						fixIdx(t, treeEntryFilter, commitTreeEntriesSchema),
						fixIdx(t, commitTreeEntriesFilter, commitTreeEntriesSchema),
					),
					false,
				),
				nil,
				[]sql.Expression{
					commitFilter,
					treeEntryFilter,
					commitTreeEntriesFilter,
					commitTreeEntriesRedundantFilter,
				},
				nil,
				gitbase.CommitsTableName,
				gitbase.TreeEntriesTableName,
			)),
		},
		{
			"refs with commit trees",
			[]sql.Table{refs, commitTrees},
			[]sql.Expression{
				refFilter,
				commitTreesFilter,
				refCommitTreesFilter,
				refCommitTreesRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitTreesIter(
					gitbase.NewRefHEADCommitsIter(
						gitbase.NewAllRefsIter(
							fixIdx(t, refFilter, gitbase.RefsSchema),
							false,
						),
						nil,
						true,
					),
					and(
						fixIdx(t, commitTreesFilter, refsCommitTreesSchema),
						fixIdx(t, refCommitTreesFilter, refsCommitTreesSchema),
					),
					false,
				),
				nil,
				[]sql.Expression{
					refFilter,
					commitTreesFilter,
					refCommitTreesFilter,
					refCommitTreesRedundantFilter,
				},
				nil,
				gitbase.ReferencesTableName,
				gitbase.CommitTreesTableName,
			)),
		},
		{
			"remotes with tree entries",
			[]sql.Table{remotes, treeEntries},
			[]sql.Expression{
				eq(
					col(0, gitbase.RemotesTableName, "repository_id"),
					col(0, gitbase.TreeEntriesTableName, "repository_id"),
				),
			},
			nil,
			nil,
			nil,
			plan.NewProject(
				[]sql.Expression{
					colT(5, sql.Text, gitbase.RemotesTableName, "repository_id"),
					colT(6, sql.Text, gitbase.RemotesTableName, "remote_name"),
					colT(7, sql.Text, gitbase.RemotesTableName, "remote_push_url"),
					colT(8, sql.Text, gitbase.RemotesTableName, "remote_fetch_url"),
					colT(9, sql.Text, gitbase.RemotesTableName, "remote_push_refspec"),
					colT(10, sql.Text, gitbase.RemotesTableName, "remote_fetch_refspec"),
					colT(0, sql.Text, gitbase.TreeEntriesTableName, "repository_id"),
					colT(1, sql.Text, gitbase.TreeEntriesTableName, "tree_entry_name"),
					colT(2, sql.Text, gitbase.TreeEntriesTableName, "blob_hash"),
					colT(3, sql.Text, gitbase.TreeEntriesTableName, "tree_hash"),
					colT(4, sql.Text, gitbase.TreeEntriesTableName, "tree_entry_mode"),
				},
				plan.NewInnerJoin(
					plan.NewExchange(2,
						plan.NewResolvedTable(treeEntries),
					),
					plan.NewResolvedTable(gitbase.NewSquashedTable(
						gitbase.NewAllRemotesIter(nil),
						nil,
						nil,
						nil,
						gitbase.RemotesTableName,
					)),
					eq(
						col(5, gitbase.RemotesTableName, "repository_id"),
						col(0, gitbase.TreeEntriesTableName, "repository_id"),
					),
				),
			),
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
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewTreeEntryBlobsIter(
					gitbase.NewAllTreeEntriesIter(
						fixIdx(t, treeEntryFilter, gitbase.TreeEntriesSchema),
					),
					and(
						fixIdx(t, blobFilter, treeEntryBlobsSchema),
						fixIdx(t, treeEntryBlobsFilter, treeEntryBlobsSchema),
					),
					false,
				),
				nil,
				[]sql.Expression{
					treeEntryFilter,
					blobFilter,
					treeEntryBlobsRedundantFilter,
					treeEntryBlobsFilter,
				},
				nil,
				gitbase.TreeEntriesTableName,
				gitbase.BlobsTableName,
			)),
		},
		{
			"remotes with blobs",
			[]sql.Table{remotes, blobs},
			[]sql.Expression{
				eq(
					col(0, gitbase.RemotesTableName, "repository_id"),
					col(0, gitbase.BlobsTableName, "repository_id"),
				),
			},
			nil,
			nil,
			nil,
			plan.NewProject(
				[]sql.Expression{
					colT(4, sql.Text, gitbase.RemotesTableName, "repository_id"),
					colT(5, sql.Text, gitbase.RemotesTableName, "remote_name"),
					colT(6, sql.Text, gitbase.RemotesTableName, "remote_push_url"),
					colT(7, sql.Text, gitbase.RemotesTableName, "remote_fetch_url"),
					colT(8, sql.Text, gitbase.RemotesTableName, "remote_push_refspec"),
					colT(9, sql.Text, gitbase.RemotesTableName, "remote_fetch_refspec"),
					colT(0, sql.Text, gitbase.BlobsTableName, "repository_id"),
					colT(1, sql.Text, gitbase.BlobsTableName, "blob_hash"),
					colT(2, sql.Int64, gitbase.BlobsTableName, "blob_size"),
					colT(3, sql.Blob, gitbase.BlobsTableName, "blob_content"),
				},
				plan.NewInnerJoin(
					plan.NewExchange(2,
						plan.NewResolvedTable(blobs),
					),
					plan.NewResolvedTable(gitbase.NewSquashedTable(
						gitbase.NewAllRemotesIter(nil),
						nil,
						nil,
						nil,
						gitbase.RemotesTableName,
					)),
					eq(
						col(4, gitbase.RemotesTableName, "repository_id"),
						col(0, gitbase.BlobsTableName, "repository_id"),
					),
				),
			),
		},
		{
			"refs with blobs",
			[]sql.Table{refs, blobs},
			[]sql.Expression{
				eq(
					col(0, gitbase.ReferencesTableName, "repository_id"),
					col(0, gitbase.BlobsTableName, "repository_id"),
				),
			},
			nil,
			nil,
			nil,
			plan.NewProject(
				[]sql.Expression{
					colT(4, sql.Text, gitbase.ReferencesTableName, "repository_id"),
					colT(5, sql.Text, gitbase.ReferencesTableName, "ref_name"),
					colT(6, sql.Text, gitbase.ReferencesTableName, "commit_hash"),
					colT(0, sql.Text, gitbase.BlobsTableName, "repository_id"),
					colT(1, sql.Text, gitbase.BlobsTableName, "blob_hash"),
					colT(2, sql.Int64, gitbase.BlobsTableName, "blob_size"),
					colT(3, sql.Blob, gitbase.BlobsTableName, "blob_content"),
				},
				plan.NewInnerJoin(
					plan.NewExchange(2,
						plan.NewResolvedTable(blobs),
					),
					plan.NewResolvedTable(gitbase.NewSquashedTable(
						gitbase.NewAllRefsIter(nil, false),
						nil,
						nil,
						nil,
						gitbase.ReferencesTableName,
					)),
					eq(
						col(4, gitbase.ReferencesTableName, "repository_id"),
						col(0, gitbase.BlobsTableName, "repository_id"),
					),
				),
			),
		},
		{
			"commits with blobs",
			[]sql.Table{commits, blobs},
			[]sql.Expression{
				eq(
					col(0, gitbase.CommitsTableName, "repository_id"),
					col(0, gitbase.BlobsTableName, "repository_id"),
				),
			},
			nil,
			nil,
			nil,
			plan.NewProject(
				[]sql.Expression{
					colT(4, sql.Text, gitbase.CommitsTableName, "repository_id"),
					colT(5, sql.Text, gitbase.CommitsTableName, "commit_hash"),
					colT(6, sql.Text, gitbase.CommitsTableName, "commit_author_name"),
					colT(7, sql.Text, gitbase.CommitsTableName, "commit_author_email"),
					colT(8, sql.Timestamp, gitbase.CommitsTableName, "commit_author_when"),
					colT(9, sql.Text, gitbase.CommitsTableName, "committer_name"),
					colT(10, sql.Text, gitbase.CommitsTableName, "committer_email"),
					colT(11, sql.Timestamp, gitbase.CommitsTableName, "committer_when"),
					colT(12, sql.Text, gitbase.CommitsTableName, "commit_message"),
					colT(13, sql.Text, gitbase.CommitsTableName, "tree_hash"),
					colT(14, sql.Array(sql.Text), gitbase.CommitsTableName, "commit_parents"),
					colT(0, sql.Text, gitbase.BlobsTableName, "repository_id"),
					colT(1, sql.Text, gitbase.BlobsTableName, "blob_hash"),
					colT(2, sql.Int64, gitbase.BlobsTableName, "blob_size"),
					colT(3, sql.Blob, gitbase.BlobsTableName, "blob_content"),
				},
				plan.NewInnerJoin(
					plan.NewExchange(2,
						plan.NewResolvedTable(blobs),
					),
					plan.NewResolvedTable(gitbase.NewSquashedTable(
						gitbase.NewAllCommitsIter(nil, false),
						nil,
						nil,
						nil,
						gitbase.CommitsTableName,
					)),
					eq(
						col(4, gitbase.CommitsTableName, "repository_id"),
						col(0, gitbase.BlobsTableName, "repository_id"),
					),
				),
			),
		},
		{
			"repos with commits",
			[]sql.Table{repositories, commits},
			[]sql.Expression{
				repoFilter,
				commitFilter,
				repoCommitsFilter,
				repoCommitsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRepoCommitsIter(
					gitbase.NewAllReposIter(repoFilter),
					and(
						fixIdx(t, commitFilter, repoCommitsSchema),
						fixIdx(t, repoCommitsFilter, repoCommitsSchema),
					),
				),
				nil,
				[]sql.Expression{
					repoFilter,
					commitFilter,
					repoCommitsFilter,
					repoCommitsRedundantFilter,
				},
				nil,
				gitbase.RepositoriesTableName,
				gitbase.CommitsTableName,
			)),
		},
		{
			"refs with ref commits",
			[]sql.Table{refs, refCommits},
			[]sql.Expression{
				refFilter,
				refCommitsFilter,
				refsRefCommitsFilter,
				refsRefCommitsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRefRefCommitsIter(
					gitbase.NewAllRefsIter(
						fixIdx(t, refFilter, gitbase.RefsSchema),
						false,
					),
					and(
						fixIdx(t, refCommitsFilter, refsRefCommitsCommitsSchema),
						fixIdx(t, refsRefCommitsFilter, refsRefCommitsCommitsSchema),
					),
				),
				nil,
				[]sql.Expression{
					refFilter,
					refCommitsFilter,
					refsRefCommitsFilter,
					refsRefCommitsRedundantFilter,
				},
				nil,
				gitbase.ReferencesTableName,
				gitbase.RefCommitsTableName,
			)),
		},
		{
			"refs with ref commits by commit hash",
			[]sql.Table{refs, refCommits},
			[]sql.Expression{
				refFilter,
				refCommitsFilter,
				refsRefCommitsFilter,
				refsRefCommitsHeadRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRefHeadRefCommitsIter(
					gitbase.NewAllRefsIter(
						fixIdx(t, refFilter, gitbase.RefsSchema),
						false,
					),
					and(
						fixIdx(t, refCommitsFilter, refsRefCommitsCommitsSchema),
						fixIdx(t, refsRefCommitsFilter, refsRefCommitsCommitsSchema),
					),
				),
				nil,
				[]sql.Expression{
					refFilter,
					refCommitsFilter,
					refsRefCommitsFilter,
					refsRefCommitsHeadRedundantFilter,
				},
				nil,
				gitbase.ReferencesTableName,
				gitbase.RefCommitsTableName,
			)),
		},
		{
			"refs commits with commits",
			[]sql.Table{refCommits, commits},
			[]sql.Expression{
				refCommitsFilter,
				commitFilter,
				refCommitsCommitsFilter,
				refCommitsCommitsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRefCommitCommitsIter(
					gitbase.NewAllRefCommitsIter(
						fixIdx(t, refCommitsFilter, refCommitsCommitsSchema),
					),
					and(

						fixIdx(t, commitFilter, refCommitsCommitsSchema),
						fixIdx(t, refCommitsCommitsFilter, refCommitsCommitsSchema),
					),
				),
				nil,
				[]sql.Expression{
					refCommitsFilter,
					commitFilter,
					refCommitsCommitsFilter,
					refCommitsCommitsRedundantFilter,
				},
				nil,
				gitbase.RefCommitsTableName,
				gitbase.CommitsTableName,
			)),
		},
		{
			"repositories with tree entries",
			[]sql.Table{repositories, treeEntries},
			[]sql.Expression{
				repoFilter,
				treeEntryFilter,
				repoTreeEntriesFilter,
				repoTreeEntriesRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRepoTreeEntriesIter(
					gitbase.NewAllReposIter(repoFilter),
					and(
						fixIdx(t, treeEntryFilter, repoTreeEntriesSchema),
						fixIdx(t, repoTreeEntriesFilter, repoTreeEntriesSchema),
					),
				),
				nil,
				[]sql.Expression{
					repoFilter,
					treeEntryFilter,
					repoTreeEntriesFilter,
					repoTreeEntriesRedundantFilter,
				},
				nil,
				gitbase.RepositoriesTableName,
				gitbase.TreeEntriesTableName,
			)),
		},
		{
			"repositories with ref commits",
			[]sql.Table{repositories, refCommits},
			[]sql.Expression{
				repoFilter,
				refCommitsFilter,
				repoRefCommitsFilter,
				repoRefCommitsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRefRefCommitsIter(
					gitbase.NewRepoRefsIter(
						gitbase.NewAllReposIter(repoFilter),
						nil,
						true,
					),

					and(
						fixIdx(t, refCommitsFilter, repoRefCommitsSchema),
						fixIdx(t, repoRefCommitsFilter, repoRefCommitsSchema),
					),
				),
				nil,
				[]sql.Expression{
					repoFilter,
					refCommitsFilter,
					repoRefCommitsFilter,
					repoRefCommitsRedundantFilter,
				},
				nil,
				gitbase.RepositoriesTableName,
				gitbase.RefCommitsTableName,
			)),
		},
		{
			"repositories and blobs",
			[]sql.Table{repositories, blobs},
			[]sql.Expression{
				repoFilter,
				blobFilter,
				repoBlobsFilter,
				repoBlobsRedundantFilter,
			},
			[]string{"blob_content"},
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRepoBlobsIter(
					gitbase.NewAllReposIter(repoFilter),
					and(
						fixIdx(t, blobFilter, repoBlobsSchema),
						fixIdx(t, repoBlobsFilter, repoBlobsSchema),
					),
					true,
				),
				nil,
				[]sql.Expression{
					repoFilter,
					blobFilter,
					repoBlobsFilter,
					repoBlobsRedundantFilter,
				},
				nil,
				gitbase.RepositoriesTableName,
				gitbase.BlobsTableName,
			)),
		},
		{
			"refs with ref commits and commits",
			[]sql.Table{refs, refCommits, commits},
			[]sql.Expression{
				refFilter,
				refsRefCommitsFilter,
				refsRefCommitsRedundantFilter,
				refCommitsFilter,
				commitFilter,
				refCommitsCommitsFilter,
				refCommitsCommitsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRefCommitCommitsIter(
					gitbase.NewRefRefCommitsIter(
						gitbase.NewAllRefsIter(
							fixIdx(t, refFilter, refsRefCommitsCommitsSchema),
							false,
						),
						and(
							fixIdx(t, refsRefCommitsFilter, refsRefCommitsCommitsSchema),
							fixIdx(t, refCommitsFilter, refsRefCommitsCommitsSchema),
						),
					),
					and(
						fixIdx(t, commitFilter, refsRefCommitsCommitsSchema),
						fixIdx(t, refCommitsCommitsFilter, refsRefCommitsCommitsSchema),
					),
				),
				nil,
				[]sql.Expression{
					refFilter,
					refsRefCommitsFilter,
					refsRefCommitsRedundantFilter,
					refCommitsFilter,
					commitFilter,
					refCommitsCommitsFilter,
					refCommitsCommitsRedundantFilter,
				},
				nil,
				gitbase.ReferencesTableName,
				gitbase.RefCommitsTableName,
				gitbase.CommitsTableName,
			)),
		},
		{
			"refs with ref commits and commits only head",
			[]sql.Table{refs, refCommits, commits},
			[]sql.Expression{
				refFilter,
				refsRefCommitsFilter,
				refsRefCommitsHeadRedundantFilter,
				refCommitsFilter,
				commitFilter,
				refCommitsCommitsFilter,
				refCommitsCommitsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRefCommitCommitsIter(
					gitbase.NewRefHeadRefCommitsIter(
						gitbase.NewAllRefsIter(
							fixIdx(t, refFilter, refsRefCommitsCommitsSchema),
							false,
						),
						and(
							fixIdx(t, refsRefCommitsFilter, refsRefCommitsCommitsSchema),
							fixIdx(t, refCommitsFilter, refsRefCommitsCommitsSchema),
						),
					),
					and(
						fixIdx(t, commitFilter, refsRefCommitsCommitsSchema),
						fixIdx(t, refCommitsCommitsFilter, refsRefCommitsCommitsSchema),
					),
				),
				nil,
				[]sql.Expression{
					refFilter,
					refsRefCommitsFilter,
					refsRefCommitsHeadRedundantFilter,
					refCommitsFilter,
					commitFilter,
					refCommitsCommitsFilter,
					refCommitsCommitsRedundantFilter,
				},
				nil,
				gitbase.ReferencesTableName,
				gitbase.RefCommitsTableName,
				gitbase.CommitsTableName,
			)),
		},
		{
			"commit trees with tree entries",
			[]sql.Table{commitTrees, treeEntries},
			[]sql.Expression{
				commitTreesFilter,
				treeEntryFilter,
				commitTreeTreeEntriesFilter,
				commitTreeTreeEntriesRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewTreeTreeEntriesIter(
					gitbase.NewAllCommitTreesIter(
						fixIdx(t, commitTreesFilter, commitTreesTreeEntriesSchema),
					),
					and(
						fixIdx(t, treeEntryFilter, commitTreesTreeEntriesSchema),
						fixIdx(t, commitTreeTreeEntriesFilter, commitTreesTreeEntriesSchema),
					),
					false,
				),
				nil,
				[]sql.Expression{
					commitTreesFilter,
					treeEntryFilter,
					commitTreeTreeEntriesFilter,
					commitTreeTreeEntriesRedundantFilter,
				},
				nil,
				gitbase.CommitTreesTableName,
				gitbase.TreeEntriesTableName,
			)),
		},
		{
			"commits with commit trees",
			[]sql.Table{commits, commitTrees},
			[]sql.Expression{
				commitFilter,
				commitTreesFilter,
				commitCommitTreesFilter,
				commitCommitTreesRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitTreesIter(
					gitbase.NewAllCommitsIter(
						fixIdx(t, commitFilter, commitsCommitTreesSchema),
						false,
					),
					and(
						fixIdx(t, commitTreesFilter, commitsCommitTreesSchema),
						fixIdx(t, commitCommitTreesFilter, commitsCommitTreesSchema),
					),
					false,
				),
				nil,
				[]sql.Expression{
					commitFilter,
					commitTreesFilter,
					commitCommitTreesFilter,
					commitCommitTreesRedundantFilter,
				},
				nil,
				gitbase.CommitsTableName,
				gitbase.CommitTreesTableName,
			)),
		},
		{
			"commits with commit trees by tree",
			[]sql.Table{commits, commitTrees},
			[]sql.Expression{
				commitFilter,
				commitTreesFilter,
				commitCommitTreesFilter,
				commitCommitTreesByTreeRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitMainTreeIter(
					gitbase.NewAllCommitsIter(
						fixIdx(t, commitFilter, commitsCommitTreesSchema),
						false,
					),
					and(
						fixIdx(t, commitTreesFilter, commitsCommitTreesSchema),
						fixIdx(t, commitCommitTreesFilter, commitsCommitTreesSchema),
					),
					false,
				),
				nil,
				[]sql.Expression{
					commitFilter,
					commitTreesFilter,
					commitCommitTreesFilter,
					commitCommitTreesByTreeRedundantFilter,
				},
				nil,
				gitbase.CommitsTableName,
				gitbase.CommitTreesTableName,
			)),
		},
		{
			"ref commits with commit trees",
			[]sql.Table{refCommits, commitTrees},
			[]sql.Expression{
				refCommitsFilter,
				commitTreesFilter,
				refCommitCommitTreesFilter,
				refCommitCommitTreesRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitTreesIter(
					gitbase.NewAllRefCommitsIter(
						fixIdx(t, refCommitsFilter, refCommitsCommitTreesSchema),
					),
					and(
						fixIdx(t, commitTreesFilter, refCommitsCommitTreesSchema),
						fixIdx(t, refCommitCommitTreesFilter, refCommitsCommitTreesSchema),
					),
					false,
				),
				nil,
				[]sql.Expression{
					refCommitsFilter,
					commitTreesFilter,
					refCommitCommitTreesFilter,
					refCommitCommitTreesRedundantFilter,
				},
				nil,
				gitbase.RefCommitsTableName,
				gitbase.CommitTreesTableName,
			)),
		},
		{
			"refs with commit blobs",
			[]sql.Table{refs, commitBlobs},
			[]sql.Expression{
				refFilter,
				commitBlobsFilter,
				refCommitBlobsFilter,
				refCommitBlobsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitBlobsIter(
					gitbase.NewRefHEADCommitsIter(
						gitbase.NewAllRefsIter(
							fixIdx(t, refFilter, refsCommitBlobsSchema),
							false,
						),
						nil,
						true,
					),
					and(
						fixIdx(t, commitBlobsFilter, refsCommitBlobsSchema),
						fixIdx(t, refCommitBlobsFilter, refsCommitBlobsSchema),
					),
				),
				nil,
				[]sql.Expression{
					refFilter,
					commitBlobsFilter,
					refCommitBlobsFilter,
					refCommitBlobsRedundantFilter,
				},
				nil,
				gitbase.ReferencesTableName,
				gitbase.CommitBlobsTableName,
			)),
		},
		{
			"ref commits with commit blobs",
			[]sql.Table{refCommits, commitBlobs},
			[]sql.Expression{
				refCommitsFilter,
				commitBlobsFilter,
				refCommitCommitBlobsFilter,
				refCommitCommitBlobsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitBlobsIter(
					gitbase.NewAllRefCommitsIter(
						fixIdx(t, refCommitsFilter, refCommitsCommitBlobsSchema),
					),
					and(
						fixIdx(t, commitBlobsFilter, refCommitsCommitBlobsSchema),
						fixIdx(t, refCommitCommitBlobsFilter, refCommitsCommitBlobsSchema),
					),
				),
				nil,
				[]sql.Expression{
					refCommitsFilter,
					commitBlobsFilter,
					refCommitCommitBlobsFilter,
					refCommitCommitBlobsRedundantFilter,
				},
				nil,
				gitbase.RefCommitsTableName,
				gitbase.CommitBlobsTableName,
			)),
		},
		{
			"commits with commit blobs",
			[]sql.Table{commits, commitBlobs},
			[]sql.Expression{
				commitFilter,
				commitBlobsFilter,
				commitCommitBlobsFilter,
				commitCommitBlobsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitBlobsIter(
					gitbase.NewAllCommitsIter(
						fixIdx(t, commitFilter, commitsCommitBlobsSchema),
						false,
					),
					and(
						fixIdx(t, commitBlobsFilter, commitsCommitBlobsSchema),
						fixIdx(t, commitCommitBlobsFilter, commitsCommitBlobsSchema),
					),
				),
				nil,
				[]sql.Expression{
					commitFilter,
					commitBlobsFilter,
					commitCommitBlobsFilter,
					commitCommitBlobsRedundantFilter,
				},
				nil,
				gitbase.CommitsTableName,
				gitbase.CommitBlobsTableName,
			)),
		},
		{
			"commit blobs with blobs",
			[]sql.Table{commitBlobs, blobs},
			[]sql.Expression{
				blobFilter,
				commitBlobsFilter,
				commitBlobBlobsFilter,
				commitBlobBlobsRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitBlobBlobsIter(
					gitbase.NewAllCommitBlobsIter(
						fixIdx(t, commitBlobsFilter, commitBlobsBlobsSchema),
					),
					and(
						fixIdx(t, blobFilter, commitBlobsBlobsSchema),
						fixIdx(t, commitBlobBlobsFilter, commitBlobsBlobsSchema),
					),
					false,
				),
				nil,
				[]sql.Expression{
					blobFilter,
					commitBlobsFilter,
					commitBlobBlobsFilter,
					commitBlobBlobsRedundantFilter,
				},
				nil,
				gitbase.CommitBlobsTableName,
				gitbase.BlobsTableName,
			)),
		},
		{
			"refs with indexes",
			[]sql.Table{refs, refCommits},
			[]sql.Expression{
				refsRefCommitsRedundantFilter,
			},
			nil,
			map[string]sql.IndexLookup{
				gitbase.ReferencesTableName: idx1,
				gitbase.RefCommitsTableName: idx2,
			},
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRefRefCommitsIter(
					gitbase.NewIndexRefsIter(nil, idx1),
					nil,
				),
				nil,
				[]sql.Expression{
					refsRefCommitsRedundantFilter,
				},
				[]string{gitbase.ReferencesTableName},
				gitbase.ReferencesTableName,
				gitbase.RefCommitsTableName,
			)),
		},
		{
			"ref commits with indexes",
			[]sql.Table{refCommits, commits},
			[]sql.Expression{
				refCommitsCommitsRedundantFilter,
			},
			nil,
			map[string]sql.IndexLookup{
				gitbase.RefCommitsTableName: idx1,
				gitbase.CommitsTableName:    idx2,
			},
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewRefCommitCommitsIter(
					gitbase.NewIndexRefCommitsIter(idx1, nil),
					nil,
				),
				nil,
				[]sql.Expression{
					refCommitsCommitsRedundantFilter,
				},
				[]string{gitbase.RefCommitsTableName},
				gitbase.RefCommitsTableName,
				gitbase.CommitsTableName,
			)),
		},
		{
			"commits with indexes",
			[]sql.Table{commits, commitTrees},
			[]sql.Expression{
				commitCommitTreesRedundantFilter,
			},
			nil,
			map[string]sql.IndexLookup{
				gitbase.CommitsTableName:     idx1,
				gitbase.CommitTreesTableName: idx2,
			},
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitTreesIter(
					gitbase.NewIndexCommitsIter(idx1, nil),
					nil,
					false,
				),
				nil,
				[]sql.Expression{
					commitCommitTreesRedundantFilter,
				},
				[]string{gitbase.CommitsTableName},
				gitbase.CommitsTableName,
				gitbase.CommitTreesTableName,
			)),
		},
		{
			"commit trees with indexes",
			[]sql.Table{commitTrees, treeEntries},
			[]sql.Expression{
				commitTreeTreeEntriesRedundantFilter,
			},
			nil,
			map[string]sql.IndexLookup{
				gitbase.CommitTreesTableName: idx1,
				gitbase.TreeEntriesTableName: idx2,
			},
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewTreeTreeEntriesIter(
					gitbase.NewIndexCommitTreesIter(idx1, nil),
					nil,
					false,
				),
				nil,
				[]sql.Expression{
					commitTreeTreeEntriesRedundantFilter,
				},
				[]string{gitbase.CommitTreesTableName},
				gitbase.CommitTreesTableName,
				gitbase.TreeEntriesTableName,
			)),
		},
		{
			"commit blobs with indexes",
			[]sql.Table{commitBlobs, blobs},
			[]sql.Expression{
				commitBlobBlobsRedundantFilter,
			},
			nil,
			map[string]sql.IndexLookup{
				gitbase.CommitBlobsTableName: idx1,
				gitbase.BlobsTableName:       idx2,
			},
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitBlobBlobsIter(
					gitbase.NewIndexCommitBlobsIter(idx1, nil),
					nil,
					false,
				),
				nil,
				[]sql.Expression{
					commitBlobBlobsRedundantFilter,
				},
				[]string{gitbase.CommitBlobsTableName},
				gitbase.CommitBlobsTableName,
				gitbase.BlobsTableName,
			)),
		},
		{
			"tree entries with indexes",
			[]sql.Table{treeEntries, blobs},
			[]sql.Expression{
				treeEntryBlobsRedundantFilter,
			},
			nil,
			map[string]sql.IndexLookup{
				gitbase.TreeEntriesTableName: idx1,
				gitbase.BlobsTableName:       idx2,
			},
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewTreeEntryBlobsIter(
					gitbase.NewIndexTreeEntriesIter(idx1, nil),
					nil,
					false,
				),
				nil,
				[]sql.Expression{
					treeEntryBlobsRedundantFilter,
				},
				[]string{gitbase.TreeEntriesTableName},
				gitbase.TreeEntriesTableName,
				gitbase.BlobsTableName,
			)),
		},
		{
			"refs with commit_files",
			[]sql.Table{refs, commitFiles},
			[]sql.Expression{
				refFilter,
				commitFilesFilter,
				refsCommitFilesFilter,
				refsCommitFilesRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitFilesIter(
					gitbase.NewRefHEADCommitsIter(gitbase.NewAllRefsIter(
						fixIdx(t, refFilter, gitbase.RefsSchema),
						false,
					), nil, true),
					and(
						fixIdx(t, commitFilesFilter, refsCommitFilesSchema),
						fixIdx(t, refsCommitFilesFilter, refsCommitFilesSchema),
					),
				),
				nil,
				[]sql.Expression{
					refFilter,
					commitFilesFilter,
					refsCommitFilesFilter,
					refsCommitFilesRedundantFilter,
				},
				nil,
				gitbase.ReferencesTableName,
				gitbase.CommitFilesTableName,
			)),
		},
		{
			"commits with commit_files",
			[]sql.Table{commits, commitFiles},
			[]sql.Expression{
				commitFilter,
				commitFilesFilter,
				commitsCommitFilesFilter,
				commitsCommitFilesRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitFilesIter(
					gitbase.NewAllCommitsIter(
						fixIdx(t, commitFilter, gitbase.CommitsSchema),
						false,
					),
					and(
						fixIdx(t, commitFilesFilter, commitsCommitFilesSchema),
						fixIdx(t, commitsCommitFilesFilter, commitsCommitFilesSchema),
					),
				),
				nil,
				[]sql.Expression{
					commitFilter,
					commitFilesFilter,
					commitsCommitFilesFilter,
					commitsCommitFilesRedundantFilter,
				},
				nil,
				gitbase.CommitsTableName,
				gitbase.CommitFilesTableName,
			)),
		},
		{
			"commit_files with files",
			[]sql.Table{commitFiles, files},
			[]sql.Expression{
				filesFilter,
				commitFilesFilter,
				commitFilesFilesFilter,
				commitFilesFilesFilePathRedundantFilter,
				commitFilesFilesTreeHashRedundantFilter,
				commitFilesFilesBlobHashRedundantFilter,
			},
			nil,
			nil,
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitFileFilesIter(
					gitbase.NewAllCommitFilesIter(
						fixIdx(t, commitFilesFilter, gitbase.CommitFilesSchema),
					),
					and(
						fixIdx(t, filesFilter, commitFilesFilesSchema),
						fixIdx(t, commitFilesFilesFilter, commitFilesFilesSchema),
					),
					false,
				),
				nil,
				[]sql.Expression{
					filesFilter,
					commitFilesFilter,
					commitFilesFilesFilter,
					commitFilesFilesFilePathRedundantFilter,
					commitFilesFilesTreeHashRedundantFilter,
					commitFilesFilesBlobHashRedundantFilter,
				},
				nil,
				gitbase.CommitFilesTableName,
				gitbase.FilesTableName,
			)),
		},
		{
			"commit_files with indexes",
			[]sql.Table{commitFiles, files},
			[]sql.Expression{
				commitFilesFilesBlobHashRedundantFilter,
				commitFilesFilesTreeHashRedundantFilter,
				commitFilesFilesFilePathRedundantFilter,
			},
			nil,
			map[string]sql.IndexLookup{
				gitbase.CommitFilesTableName: idx1,
				gitbase.FilesTableName:       idx2,
			},
			nil,
			plan.NewResolvedTable(gitbase.NewSquashedTable(
				gitbase.NewCommitFileFilesIter(
					gitbase.NewIndexCommitFilesIter(idx1, nil),
					nil,
					false,
				),
				nil,
				[]sql.Expression{
					commitFilesFilesBlobHashRedundantFilter,
					commitFilesFilesTreeHashRedundantFilter,
					commitFilesFilesFilePathRedundantFilter,
				},
				[]string{gitbase.CommitFilesTableName},
				gitbase.CommitFilesTableName,
				gitbase.FilesTableName,
			)),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			if tt.indexes == nil {
				tt.indexes = make(map[string]sql.IndexLookup)
			}

			result, err := buildSquashedTable(
				analyzer.NewBuilder(nil).WithParallelism(2).Build(),
				tt.tables,
				tt.filters,
				tt.columns,
				tt.indexes,
			)
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
	tables := gitbaseTables()
	t1 := plan.NewResolvedTable(
		tables[gitbase.RepositoriesTableName],
	)
	t2 := plan.NewResolvedTable(
		tables[gitbase.ReferencesTableName],
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
	tables := gitbase.NewDatabase("foo", gitbase.NewRepositoryPool(0)).Tables()

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
			col(0, gitbase.ReferencesTableName, "commit_hash"),
			lit(0),
		),
		eq(
			col(0, gitbase.RemotesTableName, "remote_name"),
			lit(1),
		),
		eq(
			col(0, gitbase.RepositoriesTableName, "repository_id"),
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
				col(8, gitbase.ReferencesTableName, "commit_hash"),
				lit(0),
			),
			eq(
				col(1, gitbase.RemotesTableName, "remote_name"),
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
			col(0, gitbase.ReferencesTableName, "commit_hash"),
			lit(0),
		),
		eq(
			col(0, gitbase.ReferencesTableName, "commit_hash"),
			lit(1),
		),
		eq(
			col(0, gitbase.RepositoriesTableName, "repository_id"),
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
				col(2, gitbase.ReferencesTableName, "commit_hash"),
				lit(0),
			),
			eq(
				col(2, gitbase.ReferencesTableName, "commit_hash"),
				lit(1),
			),
		),
		filter,
	)
}

func TestRemoveRedundantFilters(t *testing.T) {
	f1 := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		col(0, gitbase.ReferencesTableName, "repository_id"),
	)

	f2 := eq(
		col(0, gitbase.RepositoriesTableName, "repository_id"),
		lit(0),
	)

	result := removeRedundantFilters(
		[]sql.Expression{f1, f2},
		gitbase.RepositoriesTableName,
		gitbase.ReferencesTableName,
	)

	require.Equal(t, []sql.Expression{f2}, result)
}

func TestRemoveRedundantCompoundFilters(t *testing.T) {
	f := eq(
		col(0, gitbase.CommitFilesTableName, "file_path"),
		col(0, gitbase.FilesTableName, "repository_id"),
	)

	result := removeRedundantCompoundFilters(
		[]sql.Expression{
			f,
			eq(
				col(0, gitbase.CommitFilesTableName, "tree_hash"),
				col(0, gitbase.FilesTableName, "tree_hash"),
			),
			eq(
				col(0, gitbase.CommitFilesTableName, "blob_hash"),
				col(0, gitbase.FilesTableName, "blob_hash"),
			),
			eq(
				col(0, gitbase.CommitFilesTableName, "file_path"),
				col(0, gitbase.FilesTableName, "file_path"),
			),
		},
		gitbase.CommitFilesTableName,
		gitbase.FilesTableName,
	)

	require.Equal(t, []sql.Expression{f}, result)
}

func TestIsJoinCondSquashable(t *testing.T) {
	require := require.New(t)
	tables := gitbaseTables()
	repos := plan.NewResolvedTable(
		tables[gitbase.ReferencesTableName],
	)
	refs := plan.NewResolvedTable(
		tables[gitbase.ReferencesTableName],
	)
	commits := plan.NewResolvedTable(
		tables[gitbase.CommitsTableName],
	)

	node := plan.NewInnerJoin(
		refs,
		commits,
		and(
			eq(
				col(0, gitbase.ReferencesTableName, "commit_hash"),
				col(0, gitbase.CommitsTableName, "commit_hash"),
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
				col(0, gitbase.ReferencesTableName, "commit_hash"),
				col(0, gitbase.CommitsTableName, "commit_message"),
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
				col(0, gitbase.ReferencesTableName, "commit_hash"),
				col(0, gitbase.CommitsTableName, "commit_hash"),
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
				col(0, gitbase.RepositoriesTableName, "repository_id"),
				col(0, gitbase.RemotesTableName, "repository_id"),
			),
			true,
		},
		{
			gitbase.RepositoriesTableName,
			gitbase.RemotesTableName,
			eq(
				col(0, gitbase.RemotesTableName, "repository_id"),
				col(0, gitbase.RepositoriesTableName, "repository_id"),
			),
			true,
		},
		{
			gitbase.RepositoriesTableName,
			gitbase.ReferencesTableName,
			eq(
				col(0, gitbase.RepositoriesTableName, "repository_id"),
				col(0, gitbase.ReferencesTableName, "repository_id"),
			),
			true,
		},
		{
			gitbase.RepositoriesTableName,
			gitbase.ReferencesTableName,
			eq(
				col(0, gitbase.ReferencesTableName, "repository_id"),
				col(0, gitbase.RepositoriesTableName, "repository_id"),
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
				col(0, gitbase.ReferencesTableName, "commit_hash"),
				col(0, gitbase.CommitsTableName, "commit_hash"),
			),
			true,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.RefCommitsTableName,
			eq(
				col(0, gitbase.ReferencesTableName, "commit_hash"),
				col(0, gitbase.RefCommitsTableName, "commit_hash"),
			),
			true,
		},
		{
			gitbase.RefCommitsTableName,
			gitbase.CommitsTableName,
			eq(
				col(0, gitbase.CommitsTableName, "commit_hash"),
				col(0, gitbase.RefCommitsTableName, "commit_hash"),
			),
			true,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.CommitTreesTableName,
			eq(
				col(0, gitbase.ReferencesTableName, "commit_hash"),
				col(0, gitbase.CommitTreesTableName, "commit_hash"),
			),
			true,
		},
		{
			gitbase.RefCommitsTableName,
			gitbase.CommitTreesTableName,
			eq(
				col(0, gitbase.RefCommitsTableName, "commit_hash"),
				col(0, gitbase.CommitTreesTableName, "commit_hash"),
			),
			true,
		},
		{
			gitbase.CommitsTableName,
			gitbase.CommitTreesTableName,
			eq(
				col(0, gitbase.CommitsTableName, "commit_hash"),
				col(0, gitbase.CommitTreesTableName, "commit_hash"),
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
			gitbase.TreeEntriesTableName,
			gitbase.BlobsTableName,
			eq(
				col(0, gitbase.TreeEntriesTableName, "blob_hash"),
				col(0, gitbase.BlobsTableName, "blob_hash"),
			),
			true,
		},
		{
			gitbase.TreeEntriesTableName,
			gitbase.BlobsTableName,
			eq(
				col(0, gitbase.BlobsTableName, "blob_hash"),
				col(0, gitbase.TreeEntriesTableName, "blob_hash"),
			),
			true,
		},
		{
			gitbase.TreeEntriesTableName,
			gitbase.BlobsTableName,
			eq(
				col(0, gitbase.TreeEntriesTableName, "tree_hash"),
				col(0, gitbase.BlobsTableName, "blob_hash"),
			),
			false,
		},
		{
			gitbase.RepositoriesTableName,
			gitbase.CommitsTableName,
			eq(
				col(0, gitbase.RepositoriesTableName, "repository_id"),
				col(0, gitbase.CommitsTableName, "repository_id"),
			),
			true,
		},
		{
			gitbase.RepositoriesTableName,
			gitbase.TreeEntriesTableName,
			eq(
				col(0, gitbase.RepositoriesTableName, "repository_id"),
				col(0, gitbase.TreeEntriesTableName, "repository_id"),
			),
			true,
		},
		{
			gitbase.RepositoriesTableName,
			gitbase.BlobsTableName,
			eq(
				col(0, gitbase.RepositoriesTableName, "repository_id"),
				col(0, gitbase.BlobsTableName, "repository_id"),
			),
			true,
		},
		{
			gitbase.CommitTreesTableName,
			gitbase.TreeEntriesTableName,
			eq(
				col(0, gitbase.CommitTreesTableName, "tree_hash"),
				col(0, gitbase.TreeEntriesTableName, "tree_hash"),
			),
			true,
		},
		{
			gitbase.ReferencesTableName,
			gitbase.CommitBlobsTableName,
			eq(
				col(0, gitbase.ReferencesTableName, "commit_hash"),
				col(0, gitbase.CommitBlobsTableName, "commit_hash"),
			),
			true,
		},
		{
			gitbase.RefCommitsTableName,
			gitbase.CommitBlobsTableName,
			eq(
				col(0, gitbase.RefCommitsTableName, "commit_hash"),
				col(0, gitbase.CommitBlobsTableName, "commit_hash"),
			),
			true,
		},
		{
			gitbase.CommitsTableName,
			gitbase.CommitBlobsTableName,
			eq(
				col(0, gitbase.CommitsTableName, "commit_hash"),
				col(0, gitbase.CommitBlobsTableName, "commit_hash"),
			),
			true,
		},
		{
			gitbase.CommitBlobsTableName,
			gitbase.BlobsTableName,
			eq(
				col(0, gitbase.CommitBlobsTableName, "blob_hash"),
				col(0, gitbase.BlobsTableName, "blob_hash"),
			),
			true,
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

func TestHasRedundantCompoundFilter(t *testing.T) {
	testCases := []struct {
		t1, t2   string
		filters  []sql.Expression
		expected bool
	}{
		{
			gitbase.ReferencesTableName,
			gitbase.RefCommitsTableName,
			[]sql.Expression{
				eq(
					col(0, gitbase.ReferencesTableName, "ref_name"),
					col(0, gitbase.RefCommitsTableName, "ref_name"),
				),
				eq(
					col(0, gitbase.ReferencesTableName, "commit_hash"),
					col(0, gitbase.RefCommitsTableName, "commit_hash"),
				),
			},
			false,
		},
		{
			gitbase.CommitFilesTableName,
			gitbase.FilesTableName,
			[]sql.Expression{
				eq(
					col(0, gitbase.CommitFilesTableName, "tree_hash"),
					col(0, gitbase.FilesTableName, "tree_hash"),
				),
				eq(
					col(0, gitbase.CommitFilesTableName, "blob_hash"),
					col(0, gitbase.FilesTableName, "blob_hash"),
				),
				eq(
					col(0, gitbase.CommitFilesTableName, "file_path"),
					col(0, gitbase.FilesTableName, "file_path"),
				),
			},
			true,
		},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprint(and(tt.filters...)), func(t *testing.T) {
			require.Equal(
				t,
				hasRedundantCompoundFilter(tt.filters, tt.t1, tt.t2),
				tt.expected,
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

func colT(idx int, typ sql.Type, table, name string) sql.Expression {
	return expression.NewGetFieldWithTable(idx, typ, table, name, false)
}

func lit(v interface{}) sql.Expression {
	return expression.NewLiteral(v, sql.Int64)
}

func and(exprs ...sql.Expression) sql.Expression {
	return expression.JoinAnd(exprs...)
}

type dummyLookup struct {
	n int
}

func (dummyLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	panic("dummyLookup Values is a placeholder")
}

func (l dummyLookup) Indexes() []string {
	return []string{fmt.Sprintf("index_%d", l.n)}
}

func gitbaseTables() map[string]sql.Table {
	return gitbase.NewDatabase("", gitbase.NewRepositoryPool(0)).Tables()
}
