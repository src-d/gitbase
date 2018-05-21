package rule

import (
	"testing"

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
						false,
					),
					nil,
					false,
				),
				[]int{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 0, 1, 2, 3},
				gitbase.RepositoriesTableName,
				gitbase.ReferencesTableName,
				gitbase.CommitsTableName,
			),
		),
	)

	result, err := SquashJoins(sql.NewEmptyContext(), analyzer.New(nil), node)
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

	result, err := SquashJoins(sql.NewEmptyContext(), analyzer.New(nil), node)
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
			plan.NewLimit(1, plan.NewPushdownProjectionAndFiltersTable(
				nil, nil,
				tables[gitbase.CommitsTableName].(sql.PushdownProjectionAndFiltersTable),
			)),
			newSquashedTable(
				gitbase.NewRepoRefsIter(
					gitbase.NewAllReposIter(lit(4)),
					nil,
					false,
				),
				nil,
				gitbase.RepositoriesTableName,
				gitbase.ReferencesTableName,
			),
			lit(3),
		),
	)

	result, err := SquashJoins(sql.NewEmptyContext(), analyzer.New(nil), node)
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

	result, err := SquashJoins(sql.NewEmptyContext(), analyzer.New(nil), node)
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
	refCommits := tables[gitbase.RefCommitsTableName]
	remotes := tables[gitbase.RemotesTableName]
	commits := tables[gitbase.CommitsTableName]
	treeEntries := tables[gitbase.TreeEntriesTableName]
	blobs := tables[gitbase.BlobsTableName]
	commitTrees := tables[gitbase.CommitTreesTableName]

	repoRefCommitsSchema := append(gitbase.RepositoriesSchema, gitbase.RefCommitsSchema...)
	remoteRefsSchema := append(gitbase.RemotesSchema, gitbase.RefsSchema...)
	refCommitsSchema := append(gitbase.RefsSchema, gitbase.CommitsSchema...)
	commitTreeEntriesSchema := append(gitbase.CommitsSchema, gitbase.TreeEntriesSchema...)
	treeEntryBlobsSchema := append(gitbase.TreeEntriesSchema, gitbase.BlobsSchema...)
	refsBlobsSchema := append(gitbase.RefsSchema, gitbase.BlobsSchema...)
	commitBlobsSchema := append(gitbase.CommitsSchema, gitbase.BlobsSchema...)
	repoCommitsSchema := append(gitbase.RepositoriesSchema, gitbase.CommitsSchema...)
	repoTreeEntriesSchema := append(gitbase.RepositoriesSchema, gitbase.TreeEntriesSchema...)
	repoBlobsSchema := append(gitbase.RepositoriesSchema, gitbase.BlobsSchema...)
	refsRefCommitsCommitsSchema := append(append(gitbase.RefsSchema, gitbase.RefCommitsSchema...), gitbase.CommitsSchema...)
	refCommitsCommitsSchema := append(gitbase.RefCommitsSchema, gitbase.CommitsSchema...)
	commitsCommitTreesSchema := append(gitbase.CommitsSchema, gitbase.CommitTreesSchema...)
	refsCommitTreesSchema := append(gitbase.RefsSchema, gitbase.CommitTreesSchema...)
	refCommitsCommitTreesSchema := append(gitbase.RefCommitsSchema, gitbase.CommitTreesSchema...)
	commitTreesTreeEntriesSchema := append(gitbase.CommitTreesSchema, gitbase.TreeEntriesSchema...)

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

	blobContentFilter := eq(
		col(0, gitbase.BlobsTableName, "blob_content"),
		col(0, gitbase.BlobsTableName, "blob_content"),
	)

	treeEntryBlobsRedundantFilter := eq(
		col(0, gitbase.TreeEntriesTableName, "blob_hash"),
		col(0, gitbase.BlobsTableName, "blob_hash"),
	)

	treeEntryBlobsFilter := eq(
		col(0, gitbase.TreeEntriesTableName, "tree_hash"),
		col(0, gitbase.BlobsTableName, "blob_hash"),
	)

	refBlobsRedundantFilter := commitHasBlob(
		col(0, gitbase.ReferencesTableName, "commit_hash"),
		col(0, gitbase.BlobsTableName, "blob_hash"),
	)

	refBlobsFilter := eq(
		col(0, gitbase.ReferencesTableName, "ref_name"),
		col(0, gitbase.BlobsTableName, "blob_hash"),
	)

	commitBlobsRedundantFilter := commitHasBlob(
		col(0, gitbase.CommitsTableName, "commit_hash"),
		col(0, gitbase.BlobsTableName, "blob_hash"),
	)

	commitBlobsFilter := eq(
		col(0, gitbase.CommitsTableName, "commit_hash"),
		col(0, gitbase.BlobsTableName, "blob_size"),
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

	testCases := []struct {
		name     string
		tables   []sql.Table
		filters  []sql.Expression
		columns  []sql.Expression
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
			nil,
			newSquashedTable(
				gitbase.NewRepoRefsIter(
					gitbase.NewAllReposIter(repoFilter),
					and(
						refFilter,
						repoRefsFilter,
					),
					false,
				),
				nil,
				gitbase.RepositoriesTableName,
				gitbase.ReferencesTableName,
			),
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
			newSquashedTable(
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
				gitbase.ReferencesTableName,
				gitbase.CommitsTableName,
			),
		},
		{
			"remotes with commits",
			[]sql.Table{remotes, commits},
			nil,
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
			nil,
			newSquashedTable(
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
				gitbase.CommitsTableName,
				gitbase.TreeEntriesTableName,
			),
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
			newSquashedTable(
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
				gitbase.ReferencesTableName,
				gitbase.CommitTreesTableName,
			),
		},
		{
			"remotes with tree entries",
			[]sql.Table{remotes, treeEntries},
			nil,
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
					false,
				),
				nil,
				gitbase.TreeEntriesTableName,
				gitbase.BlobsTableName,
			),
		},
		{
			"remotes with blobs",
			[]sql.Table{remotes, blobs},
			nil,
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
			nil,
			newSquashedTable(
				gitbase.NewCommitBlobsIter(
					gitbase.NewRefHEADCommitsIter(
						gitbase.NewAllRefsIter(
							fixIdx(t, refFilter, refsBlobsSchema),
							false,
						),
						nil,
						true,
					),
					and(
						fixIdx(t, blobFilter, refsBlobsSchema),
						fixIdx(t, refBlobsFilter, refsBlobsSchema),
					),
					false,
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
			nil,
			newSquashedTable(
				gitbase.NewCommitBlobsIter(
					gitbase.NewAllCommitsIter(
						fixIdx(t, commitFilter, commitBlobsSchema),
						false,
					),
					and(
						fixIdx(t, blobFilter, commitBlobsSchema),
						fixIdx(t, commitBlobsFilter, commitBlobsSchema),
					),
					false,
				),
				nil,
				gitbase.CommitsTableName,
				gitbase.BlobsTableName,
			),
		},
		{
			"commits with blobs using content",
			[]sql.Table{commits, blobs},
			[]sql.Expression{
				commitFilter,
				blobContentFilter,
				commitBlobsFilter,
				commitBlobsRedundantFilter,
			},
			[]sql.Expression{expression.NewGetFieldWithTable(
				0, sql.Int64, gitbase.BlobsTableName, "blob_content", false,
			)},
			nil,
			newSquashedTable(
				gitbase.NewCommitBlobsIter(
					gitbase.NewAllCommitsIter(
						fixIdx(t, commitFilter, commitBlobsSchema),
						false,
					),
					and(
						fixIdx(t, blobContentFilter, commitBlobsSchema),
						fixIdx(t, commitBlobsFilter, commitBlobsSchema),
					),
					true,
				),
				nil,
				gitbase.CommitsTableName,
				gitbase.BlobsTableName,
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
			newSquashedTable(
				gitbase.NewRepoCommitsIter(
					gitbase.NewAllReposIter(repoFilter),
					and(
						fixIdx(t, commitFilter, repoCommitsSchema),
						fixIdx(t, repoCommitsFilter, repoCommitsSchema),
					),
				),
				nil,
				gitbase.RepositoriesTableName,
				gitbase.CommitsTableName,
			),
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
			newSquashedTable(
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
				gitbase.ReferencesTableName,
				gitbase.RefCommitsTableName,
			),
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
			newSquashedTable(
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
				gitbase.ReferencesTableName,
				gitbase.RefCommitsTableName,
			),
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
			newSquashedTable(
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
				gitbase.RefCommitsTableName,
				gitbase.CommitsTableName,
			),
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
			newSquashedTable(
				gitbase.NewRepoTreeEntriesIter(
					gitbase.NewAllReposIter(repoFilter),
					and(
						fixIdx(t, treeEntryFilter, repoTreeEntriesSchema),
						fixIdx(t, repoTreeEntriesFilter, repoTreeEntriesSchema),
					),
				),
				nil,
				gitbase.RepositoriesTableName,
				gitbase.TreeEntriesTableName,
			),
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
			newSquashedTable(
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
				gitbase.RepositoriesTableName,
				gitbase.RefCommitsTableName,
			),
		},
		{
			"blobs with tree entries",
			[]sql.Table{repositories, blobs},
			[]sql.Expression{
				repoFilter,
				blobFilter,
				repoBlobsFilter,
				repoBlobsRedundantFilter,
			},
			[]sql.Expression{expression.NewGetFieldWithTable(
				0, sql.Int64, gitbase.BlobsTableName, "blob_content", false,
			)},
			nil,
			newSquashedTable(
				gitbase.NewRepoBlobsIter(
					gitbase.NewAllReposIter(repoFilter),
					and(
						fixIdx(t, blobFilter, repoBlobsSchema),
						fixIdx(t, repoBlobsFilter, repoBlobsSchema),
					),
					true,
				),
				nil,
				gitbase.RepositoriesTableName,
				gitbase.BlobsTableName,
			),
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
			newSquashedTable(
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
				gitbase.ReferencesTableName,
				gitbase.RefCommitsTableName,
				gitbase.CommitsTableName,
			),
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
			newSquashedTable(
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
				gitbase.ReferencesTableName,
				gitbase.RefCommitsTableName,
				gitbase.CommitsTableName,
			),
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
			newSquashedTable(
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
				gitbase.CommitTreesTableName,
				gitbase.TreeEntriesTableName,
			),
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
			newSquashedTable(
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
				gitbase.CommitsTableName,
				gitbase.CommitTreesTableName,
			),
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
			newSquashedTable(
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
				gitbase.CommitsTableName,
				gitbase.CommitTreesTableName,
			),
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
			newSquashedTable(
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
				gitbase.RefCommitsTableName,
				gitbase.CommitTreesTableName,
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := buildSquashedTable(tt.tables, tt.filters, tt.columns)
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
			gitbase.ReferencesTableName,
			gitbase.BlobsTableName,
			commitHasBlob(
				col(0, gitbase.ReferencesTableName, "commit_hash"),
				col(0, gitbase.BlobsTableName, "blob_hash"),
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
				col(0, gitbase.CommitsTableName, "commit_hash"),
				col(0, gitbase.BlobsTableName, "blob_hash"),
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

func commitHasBlob(left, right sql.Expression) sql.Expression {
	return function.NewCommitHasBlob(left, right)
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
