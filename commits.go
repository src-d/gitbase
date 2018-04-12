package gitbase

import (
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type commitsTable struct {
}

// CommitsSchema is the schema for the commits table.
var CommitsSchema = sql.Schema{
	{Name: "hash", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "author_name", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "author_email", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "author_when", Type: sql.Timestamp, Nullable: false, Source: CommitsTableName},
	{Name: "committer_name", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "committer_email", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "committer_when", Type: sql.Timestamp, Nullable: false, Source: CommitsTableName},
	{Name: "message", Type: sql.Text, Nullable: false, Source: CommitsTableName},
	{Name: "tree_hash", Type: sql.Text, Nullable: false, Source: CommitsTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*commitsTable)(nil)

func newCommitsTable() sql.Table {
	return new(commitsTable)
}

var _ Table = (*commitsTable)(nil)

func (commitsTable) isGitbaseTable() {}

func (commitsTable) String() string {
	return printTable(CommitsTableName, CommitsSchema)
}

func (commitsTable) Resolved() bool {
	return true
}

func (commitsTable) Name() string {
	return CommitsTableName
}

func (commitsTable) Schema() sql.Schema {
	return CommitsSchema
}

func (r *commitsTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(r)
}

func (r *commitsTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return r, nil
}

func (r commitsTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitsTable")
	iter := new(commitIter)

	repoIter, err := NewRowRepoIter(ctx, iter)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, repoIter), nil
}

func (commitsTable) Children() []sql.Node {
	return nil
}

func (commitsTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(CommitsTableName, CommitsSchema, filters)
}

func (r *commitsTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.CommitsTable")
	iter, err := rowIterWithSelectors(
		ctx, CommitsSchema, CommitsTableName, filters,
		[]string{"hash"},
		func(selectors selectors) (RowRepoIter, error) {
			if len(selectors["hash"]) == 0 {
				return new(commitIter), nil
			}

			hashes, err := selectors.textValues("hash")
			if err != nil {
				return nil, err
			}

			return &commitsByHashIter{hashes: hashes}, nil
		},
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

type commitIter struct {
	iter object.CommitIter
}

func (i *commitIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	iter, err := repo.Repo.CommitObjects()
	if err != nil {
		return nil, err
	}

	return &commitIter{iter: iter}, nil
}

func (i *commitIter) Next() (sql.Row, error) {
	o, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	return commitToRow(o), nil
}

func (i *commitIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	return nil
}

type commitsByHashIter struct {
	repo   *Repository
	pos    int
	hashes []string
}

func (i *commitsByHashIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	return &commitsByHashIter{repo, 0, i.hashes}, nil
}

func (i *commitsByHashIter) Next() (sql.Row, error) {
	for {
		if i.pos >= len(i.hashes) {
			return nil, io.EOF
		}

		hash := plumbing.NewHash(i.hashes[i.pos])
		i.pos++
		commit, err := i.repo.Repo.CommitObject(hash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		if err != nil {
			return nil, err
		}

		return commitToRow(commit), nil
	}
}

func (i *commitsByHashIter) Close() error {
	return nil
}

func commitToRow(c *object.Commit) sql.Row {
	return sql.NewRow(
		c.Hash.String(),
		c.Author.Name,
		c.Author.Email,
		c.Author.When,
		c.Committer.Name,
		c.Committer.Email,
		c.Committer.When,
		c.Message,
		c.TreeHash.String(),
	)
}
