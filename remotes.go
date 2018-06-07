package gitbase

import (
	"fmt"
	"io"

	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

type remotesTable struct{}

// RemotesSchema is the schema for the remotes table.
var RemotesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: RemotesTableName},
	{Name: "remote_name", Type: sql.Text, Nullable: false, Source: RemotesTableName},
	{Name: "remote_push_url", Type: sql.Text, Nullable: false, Source: RemotesTableName},
	{Name: "remote_fetch_url", Type: sql.Text, Nullable: false, Source: RemotesTableName},
	{Name: "remote_push_refspec", Type: sql.Text, Nullable: false, Source: RemotesTableName},
	{Name: "remote_fetch_refspec", Type: sql.Text, Nullable: false, Source: RemotesTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*remotesTable)(nil)

func newRemotesTable() Indexable {
	return new(remotesTable)
}

var _ Table = (*remotesTable)(nil)
var _ Squashable = (*remotesTable)(nil)

func (remotesTable) isSquashable()   {}
func (remotesTable) isGitbaseTable() {}

func (remotesTable) Resolved() bool {
	return true
}

func (remotesTable) Name() string {
	return RemotesTableName
}

func (remotesTable) Schema() sql.Schema {
	return RemotesSchema
}

func (r remotesTable) String() string {
	return printTable(RemotesTableName, RemotesSchema)
}

func (r *remotesTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(r)
}

func (r *remotesTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return r, nil
}

func (r remotesTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.RemotesTable")
	iter := new(remotesIter)

	rowRepoIter, err := NewRowRepoIter(ctx, iter)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, rowRepoIter), nil
}

func (remotesTable) Children() []sql.Node {
	return nil
}

func (remotesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(RemotesTableName, RemotesSchema, filters)
}

func (remotesTable) handledColumns() []string { return []string{} }

func (r *remotesTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.RemotesTable")
	iter, err := rowIterWithSelectors(
		ctx, RemotesSchema, RemotesTableName,
		filters, nil,
		r.handledColumns(),
		remotesIterBuilder,
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// IndexKeyValueIter implements the sql.Indexable interface.
func (*remotesTable) IndexKeyValueIter(
	ctx *sql.Context,
	colNames []string,
) (sql.IndexKeyValueIter, error) {
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	iter, err := s.Pool.RepoIter()
	if err != nil {
		return nil, err
	}

	return &remotesKeyValueIter{repos: iter, columns: colNames}, nil
}

// WithProjectFiltersAndIndex implements sql.Indexable interface.
func (*remotesTable) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.ReferencesTable.WithProjectFiltersAndIndex")
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		span.Finish()
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	var iter sql.RowIter = &remotesIndexIter{index: index, pool: s.Pool}

	if len(filters) > 0 {
		iter = plan.NewFilterIter(ctx, expression.JoinAnd(filters...), iter)
	}

	return sql.NewSpanIter(span, iter), nil
}

func remotesIterBuilder(_ *sql.Context, _ selectors, _ []sql.Expression) (RowRepoIter, error) {
	// it's not worth to manually filter with the selectors
	return new(remotesIter), nil
}

type remotesIter struct {
	repositoryID string
	remotes      []*git.Remote
	remotePos    int
	urlPos       int
}

func (i *remotesIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	remotes, err := repo.Repo.Remotes()
	if err != nil {
		return nil, err
	}

	return &remotesIter{
		repositoryID: repo.ID,
		remotes:      remotes,
		remotePos:    0,
		urlPos:       0}, nil
}

func (i *remotesIter) Next() (sql.Row, error) {
	if i.remotePos >= len(i.remotes) {
		return nil, io.EOF
	}

	remote := i.remotes[i.remotePos]
	config := remote.Config()

	if i.urlPos >= len(config.URLs) {
		i.remotePos++
		if i.remotePos >= len(i.remotes) {
			return nil, io.EOF
		}

		remote = i.remotes[i.remotePos]
		config = remote.Config()
		i.urlPos = 0
	}

	row := remoteToRow(i.repositoryID, config, i.urlPos)
	i.urlPos++

	return row, nil
}

func (i *remotesIter) Close() error {
	return nil
}

func remoteToRow(repoID string, config *config.RemoteConfig, pos int) sql.Row {
	return sql.NewRow(
		repoID,
		config.Name,
		config.URLs[pos],
		config.URLs[pos],
		config.Fetch[pos].String(),
		config.Fetch[pos].String(),
	)
}

type remoteIndexKey struct {
	Repository string
	Pos        int
	URLPos     int
}

type remotesKeyValueIter struct {
	repos   *RepositoryIter
	repo    *Repository
	columns []string
	remotes []*git.Remote
	pos     int
	urlPos  int
}

func (i *remotesKeyValueIter) Next() ([]interface{}, []byte, error) {
	for {
		if len(i.remotes) == 0 {
			var err error
			i.repo, err = i.repos.Next()
			if err != nil {
				return nil, nil, err
			}

			i.remotes, err = i.repo.Repo.Remotes()
			if err != nil {
				return nil, nil, err
			}

			i.pos = 0
			i.urlPos = 0
		}

		if i.pos >= len(i.remotes) {
			i.remotes = nil
			continue
		}

		cfg := i.remotes[i.pos].Config()
		if i.urlPos >= len(cfg.URLs) {
			i.pos++
			continue
		}

		i.urlPos++

		fmt.Println(remoteIndexKey{i.repo.ID, i.pos, i.urlPos - 1})

		key, err := encodeIndexKey(remoteIndexKey{i.repo.ID, i.pos, i.urlPos - 1})
		if err != nil {
			return nil, nil, err
		}

		row := remoteToRow(i.repo.ID, cfg, i.urlPos-1)
		values, err := rowIndexValues(row, i.columns, RemotesSchema)
		if err != nil {
			return nil, nil, err
		}

		return values, key, nil
	}
}

func (i *remotesKeyValueIter) Close() error {
	return i.repos.Close()
}

type remotesIndexIter struct {
	index   sql.IndexValueIter
	pool    *RepositoryPool
	repo    *Repository
	remotes []*git.Remote
}

func (i *remotesIndexIter) Next() (sql.Row, error) {
	data, err := i.index.Next()
	if err != nil {
		return nil, err
	}

	var key remoteIndexKey
	if err := decodeIndexKey(data, &key); err != nil {
		return nil, err
	}

	if i.repo == nil || i.repo.ID != key.Repository {
		i.repo, err = i.pool.GetRepo(key.Repository)
		if err != nil {
			return nil, err
		}

		i.remotes, err = i.repo.Repo.Remotes()
		if err != nil {
			return nil, err
		}
	}

	config := i.remotes[key.Pos].Config()
	return remoteToRow(key.Repository, config, key.URLPos), nil
}

func (i *remotesIndexIter) Close() error { return i.index.Close() }
