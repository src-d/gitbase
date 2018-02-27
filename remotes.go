package gitquery

import (
	"io"

	git "gopkg.in/src-d/go-git.v4"
	gitconfig "gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type remotesTable struct {
	pool *RepositoryPool
}

func newRemotesTable(pool *RepositoryPool) sql.Table {
	return &remotesTable{pool: pool}
}

func (remotesTable) Resolved() bool {
	return true
}

func (remotesTable) Name() string {
	return remotesTableName
}

func (remotesTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "repository_id", Type: sql.Text, Nullable: false},
		{Name: "name", Type: sql.Text, Nullable: false},
		{Name: "push_url", Type: sql.Text, Nullable: false},
		{Name: "fetch_url", Type: sql.Text, Nullable: false},
		{Name: "push_refspec", Type: sql.Text, Nullable: false},
		{Name: "fetch_refspec", Type: sql.Text, Nullable: false},
	}
}

func (r *remotesTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(r)
}

func (r *remotesTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return r
}

func (r remotesTable) RowIter() (sql.RowIter, error) {
	iter := &remotesIter{}

	rowRepoIter, err := NewRowRepoIter(r.pool, iter)
	if err != nil {
		return nil, err
	}

	return rowRepoIter, nil
}

func (remotesTable) Children() []sql.Node {
	return []sql.Node{}
}

type remotesIter struct {
	repositoryID string
	remotes      []*git.Remote
	conf         *gitconfig.RemoteConfig
	remotePos    int
	urlPos       int
}

func (i *remotesIter) NewIterator(
	repo *Repository) (RowRepoIter, error) {

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

	row := sql.NewRow(
		i.repositoryID,
		config.Name,
		config.URLs[i.urlPos],
		config.URLs[i.urlPos],
		config.Fetch[i.urlPos],
		config.Fetch[i.urlPos],
	)

	i.urlPos++

	return row, nil
}

func (i *remotesIter) Close() error {
	return nil
}
