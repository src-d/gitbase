package gitbase

import (
	"bytes"
	"io"

	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"github.com/src-d/go-mysql-server/sql"
)

type remotesTable struct {
	checksumable
	partitioned
	filters []sql.Expression
	index   sql.IndexLookup
}

// RemotesSchema is the schema for the remotes table.
var RemotesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: RemotesTableName},
	{Name: "remote_name", Type: sql.Text, Nullable: false, Source: RemotesTableName},
	{Name: "remote_push_url", Type: sql.Text, Nullable: false, Source: RemotesTableName},
	{Name: "remote_fetch_url", Type: sql.Text, Nullable: false, Source: RemotesTableName},
	{Name: "remote_push_refspec", Type: sql.Text, Nullable: false, Source: RemotesTableName},
	{Name: "remote_fetch_refspec", Type: sql.Text, Nullable: false, Source: RemotesTableName},
}

func newRemotesTable(pool *RepositoryPool) *remotesTable {
	return &remotesTable{checksumable: checksumable{pool}}
}

var _ Table = (*remotesTable)(nil)
var _ Squashable = (*remotesTable)(nil)

func (remotesTable) isSquashable()   {}
func (remotesTable) isGitbaseTable() {}

func (remotesTable) Name() string {
	return RemotesTableName
}

func (remotesTable) Schema() sql.Schema {
	return RemotesSchema
}

func (r remotesTable) String() string {
	return printTable(
		RemotesTableName,
		RemotesSchema,
		nil,
		r.filters,
		r.index,
	)
}

func (r *remotesTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *r
	nt.filters = filters
	return &nt
}

func (r *remotesTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *r
	nt.index = idx
	return &nt
}

func (r *remotesTable) IndexLookup() sql.IndexLookup { return r.index }
func (r *remotesTable) Filters() []sql.Expression    { return r.filters }

func (r *remotesTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.RemotesTable")
	iter, err := rowIterWithSelectors(
		ctx, RemotesSchema, RemotesTableName,
		r.filters,
		r.handledColumns(),
		func(selectors) (sql.RowIter, error) {
			remotes, err := repo.Remotes()
			if err != nil {
				return nil, err
			}

			if r.index != nil {
				values, err := r.index.Values(p)
				if err != nil {
					return nil, err
				}

				return &remotesIndexIter{
					index:   values,
					repo:    repo,
					remotes: remotes,
				}, nil
			}

			return &remotesRowIter{
				repo:    repo,
				remotes: remotes,
			}, nil
		},
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (remotesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(RemotesTableName, RemotesSchema, filters)
}

func (remotesTable) handledColumns() []string { return nil }

// IndexKeyValues implements the sql.IndexableTable interface.
func (r *remotesTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newPartitionedIndexKeyValueIter(
		ctx,
		newRemotesTable(r.pool),
		colNames,
		newRemotesKeyValueIter,
	)
}

type remotesRowIter struct {
	repo      *Repository
	remotes   []*git.Remote
	remotePos int
	urlPos    int
}

func (i *remotesRowIter) Next() (sql.Row, error) {
	for {
		if i.remotePos >= len(i.remotes) {
			return nil, io.EOF
		}

		remote := i.remotes[i.remotePos]
		config := remote.Config()

		if i.urlPos >= len(config.URLs) && i.urlPos >= len(config.Fetch) {
			i.remotePos++
			i.urlPos = 0
			continue
		}

		row := remoteToRow(i.repo.ID, config, i.urlPos)
		i.urlPos++

		return row, nil
	}
}

func (i *remotesRowIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}

	return nil
}

func remoteToRow(repoID string, config *config.RemoteConfig, pos int) sql.Row {
	var url interface{}
	if pos < len(config.URLs) {
		url = config.URLs[pos]
	}

	fetch := remoteFetchURL(config, pos)
	return sql.NewRow(
		repoID,
		config.Name,
		url,
		url,
		fetch,
		fetch,
	)
}

type remoteIndexKey struct {
	Repository string
	Pos        int
	URLPos     int
}

func (k *remoteIndexKey) encode() ([]byte, error) {
	var buf bytes.Buffer
	writeString(&buf, k.Repository)
	writeInt64(&buf, int64(k.Pos))
	writeInt64(&buf, int64(k.URLPos))
	return buf.Bytes(), nil
}

func (k *remoteIndexKey) decode(data []byte) error {
	var buf = bytes.NewBuffer(data)
	var err error
	if k.Repository, err = readString(buf); err != nil {
		return err
	}

	pos, err := readInt64(buf)
	if err != nil {
		return err
	}

	urlPos, err := readInt64(buf)
	if err != nil {
		return err
	}

	k.Pos = int(pos)
	k.URLPos = int(urlPos)
	return nil
}

type remotesKeyValueIter struct {
	repo    *Repository
	columns []string
	remotes []*git.Remote

	pos    int
	urlPos int
}

func newRemotesKeyValueIter(
	_ *RepositoryPool,
	repo *Repository,
	columns []string,
) (sql.IndexKeyValueIter, error) {
	remotes, err := repo.Remotes()
	if err != nil {
		return nil, err
	}

	return &remotesKeyValueIter{
		repo:    repo,
		columns: columns,
		remotes: remotes,
	}, nil
}

func (i *remotesKeyValueIter) Next() ([]interface{}, []byte, error) {
	for {
		if i.pos >= len(i.remotes) {
			return nil, nil, io.EOF
		}

		cfg := i.remotes[i.pos].Config()
		if i.urlPos >= len(cfg.URLs) && i.urlPos >= len(cfg.Fetch) {
			i.urlPos = 0
			i.pos++
			continue
		}

		i.urlPos++

		key, err := encodeIndexKey(&remoteIndexKey{i.repo.ID, i.pos, i.urlPos - 1})
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
	if i.repo != nil {
		i.repo.Close()
	}

	return nil
}

type remotesIndexIter struct {
	index   sql.IndexValueIter
	repo    *Repository
	remotes []*git.Remote
}

func (i *remotesIndexIter) Next() (sql.Row, error) {
	var err error
	var data []byte
	defer closeIndexOnError(&err, i.index)

	data, err = i.index.Next()
	if err != nil {
		return nil, err
	}

	var key remoteIndexKey
	if err := decodeIndexKey(data, &key); err != nil {
		return nil, err
	}

	config := i.remotes[key.Pos].Config()
	return remoteToRow(key.Repository, config, key.URLPos), nil
}

func (i *remotesIndexIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}

	return i.index.Close()
}

func remoteFetchURL(config *config.RemoteConfig, pos int) string {
	if len(config.Fetch) > 0 {
		var fpos = pos
		if fpos >= len(config.Fetch) {
			fpos = len(config.Fetch) - 1
		}
		return config.Fetch[fpos].String()
	}

	return config.URLs[pos]
}
