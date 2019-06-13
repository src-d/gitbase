package gitbase

import (
	"bufio"
	"io"
	"io/ioutil"

	"github.com/src-d/go-mysql-server/sql"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

const (
	blobsMaxSizeKey     = "GITBASE_BLOBS_MAX_SIZE"
	blobsAllowBinaryKey = "GITBASE_BLOBS_ALLOW_BINARY"

	b   = 1
	kib = 1024 * b
	mib = 1024 * kib
)

var (
	blobsAllowBinary = getBoolEnv(blobsAllowBinaryKey, false)
	blobsMaxSize     = getIntEnv(blobsMaxSizeKey, 5) * mib
)

type blobsTable struct {
	checksumable
	partitioned
	filters    []sql.Expression
	projection []string
	index      sql.IndexLookup
}

// BlobsSchema is the schema for the blobs table.
var BlobsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: BlobsTableName},
	{Name: "blob_hash", Type: sql.Text, Nullable: false, Source: BlobsTableName},
	{Name: "blob_size", Type: sql.Int64, Nullable: false, Source: BlobsTableName},
	{Name: "blob_content", Type: sql.Blob, Nullable: false, Source: BlobsTableName},
}

func newBlobsTable(pool *RepositoryPool) *blobsTable {
	return &blobsTable{checksumable: checksumable{pool}}
}

var _ Table = (*blobsTable)(nil)
var _ Squashable = (*blobsTable)(nil)

func (blobsTable) isSquashable()   {}
func (blobsTable) isGitbaseTable() {}

func (r blobsTable) String() string {
	return printTable(
		BlobsTableName,
		BlobsSchema,
		r.projection,
		r.filters,
		r.index,
	)
}

func (blobsTable) Name() string {
	return BlobsTableName
}

func (blobsTable) Schema() sql.Schema {
	return BlobsSchema
}

func (r *blobsTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *r
	nt.filters = filters
	return &nt
}

func (r *blobsTable) WithProjection(colNames []string) sql.Table {
	nt := *r
	nt.projection = colNames
	return &nt
}

func (r *blobsTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *r
	nt.index = idx
	return &nt
}

func (r *blobsTable) IndexLookup() sql.IndexLookup { return r.index }
func (r *blobsTable) Filters() []sql.Expression    { return r.filters }
func (r *blobsTable) Projection() []string         { return r.projection }

func (r *blobsTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.BlobsTable")
	iter, err := rowIterWithSelectors(
		ctx, BlobsSchema, BlobsTableName,
		r.filters,
		r.handledColumns(),
		func(selectors selectors) (sql.RowIter, error) {
			var hashes []string
			hashes, err = selectors.textValues("blob_hash")
			if err != nil {
				return nil, err
			}

			if r.index != nil {
				var indexValues sql.IndexValueIter
				indexValues, err = r.index.Values(p)
				if err != nil {
					return nil, err
				}

				s, err := getSession(ctx)
				if err != nil {
					return nil, err
				}

				return newBlobsIndexIter(
					indexValues,
					s.Pool,
					shouldReadContent(r.projection),
					stringsToHashes(hashes),
				), nil
			}

			return &blobRowIter{
				hashes:        stringsToHashes(hashes),
				repo:          repo,
				readContent:   shouldReadContent(r.projection),
				skipGitErrors: shouldSkipErrors(ctx),
			}, nil
		},
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (blobsTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(BlobsTableName, BlobsSchema, filters)
}

func (*blobsTable) handledColumns() []string {
	return []string{"blob_hash"}
}

// IndexKeyValues implements the sql.IndexableTable interface.
func (r *blobsTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newPartitionedIndexKeyValueIter(
		ctx,
		newBlobsTable(r.pool),
		colNames,
		newBlobsKeyValueIter,
	)
}

type blobRowIter struct {
	repo          *Repository
	iter          *object.BlobIter
	hashes        []plumbing.Hash
	pos           int
	readContent   bool
	skipGitErrors bool
}

func (i *blobRowIter) init() error {
	var err error
	i.iter, err = i.repo.BlobObjects()
	return err
}

func (i *blobRowIter) Next() (sql.Row, error) {
	if len(i.hashes) > 0 {
		return i.nextByHash()
	}

	return i.next()
}

func (i *blobRowIter) nextByHash() (sql.Row, error) {
	for {
		if i.pos >= len(i.hashes) {
			return nil, io.EOF
		}

		blob, err := i.repo.BlobObject(i.hashes[i.pos])
		i.pos++
		if err != nil {
			if err == plumbing.ErrObjectNotFound || i.skipGitErrors {
				continue
			}

			return nil, err
		}

		return blobToRow(i.repo.ID(), blob, i.readContent)
	}
}

func (i *blobRowIter) next() (sql.Row, error) {
	for {
		if i.iter == nil {
			if err := i.init(); err != nil {
				if i.skipGitErrors {
					return nil, io.EOF
				}
				return nil, err
			}
		}

		o, err := i.iter.Next()
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}

			if i.skipGitErrors {
				continue
			}
			return nil, err
		}

		return blobToRow(i.repo.ID(), o, i.readContent)
	}
}

func (i *blobRowIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	i.repo.Close()

	return nil
}

func blobContent(c *object.Blob, readContent bool) ([]byte, error) {
	var content []byte
	var isAllowed = blobsAllowBinary
	if !isAllowed && readContent {
		ok, err := isBinary(c)
		if err != nil {
			return nil, err
		}
		isAllowed = !ok
	}

	if c.Size <= int64(blobsMaxSize) && isAllowed && readContent {
		r, err := c.Reader()
		if err != nil {
			return nil, err
		}

		content, err = ioutil.ReadAll(r)
		if err != nil {
			return nil, err
		}
	}

	return content, nil
}

func blobToRow(repoID string, c *object.Blob, readContent bool) (sql.Row, error) {
	content, err := blobContent(c, readContent)
	if err != nil {
		return nil, err
	}

	return sql.NewRow(
		repoID,
		c.Hash.String(),
		c.Size,
		content,
	), nil
}

const sniffLen = 8000

// isBinary detects if data is a binary value based on:
// http://git.kernel.org/cgit/git/git.git/tree/xdiff-interface.c?id=HEAD#n198
func isBinary(blob *object.Blob) (bool, error) {
	r, err := blob.Reader()
	if err != nil {
		return false, err
	}

	defer r.Close()

	rd := bufio.NewReader(r)
	var i int
	for {
		if i >= sniffLen {
			return false, nil
		}
		i++

		b, err := rd.ReadByte()
		if err == io.EOF {
			return false, nil
		}

		if err != nil {
			return false, err
		}

		if b == 0 {
			return true, nil
		}
	}
}

func shouldReadContent(columns []string) bool {
	return stringContains(columns, "blob_content")
}

type blobsKeyValueIter struct {
	pool    *RepositoryPool
	repo    *Repository
	blobs   *object.BlobIter
	idx     *repositoryIndex
	columns []string
}

func newBlobsKeyValueIter(
	pool *RepositoryPool,
	repo *Repository,
	columns []string,
) (sql.IndexKeyValueIter, error) {
	blobs, err := repo.BlobObjects()
	if err != nil {
		return nil, err
	}

	r, err := pool.GetRepo(repo.ID())
	if err != nil {
		return nil, err
	}

	idx, err := newRepositoryIndex(r)
	if err != nil {
		return nil, err
	}

	return &blobsKeyValueIter{
		pool:    pool,
		repo:    repo,
		columns: columns,
		idx:     idx,
		blobs:   blobs,
	}, nil
}

func (i *blobsKeyValueIter) Next() ([]interface{}, []byte, error) {
	blob, err := i.blobs.Next()
	if err != nil {
		return nil, nil, err
	}

	offset, packfile, err := i.idx.find(blob.Hash)
	if err != nil {
		return nil, nil, err
	}

	var hash string
	if offset < 0 {
		hash = blob.Hash.String()
	}

	key, err := encodeIndexKey(&packOffsetIndexKey{
		Repository: i.repo.ID(),
		Packfile:   packfile.String(),
		Offset:     offset,
		Hash:       hash,
	})
	if err != nil {
		return nil, nil, err
	}

	row, err := blobToRow(i.repo.ID(), blob, stringContains(i.columns, "blob_content"))
	if err != nil {
		return nil, nil, err
	}

	values, err := rowIndexValues(row, i.columns, BlobsSchema)
	if err != nil {
		return nil, nil, err
	}

	return values, key, nil
}

func (i *blobsKeyValueIter) Close() error {
	if i.blobs != nil {
		i.blobs.Close()
	}

	if i.idx != nil {
		i.idx.Close()
	}

	if i.repo != nil {
		i.repo.Close()
	}

	return nil
}

type blobsIndexIter struct {
	index       sql.IndexValueIter
	decoder     *objectDecoder
	readContent bool
	hashes      []plumbing.Hash
}

func newBlobsIndexIter(
	index sql.IndexValueIter,
	pool *RepositoryPool,
	readContent bool,
	hashes []plumbing.Hash,
) *blobsIndexIter {
	return &blobsIndexIter{
		index:       index,
		decoder:     newObjectDecoder(pool),
		readContent: readContent,
		hashes:      hashes,
	}
}

func (i *blobsIndexIter) Next() (sql.Row, error) {
	for {
		var err error
		var data []byte
		defer closeIndexOnError(&err, i.index)

		data, err = i.index.Next()
		if err != nil {
			return nil, err
		}

		var key packOffsetIndexKey
		if err := decodeIndexKey(data, &key); err != nil {
			return nil, err
		}

		obj, err := i.decoder.decode(
			key.Repository,
			plumbing.NewHash(key.Packfile),
			key.Offset,
			plumbing.NewHash(key.Hash),
		)
		if err != nil {
			return nil, err
		}

		blob, ok := obj.(*object.Blob)
		if !ok {
			return nil, ErrInvalidObjectType.New(obj, "*object.Blob")
		}

		if len(i.hashes) > 0 && !hashContains(i.hashes, blob.Hash) {
			continue
		}

		return blobToRow(key.Repository, blob, i.readContent)
	}
}

func (i *blobsIndexIter) Close() error {
	if i.decoder != nil {
		if err := i.decoder.Close(); err != nil {
			_ = i.index.Close()
			return err
		}
	}

	return i.index.Close()
}
