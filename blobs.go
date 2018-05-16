package gitbase

import (
	"bufio"
	"io"
	"io/ioutil"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"

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

type blobsTable struct{}

// BlobsSchema is the schema for the blobs table.
var BlobsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: BlobsTableName},
	{Name: "blob_hash", Type: sql.Text, Nullable: false, Source: BlobsTableName},
	{Name: "blob_size", Type: sql.Int64, Nullable: false, Source: BlobsTableName},
	{Name: "blob_content", Type: sql.Blob, Nullable: false, Source: BlobsTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*blobsTable)(nil)

func newBlobsTable() Indexable {
	return &indexableTable{
		PushdownTable:          new(blobsTable),
		buildIterWithSelectors: blobsIterBuilder,
	}
}

var _ Table = (*blobsTable)(nil)

func (blobsTable) isGitbaseTable() {}

func (blobsTable) String() string {
	return printTable(BlobsTableName, BlobsSchema)
}

func (blobsTable) Resolved() bool {
	return true
}

func (blobsTable) Name() string {
	return BlobsTableName
}

func (blobsTable) Schema() sql.Schema {
	return BlobsSchema
}

func (r *blobsTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(r)
}

func (r *blobsTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return r, nil
}

func (r blobsTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.BlobsTable")
	iter := &blobIter{readContent: true}

	repoIter, err := NewRowRepoIter(ctx, iter)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, repoIter), nil
}

func (blobsTable) Children() []sql.Node {
	return nil
}

func (blobsTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(BlobsTableName, BlobsSchema, filters)
}

func (*blobsTable) handledColumns() []string {
	return []string{"blob_hash"}
}

func (r *blobsTable) WithProjectAndFilters(
	ctx *sql.Context,
	columns, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.BlobsTable")
	iter, err := rowIterWithSelectors(
		ctx, BlobsSchema, BlobsTableName,
		filters, columns,
		r.handledColumns(),
		blobsIterBuilder,
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func blobsIterBuilder(_ *sql.Context, selectors selectors, columns []sql.Expression) (RowRepoIter, error) {
	if len(selectors["blob_hash"]) == 0 {
		return &blobIter{readContent: shouldReadContent(columns)}, nil
	}

	hashes, err := selectors.textValues("blob_hash")
	if err != nil {
		return nil, err
	}

	return &blobsByHashIter{
		hashes:      hashes,
		readContent: shouldReadContent(columns),
	}, nil
}

type blobIter struct {
	repoID      string
	iter        *object.BlobIter
	readContent bool
	lastHash    string
}

func (i *blobIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	iter, err := repo.Repo.BlobObjects()
	if err != nil {
		return nil, err
	}

	return &blobIter{repoID: repo.ID, iter: iter, readContent: i.readContent}, nil
}

func (i *blobIter) Repository() string { return i.repoID }

func (i *blobIter) LastObject() string { return i.lastHash }

func (i *blobIter) Next() (sql.Row, error) {
	o, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	i.lastHash = o.Hash.String()
	return blobToRow(i.repoID, o, i.readContent)
}

func (i *blobIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	return nil
}

type blobsByHashIter struct {
	repo        *Repository
	pos         int
	hashes      []string
	readContent bool
	lastHash    string
}

func (i *blobsByHashIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	return &blobsByHashIter{repo, 0, i.hashes, i.readContent, ""}, nil
}

func (i *blobsByHashIter) Repository() string { return i.repo.ID }

func (i *blobsByHashIter) LastObject() string { return i.lastHash }

func (i *blobsByHashIter) Next() (sql.Row, error) {
	for {
		if i.pos >= len(i.hashes) {
			return nil, io.EOF
		}

		hash := plumbing.NewHash(i.hashes[i.pos])
		i.pos++
		blob, err := i.repo.Repo.BlobObject(hash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		if err != nil {
			return nil, err
		}

		i.lastHash = hash.String()
		return blobToRow(i.repo.ID, blob, i.readContent)
	}
}

func (i *blobsByHashIter) Close() error {
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

func shouldReadContent(columns []sql.Expression) bool {
	for _, e := range columns {
		var found bool
		expression.Inspect(e, func(e sql.Expression) bool {
			gf, ok := e.(*expression.GetField)
			found = ok && gf.Name() == "blob_content"
			return !found
		})

		if found {
			return true
		}
	}
	return false
}
