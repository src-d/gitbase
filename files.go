package gitbase

import (
	"bytes"
	"io"

	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"github.com/src-d/go-mysql-server/sql"
)

type filesTable struct {
	checksumable
	partitioned
	filters    []sql.Expression
	projection []string
	index      sql.IndexLookup
}

// FilesSchema is the schema for the files table.
var FilesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Source: "files"},
	{Name: "file_path", Type: sql.Text, Source: "files"},
	{Name: "blob_hash", Type: sql.Text, Source: "files"},
	{Name: "tree_hash", Type: sql.Text, Source: "files"},
	{Name: "tree_entry_mode", Type: sql.Text, Source: "files"},
	{Name: "blob_content", Type: sql.Blob, Source: "files"},
	{Name: "blob_size", Type: sql.Int64, Source: "files"},
}

func newFilesTable(pool *RepositoryPool) *filesTable {
	return &filesTable{checksumable: checksumable{pool}}
}

var _ Table = (*filesTable)(nil)
var _ Squashable = (*filesTable)(nil)

func (filesTable) isGitbaseTable()    {}
func (filesTable) isSquashable()      {}
func (filesTable) Name() string       { return FilesTableName }
func (filesTable) Schema() sql.Schema { return FilesSchema }

func (r *filesTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *r
	nt.filters = filters
	return &nt
}

func (r *filesTable) WithProjection(colNames []string) sql.Table {
	nt := *r
	nt.projection = colNames
	return &nt
}

func (r *filesTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *r
	nt.index = idx
	return &nt
}

func (r *filesTable) IndexLookup() sql.IndexLookup { return r.index }
func (r *filesTable) Filters() []sql.Expression    { return r.filters }
func (r *filesTable) Projection() []string         { return r.projection }

func (r *filesTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.FilesTable")
	iter, err := rowIterWithSelectors(
		ctx, FilesSchema, FilesTableName,
		r.filters,
		r.handledColumns(),
		func(selectors selectors) (sql.RowIter, error) {
			var repos []string
			repos, err = selectors.textValues("repository_id")
			if err != nil {
				return nil, err
			}

			if len(repos) > 0 && !stringContains(repos, repo.ID) {
				return noRows, nil
			}

			var treeHashes []string
			treeHashes, err = selectors.textValues("tree_hash")
			if err != nil {
				return nil, err
			}

			var blobHashes []string
			blobHashes, err = selectors.textValues("blob_hash")
			if err != nil {
				return nil, err
			}

			var filePaths []string
			filePaths, err = selectors.textValues("file_path")
			if err != nil {
				return nil, err
			}

			if r.index != nil {
				var values sql.IndexValueIter
				values, err = r.index.Values(p)
				if err != nil {
					return nil, err
				}

				var session *Session
				session, err = getSession(ctx)
				if err != nil {
					return nil, err
				}

				return newFilesIndexIter(
					values,
					session.Pool,
					shouldReadContent(r.projection),
					stringsToHashes(treeHashes),
					stringsToHashes(blobHashes),
					filePaths,
				), nil
			}

			return &filesRowIter{
				repo:          repo,
				treeHashes:    stringsToHashes(treeHashes),
				blobHashes:    stringsToHashes(blobHashes),
				filePaths:     filePaths,
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

func (filesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(FilesTableName, FilesSchema, filters)
}

func (filesTable) handledColumns() []string {
	return []string{"repository_id", "tree_hash", "blob_hash", "file_path"}
}

func (r filesTable) String() string {
	return printTable(
		FilesTableName,
		FilesSchema,
		r.projection,
		r.filters,
		r.index,
	)
}

// IndexKeyValues implements the sql.IndexableTable interface.
func (r *filesTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newPartitionedIndexKeyValueIter(
		ctx,
		newFilesTable(r.pool),
		colNames,
		newFilesKeyValueIter,
	)
}

type filesRowIter struct {
	repo     *Repository
	commits  object.CommitIter
	seen     map[plumbing.Hash]struct{}
	files    *object.FileIter
	treeHash plumbing.Hash

	readContent   bool
	skipGitErrors bool

	// selectors for faster filtering
	filePaths  []string
	blobHashes []plumbing.Hash
	treeHashes []plumbing.Hash
}

func (i *filesRowIter) init() error {
	var err error
	i.seen = make(map[plumbing.Hash]struct{})
	i.commits, err = i.repo.Log(&git.LogOptions{
		All: true,
	})
	return err
}

func (i *filesRowIter) shouldVisitTree(hash plumbing.Hash) bool {
	if _, ok := i.seen[hash]; ok {
		return false
	}

	if len(i.treeHashes) > 0 && !hashContains(i.treeHashes, hash) {
		return false
	}

	return true
}

func (i *filesRowIter) shouldVisitFile(file *object.File) bool {
	if len(i.filePaths) > 0 && !stringContains(i.filePaths, file.Name) {
		return false
	}

	if len(i.blobHashes) > 0 && !hashContains(i.blobHashes, file.Blob.Hash) {
		return false
	}

	return true
}

func (i *filesRowIter) Next() (sql.Row, error) {
	if i.commits == nil {
		if err := i.init(); err != nil {
			if i.skipGitErrors {
				return nil, io.EOF
			}
			return nil, err
		}
	}

	for {
		if i.files == nil {
			for {
				commit, err := i.commits.Next()
				if err != nil {
					if err != io.EOF && i.skipGitErrors {
						continue
					}

					return nil, err
				}

				if !i.shouldVisitTree(commit.TreeHash) {
					continue
				}

				i.treeHash = commit.TreeHash
				i.seen[commit.TreeHash] = struct{}{}

				if i.files, err = commit.Files(); err != nil {
					if i.skipGitErrors {
						continue
					}

					return nil, err
				}

				break
			}
		}

		f, err := i.files.Next()
		if err != nil {
			if err == io.EOF {
				i.files = nil
				continue
			}

			if i.skipGitErrors {
				continue
			}

			return nil, err
		}

		if !i.shouldVisitFile(f) {
			continue
		}

		return fileToRow(i.repo.ID, i.treeHash, f, i.readContent)
	}
}

func (i *filesRowIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

	i.repo.Close()

	return nil
}

func fileToRow(
	repoID string,
	treeHash plumbing.Hash,
	file *object.File,
	readContent bool,
) (sql.Row, error) {
	content, err := blobContent(&file.Blob, readContent)
	if err != nil {
		return nil, err
	}

	return sql.NewRow(
		repoID,
		file.Name,
		file.Hash.String(),
		treeHash.String(),
		file.Mode.String(),
		content,
		file.Size,
	), nil
}

type fileIndexKey struct {
	Repository string
	Packfile   string
	Hash       string
	Offset     int64
	Name       string
	Mode       int64
	Tree       string
}

func (k *fileIndexKey) encode() ([]byte, error) {
	var buf bytes.Buffer
	writeString(&buf, k.Repository)
	if err := writeHash(&buf, k.Packfile); err != nil {
		return nil, err
	}

	writeBool(&buf, k.Offset >= 0)
	if k.Offset >= 0 {
		writeInt64(&buf, k.Offset)
	} else {
		if err := writeHash(&buf, k.Hash); err != nil {
			return nil, err
		}
	}

	writeString(&buf, k.Name)
	writeInt64(&buf, k.Mode)

	if err := writeHash(&buf, k.Tree); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (k *fileIndexKey) decode(data []byte) error {
	var buf = bytes.NewBuffer(data)
	var err error
	if k.Repository, err = readString(buf); err != nil {
		return err
	}

	if k.Packfile, err = readHash(buf); err != nil {
		return err
	}

	ok, err := readBool(buf)
	if err != nil {
		return err
	}

	if ok {
		if k.Offset, err = readInt64(buf); err != nil {
			return err
		}
		k.Hash = ""
	} else {
		if k.Hash, err = readHash(buf); err != nil {
			return err
		}
		k.Offset = -1
	}

	if k.Name, err = readString(buf); err != nil {
		return err
	}

	if k.Mode, err = readInt64(buf); err != nil {
		return err
	}

	if k.Tree, err = readHash(buf); err != nil {
		return err
	}

	return nil
}

type filesKeyValueIter struct {
	repo    *Repository
	commits object.CommitIter
	files   *object.FileIter
	commit  *object.Commit
	idx     *repositoryIndex
	columns []string
	seen    map[plumbing.Hash]struct{}
}

func newFilesKeyValueIter(pool *RepositoryPool, repo *Repository, columns []string) (sql.IndexKeyValueIter, error) {
	r := pool.repositories[repo.ID]
	idx, err := newRepositoryIndex(r)
	if err != nil {
		return nil, err
	}

	commits, err := repo.Log(&git.LogOptions{
		All: true,
	})
	if err != nil {
		return nil, err
	}
	return &filesKeyValueIter{
		repo:    repo,
		columns: columns,
		idx:     idx,
		commits: commits,
		seen:    make(map[plumbing.Hash]struct{}),
	}, nil
}

func (i *filesKeyValueIter) Next() ([]interface{}, []byte, error) {
	for {
		if i.files == nil {
			var err error
			i.commit, err = i.commits.Next()
			if err != nil {
				return nil, nil, err
			}

			if _, ok := i.seen[i.commit.TreeHash]; ok {
				continue
			}
			i.seen[i.commit.TreeHash] = struct{}{}

			i.files, err = i.commit.Files()
			if err != nil {
				return nil, nil, err
			}
		}

		f, err := i.files.Next()
		if err != nil {
			if err == io.EOF {
				i.files = nil
				continue
			}
		}

		offset, packfile, err := i.idx.find(f.Blob.Hash)
		if err != nil {
			return nil, nil, err
		}

		// only fill hash if the object is an unpacked object
		var hash string
		if offset < 0 {
			hash = f.Blob.Hash.String()
		}

		key, err := encodeIndexKey(&fileIndexKey{
			Repository: i.repo.ID,
			Packfile:   packfile.String(),
			Hash:       hash,
			Offset:     offset,
			Name:       f.Name,
			Tree:       i.commit.TreeHash.String(),
			Mode:       int64(f.Mode),
		})
		if err != nil {
			return nil, nil, err
		}

		row, err := fileToRow(i.repo.ID, i.commit.TreeHash, f, stringContains(i.columns, "blob_content"))
		if err != nil {
			return nil, nil, err
		}

		values, err := rowIndexValues(row, i.columns, FilesSchema)
		if err != nil {
			return nil, nil, err
		}

		return values, key, nil
	}
}

func (i *filesKeyValueIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

	if i.files != nil {
		i.files.Close()
	}

	if i.idx != nil {
		i.idx.Close()
	}

	if i.repo != nil {
		i.repo.Close()
	}

	return nil
}

type filesIndexIter struct {
	index       sql.IndexValueIter
	decoder     *objectDecoder
	readContent bool
	treeHashes  []plumbing.Hash
	blobHashes  []plumbing.Hash
	filePaths   []string
}

func newFilesIndexIter(
	index sql.IndexValueIter,
	pool *RepositoryPool,
	readContent bool,
	treeHashes []plumbing.Hash,
	blobHashes []plumbing.Hash,
	filePaths []string,
) *filesIndexIter {
	return &filesIndexIter{
		index:       index,
		decoder:     newObjectDecoder(pool),
		readContent: readContent,
		treeHashes:  treeHashes,
		blobHashes:  blobHashes,
		filePaths:   filePaths,
	}
}

func (i *filesIndexIter) Next() (sql.Row, error) {
	for {
		var err error
		var data []byte
		defer closeIndexOnError(&err, i.index)

		data, err = i.index.Next()
		if err != nil {
			return nil, err
		}

		var key fileIndexKey
		if err := decodeIndexKey(data, &key); err != nil {
			return nil, err
		}

		if len(i.treeHashes) > 0 &&
			!hashContains(i.treeHashes, plumbing.NewHash(key.Tree)) {
			continue
		}

		if len(i.filePaths) > 0 &&
			!stringContains(i.filePaths, key.Name) {
			continue
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

		if len(i.blobHashes) > 0 && !hashContains(i.blobHashes, blob.Hash) {
			continue
		}

		file := &object.File{
			Blob: *blob,
			Name: key.Name,
			Mode: filemode.FileMode(key.Mode),
		}

		return fileToRow(key.Repository, plumbing.NewHash(key.Tree), file, i.readContent)
	}
}

func (i *filesIndexIter) Close() error {
	if i.decoder != nil {
		if err := i.decoder.Close(); err != nil {
			_ = i.index.Close()
			return err
		}
	}

	return i.index.Close()
}
