package gitbase

import (
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type filesTable struct{}

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

func newFilesTable() sql.Table {
	return new(filesTable)
}

var _ sql.PushdownProjectionAndFiltersTable = (*filesTable)(nil)

func (filesTable) Resolved() bool       { return true }
func (filesTable) Name() string         { return FilesTableName }
func (filesTable) Schema() sql.Schema   { return FilesSchema }
func (filesTable) Children() []sql.Node { return nil }

func (t *filesTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

func (t *filesTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}

func (filesTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.FilesTable")
	iter := &filesIter{readContent: true}

	repoIter, err := NewRowRepoIter(ctx, iter)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, repoIter), nil
}

func (filesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(FilesTableName, FilesSchema, filters)
}

func (filesTable) WithProjectAndFilters(
	ctx *sql.Context,
	columns, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.FilesTable")
	iter, err := rowIterWithSelectors(
		ctx, FilesSchema, FilesTableName, filters,
		[]string{"repository_id", "blob_hash", "file_path", "tree_hash"},
		func(selectors selectors) (RowRepoIter, error) {
			repos, err := selectors.textValues("repository_id")
			if err != nil {
				return nil, err
			}

			treeHashes, err := selectors.textValues("tree_hash")
			if err != nil {
				return nil, err
			}

			blobHashes, err := selectors.textValues("blob_hash")
			if err != nil {
				return nil, err
			}

			filePaths, err := selectors.textValues("file_path")
			if err != nil {
				return nil, err
			}

			return &filesIter{
				repos:       repos,
				treeHashes:  stringsToHashes(treeHashes),
				blobHashes:  stringsToHashes(blobHashes),
				filePaths:   filePaths,
				readContent: shouldReadContent(columns),
			}, nil
		},
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (filesTable) String() string {
	return printTable(FilesTableName, FilesSchema)
}

type filesIter struct {
	repo     *Repository
	commits  object.CommitIter
	seen     map[plumbing.Hash]struct{}
	files    *object.FileIter
	treeHash plumbing.Hash

	readContent bool

	// selectors for faster filtering
	repos      []string
	filePaths  []string
	blobHashes []plumbing.Hash
	treeHashes []plumbing.Hash
}

func (i *filesIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	var iter object.CommitIter
	if len(i.repos) == 0 || stringContains(i.repos, repo.ID) {
		var err error
		iter, err = repo.Repo.CommitObjects()
		if err != nil {
			return nil, err
		}
	}

	return &filesIter{
		repo:        repo,
		commits:     iter,
		seen:        make(map[plumbing.Hash]struct{}),
		readContent: i.readContent,
		filePaths:   i.filePaths,
		blobHashes:  i.blobHashes,
		treeHashes:  i.treeHashes,
	}, nil
}

func (i *filesIter) shouldVisitTree(hash plumbing.Hash) bool {
	if _, ok := i.seen[hash]; ok {
		return false
	}

	if len(i.treeHashes) > 0 && !hashContains(i.treeHashes, hash) {
		return false
	}

	return true
}

func (i *filesIter) shouldVisitFile(file *object.File) bool {
	if len(i.filePaths) > 0 && !stringContains(i.filePaths, file.Name) {
		return false
	}

	if len(i.blobHashes) > 0 && !hashContains(i.blobHashes, file.Blob.Hash) {
		return false
	}

	return true
}

func (i *filesIter) Next() (sql.Row, error) {
	if i.commits == nil {
		return nil, io.EOF
	}

	for {
		if i.files == nil {
			for {
				commit, err := i.commits.Next()
				if err != nil {
					return nil, err
				}

				if !i.shouldVisitTree(commit.TreeHash) {
					continue
				}

				i.treeHash = commit.TreeHash
				i.seen[commit.TreeHash] = struct{}{}

				if i.files, err = commit.Files(); err != nil {
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
		}

		if !i.shouldVisitFile(f) {
			continue
		}

		return fileToRow(i.repo.ID, i.treeHash, f, i.readContent)
	}
}

func (i *filesIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

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
