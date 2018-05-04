package gitbase

import (
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type referencesTable struct{}

// RefsSchema is the schema for the refs table.
var RefsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: ReferencesTableName},
	{Name: "name", Type: sql.Text, Nullable: false, Source: ReferencesTableName},
	{Name: "hash", Type: sql.Text, Nullable: false, Source: ReferencesTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*referencesTable)(nil)

func newReferencesTable() sql.Table {
	return new(referencesTable)
}

var _ Table = (*referencesTable)(nil)

func (referencesTable) isGitbaseTable() {}

func (r referencesTable) String() string {
	return printTable(ReferencesTableName, RefsSchema)
}

func (referencesTable) Resolved() bool {
	return true
}

func (referencesTable) Name() string {
	return ReferencesTableName
}

func (referencesTable) Schema() sql.Schema {
	return RefsSchema
}

func (r *referencesTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(r)
}

func (r *referencesTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return r, nil
}

func (r referencesTable) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.ReferencesTable")
	iter := new(referenceIter)

	repoIter, err := NewRowRepoIter(ctx, iter)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, repoIter), nil
}

func (referencesTable) Children() []sql.Node {
	return nil
}

func (referencesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(ReferencesTableName, RefsSchema, filters)
}

func (r *referencesTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.ReferencesTable")
	iter, err := rowIterWithSelectors(
		ctx, RefsSchema, ReferencesTableName, filters,
		[]string{"hash", "name"},
		func(selectors selectors) (RowRepoIter, error) {
			if len(selectors["hash"]) == 0 && len(selectors["name"]) == 0 {
				return new(referenceIter), nil
			}

			hashes, err := selectors.textValues("hash")
			if err != nil {
				return nil, err
			}

			names, err := selectors.textValues("name")
			if err != nil {
				return nil, err
			}

			for i := range names {
				names[i] = strings.ToLower(names[i])
			}

			return &filteredReferencesIter{hashes: stringsToHashes(hashes), names: names}, nil
		},
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

type referenceIter struct {
	head         *plumbing.Reference
	repositoryID string
	iter         storer.ReferenceIter
}

func (i *referenceIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	iter, err := repo.Repo.References()
	if err != nil {
		return nil, err
	}

	head, err := repo.Repo.Head()
	if err != nil {
		if err != plumbing.ErrReferenceNotFound {
			return nil, err
		}

		logrus.WithField("repo", repo.ID).Debug("unable to get HEAD of repository")
	}

	return &referenceIter{
		head:         head,
		repositoryID: repo.ID,
		iter:         iter,
	}, nil
}

func (i *referenceIter) Next() (sql.Row, error) {
	for {
		if i.head != nil {
			o := i.head
			i.head = nil
			return sql.NewRow(
				i.repositoryID,
				"HEAD",
				o.Hash().String(),
			), nil
		}

		o, err := i.iter.Next()
		if err != nil {
			return nil, err
		}

		if o.Type() != plumbing.HashReference {
			logrus.WithFields(logrus.Fields{
				"type": o.Type(),
				"ref":  o.Name(),
			}).Debug("ignoring reference, it's not a hash reference")
			continue
		}

		return referenceToRow(i.repositoryID, o), nil
	}
}

func (i *referenceIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	return nil
}

type filteredReferencesIter struct {
	head   *plumbing.Reference
	hashes []plumbing.Hash
	names  []string
	repoID string
	iter   storer.ReferenceIter
}

func (i *filteredReferencesIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	iter, err := repo.Repo.References()
	if err != nil {
		return nil, err
	}

	head, err := repo.Repo.Head()
	if err != nil {
		if err != plumbing.ErrReferenceNotFound {
			return nil, err
		}

		logrus.WithField("repo", repo.ID).Debug("unable to get HEAD of repository")
	}

	return &filteredReferencesIter{
		head:   head,
		hashes: i.hashes,
		names:  i.names,
		repoID: repo.ID,
		iter:   iter,
	}, nil
}

func (i *filteredReferencesIter) Next() (sql.Row, error) {
	for {
		if i.head != nil {
			o := i.head
			i.head = nil

			if len(i.hashes) > 0 && !hashContains(i.hashes, o.Hash()) {
				continue
			}

			if len(i.names) > 0 && !stringContains(i.names, "head") {
				continue
			}

			return sql.NewRow(
				i.repoID,
				"HEAD",
				o.Hash().String(),
			), nil
		}

		o, err := i.iter.Next()
		if err != nil {
			return nil, err
		}

		if o.Type() != plumbing.HashReference {
			logrus.WithFields(logrus.Fields{
				"type": o.Type(),
				"ref":  o.Name(),
			}).Debug("ignoring reference, it's not a hash reference")
			continue
		}

		if len(i.hashes) > 0 && !hashContains(i.hashes, o.Hash()) {
			continue
		}

		if len(i.names) > 0 && !stringContains(i.names, strings.ToLower(o.Name().String())) {
			continue
		}

		return referenceToRow(i.repoID, o), nil
	}
}

func (i *filteredReferencesIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}
	return nil
}

func referenceToRow(repositoryID string, c *plumbing.Reference) sql.Row {
	hash := c.Hash().String()

	return sql.NewRow(
		repositoryID,
		c.Name().String(),
		hash,
	)
}

func stringsToHashes(strs []string) []plumbing.Hash {
	var hashes = make([]plumbing.Hash, len(strs))
	for i, s := range strs {
		hashes[i] = plumbing.NewHash(s)
	}
	return hashes
}

func hashContains(hashes []plumbing.Hash, hash plumbing.Hash) bool {
	for _, h := range hashes {
		if h == hash {
			return true
		}
	}
	return false
}
