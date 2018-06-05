package gitbase

import (
	"io"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type referencesTable struct{}

// RefsSchema is the schema for the refs table.
var RefsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: ReferencesTableName},
	{Name: "ref_name", Type: sql.Text, Nullable: false, Source: ReferencesTableName},
	{Name: "commit_hash", Type: sql.Text, Nullable: false, Source: ReferencesTableName},
}

var _ sql.PushdownProjectionAndFiltersTable = (*referencesTable)(nil)

func newReferencesTable() Indexable {
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

func (referencesTable) handledColumns() []string { return []string{"commit_hash", "ref_name"} }

func (r *referencesTable) WithProjectAndFilters(
	ctx *sql.Context,
	_, filters []sql.Expression,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("gitbase.ReferencesTable")
	iter, err := rowIterWithSelectors(
		ctx, RefsSchema, ReferencesTableName,
		filters, nil,
		r.handledColumns(),
		referencesIterBuilder,
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

// IndexKeyValueIter implements the sql.Indexable interface.
func (*referencesTable) IndexKeyValueIter(
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

	return &referenceKeyValueIter{repos: iter, columns: colNames}, nil
}

// WithProjectFiltersAndIndex implements sql.Indexable interface.
func (*referencesTable) WithProjectFiltersAndIndex(
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

	var iter sql.RowIter = &referencesIndexIter{index}

	if len(filters) > 0 {
		iter = plan.NewFilterIter(ctx, expression.JoinAnd(filters...), iter)
	}

	return sql.NewSpanIter(span, iter), nil
}

func referencesIterBuilder(_ *sql.Context, selectors selectors, _ []sql.Expression) (RowRepoIter, error) {
	if len(selectors["commit_hash"]) == 0 && len(selectors["ref_name"]) == 0 {
		return new(referenceIter), nil
	}

	hashes, err := selectors.textValues("commit_hash")
	if err != nil {
		return nil, err
	}

	names, err := selectors.textValues("ref_name")
	if err != nil {
		return nil, err
	}

	for i := range names {
		names[i] = strings.ToLower(names[i])
	}

	return &filteredReferencesIter{hashes: stringsToHashes(hashes), names: names}, nil
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

type referenceKeyValueIter struct {
	repos   *RepositoryIter
	repo    *Repository
	head    *plumbing.Reference
	refs    storer.ReferenceIter
	columns []string
}

type refIndexKey struct {
	repository string
	name       string
	commit     string
}

func (i *referenceKeyValueIter) Next() ([]interface{}, []byte, error) {
	for {
		if i.refs == nil {
			var err error
			i.repo, err = i.repos.Next()
			if err != nil {
				return nil, nil, err
			}

			i.head, err = i.repo.Repo.Head()
			if err != nil && err != plumbing.ErrReferenceNotFound {
				return nil, nil, err
			}

			i.refs, err = i.repo.Repo.References()
			if err != nil {
				return nil, nil, err
			}
		}

		var ref *plumbing.Reference
		if i.head != nil {
			ref = plumbing.NewHashReference(
				plumbing.ReferenceName("HEAD"),
				i.head.Hash(),
			)
			i.head = nil
		} else {
			var err error
			ref, err = i.refs.Next()
			if err != nil {
				if err == io.EOF {
					i.refs = nil
					continue
				}
				return nil, nil, err
			}
		}

		key, err := encodeIndexKey(refIndexKey{i.repo.ID, ref.Name().String(), ref.Hash().String()})
		if err != nil {
			return nil, nil, err
		}

		values, err := rowIndexValues(referenceToRow(i.repo.ID, ref), i.columns, RefsSchema)
		if err != nil {
			return nil, nil, err
		}

		return values, key, nil
	}
}

func (i *referenceKeyValueIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}
	return i.repos.Close()
}

type referencesIndexIter struct {
	index sql.IndexValueIter
}

func (i *referencesIndexIter) Next() (sql.Row, error) {
	data, err := i.index.Next()
	if err != nil {
		return nil, err
	}

	var key refIndexKey
	if err := decodeIndexKey(data, &key); err != nil {
		return nil, err
	}

	return referenceToRow(
		key.repository,
		plumbing.NewHashReference(
			plumbing.ReferenceName(key.name),
			plumbing.NewHash(key.commit),
		),
	), nil
}

func (i *referencesIndexIter) Close() error { return i.index.Close() }
