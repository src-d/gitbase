package gitbase

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/src-d/go-mysql-server/sql"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type referencesTable struct {
	checksumable
	partitioned
	filters []sql.Expression
	index   sql.IndexLookup
}

// RefsSchema is the schema for the refs table.
var RefsSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: ReferencesTableName},
	{Name: "ref_name", Type: sql.Text, Nullable: false, Source: ReferencesTableName},
	{Name: "commit_hash", Type: sql.Text, Nullable: false, Source: ReferencesTableName},
}

func newReferencesTable(pool *RepositoryPool) *referencesTable {
	return &referencesTable{checksumable: checksumable{pool}}
}

var _ Table = (*referencesTable)(nil)
var _ Squashable = (*referencesTable)(nil)

func (referencesTable) isSquashable()   {}
func (referencesTable) isGitbaseTable() {}

func (r referencesTable) String() string {
	return printTable(
		ReferencesTableName,
		RefsSchema,
		nil,
		r.filters,
		r.index,
	)
}

func (referencesTable) Name() string {
	return ReferencesTableName
}

func (referencesTable) Schema() sql.Schema {
	return RefsSchema
}

func (r *referencesTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *r
	nt.filters = filters
	return &nt
}

func (r *referencesTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *r
	nt.index = idx
	return &nt
}

func (r *referencesTable) IndexLookup() sql.IndexLookup { return r.index }
func (r *referencesTable) Filters() []sql.Expression    { return r.filters }

func (r *referencesTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.ReferencesTable")
	iter, err := rowIterWithSelectors(
		ctx, RefsSchema, ReferencesTableName,
		r.filters,
		r.handledColumns(),
		func(selectors selectors) (sql.RowIter, error) {
			var hashes []string
			hashes, err = selectors.textValues("commit_hash")
			if err != nil {
				return nil, err
			}

			var names []string
			names, err = selectors.textValues("ref_name")
			if err != nil {
				return nil, err
			}

			for i := range names {
				names[i] = strings.ToLower(names[i])
			}

			var indexValues sql.IndexValueIter
			if r.index != nil {
				if indexValues, err = r.index.Values(p); err != nil {
					return nil, err
				}
			}

			return &refRowIter{
				hashes:        stringsToHashes(hashes),
				repo:          repo,
				names:         names,
				index:         indexValues,
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

func (referencesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(ReferencesTableName, RefsSchema, filters)
}

func (referencesTable) handledColumns() []string { return []string{"commit_hash", "ref_name"} }

// IndexKeyValues implements the sql.IndexableTable interface.
func (r *referencesTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newTablePartitionIndexKeyValueIter(
		ctx,
		newReferencesTable(r.pool),
		ReferencesTableName,
		colNames,
		new(refRowKeyMapper),
	)
}

type refRowKeyMapper struct{}

func (refRowKeyMapper) fromRow(row sql.Row) ([]byte, error) {
	if len(row) != 3 {
		return nil, errRowKeyMapperRowLength.New(3, len(row))
	}

	repo, ok := row[0].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(0, repo, row[0])
	}

	name, ok := row[1].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(1, name, row[1])
	}

	commit, ok := row[2].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(2, commit, row[2])
	}

	var buf bytes.Buffer
	writeString(&buf, repo)
	writeString(&buf, name)

	if err := writeHash(&buf, commit); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (refRowKeyMapper) toRow(data []byte) (sql.Row, error) {
	var buf = bytes.NewBuffer(data)

	repo, err := readString(buf)
	if err != nil {
		return nil, fmt.Errorf("can't read ref repository: %s", err)
	}

	name, err := readString(buf)
	if err != nil {
		return nil, fmt.Errorf("can't read ref name: %s", err)
	}

	commit, err := readHash(buf)
	if err != nil {
		return nil, fmt.Errorf("can't read ref hash: %s", err)
	}

	return sql.Row{repo, name, commit}, nil
}

type refRowIter struct {
	repo          *Repository
	hashes        []plumbing.Hash
	names         []string
	index         sql.IndexValueIter
	skipGitErrors bool

	head   *plumbing.Reference
	iter   storer.ReferenceIter
	mapper refRowKeyMapper
}

func (i *refRowIter) Next() (sql.Row, error) {
	if i.index != nil {
		return i.nextFromIndex()
	}

	return i.next()
}

func (i *refRowIter) init() error {
	var err error
	i.iter, err = i.repo.References()
	if err != nil {
		return err
	}

	i.head, err = i.repo.Head()
	if err != nil && err != plumbing.ErrReferenceNotFound {
		return err
	}

	return nil
}

var (
	refRefNameIdx = RefsSchema.IndexOf("ref_name", ReferencesTableName)
	refHashIdx    = RefsSchema.IndexOf("commit_hash", ReferencesTableName)
)

func (i *refRowIter) nextFromIndex() (sql.Row, error) {
	for {
		key, err := i.index.Next()
		if err != nil {
			return nil, err
		}

		row, err := i.mapper.toRow(key)
		if err != nil {
			return nil, err
		}

		hash := plumbing.NewHash(row[refHashIdx].(string))
		if len(i.hashes) > 0 && !hashContains(i.hashes, hash) {
			continue
		}

		refName := strings.ToLower(row[refRefNameIdx].(string))
		if len(i.names) > 0 && !stringContains(i.names, refName) {
			continue
		}

		return row, nil
	}
}

func (i *refRowIter) next() (sql.Row, error) {
	for {
		if i.iter == nil {
			if err := i.init(); err != nil {
				if i.skipGitErrors {
					return nil, io.EOF
				}

				return nil, err
			}
		}

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
				i.repo.ID(),
				"HEAD",
				o.Hash().String(),
			), nil
		}

		o, err := i.iter.Next()
		if err != nil {
			return nil, err
		}

		if isIgnoredReference(o) {
			continue
		}

		if len(i.hashes) > 0 && !hashContains(i.hashes, o.Hash()) {
			continue
		}

		if len(i.names) > 0 && !stringContains(i.names, strings.ToLower(o.Name().String())) {
			continue
		}

		return referenceToRow(i.repo.ID(), o), nil
	}
}

func (i *refRowIter) Close() error {
	if i.iter != nil {
		i.iter.Close()
	}

	if i.repo != nil {
		i.repo.Close()
	}

	if i.index != nil {
		return i.index.Close()
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

func isIgnoredReference(r *plumbing.Reference) bool {
	return r.Type() != plumbing.HashReference
}
