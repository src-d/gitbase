package gitbase

import (
	"io"

	"github.com/src-d/go-mysql-server/sql"
)

type repositoriesTable struct {
	checksumable
	partitioned
	filters []sql.Expression
	index   sql.IndexLookup
}

// RepositoriesSchema is the schema for the repositories table.
var RepositoriesSchema = sql.Schema{
	{Name: "repository_id", Type: sql.Text, Nullable: false, Source: RepositoriesTableName},
}

func newRepositoriesTable(pool *RepositoryPool) *repositoriesTable {
	return &repositoriesTable{checksumable: checksumable{pool}}
}

var _ Table = (*repositoriesTable)(nil)
var _ Squashable = (*repositoriesTable)(nil)

func (repositoriesTable) isSquashable()   {}
func (repositoriesTable) isGitbaseTable() {}

func (repositoriesTable) Name() string {
	return RepositoriesTableName
}

func (repositoriesTable) Schema() sql.Schema {
	return RepositoriesSchema
}

func (r repositoriesTable) String() string {
	return printTable(
		RepositoriesTableName,
		RepositoriesSchema,
		nil,
		r.filters,
		r.index,
	)
}

func (repositoriesTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	return handledFilters(RepositoriesTableName, RepositoriesSchema, filters)
}

func (r *repositoriesTable) WithFilters(filters []sql.Expression) sql.Table {
	nt := *r
	nt.filters = filters
	return &nt
}

func (r *repositoriesTable) WithIndexLookup(idx sql.IndexLookup) sql.Table {
	nt := *r
	nt.index = idx
	return &nt
}

func (r *repositoriesTable) PartitionRows(
	ctx *sql.Context,
	p sql.Partition,
) (sql.RowIter, error) {
	repo, err := getPartitionRepo(ctx, p)
	if err != nil {
		return nil, err
	}

	span, ctx := ctx.Span("gitbase.RepositoriesTable")
	iter, err := rowIterWithSelectors(
		ctx, RepositoriesSchema, RepositoriesTableName,
		r.filters,
		r.handledColumns(),
		func(_ selectors) (sql.RowIter, error) {
			if r.index != nil {
				values, err := r.index.Values(p)
				if err != nil {
					return nil, err
				}
				return &rowIndexIter{new(repoRowKeyMapper), values}, nil
			}

			return &repositoriesRowIter{repo: repo}, nil
		},
	)

	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (repositoriesTable) handledColumns() []string { return nil }

func (r *repositoriesTable) IndexLookup() sql.IndexLookup { return r.index }
func (r *repositoriesTable) Filters() []sql.Expression    { return r.filters }

// IndexKeyValues implements the sql.IndexableTable interface.
func (r *repositoriesTable) IndexKeyValues(
	ctx *sql.Context,
	colNames []string,
) (sql.PartitionIndexKeyValueIter, error) {
	return newTablePartitionIndexKeyValueIter(
		ctx,
		newRepositoriesTable(r.pool),
		RepositoriesTableName,
		colNames,
		new(repoRowKeyMapper),
	)
}

type repoRowKeyMapper struct{}

func (repoRowKeyMapper) fromRow(row sql.Row) ([]byte, error) {
	if len(row) != 1 {
		return nil, errRowKeyMapperRowLength.New(1, len(row))
	}

	repo, ok := row[0].(string)
	if !ok {
		return nil, errRowKeyMapperColType.New(0, repo, row[0])
	}

	return []byte(repo), nil
}

func (repoRowKeyMapper) toRow(data []byte) (sql.Row, error) {
	return sql.Row{string(data)}, nil
}

type repositoriesRowIter struct {
	repo    *Repository
	visited bool
}

func (i *repositoriesRowIter) Next() (sql.Row, error) {
	if i.visited {
		return nil, io.EOF
	}

	i.visited = true
	return sql.NewRow(i.repo.ID), nil
}

func (i *repositoriesRowIter) Close() error {
	i.visited = true
	if i.repo != nil {
		i.repo.Close()
	}
	return nil
}
