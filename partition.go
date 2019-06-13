package gitbase

import (
	"io"

	"github.com/src-d/go-borges"
	errors "gopkg.in/src-d/go-errors.v1"
	"github.com/src-d/go-mysql-server/sql"
)

// partitioned is an embeddable helper that contains the methods for a table
// that is partitioned by repository.
type partitioned struct{}

func (partitioned) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return newRepositoryPartitionIter(ctx)
}

func (partitioned) PartitionCount(ctx *sql.Context) (int64, error) {
	s, err := getSession(ctx)
	if err != nil {
		return 0, err
	}

	it, err := s.Pool.RepoIter()
	if err != nil {
		return 0, err
	}

	var count int64
	for {
		_, err = it.Next()
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return 0, err
		}
		count++
	}
}

// RepositoryPartition represents a partition which is a repository id.
type RepositoryPartition string

// Key implements the sql.Partition interface.
func (p RepositoryPartition) Key() []byte {
	return []byte(p)
}

type repositoryPartitionIter struct {
	// repos []string
	// pos   int
	repoIter borges.RepositoryIterator
	lib      borges.Library
}

func newRepositoryPartitionIter(ctx *sql.Context) (sql.PartitionIter, error) {
	s, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	it, err := s.Pool.library.Repositories(borges.ReadOnlyMode)
	if err != nil {
		return nil, err
	}

	return &repositoryPartitionIter{
		repoIter: it,
		lib:      s.Pool.library,
	}, nil

	// return &repositoryPartitionIter{repos: s.Pool.idOrder}, nil
}

func (i *repositoryPartitionIter) Next() (sql.Partition, error) {
	// if i.pos >= len(i.repos) {
	// 	return nil, io.EOF
	// }

	// i.pos++
	// return RepositoryPartition(i.repos[i.pos-1]), nil

	r, err := i.repoIter.Next()
	if err != nil {
		return nil, err
	}

	// br := borgesRepo(i.lib, r, cache.NewObjectLRU(64*cache.MiByte))
	return RepositoryPartition(r.ID().String()), nil
}

func (i *repositoryPartitionIter) Close() error {
	// i.pos = len(i.repos)
	if i.repoIter != nil {
		i.repoIter.Close()
	}

	return nil
}

// ErrNoRepositoryPartition is returned when the partition is not a valid
// repository partition.
var ErrNoRepositoryPartition = errors.NewKind("%T not a valid repository partition")

func getPartitionRepo(ctx *sql.Context, p sql.Partition) (*Repository, error) {
	rp, ok := p.(RepositoryPartition)
	if !ok {
		return nil, ErrNoRepositoryPartition.New(p)
	}

	s, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return s.Pool.GetRepo(string(rp))
}

var errColumnNotFound = errors.NewKind("column %s not found in table %s")

type tablePartitionIndexKeyValueIter struct {
	ctx        *sql.Context
	columns    []int
	mapper     rowKeyMapper
	partitions sql.PartitionIter
	table      sql.Table
}

func newTablePartitionIndexKeyValueIter(
	ctx *sql.Context,
	t sql.Table,
	tableName string,
	colNames []string,
	mapper rowKeyMapper,
) (*tablePartitionIndexKeyValueIter, error) {
	partitions, err := t.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	schema := t.Schema()

	var columns []int
	for _, col := range colNames {
		idx := schema.IndexOf(col, tableName)
		if idx < 0 {
			return nil, errColumnNotFound.New(col, tableName)
		}

		columns = append(columns, idx)
	}

	return &tablePartitionIndexKeyValueIter{
		ctx:        ctx,
		columns:    columns,
		mapper:     mapper,
		partitions: partitions,
		table:      t,
	}, nil
}

func (i *tablePartitionIndexKeyValueIter) Next() (sql.Partition, sql.IndexKeyValueIter, error) {
	p, err := i.partitions.Next()
	if err != nil {
		return nil, nil, err
	}

	iter, err := i.table.PartitionRows(i.ctx, p)
	if err != nil {
		return nil, nil, err
	}

	return p, &indexKeyValueIter{
		iter:    iter,
		mapper:  i.mapper,
		columns: i.columns,
	}, nil
}

func (i *tablePartitionIndexKeyValueIter) Close() error {
	return i.partitions.Close()
}

type indexKeyValueIter struct {
	iter    sql.RowIter
	mapper  rowKeyMapper
	columns []int
}

func (i *indexKeyValueIter) Next() ([]interface{}, []byte, error) {
	row, err := i.iter.Next()
	if err != nil {
		return nil, nil, err
	}

	key, err := i.mapper.fromRow(row)
	if err != nil {
		return nil, nil, err
	}

	var values = make([]interface{}, len(i.columns))
	for i, col := range i.columns {
		values[i] = row[col]
	}

	return values, key, nil
}

func (i *indexKeyValueIter) Close() error {
	return i.iter.Close()
}

type indexKeyValueIterBuilder func(*RepositoryPool, *Repository, []string) (sql.IndexKeyValueIter, error)

type partitionedIndexKeyValueIter struct {
	ctx        *sql.Context
	partitions sql.PartitionIter
	columns    []string
	session    *Session
	builder    indexKeyValueIterBuilder
}

func newPartitionedIndexKeyValueIter(
	ctx *sql.Context,
	table sql.Table,
	columns []string,
	builder indexKeyValueIterBuilder,
) (sql.PartitionIndexKeyValueIter, error) {
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &partitionedIndexKeyValueIter{
		ctx:        ctx,
		session:    session,
		partitions: partitions,
		columns:    columns,
		builder:    builder,
	}, nil
}

func (i *partitionedIndexKeyValueIter) Next() (sql.Partition, sql.IndexKeyValueIter, error) {
	p, err := i.partitions.Next()
	if err != nil {
		return nil, nil, err
	}

	repo, err := getPartitionRepo(i.ctx, p)
	if err != nil {
		return nil, nil, err
	}

	iter, err := i.builder(i.session.Pool, repo, i.columns)
	if err != nil {
		return nil, nil, err
	}

	return p, iter, nil
}

func (i *partitionedIndexKeyValueIter) Close() error {
	if i.partitions != nil {
		return i.partitions.Close()
	}
	return nil
}
