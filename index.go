package gitbase

import (
	"bytes"
	"encoding/gob"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

var (
	// ErrColumnNotFound is returned when a given column is not found in the table's schema.
	ErrColumnNotFound = errors.NewKind("column %s not found for table %s")
	// ErrCreateIndexValue is returned if an index value can't be generated.
	ErrCreateIndexValue = errors.NewKind("couldn't create index value, missing %s")
	// ErrIndexValue is returned when a index value is malformed.
	ErrIndexValue = errors.NewKind("wrong index value found")
)

// Indexable represents an indexable gitbase table.
type Indexable interface {
	sql.Indexable
	PushdownTable
}

// PushdownTable represents a gitbase table that is able to pushdown projections and filters.
type PushdownTable interface {
	sql.PushdownProjectionAndFiltersTable
	gitBase
	handledColumns() []string
}

type indexableTable struct {
	PushdownTable
	buildIterWithSelectors iteratorBuilder
}

var _ sql.Indexable = (*indexableTable)(nil)

// IndexKeyValueIter implements sql.Indexable interface.
func (i *indexableTable) IndexKeyValueIter(ctx *sql.Context, colNames []string) (sql.IndexKeyValueIter, error) {
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	colIndexes := []int{}
	columns := []sql.Expression{}
	for _, colName := range colNames {
		idx := i.Schema().IndexOf(colName, i.Name())
		if idx < 0 {
			return nil, ErrColumnNotFound.New(colName, i.Name())
		}

		colIndexes = append(colIndexes, idx)

		col := expression.NewGetFieldWithTable(
			idx,
			i.Schema()[idx].Type,
			i.Schema()[idx].Source,
			i.Schema()[idx].Name,
			i.Schema()[idx].Nullable,
		)

		columns = append(columns, col)
	}

	rIter, err := s.Pool.RepoIter()
	if err != nil {
		return nil, err
	}

	tableIter, err := i.buildIterWithSelectors(ctx, nil, columns)
	if err != nil {
		return nil, err
	}

	repoIter := &rowRepoIter{
		currRepoIter:   nil,
		repositoryIter: rIter,
		iter:           tableIter,
		session:        s,
		ctx:            ctx,
	}

	return &indexKVIter{
		repoIter:   repoIter,
		colIndexes: colIndexes,
	}, nil
}

// WithProjectFiltersAndIndex implements sql.Indexable interface.
func (i *indexableTable) WithProjectFiltersAndIndex(ctx *sql.Context, columns, filters []sql.Expression, index sql.IndexValueIter) (sql.RowIter, error) {
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	selectors, filters, err := classifyFilters(i.Schema(), i.Name(),
		filters, i.handledColumns()...)
	if err != nil {
		return nil, err
	}

	rowRepoIter, err := i.buildIterWithSelectors(ctx, selectors, columns)
	if err != nil {
		return nil, err
	}

	indexIter := &indexIter{
		iter:         rowRepoIter,
		idxValueIter: index,
		pool:         s.Pool,
	}

	if len(filters) == 0 {
		return indexIter, nil
	}

	return plan.NewFilterIter(ctx, expression.JoinAnd(filters...), indexIter), nil
}

type indexIter struct {
	repoID   string
	iter     RowRepoIter
	currIter RowRepoIter

	pool         *RepositoryPool
	idxValueIter sql.IndexValueIter
}

var _ sql.RowIter = (*indexIter)(nil)

func (i *indexIter) Next() (sql.Row, error) {
	for {
		v, err := i.idxValueIter.Next()
		if err != nil {
			return nil, err
		}

		idxVal, err := unmarshalIndexValue(v)
		if err != nil || idxVal.ID == "" || idxVal.Object == "" {
			return nil, ErrIndexValue.New()
		}

		if i.repoID != idxVal.ID {
			repo, err := i.pool.GetRepo(idxVal.ID)
			if err != nil {
				return nil, err
			}

			iter, err := i.iter.NewIterator(repo)
			if err != nil {
				return nil, err
			}

			i.repoID = repo.ID
			i.currIter = iter
		}

		return i.currIter.Next()
	}
}

func (i *indexIter) Close() error {
	if i.currIter != nil {
		i.currIter.Close()
	}

	return nil
}

type indexValue struct {
	ID     string
	Object string
}

func marshalIndexValue(value *indexValue) ([]byte, error) {
	var raw bytes.Buffer
	enc := gob.NewEncoder(&raw)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}

	return raw.Bytes(), nil
}

func unmarshalIndexValue(raw []byte) (*indexValue, error) {
	value := bytes.NewReader(raw)
	dec := gob.NewDecoder(value)
	idxValue := &indexValue{}
	if err := dec.Decode(idxValue); err != nil {
		return nil, err
	}

	return idxValue, nil
}

type indexKVIter struct {
	repoIter   *rowRepoIter
	colIndexes []int
}

var _ sql.IndexKeyValueIter = (*indexKVIter)(nil)

func (i *indexKVIter) Next() ([]interface{}, []byte, error) {
	row, err := i.repoIter.Next()
	if err != nil {
		return nil, nil, err
	}

	repoID := i.repoIter.currRepoIter.Repository()
	if repoID == "" {
		return nil, nil, ErrCreateIndexValue.New("repository id")
	}

	object := i.repoIter.currRepoIter.LastObject()
	if object == "" {
		return nil, nil, ErrCreateIndexValue.New("object")
	}

	idxValue := &indexValue{repoID, object}

	colValues := []interface{}{}
	for _, idx := range i.colIndexes {
		colValues = append(colValues, row[idx])
	}

	value, err := marshalIndexValue(idxValue)
	if err != nil {
		return nil, nil, err
	}

	return colValues, value, nil
}

func (i *indexKVIter) Close() error {
	return i.repoIter.Close()
}
