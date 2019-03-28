package rule

import (
	"context"
	"io"
	"strings"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

type parallelProject struct {
	*plan.Project
	parallelism int
}

func newParallelProject(
	projection []sql.Expression,
	child sql.Node,
	parallelism int,
) *parallelProject {
	return &parallelProject{
		plan.NewProject(projection, child),
		parallelism,
	}
}

func (p *parallelProject) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span(
		"plan.Project",
		opentracing.Tag{
			Key:   "projections",
			Value: len(p.Projections),
		},
		opentracing.Tag{
			Key:   "parallelism",
			Value: p.parallelism,
		},
	)

	iter, err := p.Child.RowIter(ctx)
	if err != nil {
		span.Finish()
		return nil, err
	}

	return sql.NewSpanIter(
		span,
		newParallelIter(p.Projections, iter, ctx, p.parallelism),
	), nil
}

func (p *parallelProject) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := p.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(newParallelProject(p.Projections, child, p.parallelism))
}

func (p *parallelProject) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	var exprs = make([]sql.Expression, len(p.Projections))
	for i, e := range p.Projections {
		expr, err := e.TransformUp(f)
		if err != nil {
			return nil, err
		}

		exprs[i] = expr
	}

	child, err := p.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return newParallelProject(exprs, child, p.parallelism), nil
}

func (p *parallelProject) String() string {
	pr := sql.NewTreePrinter()
	var exprs = make([]string, len(p.Projections))
	for i, expr := range p.Projections {
		exprs[i] = expr.String()
	}

	_ = pr.WriteNode(
		"gitbase.ParallelProject(%s, parallelism=%d)",
		strings.Join(exprs, ", "),
		p.parallelism,
	)

	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

type parallelIter struct {
	projections []sql.Expression
	child       sql.RowIter
	ctx         *sql.Context
	parallelism int

	cancel context.CancelFunc
	rows   chan sql.Row
	errors chan error
	done   bool

	mut      sync.Mutex
	finished bool
}

func newParallelIter(
	projections []sql.Expression,
	child sql.RowIter,
	ctx *sql.Context,
	parallelism int,
) *parallelIter {
	var cancel context.CancelFunc
	ctx.Context, cancel = context.WithCancel(ctx.Context)

	return &parallelIter{
		projections: projections,
		child:       child,
		ctx:         ctx,
		parallelism: parallelism,
		cancel:      cancel,
		errors:      make(chan error, parallelism),
	}
}

func (i *parallelIter) Next() (sql.Row, error) {
	if i.done {
		return nil, io.EOF
	}

	if i.rows == nil {
		i.rows = make(chan sql.Row, i.parallelism)
		go i.start()
	}

	select {
	case row, ok := <-i.rows:
		if !ok {
			i.close()
			return nil, io.EOF
		}
		return row, nil
	case err := <-i.errors:
		i.close()
		return nil, err
	}
}

func (i *parallelIter) nextRow() (sql.Row, bool) {
	i.mut.Lock()
	defer i.mut.Unlock()

	if i.finished {
		return nil, true
	}

	row, err := i.child.Next()
	if err != nil {
		if err == io.EOF {
			i.finished = true
		} else {
			i.errors <- err
		}
		return nil, true
	}

	return row, false
}

func (i *parallelIter) start() {
	var wg sync.WaitGroup
	wg.Add(i.parallelism)
	for j := 0; j < i.parallelism; j++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case <-i.ctx.Done():
					i.errors <- context.Canceled
					return
				default:
				}

				row, stop := i.nextRow()
				if stop {
					return
				}

				row, err := project(i.ctx, i.projections, row)
				if err != nil {
					i.errors <- err
					return
				}

				i.rows <- row
			}
		}()
	}

	wg.Wait()
	close(i.rows)
}

func (i *parallelIter) close() {
	if !i.done {
		i.cancel()
		i.done = true
	}
}

func (i *parallelIter) Close() error {
	i.close()
	return i.child.Close()
}

func project(
	s *sql.Context,
	projections []sql.Expression,
	row sql.Row,
) (sql.Row, error) {
	var fields []interface{}
	for _, expr := range projections {
		f, err := expr.Eval(s, row)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	return sql.NewRow(fields...), nil
}
