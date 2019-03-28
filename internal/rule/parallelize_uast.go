package rule

import (
	"github.com/src-d/gitbase/internal/function"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// ParallelizeUASTProjectionsRule is the name of the rule.
const ParallelizeUASTProjectionsRule = "parallelize_uast_projections"

// ParallelizeUASTProjections is a rule that whenever it finds a projection
// with a call to any uast function, it replaces it with a parallel version
// of the project node to execute several bblfsh requests in parallel. It
// will only do so if the project is not under an exchange node.
func ParallelizeUASTProjections(
	ctx *sql.Context,
	a *analyzer.Analyzer,
	n sql.Node,
) (sql.Node, error) {
	if a.Parallelism <= 1 {
		return n, nil
	}

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Project:
			if callsUAST(n.Projections) {
				return newParallelProject(n.Projections, n.Child, a.Parallelism), nil
			}

			return n, nil
		case *plan.Exchange:
			child, err := n.Child.TransformUp(removeParallelProjects)
			if err != nil {
				return nil, err
			}

			return plan.NewExchange(n.Parallelism, child), nil
		default:
			return n, nil
		}
	})
}

func removeParallelProjects(n sql.Node) (sql.Node, error) {
	p, ok := n.(*parallelProject)
	if !ok {
		return n, nil
	}

	return plan.NewProject(p.Projections, p.Child), nil
}

func callsUAST(exprs []sql.Expression) bool {
	var seen bool
	for _, e := range exprs {
		expression.Inspect(e, func(e sql.Expression) bool {
			switch e.(type) {
			case *function.UAST, *function.UASTMode:
				seen = true
				return false
			}

			return true
		})

		if seen {
			return true
		}
	}

	return false
}
