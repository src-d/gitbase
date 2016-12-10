package sql

type TableBase struct {
	Table
}

func (TableBase) Resolved() bool {
	return true
}

func (TableBase) Children() []Node {
	return []Node{}
}

func (r *TableBase) TransformUp(f func(Node) Node) Node {
	return f(r)
}

func (r *TableBase) TransformExpressionsUp(f func(Expression) Expression) Node {
	return r
}
