package gitbase

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func printTable(name string, tableSchema sql.Schema) string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Table(%s)", name)
	var schema = make([]string, len(tableSchema))
	for i, col := range tableSchema {
		schema[i] = fmt.Sprintf(
			"Column(%s, %s, nullable=%v)",
			col.Name,
			col.Type.Type().String(),
			col.Nullable,
		)
	}
	_ = p.WriteChildren(schema...)
	return p.String()
}
