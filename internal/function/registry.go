package function

import "gopkg.in/src-d/go-mysql-server.v0/sql"

var functions = map[string]sql.Function{
	"is_tag":          sql.Function1(NewIsTag),
	"is_remote":       sql.Function1(NewIsRemote),
	"commit_contains": sql.Function2(NewCommitContains),
}

// Register all the gitquery functions in the SQL catalog.
func Register(c *sql.Catalog) {
	for k, v := range functions {
		c.RegisterFunction(k, v)
	}
}
