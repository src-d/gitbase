package function

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// Functions for gitbase queries.
var Functions = sql.Functions{
	"is_tag":          sql.Function1(NewIsTag),
	"is_remote":       sql.Function1(NewIsRemote),
	"commit_has_blob": sql.Function2(NewCommitHasBlob),
	"commit_has_tree": sql.Function2(NewCommitHasTree),
	"language":        sql.FunctionN(NewLanguage),
	"uast":            sql.FunctionN(NewUAST),
	"uast_xpath":      sql.Function2(NewUASTXPath),
}
