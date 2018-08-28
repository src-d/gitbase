package function

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// Functions for gitbase queries.
var Functions = sql.Functions{
	"is_tag":     sql.Function1(NewIsTag),
	"is_remote":  sql.Function1(NewIsRemote),
	"language":   sql.FunctionN(NewLanguage),
	"uast":       sql.FunctionN(NewUAST),
	"uast_mode":  sql.Function3(NewUASTMode),
	"uast_xpath": sql.Function2(NewUASTXPath),
}
