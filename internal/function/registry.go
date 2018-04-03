package function

import "gopkg.in/src-d/go-mysql-server.v0/sql"

var Functions = sql.Functions{
	"is_tag":          sql.Function1(NewIsTag),
	"is_remote":       sql.Function1(NewIsRemote),
	"commit_has_blob": sql.Function2(NewCommitHasBlob),
	"history_idx":     sql.Function2(NewHistoryIdx),
	"commit_has_tree": sql.Function2(NewCommitHasTree),
	"language":        sql.FunctionN(NewLanguage),
}
