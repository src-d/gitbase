package function

import "github.com/src-d/go-mysql-server/sql"

// Functions for gitbase queries.
var Functions = []sql.Function{
	sql.FunctionN{Name: "commit_stats", Fn: NewCommitStats},
	sql.FunctionN{Name: "commit_file_stats", Fn: NewCommitFileStats},
	sql.Function1{Name: "is_tag", Fn: NewIsTag},
	sql.Function1{Name: "is_remote", Fn: NewIsRemote},
	sql.FunctionN{Name: "language", Fn: NewLanguage},
	sql.FunctionN{Name: "loc", Fn: NewLOC},
	sql.FunctionN{Name: "uast", Fn: NewUAST},
	sql.Function3{Name: "uast_mode", Fn: NewUASTMode},
	sql.Function2{Name: "uast_xpath", Fn: NewUASTXPath},
	sql.Function2{Name: "uast_extract", Fn: NewUASTExtract},
	sql.Function1{Name: "uast_children", Fn: NewUASTChildren},
	sql.Function1{Name: "uast_imports", Fn: NewUASTImports},
	sql.Function1{Name: "is_vendor", Fn: NewIsVendor},
	sql.Function2{Name: "blame", Fn: NewBlame},
}
