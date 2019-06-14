package function

import (
	"fmt"

	"github.com/src-d/gitbase/internal/commitstats"

	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

// CommitFileStats calculates the diff stats of all files for a given commit.
// Vendored files are ignored in the output of this function.
type CommitFileStats struct {
	Repository sql.Expression
	From       sql.Expression
	To         sql.Expression
}

// NewCommitFileStats creates a new COMMIT_FILE_STATS function.
func NewCommitFileStats(args ...sql.Expression) (sql.Expression, error) {
	var f CommitFileStats
	switch len(args) {
	case 2:
		f.Repository, f.To = args[0], args[1]
	case 3:
		f.Repository, f.From, f.To = args[0], args[1], args[2]
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("COMMIT_FILE_STATS", "2 or 3", len(args))
	}

	return &f, nil
}

func (f *CommitFileStats) String() string {
	if f.From == nil {
		return fmt.Sprintf("commit_file_stats(%s, %s)", f.Repository, f.To)
	}

	return fmt.Sprintf("commit_file_stats(%s, %s, %s)", f.Repository, f.From, f.To)
}

// Type implements the Expression interface.
func (CommitFileStats) Type() sql.Type {
	return sql.Array(sql.JSON)
}

// TransformUp implements the Expression interface.
func (f *CommitFileStats) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	repo, err := f.Repository.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	to, err := f.To.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	if f.From == nil {
		return fn(&CommitFileStats{Repository: repo, To: to})
	}

	from, err := f.From.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(&CommitFileStats{Repository: repo, From: from, To: to})
}

// Children implements the Expression interface.
func (f *CommitFileStats) Children() []sql.Expression {
	if f.From == nil {
		return []sql.Expression{f.Repository, f.To}
	}

	return []sql.Expression{f.Repository, f.From, f.To}
}

// IsNullable implements the Expression interface.
func (*CommitFileStats) IsNullable() bool {
	return true
}

// Resolved implements the Expression interface.
func (f *CommitFileStats) Resolved() bool {
	return f.Repository.Resolved() &&
		f.To.Resolved() &&
		(f.From == nil || f.From.Resolved())
}

// Eval implements the Expression interface.
func (f *CommitFileStats) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return evalStatsFunc(
		ctx,
		"commit_file_stats",
		row,
		f.Repository, f.From, f.To,
		func(r *git.Repository, from, to *object.Commit) (interface{}, error) {
			stats, err := commitstats.CalculateByFile(r, from, to)
			if err != nil {
				return nil, err
			}

			// Since the type is an array, it must be converted to []interface{}.
			var result = make([]interface{}, len(stats))
			for i, s := range stats {
				result[i] = s
			}
			return result, nil
		},
	)
}
