package function

import (
	"fmt"
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"

	"github.com/src-d/gitquery"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// CommitHasTree is a function that checks whether a tree is part of a commit
// or not.
type CommitHasTree struct {
	expression.BinaryExpression
}

// NewCommitHasTree creates a new CommitHasTree function.
func NewCommitHasTree(commit, tree sql.Expression) sql.Expression {
	return &CommitHasTree{expression.BinaryExpression{
		Left:  commit,
		Right: tree,
	}}
}

func (f CommitHasTree) String() string {
	return fmt.Sprintf("commit_has_tree(%s, %s)", f.Left, f.Right)
}

// Type implements the Expression interface.
func (CommitHasTree) Type() sql.Type { return sql.Boolean }

// Eval implements the Expression interface.
func (f *CommitHasTree) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	s, ok := ctx.Session.(*gitquery.Session)
	if !ok {
		return nil, gitquery.ErrInvalidGitQuerySession.New(ctx.Session)
	}

	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	left, err = sql.Text.Convert(left)
	if err != nil {
		return nil, err
	}

	right, err := f.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if right == nil {
		return nil, nil
	}

	right, err = sql.Text.Convert(right)
	if err != nil {
		return nil, err
	}

	commitHash := plumbing.NewHash(left.(string))
	treeHash := plumbing.NewHash(right.(string))

	iter, err := s.Pool.RepoIter()
	if err != nil {
		return nil, err
	}

	for {
		repo, err := iter.Next()
		if err == io.EOF {
			return false, nil
		}

		if err != nil {
			return nil, err
		}

		ok, err := commitHasTree(repo.Repo, commitHash, treeHash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		return ok, nil
	}
}

func commitHasTree(
	repo *git.Repository,
	commitHash, treeHash plumbing.Hash,
) (bool, error) {
	commit, err := repo.CommitObject(commitHash)
	if err != nil {
		return false, err
	}

	if commit.TreeHash == treeHash {
		return true, nil
	}

	tree, err := commit.Tree()
	if err != nil {
		return false, err
	}

	return treeInEntries(repo, tree.Entries, treeHash)
}

func treeInEntries(
	repo *git.Repository,
	entries []object.TreeEntry,
	hash plumbing.Hash,
) (bool, error) {
	type stackFrame struct {
		pos     int
		entries []object.TreeEntry
	}
	var stack = []*stackFrame{{0, entries}}

	for {
		if len(stack) == 0 {
			return false, nil
		}

		frame := stack[len(stack)-1]
		if frame.pos >= len(frame.entries) {
			stack = stack[:len(stack)-1]
			continue
		}

		entry := frame.entries[frame.pos]
		frame.pos++
		if entry.Mode == filemode.Dir {
			if entry.Hash == hash {
				return true, nil
			}

			tree, err := repo.TreeObject(entry.Hash)
			if err != nil {
				return false, nil
			}

			stack = append(stack, &stackFrame{0, tree.Entries})
		}
	}
}

// TransformUp implements the Expression interface.
func (f *CommitHasTree) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	left, err := f.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	right, err := f.Right.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(NewCommitHasTree(left, right))
}
