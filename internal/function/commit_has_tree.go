package function

import (
	"fmt"
	"io"

	"github.com/hashicorp/golang-lru"

	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"

	"github.com/src-d/gitbase"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// CommitHasTree is a function that checks whether a tree is part of a commit
// or not.
type CommitHasTree struct {
	expression.BinaryExpression
	cache *lru.TwoQueueCache
}

// TODO: set as config
const commitHasTreeCacheSize = 100

// NewCommitHasTree creates a new CommitHasTree function.
func NewCommitHasTree(commit, tree sql.Expression) sql.Expression {
	// NewARC can only fail if size is negative, and we know it is not,
	// so it is safe to ignore the error here.
	cache, _ := lru.New2Q(commitHasTreeCacheSize)
	return &CommitHasTree{expression.BinaryExpression{
		Left:  commit,
		Right: tree,
	}, cache}
}

func (f CommitHasTree) String() string {
	return fmt.Sprintf("commit_has_tree(%s, %s)", f.Left, f.Right)
}

// Type implements the Expression interface.
func (CommitHasTree) Type() sql.Type { return sql.Boolean }

// Eval implements the Expression interface.
func (f *CommitHasTree) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.CommitHasTree")
	defer span.Finish()

	s, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return nil, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
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

	if val, ok := f.cache.Get(commitTreeKey{commitHash, treeHash}); ok {
		return val.(bool), nil
	}

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

		ok, err := f.commitHasTree(repo.Repo, commitHash, treeHash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		return ok, nil
	}
}

type commitTreeKey struct {
	commit plumbing.Hash
	tree   plumbing.Hash
}

func (f *CommitHasTree) commitHasTree(
	repo *git.Repository,
	commitHash, treeHash plumbing.Hash,
) (bool, error) {
	commit, err := repo.CommitObject(commitHash)
	if err != nil {
		return false, err
	}

	f.cache.Add(commitTreeKey{commitHash, commit.TreeHash}, true)

	if commit.TreeHash == treeHash {
		return true, nil
	}

	tree, err := commit.Tree()
	if err != nil {
		return false, err
	}

	return f.treeInEntries(repo, tree.Entries, commitHash, treeHash)
}

func (f *CommitHasTree) treeInEntries(
	repo *git.Repository,
	entries []object.TreeEntry,
	commitHash, hash plumbing.Hash,
) (bool, error) {
	type stackFrame struct {
		pos     int
		entries []object.TreeEntry
	}
	var stack = []*stackFrame{{0, entries}}

	for {
		if len(stack) == 0 {
			f.cache.Add(commitTreeKey{commitHash, hash}, false)
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
			f.cache.Add(commitTreeKey{commitHash, entry.Hash}, true)
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
