package function

import (
	"fmt"
	"io"

	"github.com/hashicorp/golang-lru"

	"github.com/src-d/gitbase"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// CommitHasBlob is a function that checks whether a blob is in a commit.
type CommitHasBlob struct {
	expression.BinaryExpression
	cache *lru.TwoQueueCache
}

const commitHasBlobCacheSize = 200

// NewCommitHasBlob creates a new commit_has_blob function.
func NewCommitHasBlob(commitHash, blob sql.Expression) sql.Expression {
	cache, _ := lru.New2Q(commitHasBlobCacheSize)
	return &CommitHasBlob{
		expression.BinaryExpression{
			Left:  commitHash,
			Right: blob,
		},
		cache,
	}
}

// Type implements the Expression interface.
func (CommitHasBlob) Type() sql.Type {
	return sql.Boolean
}

// Eval implements the Expression interface.
func (f *CommitHasBlob) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	s, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return nil, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
	}

	commitHash, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if commitHash == nil {
		return nil, err
	}

	commitHash, err = sql.Text.Convert(commitHash)
	if err != nil {
		return nil, err
	}

	blob, err := f.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if blob == nil {
		return nil, err
	}

	blob, err = sql.Text.Convert(blob)
	if err != nil {
		return nil, err
	}

	return f.commitHasBlob(
		s.Pool,
		plumbing.NewHash(commitHash.(string)),
		plumbing.NewHash(blob.(string)),
	)
}

type commitBlobKey struct {
	commit, blob plumbing.Hash
}

func (f *CommitHasBlob) commitHasBlob(
	pool *gitbase.RepositoryPool,
	commitHash, blob plumbing.Hash,
) (bool, error) {
	if val, ok := f.cache.Get(commitBlobKey{commitHash, blob}); ok {
		return val.(bool), nil
	}

	iter, err := pool.RepoIter()
	if err != nil {
		return false, err
	}

	for {
		repository, err := iter.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return false, err
		}

		repo := repository.Repo
		commit, err := repo.CommitObject(commitHash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		if err != nil {
			return false, err
		}

		tree, err := commit.Tree()
		if err != nil {
			return false, err
		}

		contained, err := f.hashInTree(blob, commitHash, tree)
		if err != nil {
			return false, err
		}

		if contained {
			return true, nil
		}
		f.cache.Add(commitBlobKey{commitHash, blob}, false)
	}

	return false, nil
}

func (f *CommitHasBlob) hashInTree(
	hash plumbing.Hash,
	commit plumbing.Hash,
	tree *object.Tree,
) (bool, error) {
	var contained bool
	err := tree.Files().ForEach(func(fi *object.File) error {
		f.cache.Add(commitBlobKey{commit, fi.Blob.Hash}, true)
		if fi.Blob.Hash == hash {
			contained = true
			return io.EOF
		}
		return nil
	})

	if err != nil && err != io.EOF {
		return false, err
	}

	return contained, nil
}

// IsNullable implements the Expression interface.
func (f CommitHasBlob) IsNullable() bool {
	return f.Left.IsNullable() || f.Right.IsNullable()
}

// Resolved implements the Expression interface.
func (f CommitHasBlob) Resolved() bool {
	return f.Left.Resolved() && f.Right.Resolved()
}

// TransformUp implements the Expression interface.
func (f CommitHasBlob) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	commitHash, err := f.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	blob, err := f.Right.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(NewCommitHasBlob(commitHash, blob))
}

func (f CommitHasBlob) String() string {
	return fmt.Sprintf("commit_has_blob(%s, %s)", f.Left, f.Right)
}

// Children implements the Expression interface.
func (f CommitHasBlob) Children() []sql.Expression {
	return []sql.Expression{
		f.Left,
		f.Right,
	}
}
