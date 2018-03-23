package function

import (
	"fmt"
	"io"

	"github.com/src-d/gitquery"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// CommitHasBlob is a function that checks whether a blob is in a commit.
type CommitHasBlob struct {
	commitHash sql.Expression
	blob       sql.Expression
}

// NewCommitHasBlob creates a new commit_has_blob function.
func NewCommitHasBlob(commitHash, blob sql.Expression) sql.Expression {
	return &CommitHasBlob{
		commitHash: commitHash,
		blob:       blob,
	}
}

// Type implements the Expression interface.
func (CommitHasBlob) Type() sql.Type {
	return sql.Boolean
}

// Eval implements the Expression interface.
func (f *CommitHasBlob) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	s, ok := ctx.Session.(*gitquery.Session)
	if !ok {
		return nil, gitquery.ErrInvalidGitQuerySession.New(ctx.Session)
	}

	commitHash, err := f.commitHash.Eval(ctx, row)
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

	blob, err := f.blob.Eval(ctx, row)
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

func (f *CommitHasBlob) commitHasBlob(
	pool *gitquery.RepositoryPool,
	commitHash, blob plumbing.Hash,
) (bool, error) {
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

		contained, err := hashInTree(blob, tree)
		if err != nil {
			return false, err
		}

		if contained {
			return true, nil
		}
	}

	return false, nil
}

func hashInTree(hash plumbing.Hash, tree *object.Tree) (bool, error) {
	var contained bool
	err := tree.Files().ForEach(func(f *object.File) error {
		if f.Blob.Hash == hash {
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
	return f.commitHash.IsNullable() || f.blob.IsNullable()
}

// Resolved implements the Expression interface.
func (f CommitHasBlob) Resolved() bool {
	return f.commitHash.Resolved() && f.blob.Resolved()
}

// TransformUp implements the Expression interface.
func (f CommitHasBlob) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	commitHash, err := f.commitHash.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	blob, err := f.blob.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(NewCommitHasBlob(commitHash, blob))
}

func (f CommitHasBlob) String() string {
	return fmt.Sprintf("commit_has_blob(%s, %s)", f.commitHash, f.blob)
}

// Children implements the Expression interface.
func (f CommitHasBlob) Children() []sql.Expression {
	return []sql.Expression{
		f.commitHash,
		f.blob,
	}
}
