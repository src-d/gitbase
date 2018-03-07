package function

import (
	"io"

	"github.com/src-d/gitquery"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// CommitContains is a function that checks whether a blob is in a commit.
type CommitContains struct {
	commitHash sql.Expression
	blob       sql.Expression
}

// NewCommitContains creates a new commit_contains function.
func NewCommitContains(commitHash, blob sql.Expression) sql.Expression {
	return &CommitContains{
		commitHash: commitHash,
		blob:       blob,
	}
}

// Type implements the Expression interface.
func (CommitContains) Type() sql.Type {
	return sql.Boolean
}

// Eval implements the Expression interface.
func (f *CommitContains) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	s, ok := session.(*gitquery.Session)
	if !ok {
		return nil, gitquery.ErrInvalidGitQuerySession.New(session)
	}

	commitHash, err := f.commitHash.Eval(s, row)
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

	blob, err := f.blob.Eval(s, row)
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

	return f.commitContains(
		s.Pool,
		plumbing.NewHash(commitHash.(string)),
		plumbing.NewHash(blob.(string)),
	)
}

func (f *CommitContains) commitContains(
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

// Name implements the Expression interface.
func (CommitContains) Name() string {
	return "commit_contains"
}

// IsNullable implements the Expression interface.
func (f CommitContains) IsNullable() bool {
	return f.commitHash.IsNullable() || f.blob.IsNullable()
}

// Resolved implements the Expression interface.
func (f CommitContains) Resolved() bool {
	return f.commitHash.Resolved() && f.blob.Resolved()
}

// TransformUp implements the Expression interface.
func (f CommitContains) TransformUp(fn func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	commitHash, err := f.commitHash.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	blob, err := f.blob.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(NewCommitContains(commitHash, blob))
}
