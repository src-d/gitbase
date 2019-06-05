package function

import (
	"fmt"
	"github.com/src-d/gitbase"
	"gopkg.in/src-d/go-git.v4"
	"github.com/src-d/go-mysql-server/sql"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type (
	// Blame implements git-blame function as UDF
	Blame struct {
		repo   sql.Expression
		commit sql.Expression
	}

	// BlameLine represents each line of git blame's output
	BlameLine struct {
		Commit  string `json:"commit"`
		File    string `json:"file"`
		LineNum int    `json:"linenum"`
		Author  string `json:"author"`
		Text    string `json:"text"`
	}
)

// NewBlame constructor
func NewBlame(repo, commit sql.Expression) sql.Expression {
	return &Blame{repo, commit}
}

func (b *Blame) String() string {
	return fmt.Sprintf("blame(%s, %s)", b.repo, b.commit)
}

// Type implements the sql.Expression interface
func (*Blame) Type() sql.Type {
	return sql.Array(sql.JSON)
}

func (b *Blame) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(b, len(children), 2)
	}

	return NewBlame(children[0], children[1]), nil
}

// Children implements the Expression interface.
func (b *Blame) Children() []sql.Expression {
	return []sql.Expression{b.repo, b.commit}
}

// IsNullable implements the Expression interface.
func (*Blame) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (b *Blame) Resolved() bool {
	return b.repo.Resolved() && b.commit.Resolved()
}

// Eval implements the sql.Expression interface.
func (b *Blame) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.Blame")
	defer span.Finish()

	repo, err := b.resolveRepo(ctx, row)
	if err != nil {
		return nil, err
	}

	commit, err := b.resolveCommit(ctx, repo, row)
	if err != nil {
		return nil, err
	}

	fIter, err := commit.Files()
	if err != nil {
		return nil, err
	}
	defer fIter.Close()

	var lines []BlameLine
	for f, err := fIter.Next(); err == nil; f, err = fIter.Next() {
		result, err := git.Blame(commit, f.Name)
		if err != nil {
			return nil, err
		}

		for i, l := range result.Lines {
			lines = append(lines, BlameLine{
				Commit:  commit.Hash.String(),
				File:    f.Name,
				LineNum: i,
				Author:  l.Author,
				Text:    l.Text,
			})
		}
	}

	return lines, nil
}

func (b *Blame) resolveCommit(ctx *sql.Context, repo *gitbase.Repository, row sql.Row) (*object.Commit, error) {
	str, err := exprToString(ctx, b.commit, row)
	if err != nil {
		return nil, err
	}

	commitHash, err := repo.ResolveRevision(plumbing.Revision(str))
	if err != nil {
		h := plumbing.NewHash(str)
		commitHash = &h
	}
	to, err := repo.CommitObject(*commitHash)
	if err != nil {
		return nil, err
	}

	return to, nil
}

func (b *Blame) resolveRepo(ctx *sql.Context, r sql.Row) (*gitbase.Repository, error) {
	repoID, err := exprToString(ctx, b.repo, r)
	if err != nil {
		return nil, err
	}
	s, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return nil, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
	}
	return s.Pool.GetRepo(repoID)
}
