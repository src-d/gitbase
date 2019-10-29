package function

import (
	"fmt"
	"io"

	"github.com/src-d/gitbase"
	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-git.v4"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type BlameGenerator struct {
	commit  *object.Commit
	file    string
	curLine int
	lines   []*git.Line
}

func NewBlameGenerator(c *object.Commit, f string) (*BlameGenerator, error) {
	result, err := git.Blame(c, f)
	if err != nil {
		return nil, err
	}
	return &BlameGenerator{commit: c, file: f, curLine: 0, lines: result.Lines}, nil
}

func (g *BlameGenerator) Next() (interface{}, error) {
	if len(g.lines) == 0 || g.curLine >= len(g.lines) {
		return nil, io.EOF
	}

	l := g.lines[g.curLine]
	b := BlameLine{
		LineNum: g.curLine,
		Author:  l.Author,
		Text:    l.Text,
	}
	g.curLine++
	return b, nil
}

func (g *BlameGenerator) Close() error {
	return nil
}

var _ sql.Generator = (*BlameGenerator)(nil)

type (
	// Blame implements git-blame function as UDF
	Blame struct {
		repo   sql.Expression
		commit sql.Expression
		file   sql.Expression
	}

	// BlameLine represents each line of git blame's output
	BlameLine struct {
		LineNum int    `json:"linenum"`
		Author  string `json:"author"`
		Text    string `json:"text"`
	}
)

// NewBlame constructor
func NewBlame(repo, commit, file sql.Expression) sql.Expression {
	return &Blame{repo, commit, file}
}

func (b *Blame) String() string {
	return fmt.Sprintf("blame(%s, %s)", b.repo, b.commit)
}

// Type implements the sql.Expression interface
func (*Blame) Type() sql.Type {
	return sql.Array(sql.JSON)
}

func (b *Blame) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(b, len(children), 2)
	}

	return NewBlame(children[0], children[1], children[2]), nil
}

// Children implements the Expression interface.
func (b *Blame) Children() []sql.Expression {
	return []sql.Expression{b.repo, b.commit, b.file}
}

// IsNullable implements the Expression interface.
func (b *Blame) IsNullable() bool {
	return b.repo.IsNullable() || (b.commit.IsNullable()) || (b.file.IsNullable())
}

// Resolved implements the Expression interface.
func (b *Blame) Resolved() bool {
	return b.repo.Resolved() && b.commit.Resolved() && b.file.Resolved()
}

// Eval implements the sql.Expression interface.
func (b *Blame) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.Blame")
	defer span.Finish()

	repo, err := b.resolveRepo(ctx, row)
	if err != nil {
		ctx.Warn(0, err.Error())
		return nil, nil
	}

	commit, err := b.resolveCommit(ctx, repo, row)
	if err != nil {
		ctx.Warn(0, err.Error())
		return nil, nil
	}

	file, err := exprToString(ctx, b.file, row)
	if err != nil {
		ctx.Warn(0, err.Error())
		return nil, nil
	}

	bg, err := NewBlameGenerator(commit, file)
	if err != nil {
		ctx.Warn(0, err.Error())
		return nil, nil
	}

	return bg, nil
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
