package function

import (
	"context"
	"fmt"

	"gopkg.in/src-d/go-git.v4/plumbing"
	fmtdiff "gopkg.in/src-d/go-git.v4/plumbing/format/diff"
	"gopkg.in/src-d/go-git.v4/plumbing/object"

	"github.com/src-d/gitbase"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

type (
	// Diff implements git-diff function as a UDF
	Diff struct {
		repo sql.Expression
		from sql.Expression
		to   sql.Expression
	}

	// diffItem stands for a single payload item
	// (response payload is an array of JSON objects as strings)
	// Indexes are local per file.
	// Many objects for different files may have the same index.
	diffItem struct {
		Index  int    `json:"index"`
		File   string `json:"file"`
		Chunk  string `json:"chunk"`
		Status string `json:"status"`
	}
)

// NewDiff constructor
func NewDiff(args ...sql.Expression) (sql.Expression, error) {
	d := &Diff{}
	switch len(args) {
	case 2:
		d.repo, d.to = args[0], args[1]
	case 3:
		d.repo, d.from, d.to = args[0], args[1], args[2]
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("DIFF", "2 or 3", len(args))
	}

	return d, nil
}

func (d *Diff) String() string {
	if d.from == nil {
		return fmt.Sprintf("diff(%s, %s)", d.repo, d.to)
	}

	return fmt.Sprintf("diff(%s, %s, %s)", d.repo, d.from, d.to)
}

// Type implements the sql.Expression interface.
func (*Diff) Type() sql.Type {
	return sql.Array(sql.JSON)
}

// TransformUp implements the Expression interface.
func (d *Diff) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	repo, err := d.repo.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	to, err := d.to.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	if d.from == nil {
		return fn(&Diff{repo: repo, to: to})
	}

	from, err := d.from.TransformUp(fn)
	if err != nil {
		return nil, err
	}
	return fn(&Diff{repo: repo, from: from, to: to})
}

// Children implements the Expression interface.
func (d *Diff) Children() []sql.Expression {
	if d.from == nil {
		return []sql.Expression{d.repo, d.to}
	}
	return []sql.Expression{d.repo, d.from, d.to}
}

// IsNullable implements the Expression interface.
func (*Diff) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (d *Diff) Resolved() bool {
	return d.to.Resolved() && (d.from == nil || d.from.Resolved())
}

// Eval implements the sql.Expression interface.
func (d *Diff) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.Diff")
	defer span.Finish()

	repo, err := d.resolveRepo(ctx, row)
	if err != nil {
		return nil, err
	}

	from, to, err := d.resolveCommits(ctx, repo, row)
	if err != nil {
		return nil, err
	}

	return diff(ctx, from, to)
}

func (d *Diff) resolveRepo(ctx *sql.Context, r sql.Row) (*gitbase.Repository, error) {
	repoID, err := exprToString(ctx, d.repo, r)
	if err != nil {
		return nil, err
	}
	s, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return nil, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
	}
	return s.Pool.GetRepo(repoID)
}

func (d *Diff) resolveCommits(ctx *sql.Context, repo *gitbase.Repository, row sql.Row) (*object.Commit, *object.Commit, error) {
	str, err := exprToString(ctx, d.to, row)
	if err != nil {
		return nil, nil, err
	}

	commitHash, err := repo.ResolveRevision(plumbing.Revision(str))
	if err != nil {
		h := plumbing.NewHash(str)
		commitHash = &h
	}
	to, err := repo.CommitObject(*commitHash)
	if err != nil {
		return nil, nil, err
	}

	var from *object.Commit
	if d.from != nil {
		str, err = exprToString(ctx, d.from, row)
		if err != nil {
			return nil, nil, err
		}

		commitHash, err = repo.ResolveRevision(plumbing.Revision(str))
		if err != nil {
			h := plumbing.NewHash(str)
			commitHash = &h
		}
		from, err = repo.CommitObject(*commitHash)
	} else {
		from, err = to.Parent(0)
	}
	if err != nil && err != object.ErrParentNotFound {
		return nil, nil, err
	}

	return from, to, nil
}

// diff compares two commits and returns a slice of diff items.
// The function ignores binary files and equal operations.
func diff(ctx context.Context, from, to *object.Commit) ([]*diffItem, error) {
	changes, err := resolveChanges(ctx, from, to)
	if err != nil {
		return nil, err
	}
	patch, err := changes.PatchContext(ctx)
	if err != nil {
		return nil, err
	}

	items := make([]*diffItem, 0)
out:
	for _, fp := range patch.FilePatches() {
		if fp.IsBinary() {
			continue
		}
		fromFile, toFile := fp.Files()
		for i, ch := range fp.Chunks() {
			select {
			case <-ctx.Done():
				break out

			default:
				filename, status := resolveFileAndStatus(fromFile, toFile, ch.Type())
				if status == "" {
					continue
				}

				items = append(items, &diffItem{
					Index:  i, // indexes are local per file
					File:   filename,
					Chunk:  ch.Content(),
					Status: status,
				})
			}
		}
	}

	return items, nil
}

func resolveChanges(ctx context.Context, from, to *object.Commit) (object.Changes, error) {
	var (
		fromTree *object.Tree
		toTree   *object.Tree
		err      error
	)
	if from != nil {
		if fromTree, err = from.Tree(); err != nil {
			return nil, err
		}
	}

	if to != nil {
		if toTree, err = to.Tree(); err != nil {
			return nil, err
		}
	}

	return object.DiffTreeContext(ctx, fromTree, toTree)
}

func resolveFileAndStatus(fromFile, toFile fmtdiff.File, op fmtdiff.Operation) (string, string) {
	var (
		filename string
		status   string
	)
	switch op {
	case fmtdiff.Equal:
		// ignore `equal` operation to avoid noisy payloads
		break

	case fmtdiff.Add:
		status = "+"
		if toFile != nil {
			filename = toFile.Path()
		}

	case fmtdiff.Delete:
		status = "-"
		// for delete operation take the file name from the parent.
		if fromFile != nil {
			filename = fromFile.Path()
		}
	}

	return filename, status
}
