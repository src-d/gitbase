package function

import (
	"fmt"
	"io"

	"github.com/hashicorp/golang-lru"
	"gopkg.in/src-d/go-log.v0"

	"github.com/src-d/gitbase"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// HistoryIdx is a function that returns the index of a commit in the history
// of another commit.
type HistoryIdx struct {
	expression.BinaryExpression
	cache *lru.TwoQueueCache
}

const historyIdxCacheSize = 300

// NewHistoryIdx creates a new HistoryIdx udf.
func NewHistoryIdx(start, target sql.Expression) sql.Expression {
	cache, _ := lru.New2Q(historyIdxCacheSize)
	return &HistoryIdx{expression.BinaryExpression{Left: start, Right: target}, cache}
}

func (f HistoryIdx) String() string {
	return fmt.Sprintf("history_idx(%s, %s)", f.Left, f.Right)
}

type historyKey struct {
	start, target plumbing.Hash
}

// Eval implements the Expression interface.
func (f *HistoryIdx) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("gitbase.HistoryIdx")
	defer span.Finish()

	logger, _ := log.New()
	logger = logger.New(log.Fields{
		"function": "history_idx",
		"row":      row,
		"left":     f.Left.String(),
		"right":    f.Right.String(),
	})

	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		logger.Error(err, "cannot eval left side")
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	left, err = sql.Text.Convert(left)
	if err != nil {
		logger.Error(err, "cannot convert left side")
		return nil, err
	}

	right, err := f.Right.Eval(ctx, row)
	if err != nil {
		logger.Error(err, "cannot eval right side")
		return nil, err
	}

	if right == nil {
		return nil, nil
	}

	right, err = sql.Text.Convert(right)
	if err != nil {
		logger.Error(err, "cannot convert right side")
		return nil, err
	}

	start := plumbing.NewHash(left.(string))
	target := plumbing.NewHash(right.(string))

	if val, ok := f.cache.Get(historyKey{start, target}); ok {
		return val.(int64), nil
	}

	// fast path for equal hashes
	if start == target {
		return int64(0), nil
	}

	return f.historyIdx(ctx, logger, start, target)
}

func (f *HistoryIdx) historyIdx(
	ctx *sql.Context,
	logger log.Logger,
	start, target plumbing.Hash,
) (int64, error) {
	s, ok := ctx.Session.(*gitbase.Session)
	if !ok {
		return 0, gitbase.ErrInvalidGitbaseSession.New(ctx.Session)
	}

	pool := s.Pool

	iter, err := pool.RepoIter()
	if err != nil {
		logger.Error(err, "cannot create repository iterator")
		return 0, err
	}

	for {
		select {
		case <-ctx.Done():
			logger.Debugf("query canceled")
			return 0, gitbase.ErrSessionCanceled.New()
		default:
		}

		repo, err := iter.Next()
		if err == io.EOF {
			return -1, nil
		}

		if err != nil {
			logger.Error(err, "could not get next repository")

			if s.SkipGitErrors {
				continue
			}
			return 0, err
		}

		idx, err := f.repoHistoryIdx(ctx, repo, logger, start, target)
		if err != nil {
			logger.New(log.Fields{
				"repo": repo.ID,
			}).Error(err, "error searching history")

			if s.SkipGitErrors {
				continue
			}
			return 0, err
		}

		if idx > -1 {
			return idx, nil
		}
	}
}

type stackFrame struct {
	// idx from the start commit
	idx int64
	// pos in the hashes slice
	pos    int
	hashes []plumbing.Hash
}

func (f *HistoryIdx) repoHistoryIdx(
	ctx *sql.Context,
	r *gitbase.Repository,
	logger log.Logger,
	start, target plumbing.Hash,
) (int64, error) {
	s := ctx.Session.(*gitbase.Session)
	repo := r.Repo
	logger = logger.New(log.Fields{
		"repo":   r.ID,
		"target": target.String(),
	})

	// If the target is not on the repo we can avoid starting to traverse the
	// tree completely.
	_, err := repo.CommitObject(target)
	if err == plumbing.ErrObjectNotFound {
		return -1, nil
	}

	if err != nil {
		logger.New(log.Fields{
			"commit": target.String(),
		}).Error(err, "could not get commit")
		return 0, err
	}

	// Since commits can have multiple parents we cannot just do a repo.Log and
	// keep counting with an index how far it is, because it might go back in
	// the history and try another branch.
	// Because of that, the traversal of the history is done manually using a
	// stack with frames with N commit hashes, representing each level in the
	// history. Because the frame keeps track of which was its index, we can
	// return accurate indexes even if there are multiple branches.
	stack := []*stackFrame{{0, 0, []plumbing.Hash{start}}}
	visitedHashes := make(map[plumbing.Hash]struct{})

	for {
		select {
		case <-ctx.Done():
			logger.Debugf("query canceled")
			return 0, gitbase.ErrSessionCanceled.New()
		default:
		}

		if len(stack) == 0 {
			f.cache.Add(historyKey{start, target}, int64(-1))
			return -1, nil
		}

		frame := stack[len(stack)-1]

		h := frame.hashes[frame.pos]
		if _, ok := visitedHashes[h]; !ok {
			visitedHashes[h] = struct{}{}
		}

		c, err := repo.CommitObject(h)
		if err == plumbing.ErrObjectNotFound {
			return -1, nil
		}

		if err != nil {
			logger.New(log.Fields{
				"commit": h.String(),
			}).Error(err, "could not get commit")

			if !s.SkipGitErrors {
				return 0, err
			}

			c = &object.Commit{
				Hash: h,
			}
		}

		frame.pos++

		f.cache.Add(historyKey{start, c.Hash}, frame.idx)

		if c.Hash == target {
			return frame.idx, nil
		}

		if frame.pos >= len(frame.hashes) {
			stack = stack[:len(stack)-1]
		}

		if c.NumParents() > 0 {
			newParents := make([]plumbing.Hash, 0, c.NumParents())
			for _, h = range c.ParentHashes {
				if _, ok := visitedHashes[h]; !ok {
					newParents = append(newParents, h)
				}
			}

			if len(newParents) > 0 {
				stack = append(stack, &stackFrame{frame.idx + 1, 0, newParents})
			}
		}
	}
}

// Type implements the Expression interface.
func (HistoryIdx) Type() sql.Type { return sql.Int64 }

// TransformUp implements the Expression interface.
func (f *HistoryIdx) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	left, err := f.Left.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	right, err := f.Right.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	return fn(NewHistoryIdx(left, right))
}
