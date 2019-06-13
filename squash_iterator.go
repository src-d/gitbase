package gitbase

import (
	"fmt"
	"io"

	errors "gopkg.in/src-d/go-errors.v1"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"github.com/src-d/go-mysql-server/sql"

	"github.com/sirupsen/logrus"
)

// ChainableIter is an iterator meant to have a chaining-friendly API.
type ChainableIter interface {
	// New creates a new Chainable Iterator.
	New(*sql.Context, *Repository) (ChainableIter, error)
	// Close closes the iterator.
	Close() error
	// Repository returns the current repository the iterator is working on.
	Repository() *Repository
	// Row returns the current row. All calls to Row return the same row
	// until another call to Advance. Advance should be called before
	// calling Row.
	Row() sql.Row
	// Advance advances the position of the iterator by one. After io.EOF
	// or any other error, this method should not be called.
	Advance() error
	// Schema returns the schema of the rows returned by this iterator.
	Schema() sql.Schema
}

// ReposIter is a chainable iterator that operates with repositories.
type ReposIter interface {
	ChainableIter
	isReposIter()
}

type squashReposIter struct {
	ctx           *sql.Context
	filters       sql.Expression
	done          bool
	repo          *Repository
	row           sql.Row
	skipGitErrors bool
}

// NewAllReposIter returns an iterator that will return all repositories
// that match the given filters.
func NewAllReposIter(filters sql.Expression) ReposIter {
	return &squashReposIter{filters: filters}
}

func (i *squashReposIter) Repo() *Repository { return i.repo }
func (i *squashReposIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}

	return nil
}
func (i *squashReposIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashReposIter{
		ctx:           ctx,
		filters:       i.filters,
		repo:          repo,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashReposIter) Repository() *Repository { return i.repo }
func (i *squashReposIter) Row() sql.Row            { return i.row }
func (i *squashReposIter) Advance() error {
	for {
		select {
		case <-i.ctx.Done():
			return ErrSessionCanceled.New()
		default:
		}

		if i.done {
			return io.EOF
		}

		i.done = true

		i.row = sql.NewRow(i.repo.ID())
		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashReposIter) Schema() sql.Schema { return RepositoriesSchema }

func (squashReposIter) isReposIter() {}

// Remote is the info of a single repository remote.
type Remote struct {
	RepoID string
	Name   string
	URL    string
	Fetch  string
}

// RemotesIter is a chainable iterator that operates with remotes.
type RemotesIter interface {
	ChainableIter
	// Remote returns the current repository. All calls to Remote return the
	// same remote until another call to Advance. Advance should
	// be called before calling Remote.
	Remote() *Remote
}

type squashRemoteIter struct {
	ctx               *sql.Context
	repo              *Repository
	filters           sql.Expression
	remotePos, urlPos int
	remotes           []*git.Remote
	remote            *Remote
	row               sql.Row
	skipGitErrors     bool
}

// NewAllRemotesIter returns an iterator that will return all remotes
// that match the given filters.
func NewAllRemotesIter(filters sql.Expression) RemotesIter {
	return &squashRemoteIter{filters: filters}
}

func (i *squashRemoteIter) Remote() *Remote { return i.remote }
func (i *squashRemoteIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}

	return nil
}
func (i *squashRemoteIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	remotes, err := repo.Remotes()
	if err != nil {
		if !i.skipGitErrors {
			return nil, err
		}
	}

	return &squashRemoteIter{
		ctx:           ctx,
		filters:       i.filters,
		repo:          repo,
		remotes:       remotes,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashRemoteIter) Repository() *Repository { return i.repo }
func (i *squashRemoteIter) Row() sql.Row            { return i.row }
func (i *squashRemoteIter) Advance() error {
	for {
		select {
		case <-i.ctx.Done():
			return ErrSessionCanceled.New()
		default:
		}

		if i.remotePos >= len(i.remotes) {
			return io.EOF
		}

		remote := i.remotes[i.remotePos]
		config := remote.Config()
		if i.urlPos >= len(config.URLs) {
			i.remotePos++
			i.urlPos = 0
			if i.remotePos >= len(i.remotes) {
				return io.EOF
			}

			remote = i.remotes[i.remotePos]
			config = remote.Config()
		}

		i.remote = &Remote{
			RepoID: i.repo.ID(),
			Name:   config.Name,
			URL:    config.URLs[i.urlPos],
			Fetch:  remoteFetchURL(config, i.urlPos),
		}

		i.row = sql.NewRow(
			i.remote.RepoID,
			i.remote.Name,
			i.remote.URL,
			i.remote.URL,
			i.remote.Fetch,
			i.remote.Fetch,
		)

		i.urlPos++

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRemoteIter) Schema() sql.Schema { return RemotesSchema }

type squashRepoRemotesIter struct {
	ctx               *sql.Context
	repos             ReposIter
	filters           sql.Expression
	remotePos, urlPos int
	remotes           []*git.Remote
	remote            *Remote
	row               sql.Row
	skipGitErrors     bool
}

// NewRepoRemotesIter returns an iterator that will return all remotes for the
// given ReposIter repositories that match the given filters.
func NewRepoRemotesIter(squashReposIter ReposIter, filters sql.Expression) RemotesIter {
	return &squashRepoRemotesIter{repos: squashReposIter, filters: filters}
}

func (i *squashRepoRemotesIter) Repository() *Repository { return i.repos.Repository() }
func (i *squashRepoRemotesIter) Remote() *Remote         { return i.remote }
func (i *squashRepoRemotesIter) Close() error {
	if i.repos != nil {
		return i.repos.Close()
	}
	return nil
}
func (i *squashRepoRemotesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.repos.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashRepoRemotesIter{
		ctx:           ctx,
		repos:         iter.(ReposIter),
		filters:       i.filters,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashRepoRemotesIter) Row() sql.Row { return i.row }
func (i *squashRepoRemotesIter) Advance() error {
	for {
		if i.remotes == nil {
			err := i.repos.Advance()
			if err != nil {
				return err
			}

			i.remotes, err = i.repos.Repository().Remotes()
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"iter":  "repoRemoteIter",
					"repo":  i.repos.Repository().ID(),
					"error": err,
				}).Error("unable to retrieve repository remotes")

				if i.skipGitErrors {
					continue
				}

				return err
			}

			i.remotePos = 0
			i.urlPos = 0
		}

		if i.remotePos >= len(i.remotes) {
			i.remotes = nil
			continue
		}

		remote := i.remotes[i.remotePos]
		config := remote.Config()
		if i.urlPos >= len(config.URLs) {
			i.remotePos++
			i.urlPos = 0
			if i.remotePos >= len(i.remotes) {
				i.remotes = nil
				continue
			}

			remote = i.remotes[i.remotePos]
			config = remote.Config()
		}

		i.remote = &Remote{
			RepoID: i.repos.Repository().ID(),
			Name:   config.Name,
			URL:    config.URLs[i.urlPos],
			Fetch:  remoteFetchURL(config, i.urlPos),
		}

		i.urlPos++

		i.row = append(
			i.repos.Row(),
			i.remote.RepoID,
			i.remote.Name,
			i.remote.URL,
			i.remote.URL,
			i.remote.Fetch,
			i.remote.Fetch,
		)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRepoRemotesIter) Schema() sql.Schema {
	return append(i.repos.Schema(), RemotesSchema...)
}

// Ref is a git reference with the repo id.
type Ref struct {
	RepoID string
	*plumbing.Reference
}

// RefsIter is a chainable iterator that operates on references.
type RefsIter interface {
	ChainableIter
	// Ref returns the current repository. All calls to Ref return the
	// same reference until another call to Advance. Advance should
	// be called before calling Ref.
	Ref() *Ref
}

type squashRefIter struct {
	ctx           *sql.Context
	repo          *Repository
	repos         *RepositoryIter
	filters       sql.Expression
	refs          storer.ReferenceIter
	head          *plumbing.Reference
	ref           *Ref
	row           sql.Row
	virtual       bool
	skipGitErrors bool
}

// NewAllRefsIter returns an iterator that will return all references
// that match the given filters. If the iterator is virtual, it will
// always return an empty row and an empty schema. This is useful for
// passing it to other iterators that are chained with references but
// don't need the reference data in their output rows.
func NewAllRefsIter(filters sql.Expression, virtual bool) RefsIter {
	return &squashRefIter{filters: filters, virtual: virtual}
}

func (i *squashRefIter) Repository() *Repository { return i.repo }
func (i *squashRefIter) Ref() *Ref               { return i.ref }
func (i *squashRefIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}
	if i.repo != nil {
		i.repo.Close()
	}
	return i.repos.Close()
}
func (i *squashRefIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	refs, err := repo.References()
	if err != nil && !i.skipGitErrors {
		return nil, err
	}

	head, err := repo.Head()
	if err != nil && !i.skipGitErrors &&
		err != plumbing.ErrReferenceNotFound {
		return nil, err
	}

	return &squashRefIter{
		ctx:           ctx,
		repo:          repo,
		filters:       i.filters,
		virtual:       i.virtual,
		head:          head,
		refs:          refs,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}

func (i *squashRefIter) Row() sql.Row {
	if i.virtual {
		return nil
	}
	return i.row
}

func (i *squashRefIter) Advance() error {
	for {
		select {
		case <-i.ctx.Done():
			return ErrSessionCanceled.New()
		default:
		}

		if i.refs == nil {
			return io.EOF
		}

		var ref *plumbing.Reference
		if i.head != nil {
			ref = plumbing.NewHashReference(
				plumbing.ReferenceName("HEAD"),
				i.head.Hash(),
			)
			i.head = nil
		} else {
			var err error
			ref, err = i.refs.Next()
			if err != nil {
				if err == io.EOF {
					return io.EOF
				}

				if i.skipGitErrors {
					continue
				}
				return err
			}
		}

		if isIgnoredReference(ref) {
			continue
		}

		i.ref = &Ref{i.repo.ID(), ref}
		i.row = referenceToRow(i.repo.ID(), ref)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRefIter) Schema() sql.Schema {
	if i.virtual {
		return nil
	}
	return RefsSchema
}

type squashRefIndexIter struct {
	ctx           *sql.Context
	index         sql.IndexLookup
	iter          sql.RowIter
	filters       sql.Expression
	pool          *RepositoryPool
	ref           *Ref
	repo          *Repository
	row           sql.Row
	skipGitErrors bool
}

// NewIndexRefsIter returns an iterator that will return all references
// that match the given filters in the given index.
func NewIndexRefsIter(filters sql.Expression, index sql.IndexLookup) RefsIter {
	return &squashRefIndexIter{filters: filters, index: index}
}

func (i *squashRefIndexIter) Repository() *Repository { return i.repo }
func (i *squashRefIndexIter) Ref() *Ref               { return i.ref }
func (i *squashRefIndexIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}

	return i.iter.Close()
}
func (i *squashRefIndexIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	values, err := i.index.Values(RepositoryPartition(repo.ID()))
	if err != nil {
		return nil, err
	}

	return &squashRefIndexIter{
		ctx:           ctx,
		filters:       i.filters,
		skipGitErrors: session.SkipGitErrors,
		pool:          session.Pool,
		iter:          &rowIndexIter{new(refRowKeyMapper), values},
	}, nil
}

func (i *squashRefIndexIter) Row() sql.Row {
	return i.row
}

func (i *squashRefIndexIter) Advance() error {
	for {
		select {
		case <-i.ctx.Done():
			return ErrSessionCanceled.New()
		default:
		}

		var err error
		i.row, err = i.iter.Next()
		if err != nil {
			return err
		}

		repoID := i.row[0].(string)
		if i.repo == nil || repoID != i.repo.ID() {
			i.repo, err = i.pool.GetRepo(repoID)
			if err != nil {
				if i.skipGitErrors {
					continue
				}

				return err
			}
		}

		refName := plumbing.ReferenceName(i.row[1].(string))
		ref, err := i.repo.Reference(refName, true)
		if err != nil {
			if i.skipGitErrors {
				continue
			}

			return err
		}

		i.ref = &Ref{repoID, ref}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRefIndexIter) Schema() sql.Schema {
	return RefsSchema
}

type squashRepoRefsIter struct {
	ctx           *sql.Context
	repos         ReposIter
	filters       sql.Expression
	refs          storer.ReferenceIter
	head          *plumbing.Reference
	ref           *Ref
	row           sql.Row
	virtual       bool
	skipGitErrors bool
}

// NewRepoRefsIter returns an iterator that will return all references
// for the repositories of the given repos iterator that match the given
// filters.
func NewRepoRefsIter(
	squashReposIter ReposIter,
	filters sql.Expression,
	virtual bool,
) RefsIter {
	return &squashRepoRefsIter{
		repos:   squashReposIter,
		filters: filters,
		virtual: virtual,
	}
}

func (i *squashRepoRefsIter) Repository() *Repository { return i.repos.Repository() }
func (i *squashRepoRefsIter) Ref() *Ref               { return i.ref }
func (i *squashRepoRefsIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}

	if i.repos != nil {
		return i.repos.Close()
	}

	return nil
}
func (i *squashRepoRefsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	repos, err := i.repos.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashRepoRefsIter{
		ctx:           ctx,
		repos:         repos.(ReposIter),
		filters:       i.filters,
		virtual:       i.virtual,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashRepoRefsIter) Row() sql.Row { return i.row }
func (i *squashRepoRefsIter) Advance() error {
	for {
		if i.refs == nil {
			err := i.repos.Advance()
			if err != nil {
				return err
			}

			i.refs, err = i.repos.Repository().References()
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"error": err,
					"repo":  i.repos.Repository().ID(),
				}).Error("unable to retrieve references")

				if i.skipGitErrors {
					continue
				}

				return err
			}

			i.head, err = i.repos.Repository().Head()
			if err != nil &&
				err != plumbing.ErrReferenceNotFound &&
				!i.skipGitErrors {
				return err
			}
		}

		var ref *plumbing.Reference
		if i.head != nil {
			ref = plumbing.NewHashReference(
				plumbing.ReferenceName("HEAD"),
				i.head.Hash(),
			)
			i.head = nil
		} else {
			var err error
			ref, err = i.refs.Next()
			if err == io.EOF {
				i.refs = nil
				continue
			}

			if err != nil {
				return err
			}
		}

		if isIgnoredReference(ref) {
			continue
		}

		i.ref = &Ref{i.repos.Repository().ID(), ref}
		if i.virtual {
			i.row = i.repos.Row()
		} else {
			i.row = append(i.repos.Row(), referenceToRow(i.ref.RepoID, ref)...)
		}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRepoRefsIter) Schema() sql.Schema {
	if i.virtual {
		return i.repos.Schema()
	}
	return append(i.repos.Schema(), RefsSchema...)
}

type squashRemoteRefsIter struct {
	ctx           *sql.Context
	remotes       RemotesIter
	filters       sql.Expression
	refs          storer.ReferenceIter
	head          *plumbing.Reference
	ref           *Ref
	row           sql.Row
	skipGitErrors bool
}

// NewRemoteRefsIter returns an iterator that will return all references
// for the remotes returned by the given remotes iterator that match the given
// filters.
func NewRemoteRefsIter(
	remotesIter RemotesIter,
	filters sql.Expression,
) RefsIter {
	return &squashRemoteRefsIter{
		remotes: remotesIter,
		filters: filters,
	}
}

func (i *squashRemoteRefsIter) Repository() *Repository { return i.remotes.Repository() }
func (i *squashRemoteRefsIter) Ref() *Ref               { return i.ref }
func (i *squashRemoteRefsIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}

	if i.remotes != nil {
		return i.remotes.Close()
	}

	return nil
}
func (i *squashRemoteRefsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.remotes.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashRemoteRefsIter{
		ctx:           ctx,
		remotes:       iter.(RemotesIter),
		filters:       i.filters,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashRemoteRefsIter) Row() sql.Row { return i.row }
func (i *squashRemoteRefsIter) Advance() error {
	for {
		if i.refs == nil {
			err := i.remotes.Advance()
			if err != nil {
				return err
			}

			i.refs, err = i.Repository().References()
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"error": err,
					"repo":  i.Repository().ID(),
				}).Error("unable to retrieve references")

				if i.skipGitErrors {
					continue
				}

				return err
			}

			i.head, err = i.Repository().Head()
			if err != nil &&
				err != plumbing.ErrReferenceNotFound &&
				!i.skipGitErrors {
				return err
			}
		}

		var ref *plumbing.Reference
		if i.head != nil {
			ref = plumbing.NewHashReference(
				plumbing.ReferenceName("HEAD"),
				i.head.Hash(),
			)
			i.head = nil
		} else {
			var err error
			ref, err = i.refs.Next()
			if err == io.EOF {
				i.refs = nil
				continue
			}

			if err != nil {
				return err
			}
		}

		if isIgnoredReference(ref) {
			continue
		}

		i.ref = &Ref{i.remotes.Remote().RepoID, ref}
		i.row = append(i.remotes.Row(), referenceToRow(i.ref.RepoID, ref)...)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRemoteRefsIter) Schema() sql.Schema {
	return append(i.remotes.Schema(), RefsSchema...)
}

// CommitsIter is a chainable iterator that operates on commits.
type CommitsIter interface {
	ChainableIter
	// Commit returns the current repository. All calls to Commit return the
	// same commit until another call to Advance. Advance should
	// be called before calling Commit.
	Commit() *object.Commit
}

// RefCommitsIter is a chainable iterator that operates on ref_commits.
type RefCommitsIter interface {
	CommitsIter
	isRefCommitsIter()
}

// NewAllRefCommitsIter returns an iterator that will return all ref_commit
// rows.
func NewAllRefCommitsIter(filters sql.Expression) CommitsIter {
	return NewRefRefCommitsIter(NewAllRefsIter(nil, true), filters)
}

type squashRefRefCommitsIter struct {
	ctx           *sql.Context
	refs          RefsIter
	skipGitErrors bool
	filters       sql.Expression
	commits       *indexedCommitIter
	commit        *object.Commit
	row           sql.Row
}

// NewRefRefCommitsIter returns an iterator that will return all ref_commits
// for all the references in the given iterator.
func NewRefRefCommitsIter(refsIter RefsIter, filters sql.Expression) CommitsIter {
	return &squashRefRefCommitsIter{refs: refsIter, filters: filters}
}

func (squashRefRefCommitsIter) isRefCommitsIter()          {}
func (i *squashRefRefCommitsIter) Repository() *Repository { return i.refs.Repository() }
func (i *squashRefRefCommitsIter) Commit() *object.Commit  { return i.commit }
func (i *squashRefRefCommitsIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}

	if i.commits != nil {
		i.commits.Close()
	}
	return nil
}
func (i *squashRefRefCommitsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	refs, err := i.refs.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &squashRefRefCommitsIter{
		ctx:           ctx,
		skipGitErrors: session.SkipGitErrors,
		refs:          refs.(RefsIter),
		filters:       i.filters,
	}, nil
}

func (i *squashRefRefCommitsIter) Row() sql.Row { return i.row }

func (i *squashRefRefCommitsIter) Advance() error {
	for {
		if i.commits == nil {
			err := i.refs.Advance()
			if err != nil {
				return err
			}

			commit, err := resolveCommit(i.Repository(), i.refs.Ref().Hash())
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"repo":  i.Repository().ID(),
					"error": err,
				}).Error("unable to get commit")

				if errInvalidCommit.Is(err) || i.skipGitErrors {
					continue
				}

				return err
			}

			i.commits = newIndexedCommitIter(i.skipGitErrors, i.Repository(), commit)
		}

		commit, idx, err := i.commits.Next()
		if err != nil {
			if err == io.EOF {
				i.commits = nil
				continue
			}

			return err
		}

		i.commit = commit
		i.row = append(
			i.refs.Row(),
			i.Repository().ID(),
			commit.Hash.String(),
			i.refs.Ref().Name().String(),
			int64(idx),
		)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRefRefCommitsIter) Schema() sql.Schema {
	return append(i.refs.Schema(), RefCommitsSchema...)
}

type squashRefHeadRefCommitsIter struct {
	skipGitErrors bool
	ctx           *sql.Context
	filters       sql.Expression
	refs          RefsIter
	row           sql.Row
	commit        *object.Commit
}

// NewRefHeadRefCommitsIter returns an iterator that will return all ref_commit
// rows of the HEAD commits in references of the given iterator.
func NewRefHeadRefCommitsIter(refs RefsIter, filters sql.Expression) CommitsIter {
	return &squashRefHeadRefCommitsIter{refs: refs, filters: filters}
}

func (squashRefHeadRefCommitsIter) isRefCommitsIter()          {}
func (i *squashRefHeadRefCommitsIter) Repository() *Repository { return i.refs.Repository() }
func (i *squashRefHeadRefCommitsIter) Commit() *object.Commit  { return i.commit }
func (i *squashRefHeadRefCommitsIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}
	return nil
}
func (i *squashRefHeadRefCommitsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	refs, err := i.refs.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &squashRefHeadRefCommitsIter{
		ctx:           ctx,
		skipGitErrors: session.SkipGitErrors,
		refs:          refs.(RefsIter),
		filters:       i.filters,
	}, nil
}

func (i *squashRefHeadRefCommitsIter) Row() sql.Row { return i.row }

func (i *squashRefHeadRefCommitsIter) Advance() error {
	for {
		err := i.refs.Advance()
		if err != nil {
			return err
		}

		i.commit, err = resolveCommit(i.Repository(), i.refs.Ref().Hash())
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"repo":  i.Repository().ID(),
				"error": err,
			}).Error("unable to get commit")

			if errInvalidCommit.Is(err) || i.skipGitErrors {
				continue
			}

			return err
		}

		i.row = append(
			i.refs.Row(),
			i.Repository().ID(),
			i.commit.Hash.String(),
			i.refs.Ref().Name().String(),
			int64(0),
		)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRefHeadRefCommitsIter) Schema() sql.Schema {
	return append(i.refs.Schema(), RefCommitsSchema...)
}

type squashRefCommitsIndexIter struct {
	ctx           *sql.Context
	pool          *RepositoryPool
	repo          *Repository
	commit        *object.Commit
	row           sql.Row
	index         sql.IndexLookup
	iter          sql.RowIter
	filters       sql.Expression
	skipGitErrors bool
}

// NewIndexRefCommitsIter returns an iterator that will return all results in
// the given index.
func NewIndexRefCommitsIter(index sql.IndexLookup, filters sql.Expression) RefCommitsIter {
	return &squashRefCommitsIndexIter{
		index:   index,
		filters: filters,
	}
}

func (i *squashRefCommitsIndexIter) Repository() *Repository { return i.repo }
func (i *squashRefCommitsIndexIter) Commit() *object.Commit  { return i.commit }
func (i *squashRefCommitsIndexIter) isRefCommitsIter()       {}
func (i *squashRefCommitsIndexIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	values, err := i.index.Values(RepositoryPartition(repo.ID()))
	if err != nil {
		return nil, err
	}

	return &squashRefCommitsIndexIter{
		ctx:           ctx,
		index:         i.index,
		iter:          &rowIndexIter{new(refCommitsRowKeyMapper), values},
		filters:       i.filters,
		pool:          session.Pool,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashRefCommitsIndexIter) Advance() error {
	for {
		var err error
		i.row, err = i.iter.Next()
		if err != nil {
			return err
		}

		repoID := i.row[0]
		if i.repo == nil || repoID != i.repo.ID() {
			i.repo.Close()

			i.repo, err = i.pool.GetRepo(i.row[0].(string))
			if err != nil {
				if i.skipGitErrors {
					continue
				}

				return err
			}
		}

		commitHash := plumbing.NewHash(i.row[1].(string))
		i.commit, err = i.repo.CommitObject(commitHash)
		if err != nil {
			if i.skipGitErrors {
				continue
			}

			return err
		}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRefCommitsIndexIter) Row() sql.Row { return i.row }
func (i *squashRefCommitsIndexIter) Schema() sql.Schema {
	return RefCommitsSchema
}
func (i *squashRefCommitsIndexIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}
	return i.iter.Close()
}

type squashRefCommitCommitsIter struct {
	refCommits CommitsIter
	row        sql.Row
	filters    sql.Expression
	ctx        *sql.Context
}

// NewRefCommitCommitsIter returns an iterator that will return commits
// based on the ref_commits returned by the previous iterator.
func NewRefCommitCommitsIter(refCommits CommitsIter, filters sql.Expression) CommitsIter {
	return &squashRefCommitCommitsIter{refCommits: refCommits, filters: filters}
}

func (i *squashRefCommitCommitsIter) Repository() *Repository { return i.refCommits.Repository() }
func (i *squashRefCommitCommitsIter) Commit() *object.Commit  { return i.refCommits.Commit() }
func (i *squashRefCommitCommitsIter) Close() error {
	if i.refCommits != nil {
		i.refCommits.Close()
	}

	return nil
}
func (i *squashRefCommitCommitsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.refCommits.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &squashRefCommitCommitsIter{
		ctx:        ctx,
		refCommits: iter.(CommitsIter),
		filters:    i.filters,
	}, nil
}

func (i *squashRefCommitCommitsIter) Row() sql.Row { return i.row }

func (i *squashRefCommitCommitsIter) Advance() error {
	for {
		if err := i.refCommits.Advance(); err != nil {
			return err
		}

		commit := i.refCommits.Commit()
		i.row = append(
			i.refCommits.Row(),
			commitToRow(i.Repository().ID(), commit)...,
		)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRefCommitCommitsIter) Schema() sql.Schema {
	return append(i.refCommits.Schema(), CommitsSchema...)
}

type squashCommitsIter struct {
	ctx           *sql.Context
	repo          *Repository
	filters       sql.Expression
	commits       object.CommitIter
	commit        *object.Commit
	row           sql.Row
	virtual       bool
	skipGitErrors bool
}

// NewAllCommitsIter returns an iterator that will return all commits
// that match the given filters.
func NewAllCommitsIter(filters sql.Expression, virtual bool) CommitsIter {
	return &squashCommitsIter{filters: filters, virtual: virtual}
}

func (i *squashCommitsIter) Repository() *Repository { return i.repo }
func (i *squashCommitsIter) Commit() *object.Commit  { return i.commit }
func (i *squashCommitsIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}
	if i.repo != nil {
		i.repo.Close()
	}

	return nil
}
func (i *squashCommitsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	commits, err := repo.
		Log(&git.LogOptions{
			All: true,
		})
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"repo":  repo.ID(),
			"error": err,
		}).Error("unable to get commit iterator")

		if !session.SkipGitErrors {
			return nil, err
		}
	}

	return &squashCommitsIter{
		ctx:           ctx,
		repo:          repo,
		commits:       commits,
		filters:       i.filters,
		virtual:       i.virtual,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}

func (i *squashCommitsIter) Row() sql.Row {
	if i.virtual {
		return nil
	}
	return i.row
}

func (i *squashCommitsIter) Advance() error {
	for {
		select {
		case <-i.ctx.Done():
			return ErrSessionCanceled.New()
		default:
		}

		if i.commits == nil {
			return io.EOF
		}

		var err error
		i.commit, err = i.commits.Next()
		if err != nil {
			return err
		}

		i.row = commitToRow(i.repo.ID(), i.commit)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}

func (i *squashCommitsIter) Schema() sql.Schema {
	if i.virtual {
		return nil
	}

	return CommitsSchema
}

type squashCommitsIndexIter struct {
	ctx           *sql.Context
	pool          *RepositoryPool
	repo          *Repository
	row           sql.Row
	index         sql.IndexLookup
	iter          *commitsIndexIter
	filters       sql.Expression
	skipGitErrors bool
}

// NewIndexCommitsIter returns an iterator that will return all results in
// the given index.
func NewIndexCommitsIter(index sql.IndexLookup, filters sql.Expression) CommitsIter {
	return &squashCommitsIndexIter{
		index:   index,
		filters: filters,
	}
}

func (i *squashCommitsIndexIter) Repository() *Repository { return i.repo }
func (i *squashCommitsIndexIter) Commit() *object.Commit  { return i.iter.commit }
func (i *squashCommitsIndexIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	values, err := i.index.Values(RepositoryPartition(repo.ID()))
	if err != nil {
		return nil, err
	}

	return &squashCommitsIndexIter{
		ctx:           ctx,
		index:         i.index,
		iter:          newCommitsIndexIter(values, session.Pool, nil),
		filters:       i.filters,
		pool:          session.Pool,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashCommitsIndexIter) Advance() error {
	for {
		var err error
		i.row, err = i.iter.Next()
		if err != nil {
			if i.skipGitErrors {
				logrus.WithField("err", err).
					Error("unable to get next commit")
				continue
			}
			return err
		}

		if i.repo == nil || i.repo.ID() != i.iter.repoID {
			i.repo.Close()

			i.repo, err = i.pool.GetRepo(i.iter.repoID)
			if err != nil {
				if i.skipGitErrors {
					logrus.WithFields(logrus.Fields{
						"err":  err,
						"repo": i.iter.repoID,
					}).Error("unable to get repo")
					continue
				}
				return err
			}
		}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashCommitsIndexIter) Row() sql.Row { return i.row }
func (i *squashCommitsIndexIter) Schema() sql.Schema {
	return CommitsSchema
}
func (i *squashCommitsIndexIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}

	return i.iter.Close()
}

type squashRepoCommitsIter struct {
	repos         ReposIter
	commits       object.CommitIter
	ctx           *sql.Context
	filters       sql.Expression
	commit        *object.Commit
	row           sql.Row
	skipGitErrors bool
}

// NewRepoCommitsIter is an iterator that returns all commits for the
// repositories returned by the given iterator.
func NewRepoCommitsIter(repos ReposIter, filters sql.Expression) CommitsIter {
	return &squashRepoCommitsIter{repos: repos, filters: filters}
}

func (i *squashRepoCommitsIter) Repository() *Repository { return i.repos.Repository() }
func (i *squashRepoCommitsIter) Commit() *object.Commit  { return i.commit }
func (i *squashRepoCommitsIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

	if i.repos != nil {
		return i.repos.Close()
	}

	return nil
}
func (i *squashRepoCommitsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.repos.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashRepoCommitsIter{
		repos:         iter.(ReposIter),
		ctx:           ctx,
		filters:       i.filters,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashRepoCommitsIter) Row() sql.Row { return i.row }
func (i *squashRepoCommitsIter) Advance() error {
	for {
		if i.commits == nil {
			i.Repository().Close()

			err := i.repos.Advance()
			if err != nil {
				return err
			}

			i.commits, err = i.repos.Repository().
				Log(&git.LogOptions{
					All: true,
				})
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"repo":  i.repos.Repository().ID(),
					"error": err,
				}).Error("unable to get commit iterator")

				if !i.skipGitErrors {
					return err
				}

				continue
			}
		}

		var err error
		i.commit, err = i.commits.Next()
		if err == io.EOF {
			i.commits = nil
			continue
		}

		if err != nil {
			return err
		}

		i.row = append(
			i.repos.Row(),
			commitToRow(i.repos.Repository().ID(), i.commit)...,
		)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRepoCommitsIter) Schema() sql.Schema {
	return append(RepositoriesSchema, CommitsSchema...)
}

type squashRefHeadCommitsIter struct {
	ctx           *sql.Context
	filters       sql.Expression
	refs          RefsIter
	commit        *object.Commit
	row           sql.Row
	virtual       bool
	skipGitErrors bool
}

// NewRefHEADCommitsIter returns an iterator that will return the commit
// for the given iter reference heads that match the given filters.
func NewRefHEADCommitsIter(
	refsIter RefsIter,
	filters sql.Expression,
	virtual bool,
) CommitsIter {
	return &squashRefHeadCommitsIter{refs: refsIter, filters: filters, virtual: virtual}
}

func (i *squashRefHeadCommitsIter) Repository() *Repository { return i.refs.Repository() }
func (i *squashRefHeadCommitsIter) Commit() *object.Commit  { return i.commit }
func (i *squashRefHeadCommitsIter) Close() error {
	if i.refs != nil {
		return i.refs.Close()
	}

	return nil
}
func (i *squashRefHeadCommitsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.refs.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashRefHeadCommitsIter{
		ctx:           ctx,
		refs:          iter.(RefsIter),
		filters:       i.filters,
		virtual:       i.virtual,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashRefHeadCommitsIter) Row() sql.Row { return i.row }
func (i *squashRefHeadCommitsIter) Advance() error {
	for {
		err := i.refs.Advance()
		if err != nil {
			return err
		}

		i.commit, err = resolveCommit(i.Repository(), i.refs.Ref().Hash())
		if err != nil {
			if errInvalidCommit.Is(err) {
				logrus.WithFields(logrus.Fields{
					"ref":  i.refs.Ref().Name(),
					"hash": i.refs.Ref().Hash(),
				}).Debug("skipping reference, it's not pointing to a commit")
				continue
			}

			logrus.WithFields(logrus.Fields{
				"ref":   i.refs.Ref().Name(),
				"hash":  i.refs.Ref().Hash(),
				"error": err,
			}).Error("unable to resolve commit")

			if i.skipGitErrors {
				continue
			}

			return err
		}

		if i.virtual {
			i.row = i.refs.Row()
		} else {
			i.row = append(
				i.refs.Row(),
				commitToRow(i.Repository().ID(), i.commit)...,
			)
		}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRefHeadCommitsIter) Schema() sql.Schema {
	if i.virtual {
		return i.refs.Schema()
	}
	return append(i.refs.Schema(), CommitsSchema...)
}

// TreesIter is a chainable iterator that operates on trees.
type TreesIter interface {
	ChainableIter
	// Tree returns the current tree. All calls to Tree return the same tree
	// until another call to Advance. Advance should be called before calling
	// Tree.
	Tree() *object.Tree
}

// NewAllCommitTreesIter returns all commit trees.
func NewAllCommitTreesIter(filters sql.Expression) TreesIter {
	return NewCommitTreesIter(NewAllCommitsIter(nil, true), filters, false)
}

type squashCommitTreesIndexIter struct {
	ctx           *sql.Context
	pool          *RepositoryPool
	repo          *Repository
	tree          *object.Tree
	row           sql.Row
	index         sql.IndexLookup
	iter          sql.RowIter
	filters       sql.Expression
	skipGitErrors bool
}

// NewIndexCommitTreesIter returns an iterator that will return all results in
// the given index.
func NewIndexCommitTreesIter(index sql.IndexLookup, filters sql.Expression) TreesIter {
	return &squashCommitTreesIndexIter{
		index:   index,
		filters: filters,
	}
}

func (i *squashCommitTreesIndexIter) Repository() *Repository { return i.repo }
func (i *squashCommitTreesIndexIter) Tree() *object.Tree      { return i.tree }
func (i *squashCommitTreesIndexIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	values, err := i.index.Values(RepositoryPartition(repo.ID()))
	if err != nil {
		return nil, err
	}

	return &squashCommitTreesIndexIter{
		ctx:           ctx,
		index:         i.index,
		iter:          &rowIndexIter{new(commitTreesRowKeyMapper), values},
		filters:       i.filters,
		pool:          session.Pool,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashCommitTreesIndexIter) Advance() error {
	for {
		var err error
		i.row, err = i.iter.Next()
		if err != nil {
			return err
		}

		repoID := i.row[0]
		if i.repo == nil || repoID != i.repo.ID() {
			i.repo, err = i.pool.GetRepo(i.row[0].(string))
			if err != nil {
				if i.skipGitErrors {
					continue
				}

				return err
			}
		}

		treeHash := plumbing.NewHash(i.row[2].(string))
		i.tree, err = i.repo.TreeObject(treeHash)
		if err != nil {
			if i.skipGitErrors {
				continue
			}

			return err
		}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashCommitTreesIndexIter) Row() sql.Row { return i.row }
func (i *squashCommitTreesIndexIter) Schema() sql.Schema {
	return CommitTreesSchema
}
func (i *squashCommitTreesIndexIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}

	return i.iter.Close()
}

type squashCommitTreesIter struct {
	ctx           *sql.Context
	commits       CommitsIter
	trees         *commitTreeIter
	filters       sql.Expression
	tree          *object.Tree
	row           sql.Row
	skipGitErrors bool
	virtual       bool
}

// NewCommitTreesIter returns all trees from the commits returned by the given
// commits iterator.
func NewCommitTreesIter(
	commits CommitsIter,
	filters sql.Expression,
	virtual bool,
) TreesIter {
	return &squashCommitTreesIter{
		commits: commits,
		filters: filters,
		virtual: virtual,
	}
}

func (i *squashCommitTreesIter) Repository() *Repository { return i.commits.Repository() }
func (i *squashCommitTreesIter) Tree() *object.Tree      { return i.tree }
func (i *squashCommitTreesIter) Close() error {
	if i.trees != nil {
		i.trees.Close()
	}
	if i.commits != nil {
		return i.commits.Close()
	}

	return nil
}
func (i *squashCommitTreesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	commits, err := i.commits.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashCommitTreesIter{
		ctx:           ctx,
		commits:       commits.(CommitsIter),
		filters:       i.filters,
		skipGitErrors: session.SkipGitErrors,
		virtual:       i.virtual,
	}, nil
}
func (i *squashCommitTreesIter) Row() sql.Row { return i.row }
func (i *squashCommitTreesIter) Advance() error {
	for {
		if i.trees == nil {
			err := i.commits.Advance()
			if err != nil {
				return err
			}

			commit := i.commits.Commit()
			i.trees, err = newCommitTreeIter(
				i.Repository(),
				commit,
				make(map[plumbing.Hash]struct{}),
				i.skipGitErrors,
			)
			if err != nil {
				if i.skipGitErrors {
					continue
				}

				logrus.WithFields(logrus.Fields{
					"commit": commit.Hash,
				}).Debug("skipping commit, can't get trees")

				return err
			}
		}

		var err error
		i.tree, err = i.trees.Next()
		if err != nil {
			if err == io.EOF {
				i.trees = nil
				continue
			}

			return err
		}

		if i.virtual {
			i.row = i.commits.Row()
		} else {
			i.row = append(
				i.commits.Row(),
				i.Repository().ID(),
				i.commits.Commit().Hash.String(),
				i.tree.Hash.String(),
			)
		}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}

type squashRepoTreeEntriesIter struct {
	ctx           *sql.Context
	filters       sql.Expression
	repos         ReposIter
	trees         *object.TreeIter
	tree          *object.Tree
	cursor        int
	entry         *TreeEntry
	row           sql.Row
	skipGitErrors bool
}

// NewRepoTreeEntriesIter returns an iterator that will return all tree entries
// for every repo returned by the given iterator.
func NewRepoTreeEntriesIter(repos ReposIter, filters sql.Expression) TreeEntriesIter {
	return &squashRepoTreeEntriesIter{repos: repos, filters: filters}
}

func (i *squashRepoTreeEntriesIter) Repository() *Repository { return i.repos.Repository() }
func (i *squashRepoTreeEntriesIter) TreeEntry() *TreeEntry   { return i.entry }
func (i *squashRepoTreeEntriesIter) Close() error {
	if i.trees != nil {
		i.trees.Close()
	}

	if i.repos != nil {
		return i.repos.Close()
	}

	return nil
}
func (i *squashRepoTreeEntriesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.repos.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashRepoTreeEntriesIter{
		ctx:           ctx,
		repos:         iter.(ReposIter),
		filters:       i.filters,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashRepoTreeEntriesIter) Row() sql.Row { return i.row }
func (i *squashRepoTreeEntriesIter) Advance() error {
	for {
		if i.trees == nil {
			i.Repository().Close()

			err := i.repos.Advance()
			if err != nil {
				return err
			}

			i.trees, err = i.Repository().TreeObjects()
			if err != nil {
				if i.skipGitErrors {
					continue
				}

				return err
			}
		}

		if i.tree == nil {
			var err error
			i.tree, err = i.trees.Next()
			if err != nil {
				if err == io.EOF {
					i.trees = nil
					continue
				}

				if i.skipGitErrors {
					continue
				}

				return err
			}
			i.cursor = 0
		}

		if i.cursor >= len(i.tree.Entries) {
			i.tree = nil
			continue
		}

		i.entry = &TreeEntry{i.tree.Hash, i.tree.Entries[i.cursor]}
		i.cursor++
		i.row = append(
			i.repos.Row(),
			treeEntryToRow(i.Repository().ID(), i.entry)...,
		)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRepoTreeEntriesIter) Schema() sql.Schema {
	return append(RepositoriesSchema, TreeEntriesSchema...)
}

func (i *squashCommitTreesIter) Schema() sql.Schema {
	if i.virtual {
		return i.commits.Schema()
	}
	return append(i.commits.Schema(), CommitTreesSchema...)
}

type squashCommitMainTreeIter struct {
	ctx           *sql.Context
	commits       CommitsIter
	filters       sql.Expression
	tree          *object.Tree
	row           sql.Row
	seen          map[plumbing.Hash]struct{}
	skipGitErrors bool
	virtual       bool
}

// NewCommitMainTreeIter returns all main trees from the commits returned by the given
// commits iterator.
func NewCommitMainTreeIter(
	commits CommitsIter,
	filters sql.Expression,
	virtual bool,
) TreesIter {
	return &squashCommitMainTreeIter{
		commits: commits,
		filters: filters,
		virtual: virtual,
	}
}

func (i *squashCommitMainTreeIter) Repository() *Repository { return i.commits.Repository() }
func (i *squashCommitMainTreeIter) Tree() *object.Tree      { return i.tree }
func (i *squashCommitMainTreeIter) Close() error {
	if i.commits != nil {
		return i.commits.Close()
	}

	return nil
}
func (i *squashCommitMainTreeIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	commits, err := i.commits.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashCommitMainTreeIter{
		ctx:           ctx,
		commits:       commits.(CommitsIter),
		filters:       i.filters,
		seen:          make(map[plumbing.Hash]struct{}),
		skipGitErrors: session.SkipGitErrors,
		virtual:       i.virtual,
	}, nil
}
func (i *squashCommitMainTreeIter) Row() sql.Row { return i.row }
func (i *squashCommitMainTreeIter) Advance() error {
	for {
		err := i.commits.Advance()
		if err != nil {
			return err
		}

		i.tree, err = i.commits.Commit().Tree()
		if err != nil {
			if i.skipGitErrors {
				continue
			}
			return err
		}

		if i.virtual {
			i.row = i.commits.Row()
		} else {
			i.row = append(
				i.commits.Row(),
				i.Repository().ID(),
				i.commits.Commit().Hash.String(),
				i.tree.Hash.String(),
			)
		}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashCommitMainTreeIter) Schema() sql.Schema {
	if i.virtual {
		return i.commits.Schema()
	}
	return append(i.commits.Schema(), CommitTreesSchema...)
}

type commitTreeIter struct {
	skipGitErrors bool
	tree          *object.Tree
	repo          *Repository
	stack         []*commitTreeStackFrame
	seen          map[plumbing.Hash]struct{}
}

type commitTreeStackFrame struct {
	pos     int
	entries []object.TreeEntry
}

func newCommitTreeIter(
	repo *Repository,
	commit *object.Commit,
	seen map[plumbing.Hash]struct{},
	skipGitErrors bool,
) (*commitTreeIter, error) {
	tree, err := commit.Tree()
	if err != nil {
		return nil, err
	}

	if _, ok := seen[tree.Hash]; ok {
		return new(commitTreeIter), nil
	}

	return &commitTreeIter{
		tree:          tree,
		repo:          repo,
		stack:         []*commitTreeStackFrame{{entries: tree.Entries}},
		seen:          seen,
		skipGitErrors: skipGitErrors,
	}, nil
}

func (i *commitTreeIter) Next() (*object.Tree, error) {
	for {
		if i.tree != nil {
			tree := i.tree
			i.tree = nil
			return tree, nil
		}

		if len(i.stack) == 0 {
			return nil, io.EOF
		}

		frame := i.stack[0]

		for {
			if frame.pos >= len(frame.entries) {
				i.stack = i.stack[1:]
				break
			}

			entry := frame.entries[frame.pos]
			frame.pos++
			if entry.Mode != filemode.Dir {
				continue
			}

			tree, err := i.repo.TreeObject(entry.Hash)
			if err != nil {
				if i.skipGitErrors {
					logrus.WithFields(logrus.Fields{
						"tree": entry.Hash,
					}).Debug("skipping tree entry, can't get tree")
					continue
				}

				return nil, err
			}

			if _, ok := i.seen[tree.Hash]; ok {
				continue
			}

			if len(tree.Entries) > 0 {
				i.stack = append(i.stack, &commitTreeStackFrame{entries: tree.Entries})
			}

			return tree, nil
		}
	}
}

func (i *commitTreeIter) Close() {
	if i.repo != nil {
		i.repo.Close()
	}
}

// TreeEntriesIter is a chainable iterator that operates on Tree Entries.
type TreeEntriesIter interface {
	ChainableIter
	// TreeEntry returns the current tree entry. All calls to TreeEntry return the
	// same tree entries until another call to Advance. Advance should
	// be called before calling TreeEntry.
	TreeEntry() *TreeEntry
}

type squashTreeEntriesIter struct {
	ctx           *sql.Context
	repo          *Repository
	filters       sql.Expression
	trees         *object.TreeIter
	tree          *object.Tree
	cursor        int
	entry         *TreeEntry
	row           sql.Row
	skipGitErrors bool
}

// NewAllTreeEntriesIter returns an iterator that will return all tree entries
// that match the given filters.
func NewAllTreeEntriesIter(filters sql.Expression) TreeEntriesIter {
	return &squashTreeEntriesIter{filters: filters}
}

func (i *squashTreeEntriesIter) Repository() *Repository { return i.repo }
func (i *squashTreeEntriesIter) TreeEntry() *TreeEntry   { return i.entry }
func (i *squashTreeEntriesIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}

	if i.trees != nil {
		i.trees.Close()
	}

	return nil
}
func (i *squashTreeEntriesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	trees, err := repo.TreeObjects()
	if err != nil && !session.SkipGitErrors {
		return nil, err
	}

	return &squashTreeEntriesIter{
		ctx:           ctx,
		repo:          repo,
		filters:       i.filters,
		trees:         trees,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashTreeEntriesIter) Row() sql.Row { return i.row }
func (i *squashTreeEntriesIter) Advance() error {
	for {
		select {
		case <-i.ctx.Done():
			return ErrSessionCanceled.New()
		default:
		}

		if i.trees == nil {
			return io.EOF
		}

		if i.tree == nil {
			var err error
			i.tree, err = i.trees.Next()
			if err != nil {
				if err == io.EOF {
					return io.EOF
				}

				if i.skipGitErrors {
					continue
				}

				return err
			}
			i.cursor = 0
		}

		if i.cursor >= len(i.tree.Entries) {
			i.tree = nil
			continue
		}

		i.entry = &TreeEntry{i.tree.Hash, i.tree.Entries[i.cursor]}
		i.cursor++
		i.row = treeEntryToRow(i.repo.ID(), i.entry)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashTreeEntriesIter) Schema() sql.Schema { return TreeEntriesSchema }

type squashTreeEntriesIndexIter struct {
	ctx           *sql.Context
	pool          *RepositoryPool
	repo          *Repository
	row           sql.Row
	index         sql.IndexLookup
	iter          *treeEntriesIndexIter
	filters       sql.Expression
	skipGitErrors bool
}

// NewIndexTreeEntriesIter returns an iterator that will return all results in
// the given index.
func NewIndexTreeEntriesIter(index sql.IndexLookup, filters sql.Expression) TreeEntriesIter {
	return &squashTreeEntriesIndexIter{
		index:   index,
		filters: filters,
	}
}

func (i *squashTreeEntriesIndexIter) Repository() *Repository { return i.repo }
func (i *squashTreeEntriesIndexIter) TreeEntry() *TreeEntry   { return i.iter.entry }
func (i *squashTreeEntriesIndexIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	values, err := i.index.Values(RepositoryPartition(repo.ID()))
	if err != nil {
		return nil, err
	}

	return &squashTreeEntriesIndexIter{
		ctx:           ctx,
		index:         i.index,
		iter:          newTreeEntriesIndexIter(values, session.Pool, nil),
		filters:       i.filters,
		pool:          session.Pool,
		repo:          repo,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashTreeEntriesIndexIter) Advance() error {
	for {
		var err error
		i.row, err = i.iter.Next()
		if err != nil {
			if i.skipGitErrors && err != io.EOF {
				logrus.WithField("err", err).
					Error("unable to get next commit")
				continue
			}
			return err
		}

		if i.repo == nil || i.repo.ID() != i.iter.repoID {
			i.repo, err = i.pool.GetRepo(i.iter.repoID)
			if err != nil {
				if i.skipGitErrors {
					logrus.WithFields(logrus.Fields{
						"err":  err,
						"repo": i.iter.repoID,
					}).Error("unable to get repo")
					continue
				}
				return err
			}
		}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashTreeEntriesIndexIter) Row() sql.Row { return i.row }
func (i *squashTreeEntriesIndexIter) Schema() sql.Schema {
	return TreeEntriesSchema
}
func (i *squashTreeEntriesIndexIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}
	return i.iter.Close()
}

type squashTreeTreeEntriesIter struct {
	ctx     *sql.Context
	trees   TreesIter
	filters sql.Expression
	cursor  int
	tree    *object.Tree
	entry   *TreeEntry
	row     sql.Row
	virtual bool
}

// NewTreeTreeEntriesIter returns an iterator that will return all tree
// entries for the trees returned by the given iterator.
func NewTreeTreeEntriesIter(
	trees TreesIter,
	filters sql.Expression,
	virtual bool,
) TreeEntriesIter {
	return &squashTreeTreeEntriesIter{
		trees:   trees,
		virtual: virtual,
		filters: filters,
	}
}

func (i *squashTreeTreeEntriesIter) Repository() *Repository { return i.trees.Repository() }
func (i *squashTreeTreeEntriesIter) TreeEntry() *TreeEntry   { return i.entry }
func (i *squashTreeTreeEntriesIter) Close() error {
	if i.trees != nil {
		return i.trees.Close()
	}

	return nil
}
func (i *squashTreeTreeEntriesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.trees.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &squashTreeTreeEntriesIter{
		ctx:     ctx,
		trees:   iter.(TreesIter),
		filters: i.filters,
		virtual: i.virtual,
	}, nil
}
func (i *squashTreeTreeEntriesIter) Row() sql.Row { return i.row }
func (i *squashTreeTreeEntriesIter) Advance() error {
	for {
		if i.tree == nil {
			err := i.trees.Advance()
			if err != nil {
				return err
			}

			i.tree = i.trees.Tree()
			i.cursor = 0
		}

		if i.cursor >= len(i.tree.Entries) {
			i.tree = nil
			continue
		}

		i.entry = &TreeEntry{i.tree.Hash, i.tree.Entries[i.cursor]}
		i.cursor++

		if i.virtual {
			i.row = i.trees.Row()
		} else {
			i.row = append(
				i.trees.Row(),
				treeEntryToRow(i.Repository().ID(), i.entry)...,
			)
		}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}

func (i *squashTreeTreeEntriesIter) Schema() sql.Schema {
	if i.virtual {
		return i.trees.Schema()
	}
	return append(i.trees.Schema(), TreeEntriesSchema...)
}

// BlobsIter is a chainable iterator that operates on blobs.
type BlobsIter interface {
	ChainableIter
	Blob() *object.Blob
}

type squashCommitBlobsIndexIter struct {
	ctx           *sql.Context
	pool          *RepositoryPool
	repo          *Repository
	blob          *object.Blob
	row           sql.Row
	index         sql.IndexLookup
	iter          sql.RowIter
	filters       sql.Expression
	skipGitErrors bool
}

// NewIndexCommitBlobsIter returns an iterator that will return all results in
// the given index.
func NewIndexCommitBlobsIter(index sql.IndexLookup, filters sql.Expression) BlobsIter {
	return &squashCommitBlobsIndexIter{
		index:   index,
		filters: filters,
	}
}

func (i *squashCommitBlobsIndexIter) Repository() *Repository { return i.repo }
func (i *squashCommitBlobsIndexIter) Blob() *object.Blob      { return i.blob }
func (i *squashCommitBlobsIndexIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	values, err := i.index.Values(RepositoryPartition(repo.ID()))
	if err != nil {
		return nil, err
	}

	return &squashCommitBlobsIndexIter{
		ctx:           ctx,
		index:         i.index,
		iter:          &rowIndexIter{new(commitBlobsRowKeyMapper), values},
		filters:       i.filters,
		pool:          session.Pool,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashCommitBlobsIndexIter) Advance() error {
	for {
		var err error
		i.row, err = i.iter.Next()
		if err != nil {
			return err
		}

		repoID := i.row[0]
		if i.repo == nil || repoID != i.repo.ID() {
			i.repo, err = i.pool.GetRepo(i.row[0].(string))
			if err != nil {
				if i.skipGitErrors {
					continue
				}

				return err
			}
		}

		blobHash := plumbing.NewHash(i.row[2].(string))
		i.blob, err = i.repo.BlobObject(blobHash)
		if err != nil {
			if i.skipGitErrors {
				continue
			}

			return err
		}

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashCommitBlobsIndexIter) Row() sql.Row { return i.row }
func (i *squashCommitBlobsIndexIter) Schema() sql.Schema {
	return CommitBlobsSchema
}
func (i *squashCommitBlobsIndexIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}
	return i.iter.Close()
}

type squashCommitBlobsIter struct {
	ctx           *sql.Context
	filters       sql.Expression
	commits       CommitsIter
	files         *object.FileIter
	file          *object.File
	tree          *object.Tree
	row           sql.Row
	skipGitErrors bool
	seen          map[plumbing.Hash]struct{}
}

// NewAllCommitBlobsIter returns all commit_blobs.
func NewAllCommitBlobsIter(filters sql.Expression) BlobsIter {
	return NewCommitBlobsIter(NewAllCommitsIter(nil, true), filters)
}

// NewCommitBlobsIter returns an iterator that will return all commit blobs
// of each commit in the given iterator.
func NewCommitBlobsIter(
	commits CommitsIter,
	filters sql.Expression,
) BlobsIter {
	return &squashCommitBlobsIter{
		commits: commits,
		filters: filters,
	}
}

func (i *squashCommitBlobsIter) Close() error {
	if i.files != nil {
		i.files.Close()
	}

	if i.commits != nil {
		return i.commits.Close()
	}

	return nil
}

func (i *squashCommitBlobsIter) Repository() *Repository { return i.commits.Repository() }

func (i *squashCommitBlobsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.commits.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashCommitBlobsIter{
		ctx:           ctx,
		commits:       iter.(CommitsIter),
		filters:       i.filters,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}

func (i *squashCommitBlobsIter) Row() sql.Row       { return i.row }
func (i *squashCommitBlobsIter) Blob() *object.Blob { return &i.file.Blob }

func (i *squashCommitBlobsIter) Advance() error {
	for {
		if i.files == nil {
			err := i.commits.Advance()
			if err != nil {
				return err
			}

			i.tree, err = i.Repository().TreeObject(i.commits.Commit().TreeHash)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"repo":      i.Repository().ID(),
					"tree_hash": i.commits.Commit().TreeHash.String(),
					"error":     err,
				}).Error("unable to retrieve tree object")

				if i.skipGitErrors {
					continue
				}

				return err
			}

			i.files = i.tree.Files()
			// uniqueness of blob is per commit, so we need to reset the seen map
			i.seen = make(map[plumbing.Hash]struct{})
		}

		var err error
		i.file, err = i.files.Next()
		if err == io.EOF {
			i.files = nil
			continue
		}

		if _, ok := i.seen[i.file.Hash]; ok {
			continue
		}

		i.seen[i.file.Hash] = struct{}{}

		i.row = append(
			i.commits.Row(),
			i.Repository().ID(),
			i.commits.Commit().Hash.String(),
			i.file.Blob.Hash.String(),
		)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}

func (i *squashCommitBlobsIter) Schema() sql.Schema {
	return append(i.commits.Schema(), CommitBlobsSchema...)
}

type squashRepoBlobsIter struct {
	ctx         *sql.Context
	repos       ReposIter
	blob        *object.Blob
	filters     sql.Expression
	blobs       *object.BlobIter
	row         sql.Row
	readContent bool
}

// NewRepoBlobsIter returns an iterator that will return all blobs for the
// repositories in the given iter that match the given filters.
func NewRepoBlobsIter(
	repos ReposIter,
	filters sql.Expression,
	readContent bool,
) BlobsIter {
	return &squashRepoBlobsIter{
		repos:       repos,
		filters:     filters,
		readContent: readContent,
	}
}

func (i *squashRepoBlobsIter) Repository() *Repository { return i.repos.Repository() }
func (i *squashRepoBlobsIter) Blob() *object.Blob      { return i.blob }

func (i *squashRepoBlobsIter) Close() error {
	if i.blobs != nil {
		i.blobs.Close()
	}

	if i.repos != nil {
		return i.repos.Close()
	}

	return nil
}
func (i *squashRepoBlobsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.repos.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &squashRepoBlobsIter{
		ctx:         ctx,
		repos:       iter.(ReposIter),
		filters:     i.filters,
		readContent: i.readContent,
	}, nil
}
func (i *squashRepoBlobsIter) Row() sql.Row { return i.row }
func (i *squashRepoBlobsIter) Advance() error {
	for {
		if i.blobs == nil {
			i.repos.Repository().Close()

			err := i.repos.Advance()
			if err != nil {
				return err
			}

			i.blobs, err = i.Repository().BlobObjects()
			if err != nil {
				return err
			}
		}

		var err error
		i.blob, err = i.blobs.Next()
		if err != nil {
			if err == io.EOF {
				i.blobs = nil
				continue
			}
			return err
		}

		row, err := blobToRow(i.Repository().ID(), i.blob, i.readContent)
		if err != nil {
			return err
		}

		i.row = append(i.repos.Row(), row...)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashRepoBlobsIter) Schema() sql.Schema {
	return append(i.repos.Schema(), BlobsSchema...)
}

type squashTreeEntryBlobsIter struct {
	ctx           *sql.Context
	filters       sql.Expression
	treeEntries   TreeEntriesIter
	blob          *object.Blob
	row           sql.Row
	readContent   bool
	skipGitErrors bool
}

// NewTreeEntryBlobsIter returns an iterator that will return all blobs
// for the tree entries in the given iter that match the given filters.
func NewTreeEntryBlobsIter(
	squashTreeEntriesIter TreeEntriesIter,
	filters sql.Expression,
	readContent bool,
) BlobsIter {
	return &squashTreeEntryBlobsIter{
		treeEntries: squashTreeEntriesIter,
		filters:     filters,
		readContent: readContent,
	}
}

func (i *squashTreeEntryBlobsIter) Repository() *Repository { return i.treeEntries.Repository() }
func (i *squashTreeEntryBlobsIter) Blob() *object.Blob      { return i.blob }

func (i *squashTreeEntryBlobsIter) Close() error {
	if i.treeEntries != nil {
		return i.treeEntries.Close()
	}

	return nil
}
func (i *squashTreeEntryBlobsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.treeEntries.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashTreeEntryBlobsIter{
		ctx:           ctx,
		treeEntries:   iter.(TreeEntriesIter),
		filters:       i.filters,
		readContent:   i.readContent,
		skipGitErrors: session.SkipGitErrors,
	}, nil
}
func (i *squashTreeEntryBlobsIter) Row() sql.Row { return i.row }
func (i *squashTreeEntryBlobsIter) Advance() error {
	for {
		err := i.treeEntries.Advance()
		if err != nil {
			return err
		}

		entry := i.treeEntries.TreeEntry()
		if !entry.Mode.IsFile() {
			continue
		}

		i.blob, err = i.Repository().BlobObject(entry.Hash)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"repo":  i.Repository().ID(),
				"error": err,
				"blob":  entry.Hash,
			}).Error("blob object found not be found")

			if i.skipGitErrors {
				continue
			}

			return err
		}

		row, err := blobToRow(i.Repository().ID(), i.blob, i.readContent)
		if err != nil {
			return err
		}

		i.row = append(i.treeEntries.Row(), row...)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashTreeEntryBlobsIter) Schema() sql.Schema {
	return append(i.treeEntries.Schema(), BlobsSchema...)
}

type squashCommitBlobBlobsIter struct {
	ctx         *sql.Context
	filters     sql.Expression
	commitBlobs BlobsIter
	row         sql.Row
	readContent bool
}

// NewCommitBlobBlobsIter returns the blobs for all commit blobs in the given
// iterator.
func NewCommitBlobBlobsIter(
	commitBlobs BlobsIter,
	filters sql.Expression,
	readContent bool,
) BlobsIter {
	return &squashCommitBlobBlobsIter{
		commitBlobs: commitBlobs,
		filters:     filters,
		readContent: readContent,
	}
}

func (i *squashCommitBlobBlobsIter) Repository() *Repository { return i.commitBlobs.Repository() }
func (i *squashCommitBlobBlobsIter) Blob() *object.Blob      { return i.commitBlobs.Blob() }

func (i *squashCommitBlobBlobsIter) Close() error {
	if i.commitBlobs != nil {
		return i.commitBlobs.Close()
	}

	return nil
}
func (i *squashCommitBlobBlobsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.commitBlobs.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &squashCommitBlobBlobsIter{
		ctx:         ctx,
		commitBlobs: iter.(BlobsIter),
		filters:     i.filters,
		readContent: i.readContent,
	}, nil
}
func (i *squashCommitBlobBlobsIter) Row() sql.Row { return i.row }
func (i *squashCommitBlobBlobsIter) Advance() error {
	for {
		err := i.commitBlobs.Advance()
		if err != nil {
			return err
		}

		blob := i.commitBlobs.Blob()
		row, err := blobToRow(i.Repository().ID(), blob, i.readContent)
		if err != nil {
			return err
		}

		i.row = append(i.commitBlobs.Row(), row...)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}
func (i *squashCommitBlobBlobsIter) Schema() sql.Schema {
	return append(i.commitBlobs.Schema(), BlobsSchema...)
}

// FilesIter is a chainable iterator that operates on files.
type FilesIter interface {
	ChainableIter
	File() *object.File
	TreeHash() plumbing.Hash
}

type squashCommitFilesIter struct {
	commits       CommitsIter
	files         *object.FileIter
	file          *object.File
	commit        *object.Commit
	row           sql.Row
	filters       sql.Expression
	ctx           *sql.Context
	treeHash      plumbing.Hash
	skipGitErrors bool
}

// NewAllCommitFilesIter returns an iterator that will return all commit files.
func NewAllCommitFilesIter(filters sql.Expression) FilesIter {
	return NewCommitFilesIter(NewAllCommitsIter(nil, true), filters)
}

// NewCommitFilesIter returns an iterator that will return all commit files
// for the commits in the given iterator.
func NewCommitFilesIter(iter CommitsIter, filters sql.Expression) FilesIter {
	return &squashCommitFilesIter{commits: iter, filters: filters}
}

func (i *squashCommitFilesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.commits.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashCommitFilesIter{
		ctx:           ctx,
		skipGitErrors: session.SkipGitErrors,
		commits:       iter.(CommitsIter),
		filters:       i.filters,
	}, nil
}

func (i *squashCommitFilesIter) Advance() error {
	for {
		if i.files == nil {
			err := i.commits.Advance()
			if err != nil {
				if err != io.EOF && i.skipGitErrors {
					logrus.WithField("err", err).Error("could not get next commit")
					continue
				}

				return err
			}

			i.commit = i.commits.Commit()
			i.files, err = i.commit.Files()
			if err != nil {
				if i.skipGitErrors {
					logrus.WithFields(logrus.Fields{
						"err":    err,
						"repo":   i.Repository().ID(),
						"commit": i.commit.Hash.String(),
					}).Error("could not get files for commit")
					continue
				}

				return err
			}
		}

		var err error
		i.file, err = i.files.Next()
		if err != nil {
			if err == io.EOF {
				i.files = nil
				continue
			}

			if i.skipGitErrors {
				logrus.WithFields(logrus.Fields{
					"err":    err,
					"repo":   i.Repository().ID(),
					"commit": i.commit.Hash.String(),
				}).Error("could not get files for commit")
				continue
			}

			return err
		}

		i.treeHash = i.commits.Commit().TreeHash
		i.row = append(
			i.commits.Row(),
			newCommitFilesRow(i.Repository(), i.commit, i.file)...,
		)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}

func (i *squashCommitFilesIter) Repository() *Repository { return i.commits.Repository() }
func (i *squashCommitFilesIter) File() *object.File      { return i.file }
func (i *squashCommitFilesIter) TreeHash() plumbing.Hash { return i.treeHash }
func (i *squashCommitFilesIter) Row() sql.Row            { return i.row }
func (i *squashCommitFilesIter) Close() error {
	if i.files != nil {
		i.files.Close()
	}

	return i.commits.Close()
}
func (i *squashCommitFilesIter) Schema() sql.Schema {
	return append(i.commits.Schema(), CommitFilesSchema...)
}

type squashIndexCommitFilesIter struct {
	index         sql.IndexLookup
	pool          *RepositoryPool
	repo          *Repository
	iter          *commitFilesIndexIter
	file          *object.File
	row           sql.Row
	filters       sql.Expression
	ctx           *sql.Context
	treeHash      plumbing.Hash
	skipGitErrors bool
}

// NewIndexCommitFilesIter returns an iterator that will return all commit
// files for the commits in the given index.
func NewIndexCommitFilesIter(index sql.IndexLookup, filters sql.Expression) FilesIter {
	return &squashIndexCommitFilesIter{index: index, filters: filters}
}

func (i *squashIndexCommitFilesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	values, err := i.index.Values(RepositoryPartition(repo.ID()))
	if err != nil {
		return nil, err
	}

	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	return &squashIndexCommitFilesIter{
		iter:          newCommitFilesIndexIter(values, session.Pool),
		pool:          session.Pool,
		ctx:           ctx,
		skipGitErrors: session.SkipGitErrors,
		filters:       i.filters,
	}, nil
}

func (i *squashIndexCommitFilesIter) Advance() error {
	for {
		commitFile, err := i.iter.NextCommitFile()
		if err != nil {
			if err != io.EOF && i.skipGitErrors {
				logrus.WithField("err", err).Error("unable to get next file")
				continue
			}

			return err
		}

		i.file = commitFile.File

		if i.repo == nil || i.repo.ID() != commitFile.Repository {
			i.repo.Close()

			i.repo, err = i.pool.GetRepo(commitFile.Repository)
			if err != nil {
				if i.skipGitErrors {
					logrus.WithFields(logrus.Fields{
						"err":  err,
						"repo": commitFile.Repository,
					}).Error("unable to get repo")
					continue
				}
				return err
			}
		}

		i.treeHash = plumbing.NewHash(commitFile.TreeHash)

		i.row = sql.NewRow(
			commitFile.Repository,
			commitFile.CommitHash,
			i.file.Name,
			i.file.Blob.Hash.String(),
			commitFile.TreeHash,
		)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}

func (i *squashIndexCommitFilesIter) Repository() *Repository { return i.repo }
func (i *squashIndexCommitFilesIter) File() *object.File      { return i.file }
func (i *squashIndexCommitFilesIter) TreeHash() plumbing.Hash { return i.treeHash }
func (i *squashIndexCommitFilesIter) Row() sql.Row            { return i.row }
func (i *squashIndexCommitFilesIter) Schema() sql.Schema      { return CommitFilesSchema }

func (i *squashIndexCommitFilesIter) Close() error {
	if i.repo != nil {
		i.repo.Close()
	}
	return i.iter.Close()
}

type squashCommitFileFilesIter struct {
	files       FilesIter
	readContent bool
	row         sql.Row
	filters     sql.Expression
	ctx         *sql.Context
}

// NewCommitFileFilesIter returns all files for the commit files in the given
// iterator.
func NewCommitFileFilesIter(
	files FilesIter,
	filters sql.Expression,
	readContent bool,
) ChainableIter {
	return &squashCommitFileFilesIter{
		files:       files,
		filters:     filters,
		readContent: readContent,
	}
}

func (i *squashCommitFileFilesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.files.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &squashCommitFileFilesIter{
		files:       iter.(FilesIter),
		ctx:         ctx,
		filters:     i.filters,
		readContent: i.readContent,
	}, nil
}

func (i *squashCommitFileFilesIter) Advance() error {
	for {
		err := i.files.Advance()
		if err != nil {
			return err
		}

		f := i.files.File()
		row, err := fileToRow(i.Repository().ID(), i.files.TreeHash(), f, i.readContent)
		if err != nil {
			return err
		}

		i.row = append(i.files.Row(), row...)

		if i.filters != nil {
			ok, err := evalFilters(i.ctx, i.row, i.filters)
			if err != nil {
				return err
			}

			if !ok {
				continue
			}
		}

		return nil
	}
}

func (i *squashCommitFileFilesIter) Repository() *Repository { return i.files.Repository() }
func (i *squashCommitFileFilesIter) Row() sql.Row            { return i.row }
func (i *squashCommitFileFilesIter) Schema() sql.Schema {
	return append(i.files.Schema(), FilesSchema...)
}
func (i *squashCommitFileFilesIter) Close() error {
	return i.files.Close()
}

func evalFilters(ctx *sql.Context, row sql.Row, filters sql.Expression) (bool, error) {
	v, err := filters.Eval(ctx, row)
	if err != nil {
		return false, err
	}

	v, err = sql.Boolean.Convert(v)
	if err != nil {
		return false, err
	}

	return v.(bool), nil
}

var errInvalidCommit = errors.NewKind("invalid commit of type: %T")

func resolveCommit(repo *Repository, hash plumbing.Hash) (*object.Commit, error) {
	obj, err := repo.Object(plumbing.AnyObject, hash)
	if err != nil {
		return nil, err
	}

	switch obj := obj.(type) {
	case *object.Commit:
		return obj, nil
	case *object.Tag:
		return resolveCommit(repo, obj.Target)
	default:
		logrus.WithFields(logrus.Fields{
			"hash": hash,
			"type": fmt.Sprintf("%T", obj),
		}).Debug("expecting hash to belong to a commit object")
		return nil, errInvalidCommit.New(obj)
	}
}
