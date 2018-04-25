package gitbase

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"gopkg.in/src-d/go-log.v0"
)

// ChainableIter is an iterator meant to have a chaining-friendly API.
type ChainableIter interface {
	// New creates a new Chainable Iterator.
	New(*sql.Context, *Repository) (ChainableIter, error)
	// Close closes the iterator.
	Close() error
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
	// Repo returns the current repository. All calls to Repo return the
	// same repository until another call to Advance. Advance should
	// be called before calling Repo.
	Repo() *Repository
}

type reposIter struct {
	ctx     *sql.Context
	filters sql.Expression
	done    bool
	repo    *Repository
	row     sql.Row
}

// NewAllReposIter returns an iterator that will return all repositories
// that match the given filters.
func NewAllReposIter(filters sql.Expression) ReposIter {
	return &reposIter{filters: filters}
}

func (i *reposIter) Repo() *Repository { return i.repo }
func (i *reposIter) Close() error      { return nil }
func (i *reposIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	return &reposIter{ctx: ctx, filters: i.filters, repo: repo}, nil
}
func (i *reposIter) Row() sql.Row { return i.row }
func (i *reposIter) Advance() error {
	for {
		if i.done {
			return io.EOF
		}

		i.done = true
		i.row = sql.NewRow(i.repo.ID)
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
func (i *reposIter) Schema() sql.Schema { return RepositoriesSchema }

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

type remoteIter struct {
	ctx               *sql.Context
	repoID            string
	filters           sql.Expression
	remotePos, urlPos int
	remotes           []*git.Remote
	remote            *Remote
	row               sql.Row
}

// NewAllRemotesIter returns an iterator that will return all remotes
// that match the given filters.
func NewAllRemotesIter(filters sql.Expression) RemotesIter {
	return &remoteIter{filters: filters}
}

func (i *remoteIter) Remote() *Remote { return i.remote }
func (i *remoteIter) Close() error    { return nil }
func (i *remoteIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	remotes, err := repo.Repo.Remotes()
	if err != nil {
		return nil, err
	}
	return &remoteIter{
		ctx:     ctx,
		repoID:  repo.ID,
		filters: i.filters,
		remotes: remotes,
	}, nil
}
func (i *remoteIter) Row() sql.Row { return i.row }
func (i *remoteIter) Advance() error {
	for {
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
			RepoID: i.repoID,
			Name:   config.Name,
			URL:    config.URLs[i.urlPos],
			Fetch:  config.Fetch[i.urlPos].String(),
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
func (i *remoteIter) Schema() sql.Schema { return RemotesSchema }

type repoRemotesIter struct {
	ctx               *sql.Context
	repos             ReposIter
	filters           sql.Expression
	remotePos, urlPos int
	remotes           []*git.Remote
	remote            *Remote
	row               sql.Row
}

// NewRepoRemotesIter returns an iterator that will return all remotes for the
// given ReposIter repositories that match the given filters.
func NewRepoRemotesIter(reposIter ReposIter, filters sql.Expression) RemotesIter {
	return &repoRemotesIter{repos: reposIter, filters: filters}
}

func (i *repoRemotesIter) Remote() *Remote { return i.remote }
func (i *repoRemotesIter) Close() error {
	if i.repos != nil {
		return i.repos.Close()
	}
	return nil
}
func (i *repoRemotesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.repos.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &repoRemotesIter{
		ctx:     ctx,
		repos:   iter.(ReposIter),
		filters: i.filters,
	}, nil
}
func (i *repoRemotesIter) Row() sql.Row { return i.row }
func (i *repoRemotesIter) Advance() error {
	session, err := getSession(i.ctx)
	if err != nil {
		return err
	}

	logger, _ := log.New()

	for {
		if i.repos == nil {
			return io.EOF
		}

		if i.remotes == nil {
			err := i.repos.Advance()
			if err == io.EOF {
				i.remotes = nil
				return io.EOF
			}

			if err != nil {
				return err
			}

			i.remotes, err = i.repos.Repo().Repo.Remotes()
			if err != nil {
				logger.New(log.Fields{
					"iter": "repoRemoteIter",
					"repo": i.repos.Repo().ID,
				}).Error(err, "unable to retrieve repository remotes")

				if session.SkipGitErrors {
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
				if err := i.repos.Advance(); err != nil {
					return err
				}

				continue
			}

			remote = i.remotes[i.remotePos]
			config = remote.Config()
		}

		i.remote = &Remote{
			RepoID: i.repos.Repo().ID,
			Name:   config.Name,
			URL:    config.URLs[i.urlPos],
			Fetch:  config.Fetch[i.urlPos].String(),
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
func (i *repoRemotesIter) Schema() sql.Schema {
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

type refIter struct {
	ctx     *sql.Context
	repoID  string
	filters sql.Expression
	refs    storer.ReferenceIter
	head    *plumbing.Reference
	ref     *Ref
	row     sql.Row
}

// NewAllRefsIter returns an iterator that will return all references
// that match the given filters.
func NewAllRefsIter(filters sql.Expression) RefsIter {
	return &refIter{filters: filters}
}

func (i *refIter) Ref() *Ref { return i.ref }
func (i *refIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}
	return nil
}
func (i *refIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	refs, err := repo.Repo.References()
	if err != nil && !session.SkipGitErrors {
		return nil, err
	}

	head, err := repo.Repo.Head()
	if err != nil {
		if err != plumbing.ErrReferenceNotFound && !session.SkipGitErrors {
			return nil, err
		}

		logger, _ := log.New()
		logger.New(log.Fields{"repo": repo.ID}).
			Debugf("unable to get HEAD of repository")
	}

	return &refIter{
		ctx:     ctx,
		repoID:  repo.ID,
		filters: i.filters,
		refs:    refs,
		head:    head,
	}, nil
}
func (i *refIter) Row() sql.Row { return i.row }
func (i *refIter) Advance() error {
	session, err := getSession(i.ctx)
	if err != nil {
		return err
	}

	logger, _ := log.New()

	for {
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
			if err == io.EOF {
				i.refs = nil
				return io.EOF
			}

			if err != nil {
				if session.SkipGitErrors {
					continue
				}
				return err
			}
		}

		if ref.Type() != plumbing.HashReference {
			logger.New(log.Fields{
				"type": ref.Type(),
				"ref":  ref.Name(),
			}).Debugf("ignoring reference, it's not a hash reference")
			continue
		}

		i.ref = &Ref{i.repoID, ref}
		i.row = referenceToRow(i.repoID, ref)

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
func (i *refIter) Schema() sql.Schema { return RefsSchema }

type repoRefsIter struct {
	ctx     *sql.Context
	repos   ReposIter
	filters sql.Expression
	refs    storer.ReferenceIter
	head    *plumbing.Reference
	ref     *Ref
	row     sql.Row
}

// NewRepoRefsIter returns an iterator that will return all references
// for the repositories of the given repos iterator that match the given
// filters.
func NewRepoRefsIter(
	reposIter ReposIter,
	filters sql.Expression,
) RefsIter {
	return &repoRefsIter{repos: reposIter, filters: filters}
}

func (i *repoRefsIter) Ref() *Ref { return i.ref }
func (i *repoRefsIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}

	if i.repos != nil {
		return i.repos.Close()
	}

	return nil
}
func (i *repoRefsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	repos, err := i.repos.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &repoRefsIter{
		ctx:     ctx,
		repos:   repos.(ReposIter),
		filters: i.filters,
	}, nil
}
func (i *repoRefsIter) Row() sql.Row { return i.row }
func (i *repoRefsIter) Advance() error {
	session, err := getSession(i.ctx)
	if err != nil {
		return err
	}

	logger, _ := log.New()

	for {
		if i.repos == nil {
			return io.EOF
		}

		if i.refs == nil {
			err := i.repos.Advance()
			if err == io.EOF {
				i.repos = nil
				return io.EOF
			}

			if err != nil {
				return err
			}

			i.refs, err = i.repos.Repo().Repo.References()
			if err != nil {
				logger.New(log.Fields{
					"repo": i.repos.Repo().ID,
				}).Error(err, "unable to retrieve references")

				if session.SkipGitErrors {
					continue
				}

				return err
			}

			i.head, err = i.repos.Repo().Repo.Head()
			if err != nil {
				if err != plumbing.ErrReferenceNotFound && !session.SkipGitErrors {
					return err
				}

				logger.New(log.Fields{"repo": i.repos.Repo().ID}).
					Debugf("unable to get HEAD of repository")
				continue
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

		if ref.Type() != plumbing.HashReference {
			logger.New(log.Fields{
				"type": ref.Type(),
				"ref":  ref.Name(),
			}).Debugf("ignoring reference, it's not a hash reference")
			continue
		}

		i.ref = &Ref{i.repos.Repo().ID, ref}
		i.row = append(i.repos.Row(), referenceToRow(i.ref.RepoID, ref)...)

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
func (i *repoRefsIter) Schema() sql.Schema {
	return append(i.repos.Schema(), RefsSchema...)
}

type remoteRefsIter struct {
	ctx     *sql.Context
	repo    *Repository
	remotes RemotesIter
	filters sql.Expression
	refs    storer.ReferenceIter
	head    *plumbing.Reference
	ref     *Ref
	row     sql.Row
}

// NewRemoteRefsIter returns an iterator that will return all references
// for the remotes returned by the given remotes iterator that match the given
// filters.
func NewRemoteRefsIter(
	remotesIter RemotesIter,
	filters sql.Expression,
) RefsIter {
	return &remoteRefsIter{
		remotes: remotesIter,
		filters: filters,
	}
}

func (i *remoteRefsIter) Ref() *Ref { return i.ref }
func (i *remoteRefsIter) Close() error {
	if i.refs != nil {
		i.refs.Close()
	}

	if i.remotes != nil {
		return i.remotes.Close()
	}

	return nil
}
func (i *remoteRefsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.remotes.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &remoteRefsIter{
		ctx:     ctx,
		remotes: iter.(RemotesIter),
		filters: i.filters,
		repo:    repo,
	}, nil
}
func (i *remoteRefsIter) Row() sql.Row { return i.row }
func (i *remoteRefsIter) Advance() error {
	session, err := getSession(i.ctx)
	if err != nil {
		return err
	}

	logger, _ := log.New()

	for {
		if i.remotes == nil {
			return io.EOF
		}

		if i.refs == nil {
			err := i.remotes.Advance()
			if err == io.EOF {
				i.remotes = nil
				return io.EOF
			}

			if err != nil {
				return err
			}

			i.refs, err = i.repo.Repo.References()
			if err != nil {
				logger.New(log.Fields{
					"repo": i.repo.ID,
				}).Error(err, "unable to retrieve references")

				if session.SkipGitErrors {
					continue
				}

				return err
			}

			i.head, err = i.repo.Repo.Head()
			if err != nil {
				if err != plumbing.ErrReferenceNotFound && session.SkipGitErrors {
					return err
				}

				logger.New(log.Fields{"repo": i.remotes.Remote().RepoID}).
					Debugf("unable to get HEAD of repository")
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
				return io.EOF
			}

			if err != nil {
				return err
			}
		}

		if ref.Type() != plumbing.HashReference {
			logger.New(log.Fields{
				"type": ref.Type(),
				"ref":  ref.Name(),
			}).Debugf("ignoring reference, it's not a hash reference")
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
func (i *remoteRefsIter) Schema() sql.Schema {
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

type commitsIter struct {
	ctx     *sql.Context
	filters sql.Expression
	commits object.CommitIter
	commit  *object.Commit
	row     sql.Row
}

// NewAllCommitsIter returns an iterator that will return all commits
// that match the given filters.
func NewAllCommitsIter(filters sql.Expression) CommitsIter {
	return &commitsIter{filters: filters}
}

func (i *commitsIter) Commit() *object.Commit { return i.commit }
func (i *commitsIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}
	return nil
}
func (i *commitsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	session, err := getSession(ctx)
	if err != nil {
		return nil, err
	}

	logger, _ := log.New()

	commits, err := repo.Repo.CommitObjects()
	if err != nil {
		logger.New(log.Fields{
			"repo": repo.ID,
		}).Error(err, "unable to get commit iterator")

		if !session.SkipGitErrors {
			return nil, err
		}

		commits = nil
	}

	return &commitsIter{
		ctx:     ctx,
		commits: commits,
		filters: i.filters,
	}, nil
}
func (i *commitsIter) Row() sql.Row { return i.row }
func (i *commitsIter) Advance() error {
	for {
		if i.commits == nil {
			return io.EOF
		}

		var err error
		i.commit, err = i.commits.Next()
		if err == io.EOF {
			i.commits = nil
			return io.EOF
		}

		if err != nil {
			return err
		}

		i.row = commitToRow(i.commit)

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
func (i *commitsIter) Schema() sql.Schema { return CommitsSchema }

type refCommitsIter struct {
	ctx     *sql.Context
	repo    *Repository
	filters sql.Expression
	refs    RefsIter
	commits object.CommitIter
	commit  *object.Commit
	row     sql.Row
}

// NewRefCommitsIter returns an iterator that will return all commits
// for the given iter references that match the given filters.
// If the iterator is virtual, it will not append its columns to the
// final row.
func NewRefCommitsIter(
	refsIter RefsIter,
	filters sql.Expression,
) CommitsIter {
	return &refCommitsIter{refs: refsIter, filters: filters}
}

func (i *refCommitsIter) Commit() *object.Commit { return i.commit }
func (i *refCommitsIter) Close() error {
	if i.commits != nil {
		i.commits.Close()
	}

	if i.refs != nil {
		return i.refs.Close()
	}

	return nil
}
func (i *refCommitsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.refs.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &refCommitsIter{
		ctx:     ctx,
		repo:    repo,
		refs:    iter.(RefsIter),
		filters: i.filters,
	}, nil
}
func (i *refCommitsIter) Row() sql.Row { return i.row }
func (i *refCommitsIter) Advance() error {
	session, err := getSession(i.ctx)
	if err != nil {
		return err
	}

	logger, _ := log.New()

	for {
		if i.refs == nil {
			return io.EOF
		}

		if i.commits == nil {
			err := i.refs.Advance()
			if err != nil {
				if err == io.EOF {
					i.refs = nil
					return io.EOF
				}

				return err
			}

			_, err = resolveCommit(i.repo, i.refs.Ref().Hash())
			if err != nil {
				if errInvalidCommit.Is(err) {
					logger.New(log.Fields{
						"ref":  i.refs.Ref().Name(),
						"hash": i.refs.Ref().Hash(),
					}).Debugf("skipping reference, it's not pointing to a commit")
					continue
				}

				logger.New(log.Fields{
					"ref":  i.refs.Ref().Name(),
					"hash": i.refs.Ref().Hash(),
				}).Error(err, "unable to resolve commit")

				if session.SkipGitErrors {
					continue
				}

				return err
			}

			i.commits, err = i.repo.Repo.Log(&git.LogOptions{
				From: i.refs.Ref().Hash(),
			})
			if err != nil {
				logger.New(log.Fields{
					"ref":  i.refs.Ref().Name(),
					"hash": i.refs.Ref().Hash(),
				}).Error(err, "unable to retrieve commits")

				if session.SkipGitErrors {
					continue
				}

				return err
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

		i.row = append(i.refs.Row(), commitToRow(i.commit)...)

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
func (i *refCommitsIter) Schema() sql.Schema {
	return append(i.refs.Schema(), CommitsSchema...)
}

type refHeadCommitsIter struct {
	ctx     *sql.Context
	repo    *Repository
	filters sql.Expression
	refs    RefsIter
	commit  *object.Commit
	row     sql.Row
	virtual bool
}

// NewRefHEADCommitsIter returns an iterator that will return the commit
// for the given iter reference heads that match the given filters.
func NewRefHEADCommitsIter(
	refsIter RefsIter,
	filters sql.Expression,
	virtual bool,
) CommitsIter {
	return &refHeadCommitsIter{refs: refsIter, filters: filters, virtual: virtual}
}

func (i *refHeadCommitsIter) Commit() *object.Commit { return i.commit }
func (i *refHeadCommitsIter) Close() error {
	if i.refs != nil {
		return i.refs.Close()
	}

	return nil
}
func (i *refHeadCommitsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.refs.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &refHeadCommitsIter{
		ctx:     ctx,
		repo:    repo,
		refs:    iter.(RefsIter),
		filters: i.filters,
		virtual: i.virtual,
	}, nil
}
func (i *refHeadCommitsIter) Row() sql.Row { return i.row }
func (i *refHeadCommitsIter) Advance() error {
	session, err := getSession(i.ctx)
	if err != nil {
		return err
	}

	session, ok := i.ctx.Session.(*Session)
	if !ok {
		return ErrInvalidGitbaseSession.New(i.ctx.Session)
	}

	logger, _ := log.New()

	for {
		if i.refs == nil {
			return io.EOF
		}

		err := i.refs.Advance()
		if err != nil {
			if err == io.EOF {
				i.refs = nil
				return io.EOF
			}

			return err
		}

		i.commit, err = resolveCommit(i.repo, i.refs.Ref().Hash())
		if err != nil {
			if errInvalidCommit.Is(err) {
				logger.New(log.Fields{
					"ref":  i.refs.Ref().Name(),
					"hash": i.refs.Ref().Hash(),
				}).Debugf("skipping reference, it's not pointing to a commit")
				continue
			}

			logger.New(log.Fields{
				"ref":  i.refs.Ref().Name(),
				"hash": i.refs.Ref().Hash(),
			}).Error(err, "unable to resolve commit")

			if session.SkipGitErrors {
				continue
			}

			return err
		}

		if i.virtual {
			i.row = i.refs.Row()
		} else {
			i.row = append(i.refs.Row(), commitToRow(i.commit)...)
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
func (i *refHeadCommitsIter) Schema() sql.Schema {
	if i.virtual {
		return i.refs.Schema()
	}
	return append(i.refs.Schema(), CommitsSchema...)
}

// TreeEntriesIter is a chainable operator that operates on Tree Entries.
type TreeEntriesIter interface {
	ChainableIter
	// TreeEntry returns the current repository. All calls to TreeEntry return the
	// same tree entries until another call to Advance. Advance should
	// be called before calling TreeEntry.
	TreeEntry() *TreeEntry
}

// TreeEntry is a tree entry object.
type TreeEntry struct {
	TreeHash plumbing.Hash
	*object.File
}

type treeEntriesIter struct {
	ctx     *sql.Context
	filters sql.Expression
	trees   *object.TreeIter
	tree    *object.Tree
	files   *object.FileIter
	entry   *TreeEntry
	row     sql.Row
}

// NewAllTreeEntriesIter returns an iterator that will return all tree entries
// that match the given filters.
func NewAllTreeEntriesIter(filters sql.Expression) TreeEntriesIter {
	return &treeEntriesIter{filters: filters}
}

func (i *treeEntriesIter) TreeEntry() *TreeEntry { return i.entry }
func (i *treeEntriesIter) Close() error {
	if i.trees != nil {
		i.trees.Close()
	}

	if i.files != nil {
		i.files.Close()
	}

	return nil
}
func (i *treeEntriesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	trees, err := repo.Repo.TreeObjects()
	if err != nil {
		return nil, err
	}

	return &treeEntriesIter{
		ctx:     ctx,
		trees:   trees,
		filters: i.filters,
	}, nil
}
func (i *treeEntriesIter) Row() sql.Row { return i.row }
func (i *treeEntriesIter) Advance() error {
	for {
		if i.trees == nil {
			return io.EOF
		}

		if i.files == nil {
			var err error
			i.tree, err = i.trees.Next()
			if err == io.EOF {
				i.trees = nil
				return io.EOF
			}

			if err != nil {
				return err
			}

			i.files = i.tree.Files()
		}

		file, err := i.files.Next()
		if err == io.EOF {
			i.files = nil
			continue
		}

		if err != nil {
			return err
		}

		i.entry = &TreeEntry{i.tree.Hash, file}
		i.row = fileToRow(i.tree, i.entry.File)

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
func (i *treeEntriesIter) Schema() sql.Schema { return TreeEntriesSchema }

type commitMainTreeEntriesIter struct {
	ctx     *sql.Context
	commits CommitsIter
	filters sql.Expression
	tree    *object.Tree
	files   *object.FileIter
	entry   *TreeEntry
	row     sql.Row
	virtual bool
}

// NewCommitMainTreeEntriesIter returns an iterator that will return all tree
// entries for the main tree of the commits returned by the given commit
// iterator that match the given filters.
func NewCommitMainTreeEntriesIter(
	commitsIter CommitsIter,
	filters sql.Expression,
	virtual bool,
) TreeEntriesIter {
	return &commitMainTreeEntriesIter{
		commits: commitsIter,
		virtual: virtual,
		filters: filters,
	}
}

func (i *commitMainTreeEntriesIter) TreeEntry() *TreeEntry { return i.entry }
func (i *commitMainTreeEntriesIter) Close() error {
	if i.files != nil {
		i.files.Close()
	}

	if i.commits != nil {
		return i.commits.Close()
	}

	return nil
}
func (i *commitMainTreeEntriesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.commits.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &commitMainTreeEntriesIter{
		ctx:     ctx,
		commits: iter.(CommitsIter),
		filters: i.filters,
		virtual: i.virtual,
	}, nil
}
func (i *commitMainTreeEntriesIter) Row() sql.Row { return i.row }
func (i *commitMainTreeEntriesIter) Advance() error {
	session, err := getSession(i.ctx)
	if err != nil {
		return err
	}

	logger, _ := log.New()

	for {
		if i.commits == nil {
			return io.EOF
		}

		if i.files == nil {
			err := i.commits.Advance()
			if err == io.EOF {
				i.commits = nil
				return io.EOF
			}

			if err != nil {
				return err
			}

			i.tree, err = i.commits.Commit().Tree()
			if err != nil {
				logger.New(log.Fields{
					"commit": i.commits.Commit().Hash.String(),
				}).Error(err, "unable to retrieve tree")

				if session.SkipGitErrors {
					continue
				}

				return err
			}

			i.files = i.tree.Files()
		}

		file, err := i.files.Next()
		if err == io.EOF {
			i.files = nil
			continue
		}

		if err != nil {
			return err
		}

		i.entry = &TreeEntry{i.tree.Hash, file}

		if i.virtual {
			i.row = i.commits.Row()
		} else {
			i.row = append(i.commits.Row(), fileToRow(i.tree, file)...)
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
func (i *commitMainTreeEntriesIter) Schema() sql.Schema {
	if i.virtual {
		return i.commits.Schema()
	}
	return append(i.commits.Schema(), TreeEntriesSchema...)
}

type commitTreeEntriesIter struct {
	ctx     *sql.Context
	commits CommitsIter
	filters sql.Expression
	repo    *Repository
	files   *recursiveTreeFileIter
	entry   *TreeEntry
	row     sql.Row
	virtual bool
}

// NewCommitTreeEntriesIter returns an iterator that will return all tree
// entries for all trees of the commits returned by the given commit
// iterator that match the given filters.
func NewCommitTreeEntriesIter(
	commitsIter CommitsIter,
	filters sql.Expression,
	virtual bool,
) TreeEntriesIter {
	return &commitTreeEntriesIter{
		commits: commitsIter,
		virtual: virtual,
		filters: filters,
	}
}

func (i *commitTreeEntriesIter) TreeEntry() *TreeEntry { return i.entry }
func (i *commitTreeEntriesIter) Close() error {
	if i.files != nil {
		i.files.Close()
	}

	if i.commits != nil {
		return i.commits.Close()
	}

	return nil
}
func (i *commitTreeEntriesIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.commits.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &commitTreeEntriesIter{
		ctx:     ctx,
		commits: iter.(CommitsIter),
		repo:    repo,
		filters: i.filters,
		virtual: i.virtual,
	}, nil
}
func (i *commitTreeEntriesIter) Row() sql.Row { return i.row }
func (i *commitTreeEntriesIter) Advance() error {
	session, err := getSession(i.ctx)
	if err != nil {
		return err
	}

	logger, _ := log.New()

	for {
		if i.commits == nil {
			return io.EOF
		}

		if i.files == nil {
			err := i.commits.Advance()
			if err == io.EOF {
				i.commits = nil
				return io.EOF
			}

			if err != nil {
				return err
			}

			tree, err := i.commits.Commit().Tree()
			if err != nil {
				logger.New(log.Fields{
					"commit": i.commits.Commit().Hash.String(),
				}).Error(err, "unable to retrieve tree")

				if session.SkipGitErrors {
					continue
				}

				return err
			}

			i.files = newRecursiveTreeFileIter(i.ctx, i.repo, tree)
		}

		file, tree, err := i.files.Next()
		if err == io.EOF {
			i.files = nil
			continue
		}

		if err != nil {
			return err
		}

		i.entry = &TreeEntry{tree.Hash, file}

		if i.virtual {
			i.row = i.commits.Row()
		} else {
			i.row = append(i.commits.Row(), fileToRow(tree, file)...)
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
func (i *commitTreeEntriesIter) Schema() sql.Schema {
	if i.virtual {
		return i.commits.Schema()
	}
	return append(i.commits.Schema(), TreeEntriesSchema...)
}

type recursiveTreeFileIter struct {
	ctx          *sql.Context
	repo         *Repository
	tree         *object.Tree
	pendingTrees []*object.Tree
	stack        []*recursiveTreeFileStackFrame
	seen         map[plumbing.Hash]struct{}
}

type recursiveTreeFileStackFrame struct {
	tree *object.Tree
	pos  int
}

func newRecursiveTreeFileIter(
	ctx *sql.Context,
	repo *Repository,
	tree *object.Tree,
) *recursiveTreeFileIter {
	return &recursiveTreeFileIter{
		ctx:          ctx,
		repo:         repo,
		tree:         tree,
		pendingTrees: nil,
		stack: []*recursiveTreeFileStackFrame{
			{tree, 0},
		},
		seen: map[plumbing.Hash]struct{}{
			tree.Hash: struct{}{},
		},
	}
}

func (i *recursiveTreeFileIter) Next() (*object.File, *object.Tree, error) {
	session, err := getSession(i.ctx)
	if err != nil {
		return nil, nil, err
	}

	logger, _ := log.New()

	for {
		if i.tree == nil {
			if len(i.pendingTrees) == 0 {
				return nil, nil, io.EOF
			}

			i.tree = i.pendingTrees[0]
			i.pendingTrees = i.pendingTrees[1:]
			i.stack = []*recursiveTreeFileStackFrame{
				{i.tree, 0},
			}
		}

		if len(i.stack) == 0 {
			i.tree = nil
			continue
		}

		frame := i.stack[len(i.stack)-1]
		if frame.pos >= len(frame.tree.Entries) {
			i.stack = i.stack[:len(i.stack)-1]
			continue
		}

		entry := frame.tree.Entries[frame.pos]
		frame.pos++
		if entry.Mode == filemode.Dir {
			tree, err := i.repo.Repo.TreeObject(entry.Hash)
			if err != nil {
				logger.New(log.Fields{
					"tree": entry.Hash.String(),
					"repo": i.repo.ID,
				}).Error(err, "unable to retrieve tree object")

				if session.SkipGitErrors {
					continue
				}

				return nil, nil, err
			}

			if _, ok := i.seen[tree.Hash]; !ok {
				i.pendingTrees = append(i.pendingTrees, tree)
				i.seen[tree.Hash] = struct{}{}
			}
			i.stack = append(i.stack, &recursiveTreeFileStackFrame{tree, 0})
			continue
		} else if entry.Mode == filemode.Submodule {
			continue
		}

		var path []string
		for j := 0; j < len(i.stack); j++ {
			path = append(path, i.stack[j].tree.Entries[i.stack[j].pos-1].Name)
		}

		return &object.File{
			Name: strings.Join(path, string(filepath.Separator)),
			Mode: entry.Mode,
			Blob: object.Blob{Hash: entry.Hash},
		}, i.tree, nil
	}
}

func (i *recursiveTreeFileIter) Close() error { return nil }

// BlobsIter is a chainable iterator that operates on blobs.
type BlobsIter interface {
	ChainableIter
}

// The only blob iterator is the chained one, because it's the last step
// in the table hierarchy and it makes no sense to join with no other
// table in the squashing.
type treeEntryBlobsIter struct {
	ctx         *sql.Context
	repo        *Repository
	filters     sql.Expression
	treeEntries TreeEntriesIter
	row         sql.Row
	readContent bool
}

// NewTreeEntryBlobsIter returns an iterator that will return all blobs
// for the tree entries in the given iter that match the given filters.
func NewTreeEntryBlobsIter(
	treeEntriesIter TreeEntriesIter,
	filters sql.Expression,
	readContent bool,
) BlobsIter {
	return &treeEntryBlobsIter{
		treeEntries: treeEntriesIter,
		filters:     filters,
		readContent: readContent,
	}
}

func (i *treeEntryBlobsIter) Close() error {
	if i.treeEntries != nil {
		return i.treeEntries.Close()
	}

	return nil
}
func (i *treeEntryBlobsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.treeEntries.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &treeEntryBlobsIter{
		ctx:         ctx,
		repo:        repo,
		treeEntries: iter.(TreeEntriesIter),
		filters:     i.filters,
		readContent: i.readContent,
	}, nil
}
func (i *treeEntryBlobsIter) Row() sql.Row { return i.row }
func (i *treeEntryBlobsIter) Advance() error {
	session, err := getSession(i.ctx)
	if err != nil {
		return err
	}

	logger, _ := log.New()

	for {
		if i.treeEntries == nil {
			return io.EOF
		}

		err := i.treeEntries.Advance()
		if err == io.EOF {
			i.treeEntries = nil
			return io.EOF
		}

		if err != nil {
			return err
		}

		blob, err := i.repo.Repo.BlobObject(i.treeEntries.TreeEntry().Hash)
		if err != nil {
			logger.New(log.Fields{
				"repo": i.repo.ID,
				"hash": i.treeEntries.TreeEntry().Hash,
			}).Error(err, "error getting blob for tree hash")

			if session.SkipGitErrors {
				continue
			}

			return err
		}

		row, err := blobToRow(blob, i.readContent)
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
func (i *treeEntryBlobsIter) Schema() sql.Schema {
	return append(i.treeEntries.Schema(), BlobsSchema...)
}

type commitBlobsIter struct {
	ctx         *sql.Context
	readContent bool
	repo        *Repository
	filters     sql.Expression
	commits     CommitsIter
	files       *object.FileIter
	row         sql.Row
	seen        map[plumbing.Hash]struct{}
}

// NewCommitBlobsIter returns an iterator that will return all blobs
// for the commit in the given iter that match the given filters.
func NewCommitBlobsIter(
	commits CommitsIter,
	filters sql.Expression,
	readContent bool,
) BlobsIter {
	return &commitBlobsIter{
		commits:     commits,
		filters:     filters,
		readContent: readContent,
	}
}

func (i *commitBlobsIter) Close() error {
	if i.commits != nil {
		return i.commits.Close()
	}

	return nil
}
func (i *commitBlobsIter) New(ctx *sql.Context, repo *Repository) (ChainableIter, error) {
	iter, err := i.commits.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &commitBlobsIter{
		ctx:         ctx,
		repo:        repo,
		commits:     iter.(CommitsIter),
		filters:     i.filters,
		seen:        make(map[plumbing.Hash]struct{}),
		readContent: i.readContent,
	}, nil
}
func (i *commitBlobsIter) Row() sql.Row { return i.row }
func (i *commitBlobsIter) Advance() error {
	session, err := getSession(i.ctx)
	if err != nil {
		return err
	}

	logger, _ := log.New()

	for {
		if i.commits == nil {
			return io.EOF
		}

		if i.files == nil {
			err := i.commits.Advance()
			if err == io.EOF {
				i.commits = nil
				return io.EOF
			}

			if err != nil {
				return err
			}

			tree, err := i.repo.Repo.TreeObject(i.commits.Commit().TreeHash)
			if err != nil {
				logger.New(log.Fields{
					"repo":      i.repo.ID,
					"tree_hash": i.commits.Commit().TreeHash.String(),
				}).Error(err, "unable to retrieve tree object")

				if session.SkipGitErrors {
					continue
				}

				return err
			}

			i.files = tree.Files()
			// uniqueness of blob is per commit, so we need to reset the seen map
			i.seen = make(map[plumbing.Hash]struct{})
		}

		file, err := i.files.Next()
		if err == io.EOF {
			i.files = nil
			continue
		}

		if _, ok := i.seen[file.Hash]; ok {
			continue
		}

		i.seen[file.Hash] = struct{}{}
		blob, err := i.repo.Repo.BlobObject(file.Hash)
		if err != nil {
			return err
		}

		row, err := blobToRow(blob, i.readContent)
		if err != nil {
			return err
		}

		i.row = append(i.commits.Row(), row...)

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
func (i *commitBlobsIter) Schema() sql.Schema {
	return append(i.commits.Schema(), BlobsSchema...)
}

// NewChainableRowRepoIter creates a new RowRepoIter from a ChainableIter.
func NewChainableRowRepoIter(ctx *sql.Context, iter ChainableIter) RowRepoIter {
	return &chainableRowRepoIter{iter, ctx}
}

type chainableRowRepoIter struct {
	ChainableIter
	ctx *sql.Context
}

func (it *chainableRowRepoIter) NewIterator(repo *Repository) (RowRepoIter, error) {
	i, err := it.New(it.ctx, repo)
	if err != nil {
		return nil, err
	}

	return NewChainableRowRepoIter(it.ctx, i), nil
}

func (it *chainableRowRepoIter) Next() (sql.Row, error) {
	if err := it.Advance(); err != nil {
		return nil, err
	}

	return it.Row(), nil
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
	obj, err := repo.Repo.Object(plumbing.AnyObject, hash)
	if err != nil {
		return nil, err
	}

	logger, _ := log.New()

	switch obj := obj.(type) {
	case *object.Commit:
		return obj, nil
	case *object.Tag:
		return resolveCommit(repo, obj.Target)
	default:
		logger.New(log.Fields{
			"hash": hash,
			"type": fmt.Sprintf("%T", obj),
		}).Debugf("expecting hash to belong to a commit object")
		return nil, errInvalidCommit.New(obj)
	}
}

func getSession(ctx *sql.Context) (*Session, error) {
	if ctx == nil || ctx.Session == nil {
		return nil, ErrInvalidContext.New(ctx)
	}

	session, ok := ctx.Session.(*Session)
	if !ok {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	return session, nil
}
