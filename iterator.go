package gitbase

import (
	"io"

	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
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
	refs, err := repo.Repo.References()
	if err != nil {
		return nil, err
	}

	head, err := repo.Repo.Head()
	if err != nil {
		return nil, err
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
				return err
			}
		}

		if ref.Type() != plumbing.HashReference {
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
				return err
			}

			i.head, err = i.repos.Repo().Repo.Head()
			if err != nil {
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

		if ref.Type() != plumbing.HashReference {
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
	repo    *git.Repository
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
		repo:    repo.Repo,
	}, nil
}
func (i *remoteRefsIter) Row() sql.Row { return i.row }
func (i *remoteRefsIter) Advance() error {
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

			i.refs, err = i.repo.References()
			if err != nil {
				return err
			}

			i.head, err = i.repo.Head()
			if err != nil {
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
				return io.EOF
			}

			if err != nil {
				return err
			}
		}

		if ref.Type() != plumbing.HashReference {
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
	commits, err := repo.Repo.CommitObjects()
	if err != nil {
		return nil, err
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
	repo    *git.Repository
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
		repo:    repo.Repo,
		refs:    iter.(RefsIter),
		filters: i.filters,
	}, nil
}
func (i *refCommitsIter) Row() sql.Row { return i.row }
func (i *refCommitsIter) Advance() error {
	for {
		if i.refs == nil {
			return io.EOF
		}

		if i.commits == nil {
			err := i.refs.Advance()
			if err == io.EOF {
				i.refs = nil
				return io.EOF
			}

			if err != nil {
				return err
			}

			i.commits, err = i.repo.Log(&git.LogOptions{From: i.refs.Ref().Hash()})
			if err != nil {
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
	repo    *git.Repository
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
		repo:    repo.Repo,
		refs:    iter.(RefsIter),
		filters: i.filters,
		virtual: i.virtual,
	}, nil
}
func (i *refHeadCommitsIter) Row() sql.Row { return i.row }
func (i *refHeadCommitsIter) Advance() error {
	for {
		if i.refs == nil {
			return io.EOF
		}

		err := i.refs.Advance()
		if err == io.EOF {
			i.refs = nil
			return io.EOF
		}

		if err != nil {
			return err
		}

		i.commit, err = i.repo.CommitObject(i.refs.Ref().Hash())
		if err != nil {
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

// FIXME: just like the regular tree_entries table, this returns
// way more returns than it should. So, AllTreeEntriesIter and
// CommitTreeEntriesIter yield different results.
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

type commitTreeEntriesIter struct {
	ctx     *sql.Context
	commits CommitsIter
	filters sql.Expression
	tree    *object.Tree
	files   *object.FileIter
	entry   *TreeEntry
	row     sql.Row
	virtual bool
}

// NewCommitTreeEntriesIter returns an iterator that will return all tree
// entries for the commits returned by the given commit iterator that match
// the given filters.
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
		filters: i.filters,
		virtual: i.virtual,
	}, nil
}
func (i *commitTreeEntriesIter) Row() sql.Row { return i.row }
func (i *commitTreeEntriesIter) Advance() error {
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
func (i *commitTreeEntriesIter) Schema() sql.Schema {
	if i.virtual {
		return i.commits.Schema()
	}
	return append(i.commits.Schema(), TreeEntriesSchema...)
}

// BlobsIter is a chainable iterator that operates on blobs.
type BlobsIter interface {
	ChainableIter
}

// The only blob iterator is the chained one, because it's the last step
// in the table hierarchy and it makes no sense to join with no other
// table in the squashing.
type treeEntryBlobsIter struct {
	ctx         *sql.Context
	repo        *git.Repository
	filters     sql.Expression
	treeEntries TreeEntriesIter
	row         sql.Row
}

// NewTreeEntryBlobsIter returns an iterator that will return all blobs
// for the tree entries in the given iter that match the given filters.
func NewTreeEntryBlobsIter(
	treeEntriesIter TreeEntriesIter,
	filters sql.Expression,
) BlobsIter {
	return &treeEntryBlobsIter{treeEntries: treeEntriesIter, filters: filters}
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
		repo:        repo.Repo,
		treeEntries: iter.(TreeEntriesIter),
		filters:     i.filters,
	}, nil
}
func (i *treeEntryBlobsIter) Row() sql.Row { return i.row }
func (i *treeEntryBlobsIter) Advance() error {
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

		blob, err := i.repo.BlobObject(i.treeEntries.TreeEntry().Hash)
		if err != nil {
			return err
		}

		row, err := blobToRow(blob)
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
