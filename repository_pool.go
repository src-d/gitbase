package gitbase

import (
	"fmt"
	"io"

	"github.com/src-d/go-borges"
	billy "gopkg.in/src-d/go-billy.v4"
	errors "gopkg.in/src-d/go-errors.v1"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

var (
	errRepoAlreadyRegistered = errors.NewKind("the repository is already registered: %s")

	gitStorerOptions = filesystem.Options{
		ExclusiveAccess: true,
		KeepDescriptors: true,
	}
)

type Repository struct {
	*git.Repository

	cache cache.Object
	repo  borges.Repository
	lib   borges.Library
}

func NewRepository(
	lib borges.Library,
	repo borges.Repository,
	cache cache.Object,
) *Repository {
	return &Repository{
		Repository: repo.R(),
		lib:        lib,
		repo:       repo,
		cache:      cache,
	}
}

func (r *Repository) ID() string {
	return r.repo.ID().String()
}

func (r *Repository) FS() (billy.Filesystem, error) {
	fs := r.repo.FS()
	if fs == nil {
		return nil, fmt.Errorf("filesystem inaccesible")
	}

	return fs, nil
}

func (r *Repository) Cache() cache.Object {
	return r.cache
}

func (r *Repository) Close() {
	if r != nil && r.repo != nil {
		if closer, ok := r.repo.(io.Closer); ok {
			closer.Close()
		}
	}
}

// RepositoryPool holds a pool git repository paths and
// functionality to open and iterate them.
type RepositoryPool struct {
	cache   cache.Object
	library borges.Library
}

// NewRepositoryPool initializes a new RepositoryPool with LRU cache.
func NewRepositoryPool(
	maxCacheSize cache.FileSize,
	lib borges.Library,
) *RepositoryPool {
	return &RepositoryPool{
		cache:   cache.NewObjectLRU(maxCacheSize),
		library: lib,
	}
}

// ErrPoolRepoNotFound is returned when a repository id is not present in the pool.
var ErrPoolRepoNotFound = errors.NewKind("repository id %s not found in the pool")

// GetRepo returns a repository with the given id from the pool.
func (p *RepositoryPool) GetRepo(id string) (*Repository, error) {
	i := borges.RepositoryID(id)

	repo, err := p.library.Get(i, borges.ReadOnlyMode)
	if err != nil {
		if borges.ErrRepositoryNotExists.Is(err) {
			return nil, ErrPoolRepoNotFound.New(id)
		}

		return nil, err
	}

	r := NewRepository(p.library, repo, p.cache)
	return r, nil
}

// RepoIter creates a new Repository iterator
func (p *RepositoryPool) RepoIter() (*RepositoryIter, error) {
	it, err := p.library.Repositories(borges.ReadOnlyMode)
	if err != nil {
		return nil, err
	}

	iter := &RepositoryIter{
		pool: p,
		iter: it,
	}

	return iter, nil
}

// RepositoryIter iterates over all repositories in the pool
type RepositoryIter struct {
	pool *RepositoryPool
	iter borges.RepositoryIterator
}

// Next retrieves the next Repository. It returns io.EOF as error
// when there are no more Repositories to retrieve.
func (i *RepositoryIter) Next() (*Repository, error) {
	repo, err := i.iter.Next()
	if err != nil {
		return nil, err
	}

	r := NewRepository(i.pool.library, repo, i.pool.cache)
	return r, nil
}

// Close finished iterator. It's no-op.
func (i *RepositoryIter) Close() error {
	return nil
}
