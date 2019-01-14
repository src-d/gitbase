package gitbase

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync/atomic"

	"gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
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

// Repository struct holds an initialized repository and its ID
type Repository struct {
	*git.Repository
	ID string

	// function used to close underlying file descriptors
	closeFunc func()
}

// NewRepository creates and initializes a new Repository structure
func NewRepository(id string, repo *git.Repository, closeFunc func()) *Repository {
	return &Repository{
		Repository: repo,
		ID:         id,
		closeFunc:  closeFunc,
	}
}

// Close closes all opened files in the repository.
func (r *Repository) Close() {
	if r != nil {
		if r.Repository != nil {
			f, ok := r.Storer.(*filesystem.Storage)
			if ok {
				// The only type of error returned is "file already closed" and
				// we don't want to do anything with it.
				f.Close()
			}
		}

		if r.closeFunc != nil {
			r.closeFunc()
		}
	}
}

// NewRepositoryFromPath creates and initializes a new Repository structure
// and initializes a go-git repository
func NewRepositoryFromPath(id, path string, cache cache.Object) (*Repository, error) {
	var wt billy.Filesystem
	fs := osfs.New(path)
	f, err := fs.Stat(git.GitDirName)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if f != nil && f.IsDir() {
		wt = fs
		fs, err = fs.Chroot(git.GitDirName)
		if err != nil {
			return nil, err
		}
	}

	sto := filesystem.NewStorageWithOptions(fs, cache, gitStorerOptions)
	repo, err := git.Open(sto, wt)
	if err != nil {
		return nil, err
	}

	return NewRepository(id, repo, nil), nil
}

// NewSivaRepositoryFromPath creates and initializes a new Repository structure
// and initializes a go-git repository backed by a siva file.
func NewSivaRepositoryFromPath(id, path string, cache cache.Object) (*Repository, error) {
	localfs := osfs.New(filepath.Dir(path))

	tmpDir, err := ioutil.TempDir(os.TempDir(), "gitbase-siva")
	if err != nil {
		return nil, err
	}

	tmpfs := osfs.New(tmpDir)

	fs, err := sivafs.NewFilesystem(localfs, filepath.Base(path), tmpfs)
	if err != nil {
		return nil, err
	}

	sto := filesystem.NewStorageWithOptions(fs, cache, gitStorerOptions)

	repo, err := git.Open(sto, nil)
	if err != nil {
		return nil, err
	}

	closeFunc := func() { fs.Sync() }

	return NewRepository(id, repo, closeFunc), nil
}

type repository interface {
	ID() string
	Repo() (*Repository, error)
	FS() (billy.Filesystem, error)
	Path() string
	Cache() cache.Object
}

type gitRepository struct {
	id    string
	path  string
	cache cache.Object
}

func gitRepo(id, path string, cache cache.Object) repository {
	return &gitRepository{id, path, cache}
}

func (r *gitRepository) ID() string {
	return r.id
}

func (r *gitRepository) Repo() (*Repository, error) {
	return NewRepositoryFromPath(r.id, r.path, r.cache)
}

func (r *gitRepository) FS() (billy.Filesystem, error) {
	return osfs.New(r.path), nil
}

func (r *gitRepository) Path() string {
	return r.path
}

func (r *gitRepository) Cache() cache.Object {
	return r.cache
}

type sivaRepository struct {
	id    string
	path  string
	cache cache.Object
}

func sivaRepo(id, path string, cache cache.Object) repository {
	return &sivaRepository{id, path, cache}
}

func (r *sivaRepository) ID() string {
	return r.id
}

func (r *sivaRepository) Repo() (*Repository, error) {
	return NewSivaRepositoryFromPath(r.id, r.path, r.cache)
}

func (r *sivaRepository) FS() (billy.Filesystem, error) {
	localfs := osfs.New(filepath.Dir(r.path))

	tmpDir, err := ioutil.TempDir(os.TempDir(), "gitbase-siva")
	if err != nil {
		return nil, err
	}

	tmpfs := osfs.New(tmpDir)

	return sivafs.NewFilesystem(localfs, filepath.Base(r.path), tmpfs)
}

func (r *sivaRepository) Path() string {
	return r.path
}

func (r *sivaRepository) Cache() cache.Object {
	return r.cache
}

// RepositoryPool holds a pool git repository paths and
// functionality to open and iterate them.
type RepositoryPool struct {
	repositories map[string]repository
	idOrder      []string
	cache        cache.Object
}

// NewRepositoryPool initializes a new RepositoryPool with LRU cache.
func NewRepositoryPool(maxCacheSize cache.FileSize) *RepositoryPool {
	return &RepositoryPool{
		repositories: make(map[string]repository),
		cache:        cache.NewObjectLRU(maxCacheSize),
	}
}

// Add inserts a new repository in the pool.
func (p *RepositoryPool) Add(repo repository) error {
	id := repo.ID()
	if r, ok := p.repositories[id]; ok {
		return errRepoAlreadyRegistered.New(r.Path())
	}

	p.idOrder = append(p.idOrder, id)
	p.repositories[id] = repo

	return nil
}

// AddGit adds a git repository to the pool. It also sets its path as ID.
func (p *RepositoryPool) AddGit(path string) error {
	return p.AddGitWithID(path, path)
}

// AddGitWithID adds a git repository to the pool. ID should be specified.
func (p *RepositoryPool) AddGitWithID(id, path string) error {
	return p.Add(gitRepo(id, path, p.cache))
}

// AddSivaFile adds a siva file to the pool. It also sets its path as ID.
func (p *RepositoryPool) AddSivaFile(path string) error {
	return p.Add(sivaRepo(path, path, p.cache))
}

// AddSivaFileWithID adds a siva file to the pool. ID should be specified.
func (p *RepositoryPool) AddSivaFileWithID(id, path string) error {
	return p.Add(sivaRepo(id, path, p.cache))
}

// GetPos retrieves a repository at a given position. If the position is
// out of bounds it returns io.EOF.
func (p *RepositoryPool) GetPos(pos int) (*Repository, error) {
	if pos >= len(p.repositories) {
		return nil, io.EOF
	}

	id := p.idOrder[pos]
	if id == "" {
		return nil, io.EOF
	}

	return p.GetRepo(id)
}

// ErrPoolRepoNotFound is returned when a repository id is not present in the pool.
var ErrPoolRepoNotFound = errors.NewKind("repository id %s not found in the pool")

// GetRepo returns a repository with the given id from the pool.
func (p *RepositoryPool) GetRepo(id string) (*Repository, error) {
	r, ok := p.repositories[id]
	if !ok {
		return nil, ErrPoolRepoNotFound.New(id)
	}

	return r.Repo()
}

// RepoIter creates a new Repository iterator
func (p *RepositoryPool) RepoIter() (*RepositoryIter, error) {
	iter := &RepositoryIter{
		pool: p,
	}
	atomic.StoreInt32(&iter.pos, 0)

	return iter, nil
}

// RepositoryIter iterates over all repositories in the pool
type RepositoryIter struct {
	pos  int32
	pool *RepositoryPool
}

// Next retrieves the next Repository. It returns io.EOF as error
// when there are no more Repositories to retrieve.
func (i *RepositoryIter) Next() (*Repository, error) {
	pos := int(atomic.LoadInt32(&i.pos))
	r, err := i.pool.GetPos(pos)
	atomic.AddInt32(&i.pos, 1)

	return r, err
}

// Close finished iterator. It's no-op.
func (i *RepositoryIter) Close() error {
	return nil
}
