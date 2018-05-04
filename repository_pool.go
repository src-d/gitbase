package gitbase

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-billy-siva.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Repository struct holds an initialized repository and its ID
type Repository struct {
	ID   string
	Repo *git.Repository
}

// NewRepository creates and initializes a new Repository structure
func NewRepository(id string, repo *git.Repository) *Repository {
	return &Repository{
		ID:   id,
		Repo: repo,
	}
}

// NewRepositoryFromPath creates and initializes a new Repository structure
// and initializes a go-git repository
func NewRepositoryFromPath(id, path string) (*Repository, error) {
	repo, err := git.PlainOpen(path)
	if err != nil {
		return nil, err
	}

	return NewRepository(id, repo), nil
}

// NewSivaRepositoryFromPath creates and initializes a new Repository structure
// and initializes a go-git repository backed by a siva file.
func NewSivaRepositoryFromPath(id, path string) (*Repository, error) {
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

	sto, err := filesystem.NewStorage(fs)
	if err != nil {
		return nil, err
	}

	repo, err := git.Open(sto, nil)
	if err != nil {
		return nil, err
	}

	return NewRepository(id, repo), nil
}

type repository struct {
	kind repoKind
	path string
}

type repoKind byte

const (
	gitRepo repoKind = iota
	sivaRepo
)

// RepositoryPool holds a pool git repository paths and
// functionality to open and iterate them.
type RepositoryPool struct {
	repositories map[string]repository
	idOrder      []string
}

// NewRepositoryPool initializes a new RepositoryPool
func NewRepositoryPool() *RepositoryPool {
	return &RepositoryPool{
		repositories: make(map[string]repository),
	}
}

// Add inserts a new repository in the pool
func (p *RepositoryPool) Add(id, path string, kind repoKind) {
	if _, ok := p.repositories[id]; !ok {
		p.idOrder = append(p.idOrder, id)
	}

	p.repositories[id] = repository{kind, path}
}

// AddGit checks if a git repository can be opened and adds it to the pool. It
// also sets its path as ID.
func (p *RepositoryPool) AddGit(path string) (string, error) {
	_, err := git.PlainOpen(path)
	if err != nil {
		return "", err
	}

	p.Add(path, path, gitRepo)

	return path, nil
}

// AddDir adds all direct subdirectories from path as git repos.
func (p *RepositoryPool) AddDir(path string) error {
	dirs, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, f := range dirs {
		if f.IsDir() {
			name := filepath.Join(path, f.Name())
			if _, err := p.AddGit(name); err != nil {
				logrus.WithField("path", name).Error("repository could not be opened")
			} else {
				logrus.WithField("path", name).Debug("repository added")
			}
		}
	}

	return nil
}

// AddSivaDir adds to the repository pool all siva files found inside the given
// directory and in its children directories, but not the children of those
// directories.
func (p *RepositoryPool) AddSivaDir(path string) error {
	return p.addSivaDir(path, path, true)
}

func (p *RepositoryPool) addSivaDir(root, path string, recursive bool) error {
	dirs, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, f := range dirs {
		if f.IsDir() && recursive {
			dirPath := filepath.Join(path, f.Name())
			if err := p.addSivaDir(root, dirPath, false); err != nil {
				return err
			}
		} else {
			p.addSivaFile(root, path, f)
		}
	}

	return nil
}

// addSivaFile adds to the pool the given file if it's a siva repository,
// that is, has the .siva extension.
func (p *RepositoryPool) addSivaFile(root, path string, f os.FileInfo) {
	var relativeFileName string
	if root == path {
		relativeFileName = f.Name()
	} else {
		relPath := strings.TrimPrefix(strings.Replace(path, root, "", -1), "/\\")
		relativeFileName = filepath.Join(relPath, f.Name())
	}

	if strings.HasSuffix(f.Name(), ".siva") {
		path := filepath.Join(path, f.Name())
		p.Add(path, path, sivaRepo)
		logrus.WithField("file", relativeFileName).Debug("repository added")
	} else {
		logrus.WithField("file", relativeFileName).Warn("found a non-siva file, skipping")
	}
}

var errInvalidRepoKind = errors.NewKind("invalid repo kind: %d")

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

	r := p.repositories[id]
	var repo *Repository
	var err error
	switch r.kind {
	case gitRepo:
		repo, err = NewRepositoryFromPath(id, r.path)
	case sivaRepo:
		repo, err = NewSivaRepositoryFromPath(id, r.path)
	default:
		err = errInvalidRepoKind.New(r.kind)
	}

	if err != nil {
		return nil, err
	}

	return repo, nil
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

// RowRepoIter is the interface needed by each iterator
// implementation
type RowRepoIter interface {
	NewIterator(*Repository) (RowRepoIter, error)
	Next() (sql.Row, error)
	Close() error
}

// RowRepoIter is used as the base to iterate over all the repositories
// in the pool
type rowRepoIter struct {
	mu sync.Mutex

	currRepoIter   RowRepoIter
	repositoryIter *RepositoryIter
	iter           RowRepoIter
	session        *Session
	ctx            *sql.Context
}

// NewRowRepoIter initializes a new repository iterator.
//
// * ctx: it should contain a gitbase.Session
// * iter: specific RowRepoIter interface
//     * NewIterator: called when a new repository is about to be iterated,
//         returns a new RowRepoIter
//     * Next: called for each row
//     * Close: called when a repository finished iterating
func NewRowRepoIter(
	ctx *sql.Context,
	iter RowRepoIter,
) (sql.RowIter, error) {
	s, ok := ctx.Session.(*Session)
	if !ok || s == nil {
		return nil, ErrInvalidGitbaseSession.New(ctx.Session)
	}

	rIter, err := s.Pool.RepoIter()
	if err != nil {
		return nil, err
	}

	repoIter := rowRepoIter{
		currRepoIter:   nil,
		repositoryIter: rIter,
		iter:           iter,
		session:        s,
		ctx:            ctx,
	}

	return &repoIter, nil
}

// Next gets the next row
func (i *rowRepoIter) Next() (sql.Row, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for {
		select {
		case <-i.ctx.Done():
			return nil, ErrSessionCanceled.New()

		default:
			if i.currRepoIter == nil {
				repo, err := i.repositoryIter.Next()
				if err != nil {
					if err == io.EOF {
						return nil, io.EOF
					}

					if i.session.SkipGitErrors {
						continue
					}

					return nil, err
				}

				i.currRepoIter, err = i.iter.NewIterator(repo)
				if err != nil {
					return nil, err
				}
			}

			row, err := i.currRepoIter.Next()
			if err != nil {
				if err == io.EOF {
					i.currRepoIter.Close()
					i.currRepoIter = nil
					continue
				}

				if i.session.SkipGitErrors {
					continue
				}

				return nil, err
			}

			return row, nil
		}
	}
}

// Close called to close the iterator
func (i *rowRepoIter) Close() error {
	if i.currRepoIter != nil {
		i.currRepoIter.Close()
	}
	return i.iter.Close()
}
