package gitquery

import (
	"io"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Repository struct holds an initialized repository and its ID
type Repository struct {
	ID   string
	Repo *git.Repository
}

// NewRepository creates and initializes a new Repository structure
func NewRepository(id string, repo *git.Repository) Repository {
	return Repository{
		ID:   id,
		Repo: repo,
	}
}

// RepositoryPool holds a pool of initialized git repositories and
// functionality to open and iterate them.
type RepositoryPool struct {
	repositories []Repository
}

// NewRepositoryPool initializes a new RepositoryPool
func NewRepositoryPool() RepositoryPool {
	return RepositoryPool{}
}

// Add inserts a new repository in the pool
func (p *RepositoryPool) Add(id string, repo *git.Repository) {
	repository := NewRepository(id, repo)
	p.repositories = append(p.repositories, repository)
}

// AddGit opens a new git repository and adds it to the pool. It
// also sets its path as ID.
func (p *RepositoryPool) AddGit(path string) (string, error) {
	repo, err := git.PlainOpen(path)
	if err != nil {
		return "", err
	}

	p.Add(path, repo)

	return path, nil
}

// GetPos retrieves a repository at a given position. It returns false
// as second return value if the position is out of bounds.
func (p *RepositoryPool) GetPos(pos int) (*Repository, bool) {
	if pos >= len(p.repositories) {
		return nil, false
	}

	return &p.repositories[pos], true
}

// RepoIter creates a new Repository iterator
func (p *RepositoryPool) RepoIter() (*RepositoryIter, error) {
	iter := &RepositoryIter{
		pos:  0,
		pool: p,
	}

	return iter, nil
}

// RepositoryIter iterates over all repositories in the pool
type RepositoryIter struct {
	pos  int
	pool *RepositoryPool
}

// Next retrieves the next Repository. It returns io.EOF as error
// when there are no more Repositories to retrieve.
func (i *RepositoryIter) Next() (*Repository, error) {
	r, ok := i.pool.GetPos(i.pos)
	if !ok {
		return nil, io.EOF
	}

	i.pos++

	return r, nil
}

// Close finished iterator. It's no-op.
func (i *RepositoryIter) Close() error {
	return nil
}

// RowRepoIterImplementation is the interface needed by each iterator
// implementation
type RowRepoIterImplementation interface {
	InitRepository(Repository) error
	Next() (sql.Row, error)
	Close() error
}

// RowRepoIter is used as the base to iterate over all the repositories
// in the pool. It needs three functions that execute specific code per
// implemented iterator.
type RowRepoIter struct {
	repositoryIter *RepositoryIter
	repository     *Repository
	implementation RowRepoIterImplementation
}

// NewRowRepoIter initializes a new repository iterator.
//
// * pool: is a RepositoryPool we want to iterate
// * impl: implementation with RowRepoIterImplementation interface
//     * InitRepository: called when a new repository is about to be iterated,
//         initialize its iterator there
//     * Next: called for each row
//     * Close: called when a repository finished iterating
func NewRowRepoIter(pool *RepositoryPool,
	impl RowRepoIterImplementation) (RowRepoIter, error) {

	rIter, err := pool.RepoIter()
	if err != nil {
		return RowRepoIter{}, err
	}

	repo := RowRepoIter{
		repositoryIter: rIter,
		implementation: impl,
	}

	err = repo.nextRepository()
	if err != nil {
		return RowRepoIter{}, err
	}

	return repo, nil
}

// nextRepository is called to initialize the next repository. It is called
// when the iterator is created and when the previous repository finished
// iterating.
func (i *RowRepoIter) nextRepository() error {
	repo, err := i.repositoryIter.Next()
	if err != nil {
		return err
	}

	i.repository = repo
	err = i.implementation.InitRepository(*repo)

	return err
}

// Next gets the next row
func (i *RowRepoIter) Next() (sql.Row, error) {
	for {
		row, err := i.implementation.Next()

		switch err {
		case nil:
			return row, nil

		case io.EOF:
			i.Close()

			err := i.nextRepository()
			if err != nil {
				return nil, err
			}

		default:
			return nil, err
		}
	}

	return nil, nil
}

// Close called to close the iterator
func (i *RowRepoIter) Close() error {
	return i.implementation.Close()

	return nil
}
