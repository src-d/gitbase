package gitbase

import (
	"io"
	"os"

	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/filesystem/dotgit"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"

	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/format/idxfile"
	"gopkg.in/src-d/go-git.v4/plumbing/format/objfile"
	"gopkg.in/src-d/go-git.v4/plumbing/format/packfile"
)

func repositoryPackfiles(fs billy.Filesystem) (*dotgit.DotGit, []plumbing.Hash, error) {
	fs, err := findDotGit(fs)
	if err != nil {
		return nil, nil, err
	}

	dot := dotgit.New(fs)
	packfiles, err := dot.ObjectPacks()
	if err != nil {
		return nil, nil, err
	}

	return dot, packfiles, nil
}

type packfileIndex struct {
	packfile plumbing.Hash
	idx      idxfile.Index
}

type repositoryIndex struct {
	dir       *dotgit.DotGit
	indexes   []*packfileIndex
	closeFunc func()
}

func newRepositoryIndex(repo *Repository) (*repositoryIndex, error) {
	fs, err := repo.FS()
	if err != nil {
		return nil, err
	}

	var closeFunc func()
	if s, ok := fs.(sivafs.SivaSync); ok {
		closeFunc = func() { s.Sync() }
	}

	dot, packfiles, err := repositoryPackfiles(fs)
	if err != nil {
		return nil, err
	}

	ri := &repositoryIndex{dir: dot, closeFunc: closeFunc}
	for _, p := range packfiles {
		idx, err := openPackfileIndex(dot, p)
		if err != nil {
			return nil, err
		}

		ri.indexes = append(ri.indexes, &packfileIndex{p, idx})
	}

	return ri, nil
}

func openPackfileIndex(
	dotGit *dotgit.DotGit,
	hash plumbing.Hash,
) (*idxfile.MemoryIndex, error) {
	f, err := dotGit.ObjectPackIdx(hash)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	idx := idxfile.NewMemoryIndex()
	if err := idxfile.NewDecoder(f).Decode(idx); err != nil {
		return nil, err
	}

	return idx, nil
}

var errHashNotInIndex = errors.NewKind("object hash %s is not in repository")

func (i *repositoryIndex) find(hash plumbing.Hash) (int64, plumbing.Hash, error) {
	for _, idx := range i.indexes {
		ofs, err := idx.idx.FindOffset(hash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		if err != nil {
			return 0, plumbing.ZeroHash, err
		}

		return ofs, idx.packfile, nil
	}

	ok, err := i.isUnpacked(hash)
	if err != nil || ok {
		return -1, plumbing.ZeroHash, err
	}

	return -1, plumbing.ZeroHash, errHashNotInIndex.New(hash)
}

func (i *repositoryIndex) isUnpacked(hash plumbing.Hash) (bool, error) {
	f, err := i.dir.Object(hash)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}
	if err := f.Close(); err != nil {
		return false, err
	}

	return true, nil
}

func (i *repositoryIndex) Close() {
	if i.closeFunc != nil {
		i.closeFunc()
	}
}

func findDotGit(fs billy.Filesystem) (billy.Filesystem, error) {
	fi, err := fs.Stat(".git")
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if fi != nil && fi.IsDir() {
		return fs.Chroot(".git")
	}

	return fs, nil
}

func getUnpackedObject(repo *Repository, hash plumbing.Hash) (o object.Object, err error) {
	fs, err := repo.FS()
	if err != nil {
		return nil, err
	}

	fs, err = findDotGit(fs)
	if err != nil {
		return nil, err
	}

	dot := dotgit.New(fs)

	f, err := dot.Object(hash)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, plumbing.ErrObjectNotFound
		}

		return nil, err
	}

	defer ioutil.CheckClose(f, &err)

	storage := filesystem.NewStorage(fs, repo.Cache())

	obj := storage.NewEncodedObject()
	r, err := objfile.NewReader(f)
	if err != nil {
		return nil, err
	}

	defer ioutil.CheckClose(r, &err)

	t, size, err := r.Header()
	if err != nil {
		return nil, err
	}

	obj.SetType(t)
	obj.SetSize(size)
	w, err := obj.Writer()
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(w, r)

	o, err = object.DecodeObject(storage, obj)
	return
}

type repoObjectDecoder struct {
	repo      string
	hash      plumbing.Hash
	packfile  *packfile.Packfile
	storage   storer.EncodedObjectStorer
	closeFunc func()
}

func newRepoObjectDecoder(
	repo *Repository,
	hash plumbing.Hash,
) (*repoObjectDecoder, error) {
	fs, err := repo.FS()
	if err != nil {
		return nil, err
	}

	var closeFunc func()
	if f, ok := fs.(sivafs.SivaSync); ok {
		closeFunc = func() { f.Sync() }
	}

	fs, err = findDotGit(fs)
	if err != nil {
		return nil, err
	}

	dot := dotgit.New(fs)
	packf, err := dot.ObjectPack(hash)
	if err != nil {
		return nil, err
	}

	storage := filesystem.NewStorage(fs, repo.Cache())

	idx, err := openPackfileIndex(dot, hash)
	if err != nil {
		return nil, err
	}

	packfile := packfile.NewPackfile(idx, fs, packf)

	return &repoObjectDecoder{
		repo:      repo.ID(),
		hash:      hash,
		packfile:  packfile,
		storage:   storage,
		closeFunc: closeFunc,
	}, nil
}

func (d *repoObjectDecoder) equals(repo string, hash plumbing.Hash) bool {
	return d.repo == repo && d.hash == hash
}

func (d *repoObjectDecoder) get(offset int64) (object.Object, error) {
	encodedObj, err := d.packfile.GetByOffset(offset)
	if err != nil {
		return nil, err
	}

	return object.DecodeObject(d.storage, encodedObj)
}

func (d *repoObjectDecoder) Close() error {
	if d.closeFunc != nil {
		d.closeFunc()
	}

	return d.packfile.Close()
}

type objectDecoder struct {
	pool    *RepositoryPool
	decoder *repoObjectDecoder
}

func newObjectDecoder(pool *RepositoryPool) *objectDecoder {
	return &objectDecoder{pool: pool}
}

func (d *objectDecoder) decode(
	repository string,
	packfile plumbing.Hash,
	offset int64,
	hash plumbing.Hash,
) (object.Object, error) {
	repo, err := d.pool.GetRepo(repository)
	if err != nil {
		return nil, err
	}

	if offset >= 0 {
		if d.decoder == nil || !d.decoder.equals(repository, packfile) {
			if d.decoder != nil {
				if err := d.decoder.Close(); err != nil {
					return nil, err
				}
			}

			var err error
			d.decoder, err = newRepoObjectDecoder(repo, packfile)
			if err != nil {
				return nil, err
			}
		}

		return d.decoder.get(offset)
	}

	return getUnpackedObject(repo, hash)
}

func (d *objectDecoder) Close() error {
	if d.decoder != nil {
		return d.decoder.Close()
	}

	return nil
}
