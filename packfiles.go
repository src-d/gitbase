package gitbase

import (
	"io"
	stdioutil "io/ioutil"
	"os"
	"path/filepath"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/filesystem/dotgit"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"

	"gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/format/idxfile"
	"gopkg.in/src-d/go-git.v4/plumbing/format/objfile"
	"gopkg.in/src-d/go-git.v4/plumbing/format/packfile"
)

type packRepository struct {
	packs map[plumbing.Hash]packfile.Index
}

func repositoryPackfiles(path string, kind repoKind) (*dotgit.DotGit, []plumbing.Hash, error) {
	fs, err := repoFilesystem(path, kind)
	if err != nil {
		return nil, nil, err
	}

	fs, err = findDotGit(fs)
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
	idx      *packfile.Index
}

type repositoryIndex struct {
	dir     *dotgit.DotGit
	indexes []*packfileIndex
}

func newRepositoryIndex(path string, kind repoKind) (*repositoryIndex, error) {
	dot, packfiles, err := repositoryPackfiles(path, kind)
	if err != nil {
		return nil, err
	}

	ri := &repositoryIndex{dir: dot}
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
) (*packfile.Index, error) {
	f, err := dotGit.ObjectPackIdx(hash)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	idx := idxfile.NewIdxfile()
	if err := idxfile.NewDecoder(f).Decode(idx); err != nil {
		return nil, err
	}

	return packfile.NewIndexFromIdxFile(idx), nil
}

var errHashNotInIndex = errors.NewKind("object hash %s is not in repository")

func (i *repositoryIndex) find(hash plumbing.Hash) (int64, plumbing.Hash, error) {
	for _, idx := range i.indexes {
		if entry, ok := idx.idx.LookupHash(hash); ok {
			return int64(entry.Offset), idx.packfile, nil
		}
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

func repoFilesystem(path string, kind repoKind) (billy.Filesystem, error) {
	if kind == sivaRepo {
		localfs := osfs.New(filepath.Dir(path))

		tmpDir, err := stdioutil.TempDir(os.TempDir(), "gitbase-siva")
		if err != nil {
			return nil, err
		}

		tmpfs := osfs.New(tmpDir)

		return sivafs.NewFilesystem(localfs, filepath.Base(path), tmpfs)
	}

	return osfs.New(path), nil
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

func getUnpackedObject(repo repository, hash plumbing.Hash) (o object.Object, err error) {
	fs, err := repoFilesystem(repo.path, repo.kind)
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

	storage, err := filesystem.NewStorage(fs)
	if err != nil {
		return nil, err
	}

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
	repo     string
	packfile plumbing.Hash
	decoder  *packfile.Decoder
	storage  storer.EncodedObjectStorer
}

func newRepoObjectDecoder(
	repo repository,
	hash plumbing.Hash,
) (*repoObjectDecoder, error) {
	fs, err := repoFilesystem(repo.path, repo.kind)
	if err != nil {
		return nil, err
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

	storage, err := filesystem.NewStorage(fs)
	if err != nil {
		_ = packf.Close()
		return nil, err
	}

	decoder, err := packfile.NewDecoder(packfile.NewScanner(packf), storage)
	if err != nil {
		_ = packf.Close()
		return nil, err
	}

	return &repoObjectDecoder{
		repo:     repo.path,
		packfile: hash,
		decoder:  decoder,
		storage:  storage,
	}, nil
}

func (d *repoObjectDecoder) equals(repo string, packfile plumbing.Hash) bool {
	return d.repo == repo && d.packfile == packfile
}

func (d *repoObjectDecoder) get(offset int64) (object.Object, error) {
	encodedObj, err := d.decoder.DecodeObjectAt(offset)
	if err != nil {
		return nil, err
	}

	return object.DecodeObject(d.storage, encodedObj)
}

func (d *repoObjectDecoder) Close() error {
	return d.decoder.Close()
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
	if offset >= 0 {
		if d.decoder == nil || !d.decoder.equals(repository, packfile) {
			if d.decoder != nil {
				if err := d.decoder.Close(); err != nil {
					return nil, err
				}
			}

			var err error
			d.decoder, err = newRepoObjectDecoder(d.pool.repositories[repository], packfile)
			if err != nil {
				return nil, err
			}
		}

		return d.decoder.get(offset)
	}

	return getUnpackedObject(d.pool.repositories[repository], hash)
}

func (d *objectDecoder) Close() error {
	if d.decoder != nil {
		return d.decoder.Close()
	}
	return nil
}
