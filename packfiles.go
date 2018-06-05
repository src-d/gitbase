package gitbase

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"

	"gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/format/idxfile"
	"gopkg.in/src-d/go-git.v4/plumbing/format/packfile"
)

type packRepository struct {
	packs map[plumbing.Hash]packfile.Index
}

func repositoryPackfiles(path string, kind repoKind) (billy.Filesystem, []plumbing.Hash, error) {
	fs, err := repoFilesystem(path, kind)
	if err != nil {
		return nil, nil, err
	}

	fs, err = findDotGit(fs)
	if err != nil {
		return nil, nil, err
	}

	packfiles, err := findPackfiles(fs)
	return fs, packfiles, err
}

type packfileIndex struct {
	packfile plumbing.Hash
	idx      *packfile.Index
}

type repositoryIndex []*packfileIndex

func newRepositoryIndex(path string, kind repoKind) (*repositoryIndex, error) {
	fs, packfiles, err := repositoryPackfiles(path, kind)
	if err != nil {
		return nil, err
	}

	var result repositoryIndex
	for _, p := range packfiles {
		idx, err := openPackfileIndex(fs, path, p)
		if err != nil {
			return nil, err
		}

		result = append(result, &packfileIndex{p, idx})
	}

	return &result, nil
}

func openPackfileIndex(
	fs billy.Filesystem,
	path string,
	hash plumbing.Hash,
) (*packfile.Index, error) {
	path = fs.Join(path, "objects", "pack", fmt.Sprintf("pack-%s.idx", hash))
	f, err := fs.Open(path)
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

func (i repositoryIndex) find(hash plumbing.Hash) (int64, plumbing.Hash, error) {
	for _, idx := range i {
		if entry, ok := idx.idx.LookupHash(hash); ok {
			return int64(entry.Offset), idx.packfile, nil
		}
	}
	return 0, plumbing.NewHash(""), errHashNotInIndex.New(hash)
}

func repoFilesystem(path string, kind repoKind) (billy.Filesystem, error) {
	if kind == sivaRepo {
		localfs := osfs.New(filepath.Dir(path))

		tmpDir, err := ioutil.TempDir(os.TempDir(), "gitbase-siva")
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

	if fi.IsDir() {
		return fs.Chroot(".git")
	}

	return fs, nil
}

func findPackfiles(fs billy.Filesystem) ([]plumbing.Hash, error) {
	packDir := fs.Join("objects", "pack")
	files, err := fs.ReadDir(packDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}

	var packs []plumbing.Hash
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".pack") {
			continue
		}

		n := f.Name()
		h := plumbing.NewHash(n[5 : len(n)-5]) //pack-(hash).pack
		packs = append(packs, h)

	}

	return packs, nil
}

type objectIter struct {
	packs       *packIter
	packObjects *packObjectIter
}

func newObjectIter(
	pool *RepositoryPool,
	typ plumbing.ObjectType,
) *objectIter {
	return &objectIter{packs: newPackIter(pool, typ)}
}

type encodedObject struct {
	object.Object
	RepositoryID string
	Packfile     plumbing.Hash
	Offset       uint64
}

func (i *objectIter) Next() (*encodedObject, error) {
	for {
		if i.packObjects == nil {
			var err error
			i.packObjects, err = i.packs.Next()
			if err != nil {
				return nil, err
			}
		}

		obj, offset, err := i.packObjects.Next()
		if err != nil {
			if err == io.EOF {
				if err := i.packObjects.Close(); err != nil {
					return nil, err
				}

				i.packObjects = nil
				continue
			}
			return nil, err
		}

		return &encodedObject{
			Object:       obj,
			Offset:       offset,
			RepositoryID: i.packs.repo.path,
			Packfile:     i.packs.packfiles[i.packs.packpos],
		}, nil
	}
}

func (i *objectIter) Close() error {
	if i.packObjects != nil {
		return i.packObjects.Close()
	}
	return nil
}

type packIter struct {
	typ  plumbing.ObjectType
	pool *RepositoryPool
	pos  int

	repo *repository

	storage   storer.EncodedObjectStorer
	fs        billy.Filesystem
	packfiles []plumbing.Hash
	packpos   int
}

func newPackIter(pool *RepositoryPool, typ plumbing.ObjectType) *packIter {
	return &packIter{pool: pool, typ: typ}
}

func (i *packIter) Next() (*packObjectIter, error) {
	for {
		if i.repo == nil {
			if i.pos >= len(i.pool.repositories) {
				return nil, io.EOF
			}

			repo := i.pool.repositories[i.pool.idOrder[i.pos]]
			i.repo = &repo
			i.pos++
		}

		if len(i.packfiles) == 0 {
			var err error
			i.fs, i.packfiles, err = repositoryPackfiles(i.repo.path, i.repo.kind)
			if err != nil {
				return nil, err
			}
			i.packpos = 0
			i.storage, err = filesystem.NewStorage(i.fs)
			if err != nil {
				return nil, err
			}
		}

		if i.packpos >= len(i.packfiles) {
			i.packfiles = nil
			i.repo = nil
			continue
		}

		pf := i.packfiles[i.packpos]
		i.packpos++

		return newPackObjectIter(i.repo.path, i.fs, pf, i.storage, i.typ)
	}
}

type packObjectIter struct {
	hash    plumbing.Hash
	close   func() error
	idx     *idxfile.Idxfile
	dec     *packfile.Decoder
	pos     int
	typ     plumbing.ObjectType
	storage storer.EncodedObjectStorer
}

func newPackObjectIter(
	path string,
	fs billy.Filesystem,
	hash plumbing.Hash,
	storage storer.EncodedObjectStorer,
	typ plumbing.ObjectType,
) (*packObjectIter, error) {
	packfilePath := fs.Join(path, "objects", "pack", fmt.Sprintf("pack-%s.pack", hash))
	idxfilePath := fs.Join(path, "objects", "pack", fmt.Sprintf("pack-%s.idx", hash))

	packf, err := fs.Open(packfilePath)
	if err != nil {
		return nil, err
	}

	idxf, err := fs.Open(idxfilePath)
	if err != nil {
		return nil, err
	}
	defer idxf.Close()

	i := idxfile.NewIdxfile()
	if err := idxfile.NewDecoder(idxf).Decode(i); err != nil {
		return nil, err
	}

	decoder, err := packfile.NewDecoder(packfile.NewScanner(packf), storage)
	if err != nil {
		return nil, err
	}

	return &packObjectIter{
		hash:    hash,
		idx:     i,
		dec:     decoder,
		typ:     typ,
		storage: storage,
		close: func() error {
			if err := packf.Close(); err != nil {
				_ = decoder.Close()
				return err
			}

			return decoder.Close()
		},
	}, nil
}

func (i *packObjectIter) Next() (object.Object, uint64, error) {
	for {
		if i.close != nil {
			if err := i.close(); err != nil {
				return nil, 0, err
			}
		}

		if i.pos >= len(i.idx.Entries) {
			return nil, 0, io.EOF
		}

		offset := i.idx.Entries[i.pos].Offset
		i.pos++
		obj, err := i.dec.DecodeObjectAt(int64(offset))
		if err != nil {
			return nil, 0, err
		}

		if obj.Type() != i.typ {
			continue
		}

		decodedObj, err := object.DecodeObject(i.storage, obj)
		if err != nil {
			return nil, 0, err
		}

		return decodedObj, offset, nil
	}
}

func (i *packObjectIter) Close() error { return i.close() }

type objectDecoder struct {
	repo     string
	packfile plumbing.Hash
	decoder  *packfile.Decoder
	storage  storer.EncodedObjectStorer
	close    func() error
}

func newObjectDecoder(
	repo repository,
	hash plumbing.Hash,
) (*objectDecoder, error) {
	fs, err := repoFilesystem(repo.path, repo.kind)
	if err != nil {
		return nil, err
	}

	fs, err = findDotGit(fs)
	if err != nil {
		return nil, err
	}

	packfilePath := fs.Join(repo.path, "objects", "pack", fmt.Sprintf("pack-%s.pack", hash))
	packf, err := fs.Open(packfilePath)
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

	return &objectDecoder{
		repo:     repo.path,
		packfile: hash,
		decoder:  decoder,
		storage:  storage,
		close: func() error {
			if err := packf.Close(); err != nil {
				_ = decoder.Close()
				return err
			}

			return decoder.Close()
		},
	}, nil
}

func (d *objectDecoder) equals(repo string, packfile plumbing.Hash) bool {
	return d.repo == repo && d.packfile == packfile
}

func (d *objectDecoder) get(offset int64) (object.Object, error) {
	encodedObj, err := d.decoder.DecodeObjectAt(offset)
	if err != nil {
		return nil, err
	}

	return object.DecodeObject(d.storage, encodedObj)
}
