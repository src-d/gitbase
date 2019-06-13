package gitbase

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"io"
	"sort"
	"strings"

	"gopkg.in/src-d/go-git.v4/plumbing"
)

type checksumable struct {
	pool *RepositoryPool
}

func (c *checksumable) Checksum() (string, error) {
	hash := sha1.New()
	iter, err := c.pool.RepoIter()
	if err != nil {
		return "", err
	}
	defer iter.Close()

	var checksums checksums
	for {
		hash.Reset()

		repo, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		bytes, err := readChecksum(repo)
		if err != nil {
			return "", err
		}

		if _, err = hash.Write(bytes); err != nil {
			return "", err
		}

		bytes, err = readRefs(repo)
		if err != nil {
			return "", err
		}

		if _, err = hash.Write(bytes); err != nil {
			return "", err
		}

		c := checksum{
			name: repo.ID(),
			hash: hash.Sum(nil),
		}

		checksums = append(checksums, c)
	}

	sort.Stable(checksums)
	hash.Reset()

	for _, c := range checksums {
		if _, err = hash.Write(c.hash); err != nil {
			return "", err
		}
	}

	return base64.StdEncoding.EncodeToString(hash.Sum(nil)), nil
}

func readChecksum(r *Repository) ([]byte, error) {
	fs, err := r.FS()
	if err != nil {
		return nil, err
	}

	dot, packfiles, err := repositoryPackfiles(fs)
	if err != nil {
		return nil, err
	}

	var result []byte
	for _, p := range packfiles {
		f, err := dot.ObjectPack(p)
		if err != nil {
			return nil, err
		}

		if _, err = f.Seek(-20, io.SeekEnd); err != nil {
			return nil, err
		}

		var checksum = make([]byte, 20)
		if _, err = io.ReadFull(f, checksum); err != nil {
			return nil, err
		}

		if err = f.Close(); err != nil {
			return nil, err
		}

		result = append(result, checksum...)
	}

	return result, nil
}

type reference struct {
	name string
	hash string
}

type references []reference

type byHashAndName []reference

func (b byHashAndName) Len() int      { return len(b) }
func (b byHashAndName) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byHashAndName) Less(i, j int) bool {
	if cmp := strings.Compare(b[i].hash, b[j].hash); cmp != 0 {
		return cmp < 0
	}
	return strings.Compare(b[i].name, b[j].name) < 0
}

type checksum struct {
	name string
	hash []byte
}

type checksums []checksum

func (b checksums) Len() int      { return len(b) }
func (b checksums) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b checksums) Less(i, j int) bool {
	if cmp := bytes.Compare(b[i].hash, b[j].hash); cmp != 0 {
		return cmp < 0
	}
	return strings.Compare(b[i].name, b[j].name) < 0
}

func readRefs(repo *Repository) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	refs, err := repo.References()
	if err != nil {
		return nil, err
	}

	var references []reference
	err = refs.ForEach(func(r *plumbing.Reference) error {
		references = append(references, reference{
			name: string(r.Name()),
			hash: r.Hash().String(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Stable(byHashAndName(references))

	for _, r := range references {
		buf.WriteString(r.name)
		buf.WriteString(r.hash)
	}

	return buf.Bytes(), nil
}
