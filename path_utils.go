package gitbase

import (
	"path/filepath"
	"regexp"
	"strings"

	git "gopkg.in/src-d/go-git.v4"
)

// RegMatchChars matches a string with a glob expression inside.
var RegMatchChars = regexp.MustCompile(`(^|[^\\])([*[?])`)

// StripPrefix removes the root path from the given path. Root may be a glob.
func StripPrefix(root, path string) (string, error) {
	var err error
	root, err = filepath.Abs(cleanGlob(root))
	if err != nil {
		return "", err
	}

	if !strings.HasSuffix(root, string(filepath.Separator)) {
		root += string(filepath.Separator)
	}

	path, err = filepath.Abs(path)
	if err != nil {
		return "", err
	}

	return strings.TrimPrefix(path, root), nil
}

// cleanGlob removes all the parts of a glob that are not fixed.
func cleanGlob(pattern string) string {
	var parts []string
	for _, part := range strings.Split(pattern, string(filepath.Separator)) {
		if strings.ContainsAny(part, "*?[\\") {
			break
		}

		parts = append(parts, part)
	}

	return strings.Join(parts, string(filepath.Separator))
}

// PatternMatches returns the paths matched and any error found.
func PatternMatches(pattern string) ([]string, error) {
	abs, err := filepath.Abs(pattern)
	if err != nil {
		return nil, err
	}

	matches, err := filepath.Glob(abs)
	if err != nil {
		return nil, err
	}

	return removeDsStore(matches), nil
}

func removeDsStore(matches []string) []string {
	var result []string
	for _, m := range matches {
		if filepath.Base(m) != ".DS_Store" {
			result = append(result, m)
		}
	}
	return result
}

// IsGitRepo checks that the given path is a git repository.
func IsGitRepo(path string) (bool, error) {
	if _, err := git.PlainOpen(path); err != nil {
		if git.ErrRepositoryNotExists == err {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

//IsSivaFile checks that the given file is a siva file.
func IsSivaFile(file string) bool {
	return strings.HasSuffix(file, ".siva")
}
