package gitbase

import (
	"path/filepath"
	"regexp"
)

// RegMatchChars matches a string with a glob expression inside.
var RegMatchChars = regexp.MustCompile(`(^|[^\\])([*[?])`)

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
