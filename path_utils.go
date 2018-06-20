package gitbase

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// RegMatchChars matches a string with a glob expression inside.
var RegMatchChars = regexp.MustCompile(`(^|[^\\])([*[?])`)

// PatternMatches returns the depth of the fixed part of a patters, the paths
// matched and any error found.
func PatternMatches(pattern string) (int, []string, error) {
	abs, err := filepath.Abs(pattern)
	if err != nil {
		return 0, nil, err
	}

	matches, err := filepath.Glob(abs)
	if err != nil {
		return 0, nil, err
	}

	depth := PatternPrefixDepth(abs)

	return depth, matches, nil
}

// PatternPrefixDepth returns the number of directories before the first
// glob expression is found.
func PatternPrefixDepth(pattern string) int {
	if pattern == "" {
		return 0
	}

	parts := SplitPath(pattern)

	for i, part := range parts {
		if RegMatchChars.MatchString(part) {
			return i
		}
	}

	return len(parts)
}

// IDFromPath returns a repository ID from a path stripping a number of
// directories from it.
func IDFromPath(prefix int, path string) string {
	parts := SplitPath(path)

	if prefix >= len(parts) {
		return path
	}

	return filepath.Join(parts[prefix:]...)
}

// SplitPath slices a path in its components.
func SplitPath(path string) []string {
	parts := strings.Split(path, string(os.PathSeparator))
	saneParts := make([]string, 0, len(parts))

	for _, p := range parts {
		if p != "" {
			saneParts = append(saneParts, p)
		}
	}

	return saneParts
}
