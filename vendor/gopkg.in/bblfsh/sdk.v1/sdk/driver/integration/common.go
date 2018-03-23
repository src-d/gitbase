package integration

import (
	"path"
	"strings"
)

// RemoveExtension removes the last extension from a file
func RemoveExtension(filename string) string {
	i := strings.LastIndex(filename, path.Ext(filename))
	return filename[:i]
}

