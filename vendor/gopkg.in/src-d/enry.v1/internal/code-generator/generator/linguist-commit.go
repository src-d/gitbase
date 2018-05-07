package generator

import (
	"bytes"
)

// Commit takes a commit and builds the source file from tmplPath. It complies with type File signature.
func Commit(fileToParse, samplesDir, outPath, tmplPath, tmplName, commit string) error {
	buf := &bytes.Buffer{}
	if err := executeTemplate(buf, tmplName, tmplPath, commit, nil, nil); err != nil {
		return err
	}
	return formatedWrite(outPath, buf.Bytes())
}
