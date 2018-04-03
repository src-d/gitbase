package generator

import (
	"bytes"
	"text/template"
)

// Commit takes a commit and builds the source file from tmplPath. It complies with type File signature.
func Commit(fileToParse, samplesDir, outPath, tmplPath, tmplName, commit string) error {
	buf := &bytes.Buffer{}
	t := template.Must(template.New(tmplName).ParseFiles(tmplPath))
	if err := t.Execute(buf, commit); err != nil {
		return err
	}

	return formatedWrite(outPath, buf.Bytes())
}
