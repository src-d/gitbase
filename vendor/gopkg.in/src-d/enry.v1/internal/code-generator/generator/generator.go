package generator

import (
	"bytes"
	"go/format"
	"io"
	"io/ioutil"
	"path/filepath"
	"text/template"
)

// File is the function's type that generate source file from a file to be parsed, linguist's samples dir and a template.
type File func(fileToParse, samplesDir, outPath, tmplPath, tmplName, commit string) error

func formatedWrite(outPath string, source []byte) error {
	formatedSource, err := format.Source(source)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(outPath, formatedSource, 0666); err != nil {
		return err
	}
	return nil
}

func executeTemplate(w io.Writer, name, path, commit string, fmap template.FuncMap, data interface{}) error {
	getCommit := func() string {
		return commit
	}

	buf := bytes.NewBuffer(nil)

	const headerTmpl = "header.go.tmpl"

	headerPath := filepath.Join(filepath.Dir(path), headerTmpl)

	h := template.Must(template.New(headerTmpl).Funcs(template.FuncMap{
		"getCommit": getCommit,
	}).ParseFiles(headerPath))

	if err := h.Execute(buf, data); err != nil {
		return err
	}

	if fmap == nil {
		fmap = make(template.FuncMap)
	}
	fmap["getCommit"] = getCommit

	t := template.Must(template.New(name).Funcs(fmap).ParseFiles(path))
	if err := t.Execute(buf, data); err != nil {
		return err
	}

	src, err := format.Source(buf.Bytes())
	if err != nil {
		return err
	}
	_, err = w.Write(src)
	return err
}
