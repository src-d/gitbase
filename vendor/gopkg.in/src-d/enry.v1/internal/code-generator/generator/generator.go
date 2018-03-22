package generator

import (
	"go/format"
	"io/ioutil"
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
