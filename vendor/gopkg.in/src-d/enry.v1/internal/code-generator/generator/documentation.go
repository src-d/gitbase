package generator

import (
	"bytes"
	"io"
	"io/ioutil"
	"text/template"

	yaml "gopkg.in/yaml.v2"
)

// Documentation reads from fileToParse and builds source file from tmplPath. It complies with type File signature.
func Documentation(fileToParse, samplesDir, outPath, tmplPath, tmplName, commit string) error {
	data, err := ioutil.ReadFile(fileToParse)
	if err != nil {
		return err
	}

	var regexpList []string
	if err := yaml.Unmarshal(data, &regexpList); err != nil {
		return err
	}

	buf := &bytes.Buffer{}
	if err := executeDocumentationTemplate(buf, regexpList, tmplPath, tmplName, commit); err != nil {
		return err
	}

	return formatedWrite(outPath, buf.Bytes())
}

func executeDocumentationTemplate(out io.Writer, regexpList []string, tmplPath, tmplName, commit string) error {
	fmap := template.FuncMap{
		"getCommit": func() string { return commit },
	}

	t := template.Must(template.New(tmplName).Funcs(fmap).ParseFiles(tmplPath))
	if err := t.Execute(out, regexpList); err != nil {
		return err
	}

	return nil
}
