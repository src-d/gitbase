package generator

import (
	"bytes"
	"io"
	"io/ioutil"
	"text/template"

	yaml "gopkg.in/yaml.v2"
)

func MimeType(fileToParse, samplesDir, outPath, tmplPath, tmplName, commit string) error {
	data, err := ioutil.ReadFile(fileToParse)
	if err != nil {
		return err
	}

	languages := make(map[string]*languageInfo)
	if err := yaml.Unmarshal(data, &languages); err != nil {
		return err
	}

	langMimeMap := buildLanguageMimeMap(languages)

	buf := &bytes.Buffer{}
	if err := executeMimeTemplate(buf, langMimeMap, tmplPath, tmplName, commit); err != nil {
		return err
	}

	return formatedWrite(outPath, buf.Bytes())
}

func buildLanguageMimeMap(languages map[string]*languageInfo) map[string]string {
	langMimeMap := make(map[string]string)
	for lang, info := range languages {
		if len(info.MimeType) != 0 {
			langMimeMap[lang] = info.MimeType
		}
	}

	return langMimeMap
}

func executeMimeTemplate(out io.Writer, langMimeMap map[string]string, tmplPath, tmplName, commit string) error {
	fmap := template.FuncMap{
		"getCommit": func() string { return commit },
	}

	t := template.Must(template.New(tmplName).Funcs(fmap).ParseFiles(tmplPath))
	if err := t.Execute(out, langMimeMap); err != nil {
		return err
	}

	return nil
}
