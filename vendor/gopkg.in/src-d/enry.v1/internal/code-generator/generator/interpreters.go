package generator

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"gopkg.in/yaml.v2"
)

// Interpreters reads from fileToParse and builds source file from tmplPath. It complies with type File signature.
func Interpreters(fileToParse, samplesDir, outPath, tmplPath, tmplName, commit string) error {
	data, err := ioutil.ReadFile(fileToParse)
	if err != nil {
		return err
	}

	languages := make(map[string]*languageInfo)
	if err := yaml.Unmarshal(data, &languages); err != nil {
		return err
	}

	orderedKeys := getAlphabeticalOrderedKeys(languages)
	languagesByInterpreter := buildInterpreterLanguagesMap(languages, orderedKeys)

	buf := &bytes.Buffer{}
	if err := executeInterpretersTemplate(buf, languagesByInterpreter, tmplPath, tmplName, commit); err != nil {
		return err
	}

	return formatedWrite(outPath, buf.Bytes())
}

func buildInterpreterLanguagesMap(languages map[string]*languageInfo, orderedKeys []string) map[string][]string {
	interpreterLangsMap := make(map[string][]string)
	for _, lang := range orderedKeys {
		langInfo := languages[lang]
		for _, interpreter := range langInfo.Interpreters {
			interpreterLangsMap[interpreter] = append(interpreterLangsMap[interpreter], lang)
		}
	}

	return interpreterLangsMap
}

func executeInterpretersTemplate(out io.Writer, languagesByInterpreter map[string][]string, tmplPath, tmplName, commit string) error {
	fmap := template.FuncMap{
		"getCommit":         func() string { return commit },
		"formatStringSlice": func(slice []string) string { return `"` + strings.Join(slice, `","`) + `"` },
	}

	t := template.Must(template.New(tmplName).Funcs(fmap).ParseFiles(tmplPath))
	if err := t.Execute(out, languagesByInterpreter); err != nil {
		return err
	}

	return nil
}
