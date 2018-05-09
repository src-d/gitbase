package generator

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	yaml "gopkg.in/yaml.v2"
)

// Filenames reads from fileToParse and builds source file from tmplPath. It complies with type File signature.
func Filenames(fileToParse, samplesDir, outPath, tmplPath, tmplName, commit string) error {
	data, err := ioutil.ReadFile(fileToParse)
	if err != nil {
		return err
	}

	languages := make(map[string]*languageInfo)
	if err := yaml.Unmarshal(data, &languages); err != nil {
		return err
	}

	if err := walkSamplesFilenames(samplesDir, languages); err != nil {
		return err
	}

	languagesByFilename := buildFilenameLanguageMap(languages)

	buf := &bytes.Buffer{}
	if err := executeFilenamesTemplate(buf, languagesByFilename, tmplPath, tmplName, commit); err != nil {
		return err
	}

	return formatedWrite(outPath, buf.Bytes())
}

func walkSamplesFilenames(samplesDir string, languages map[string]*languageInfo) error {
	const filenamesDir = "filenames"
	var language string
	err := filepath.Walk(samplesDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if f.IsDir() {
			if f.Name() != filenamesDir {
				language = f.Name()
			}

			return nil
		}

		parentDir := filepath.Base(filepath.Dir(path))
		if parentDir != filenamesDir {
			return nil
		}

		info, ok := languages[language]
		if !ok {
			info = &languageInfo{Filenames: []string{}}
		}

		for _, filename := range info.Filenames {
			if filename == f.Name() {
				return nil
			}
		}

		info.Filenames = append(info.Filenames, f.Name())

		return nil
	})

	return err
}

func buildFilenameLanguageMap(languages map[string]*languageInfo) map[string][]string {
	filenameLangMap := make(map[string][]string)
	for lang, langInfo := range languages {
		for _, filename := range langInfo.Filenames {
			filenameLangMap[filename] = append(filenameLangMap[filename], lang)
		}
	}

	return filenameLangMap
}

func executeFilenamesTemplate(out io.Writer, languagesByFilename map[string][]string, tmplPath, tmplName, commit string) error {
	fmap := template.FuncMap{
		"formatStringSlice": func(slice []string) string { return `"` + strings.Join(slice, `","`) + `"` },
	}
	return executeTemplate(out, tmplName, tmplPath, commit, fmap, languagesByFilename)
}
