package generator

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	yaml "gopkg.in/yaml.v2"
)

type extensionsInfo struct {
	LanguagesByExtension map[string][]string
	ExtensionsByLanguage map[string][]string
}

// Extensions reads from fileToParse and builds source file from tmplPath. It complies with type File signature.
func Extensions(fileToParse, samplesDir, outPath, tmplPath, tmplName, commit string) error {
	data, err := ioutil.ReadFile(fileToParse)
	if err != nil {
		return err
	}

	languages := make(map[string]*languageInfo)
	if err := yaml.Unmarshal(data, &languages); err != nil {
		return err
	}

	extensionsToLower(languages)
	extInfo := &extensionsInfo{}
	orderedKeyList := getAlphabeticalOrderedKeys(languages)
	extInfo.LanguagesByExtension = buildExtensionLanguageMap(languages, orderedKeyList)
	extInfo.ExtensionsByLanguage = buildLanguageExtensionsMap(languages)

	buf := &bytes.Buffer{}
	if err := executeExtensionsTemplate(buf, extInfo, tmplPath, tmplName, commit); err != nil {
		return err
	}

	return formatedWrite(outPath, buf.Bytes())
}

func extensionsToLower(languages map[string]*languageInfo) {
	for _, info := range languages {
		info.Extensions = stringSliceToLower(info.Extensions)
	}
}

func stringSliceToLower(slice []string) []string {
	toLower := make([]string, 0, len(slice))
	for _, s := range slice {
		toLower = append(toLower, strings.ToLower(s))
	}

	return toLower
}

func buildExtensionLanguageMap(languages map[string]*languageInfo, orderedKeyList []string) map[string][]string {
	extensionLangsMap := make(map[string][]string)
	for _, lang := range orderedKeyList {
		langInfo := languages[lang]
		for _, extension := range langInfo.Extensions {
			extensionLangsMap[extension] = append(extensionLangsMap[extension], lang)
		}
	}

	return extensionLangsMap
}

func buildLanguageExtensionsMap(languages map[string]*languageInfo) map[string][]string {
	langExtensionMap := make(map[string][]string, len(languages))
	for lang, info := range languages {
		if len(info.Extensions) > 0 {
			langExtensionMap[lang] = info.Extensions
		}
	}

	return langExtensionMap
}

func executeExtensionsTemplate(out io.Writer, extInfo *extensionsInfo, tmplPath, tmplName, commit string) error {
	fmap := template.FuncMap{
		"getCommit":         func() string { return commit },
		"formatStringSlice": func(slice []string) string { return `"` + strings.Join(slice, `","`) + `"` },
	}

	t := template.Must(template.New(tmplName).Funcs(fmap).ParseFiles(tmplPath))
	if err := t.Execute(out, extInfo); err != nil {
		return err
	}

	return nil
}
