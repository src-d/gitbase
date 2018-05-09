package generator

import (
	"bytes"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"strings"
)

// Aliases reads from fileToParse and builds source file from tmplPath. It complies with type File signature.
func Aliases(fileToParse, samplesDir, outPath, tmplPath, tmplName, commit string) error {
	data, err := ioutil.ReadFile(fileToParse)
	if err != nil {
		return err
	}

	languages := make(map[string]*languageInfo)
	if err := yaml.Unmarshal(data, &languages); err != nil {
		return err
	}

	orderedLangList := getAlphabeticalOrderedKeys(languages)
	languagesByAlias := buildAliasLanguageMap(languages, orderedLangList)

	buf := &bytes.Buffer{}
	if err := executeAliasesTemplate(buf, languagesByAlias, tmplPath, tmplName, commit); err != nil {
		return err
	}

	return formatedWrite(outPath, buf.Bytes())
}

func buildAliasLanguageMap(languages map[string]*languageInfo, orderedLangList []string) map[string]string {
	aliasLangsMap := make(map[string]string)
	for _, lang := range orderedLangList {
		langInfo := languages[lang]
		key := convertToAliasKey(lang)
		aliasLangsMap[key] = lang
		for _, alias := range langInfo.Aliases {
			key := convertToAliasKey(alias)
			aliasLangsMap[key] = lang
		}
	}

	return aliasLangsMap
}

func convertToAliasKey(s string) (key string) {
	key = strings.Replace(s, ` `, `_`, -1)
	key = strings.ToLower(key)
	return
}

func executeAliasesTemplate(out io.Writer, languagesByAlias map[string]string, aliasesTmplPath, aliasesTmpl, commit string) error {
	return executeTemplate(out, aliasesTmpl, aliasesTmplPath, commit, nil, languagesByAlias)
}
