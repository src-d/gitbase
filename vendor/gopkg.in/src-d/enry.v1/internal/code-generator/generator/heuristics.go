package generator

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"text/template"

	"gopkg.in/src-d/enry.v1/regex"
)

// Heuristics reads from fileToParse and builds source file from tmplPath. It complies with type File signature.
func Heuristics(fileToParse, samplesDir, outPath, tmplPath, tmplName, commit string) error {
	data, err := ioutil.ReadFile(fileToParse)
	if err != nil {
		return err
	}

	disambiguators, err := getDisambiguators(data)
	if err != nil {
		return err
	}

	buf := &bytes.Buffer{}
	if err := executeContentTemplate(buf, disambiguators, tmplPath, tmplName, commit); err != nil {
		return err
	}

	return formatedWrite(outPath, buf.Bytes())
}

const (
	unknownLanguage = "OtherLanguage"
	emptyFile       = "^$"
)

var (
	disambLine       = regex.MustCompile(`^(\s*)disambiguate`)
	definedRegs      = make(map[string]string)
	illegalCharacter = map[string]string{
		"#": "Sharp",
		"+": "Plus",
		"-": "Dash",
	}
)

type disambiguator struct {
	Extension string                `json:"extension,omitempty"`
	Languages []*languageHeuristics `json:"languages,omitempty"`
}

func (d *disambiguator) setHeuristicsNames() {
	for _, lang := range d.Languages {
		for i, heuristic := range lang.Heuristics {
			name := buildName(d.Extension, lang.Language, i)
			heuristic.Name = name
		}
	}
}

func buildName(extension, language string, id int) string {
	extension = strings.TrimPrefix(extension, `.`)
	language = strings.Join(strings.Fields(language), ``)
	name := strings.Join([]string{extension, language, "Matcher", strconv.Itoa(id)}, `_`)
	for k, v := range illegalCharacter {
		if strings.Contains(name, k) {
			name = strings.Replace(name, k, v, -1)
		}
	}

	return name
}

type languageHeuristics struct {
	Language       string       `json:"language,omitempty"`
	Heuristics     []*heuristic `json:"heuristics,omitempty"`
	LogicRelations []string     `json:"logic_relations,omitempty"`
}

func (l *languageHeuristics) clone() (*languageHeuristics, error) {
	language := l.Language
	logicRels := make([]string, len(l.LogicRelations))
	if copy(logicRels, l.LogicRelations) != len(l.LogicRelations) {
		return nil, fmt.Errorf("error copying logic relations")
	}

	heuristics := make([]*heuristic, 0, len(l.Heuristics))
	for _, h := range l.Heuristics {
		heuristic := *h
		heuristics = append(heuristics, &heuristic)
	}

	clone := &languageHeuristics{
		Language:       language,
		Heuristics:     heuristics,
		LogicRelations: logicRels,
	}

	return clone, nil
}

type heuristic struct {
	Name   string `json:"name,omitempty"`
	Regexp string `json:"regexp,omitempty"`
}

// A disambiguate block looks like:
// disambiguate ".mod", ".extension" do |data|
// 	if data.include?('<!ENTITY ') && data.include?('patata')
// 		Language["XML"]
// 	elsif /^\s*MODULE [\w\.]+;/i.match(data) || /^\s*END [\w\.]+;/i.match(data) || data.empty?
// 		Language["Modula-2"]
//	elsif (/^\s*import (scala|java)\./.match(data) || /^\s*val\s+\w+\s*=/.match(data) || /^\s*class\b/.match(data))
//              Language["Scala"]
//      elsif (data.include?("gap> "))
//		Language["GAP"]
// 	else
// 		[Language["Linux Kernel Module"], Language["AMPL"]]
// 	end
// end
func getDisambiguators(heuristics []byte) ([]*disambiguator, error) {
	seenExtensions := map[string]bool{}
	buf := bufio.NewScanner(bytes.NewReader(heuristics))
	disambiguators := make([]*disambiguator, 0, 50)
	for buf.Scan() {
		line := buf.Text()
		if disambLine.MatchString(line) {
			d, err := parseDisambiguators(line, buf, seenExtensions)
			if err != nil {
				return nil, err
			}

			disambiguators = append(disambiguators, d...)
		}

		lookForRegexpVariables(line)
	}

	if err := buf.Err(); err != nil {
		return nil, err
	}

	return disambiguators, nil
}

func lookForRegexpVariables(line string) {
	if strings.Contains(line, "ObjectiveCRegex = ") {
		line = strings.TrimSpace(line)
		reg := strings.TrimPrefix(line, "ObjectiveCRegex = ")
		definedRegs["ObjectiveCRegex"] = reg
	}

	if strings.Contains(line, "fortran_rx = ") {
		line = strings.TrimSpace(line)
		reg := strings.TrimPrefix(line, "fortran_rx = ")
		definedRegs["fortran_rx"] = reg
	}
}

func parseDisambiguators(line string, buf *bufio.Scanner, seenExtensions map[string]bool) ([]*disambiguator, error) {
	disambList := make([]*disambiguator, 0, 2)
	splitted := strings.Fields(line)

	for _, v := range splitted {
		if strings.HasPrefix(v, `"`) {
			extension := strings.Trim(v, `",`)
			if _, ok := seenExtensions[extension]; !ok {
				d := &disambiguator{Extension: extension}
				disambList = append(disambList, d)
				seenExtensions[extension] = true
			}
		}
	}

	langsHeuristics, err := getLanguagesHeuristics(buf)
	if err != nil {
		return nil, err
	}

	for i, disamb := range disambList {
		lh := langsHeuristics
		if i != 0 {
			lh = cloneLanguagesHeuristics(langsHeuristics)
		}

		disamb.Languages = lh
		disamb.setHeuristicsNames()
	}

	return disambList, nil
}

func cloneLanguagesHeuristics(list []*languageHeuristics) []*languageHeuristics {
	cloneList := make([]*languageHeuristics, 0, len(list))
	for _, langHeu := range list {
		clone, _ := langHeu.clone()
		cloneList = append(cloneList, clone)
	}

	return cloneList
}

func getLanguagesHeuristics(buf *bufio.Scanner) ([]*languageHeuristics, error) {
	langsList := make([][]string, 0, 2)
	heuristicsList := make([][]*heuristic, 0, 1)
	logicRelsList := make([][]string, 0, 1)

	lastWasMatch := false
	for buf.Scan() {
		line := buf.Text()
		if strings.TrimSpace(line) == "end" {
			break
		}

		if hasRegExp(line) {
			line := cleanRegExpLine(line)

			logicRels := getLogicRelations(line)
			heuristics := getHeuristics(line)
			if lastWasMatch {
				i := len(heuristicsList) - 1
				heuristicsList[i] = append(heuristicsList[i], heuristics...)
				i = len(logicRelsList) - 1
				logicRelsList[i] = append(logicRelsList[i], logicRels...)
			} else {
				heuristicsList = append(heuristicsList, heuristics)
				logicRelsList = append(logicRelsList, logicRels)
			}

			lastWasMatch = true
		}

		if strings.Contains(line, "Language") {
			langs := getLanguages(line)
			langsList = append(langsList, langs)
			lastWasMatch = false
		}

	}

	if err := buf.Err(); err != nil {
		return nil, err
	}

	langsHeuristics := buildLanguagesHeuristics(langsList, heuristicsList, logicRelsList)
	return langsHeuristics, nil
}

func hasRegExp(line string) bool {
	return strings.Contains(line, ".match") || strings.Contains(line, ".include?") || strings.Contains(line, ".empty?")
}

func cleanRegExpLine(line string) string {
	if strings.Contains(line, "if ") {
		line = line[strings.Index(line, `if `)+3:]
	}

	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, `(`)
	if strings.Contains(line, "))") {
		line = strings.TrimSuffix(line, `)`)
	}

	return line
}

func getLogicRelations(line string) []string {
	rels := make([]string, 0)
	splitted := strings.Split(line, "||")
	for i, v := range splitted {
		if strings.Contains(v, "&&") {
			rels = append(rels, "&&")
		}

		if i < len(splitted)-1 {
			rels = append(rels, "||")
		}
	}

	if len(rels) == 0 {
		rels = nil
	}

	return rels
}

func getHeuristics(line string) []*heuristic {
	splitted := splitByLogicOps(line)
	heuristics := make([]*heuristic, 0, len(splitted))
	for _, v := range splitted {
		v = strings.TrimSpace(v)
		var reg string

		if strings.Contains(v, ".match") {
			reg = v[:strings.Index(v, ".match")]
			reg = replaceRegexpVariables(reg)
		}

		if strings.Contains(v, ".include?") {
			reg = includeToRegExp(v)
		}

		if strings.Contains(v, ".empty?") {
			reg = emptyFile
		}

		if reg != "" {
			reg = convertToValidRegexp(reg)
			heuristics = append(heuristics, &heuristic{Regexp: reg})
		}
	}

	return heuristics
}

func splitByLogicOps(line string) []string {
	splitted := make([]string, 0, 1)
	splitOr := strings.Split(line, "||")
	for _, v := range splitOr {
		splitAnd := strings.Split(v, "&&")
		splitted = append(splitted, splitAnd...)
	}

	return splitted
}

func replaceRegexpVariables(reg string) string {
	repl := reg
	if v, ok := definedRegs[reg]; ok {
		repl = v
	}

	return repl
}

func convertToValidRegexp(reg string) string {
	// example: `/^(\s*)(<Project|<Import|<Property|<?xml|xmlns)/i``
	// Ruby modifier "m" matches multiple lines, recognizing newlines as normal characters, Go use flag "s" for that.
	const (
		caseSensitive = "i"
		matchEOL      = "s"

		rubyCaseSensitive = "i"
		rubyMultiLine     = "m"
	)

	if reg == emptyFile {
		return reg
	}

	reg = strings.TrimPrefix(reg, `/`)
	flags := "(?m"
	lastSlash := strings.LastIndex(reg, `/`)
	if lastSlash == -1 {
		return flags + ")" + reg
	}

	specialChars := reg[lastSlash:]
	reg = reg[:lastSlash]
	if lastSlash == len(reg)-1 {
		return flags + ")" + reg
	}

	if strings.Contains(specialChars, rubyCaseSensitive) {
		flags = flags + caseSensitive
	}

	if strings.Contains(specialChars, rubyMultiLine) {
		flags = flags + matchEOL
	}

	return flags + ")" + reg
}

func includeToRegExp(include string) string {
	content := include[strings.Index(include, `(`)+1 : strings.Index(include, `)`)]
	content = strings.Trim(content, `"'`)
	return regex.QuoteMeta(content)
}

func getLanguages(line string) []string {
	languages := make([]string, 0)
	splitted := strings.Split(line, `,`)
	for _, lang := range splitted {
		lang = trimLanguage(lang)
		languages = append(languages, lang)
	}

	return languages
}

func trimLanguage(enclosedLang string) string {
	lang := strings.TrimSpace(enclosedLang)
	lang = lang[strings.Index(lang, `"`)+1:]
	lang = lang[:strings.Index(lang, `"`)]
	return lang
}

func buildLanguagesHeuristics(langsList [][]string, heuristicsList [][]*heuristic, logicRelsList [][]string) []*languageHeuristics {
	langsHeuristics := make([]*languageHeuristics, 0, len(langsList))
	for i, langSlice := range langsList {
		var heuristics []*heuristic
		if i < len(heuristicsList) {
			heuristics = heuristicsList[i]
		}

		var rels []string
		if i < len(logicRelsList) {
			rels = logicRelsList[i]
		}

		for _, lang := range langSlice {
			lh := &languageHeuristics{
				Language:       lang,
				Heuristics:     heuristics,
				LogicRelations: rels,
			}

			langsHeuristics = append(langsHeuristics, lh)
		}
	}

	return langsHeuristics
}

func executeContentTemplate(out io.Writer, disambiguators []*disambiguator, tmplPath, tmplName, commit string) error {
	fmap := template.FuncMap{
		"getAllHeuristics": getAllHeuristics,
		"returnStringSlice": func(slice []string) string {
			if len(slice) == 0 {
				return "nil"
			}

			return `[]string{` + strings.Join(slice, `, `) + `}`
		},
		"returnLanguages": returnLanguages,
		"avoidLanguage":   avoidLanguage,
	}
	return executeTemplate(out, tmplName, tmplPath, commit, fmap, disambiguators)
}

func getAllHeuristics(disambiguators []*disambiguator) []*heuristic {
	heuristics := make([]*heuristic, 0)
	for _, disamb := range disambiguators {
		for _, lang := range disamb.Languages {
			if !avoidLanguage(lang) {
				heuristics = append(heuristics, lang.Heuristics...)
			}
		}
	}

	return heuristics
}

func avoidLanguage(lang *languageHeuristics) bool {
	// necessary to avoid corner cases
	for _, heuristic := range lang.Heuristics {
		if containsInvalidRegexp(heuristic.Regexp) {
			return true
		}
	}

	return false
}

func containsInvalidRegexp(reg string) bool {
	return strings.Contains(reg, `(?<`) || strings.Contains(reg, `\1`)
}

func returnLanguages(langsHeuristics []*languageHeuristics) []string {
	langs := make([]string, 0)
	for _, langHeu := range langsHeuristics {
		if len(langHeu.Heuristics) == 0 {
			langs = append(langs, `"`+langHeu.Language+`"`)
		}
	}

	return langs
}
