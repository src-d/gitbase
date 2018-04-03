package enry

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"gopkg.in/src-d/enry.v1/data"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const linguistURL = "https://github.com/github/linguist.git"
const linguistClonedEnvVar = "ENRY_TEST_REPO"

type EnryTestSuite struct {
	suite.Suite
	repoLinguist string
	samplesDir   string
	cloned       bool
}

func TestEnryTestSuite(t *testing.T) {
	suite.Run(t, new(EnryTestSuite))
}

func (s *EnryTestSuite) SetupSuite() {
	var err error
	s.repoLinguist = os.Getenv(linguistClonedEnvVar)
	s.cloned = s.repoLinguist == ""
	if s.cloned {
		s.repoLinguist, err = ioutil.TempDir("", "linguist-")
		assert.NoError(s.T(), err)
	}

	s.samplesDir = filepath.Join(s.repoLinguist, "samples")

	if s.cloned {
		cmd := exec.Command("git", "clone", linguistURL, s.repoLinguist)
		err = cmd.Run()
		assert.NoError(s.T(), err)
	}

	cwd, err := os.Getwd()
	assert.NoError(s.T(), err)

	err = os.Chdir(s.repoLinguist)
	assert.NoError(s.T(), err)

	cmd := exec.Command("git", "checkout", data.LinguistCommit)
	err = cmd.Run()
	assert.NoError(s.T(), err)

	err = os.Chdir(cwd)
	assert.NoError(s.T(), err)
}

func (s *EnryTestSuite) TearDownSuite() {
	if s.cloned {
		err := os.RemoveAll(s.repoLinguist)
		assert.NoError(s.T(), err)
	}
}

func (s *EnryTestSuite) TestGetLanguage() {
	tests := []struct {
		name     string
		filename string
		content  []byte
		expected string
		safe     bool
	}{
		{name: "TestGetLanguage_1", filename: "foo.py", content: []byte{}, expected: "Python"},
		{name: "TestGetLanguage_2", filename: "foo.m", content: []byte(":- module"), expected: "Mercury"},
		{name: "TestGetLanguage_3", filename: "foo.m", content: nil, expected: OtherLanguage},
		{name: "TestGetLanguage_4", filename: "foo.mo", content: []byte{0xDE, 0x12, 0x04, 0x95, 0x00, 0x00, 0x00, 0x00}, expected: OtherLanguage},
		{name: "TestGetLanguage_5", filename: "", content: nil, expected: OtherLanguage},
	}

	for _, test := range tests {
		language := GetLanguage(test.filename, test.content)
		assert.Equal(s.T(), test.expected, language, fmt.Sprintf("%v: %v, expected: %v", test.name, language, test.expected))
	}
}

func (s *EnryTestSuite) TestGetLanguagesByModelineLinguist() {
	var modelinesDir = filepath.Join(s.repoLinguist, "test/fixtures/Data/Modelines")

	tests := []struct {
		name       string
		filename   string
		candidates []string
		expected   []string
	}{
		// Emacs
		{name: "TestGetLanguagesByModelineLinguist_1", filename: filepath.Join(modelinesDir, "example_smalltalk.md"), expected: []string{"Smalltalk"}},
		{name: "TestGetLanguagesByModelineLinguist_2", filename: filepath.Join(modelinesDir, "fundamentalEmacs.c"), expected: []string{"Text"}},
		{name: "TestGetLanguagesByModelineLinguist_3", filename: filepath.Join(modelinesDir, "iamphp.inc"), expected: []string{"PHP"}},
		{name: "TestGetLanguagesByModelineLinguist_4", filename: filepath.Join(modelinesDir, "seeplusplusEmacs1"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_5", filename: filepath.Join(modelinesDir, "seeplusplusEmacs2"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_6", filename: filepath.Join(modelinesDir, "seeplusplusEmacs3"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_7", filename: filepath.Join(modelinesDir, "seeplusplusEmacs4"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_8", filename: filepath.Join(modelinesDir, "seeplusplusEmacs5"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_9", filename: filepath.Join(modelinesDir, "seeplusplusEmacs6"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_10", filename: filepath.Join(modelinesDir, "seeplusplusEmacs7"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_11", filename: filepath.Join(modelinesDir, "seeplusplusEmacs9"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_12", filename: filepath.Join(modelinesDir, "seeplusplusEmacs10"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_13", filename: filepath.Join(modelinesDir, "seeplusplusEmacs11"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_14", filename: filepath.Join(modelinesDir, "seeplusplusEmacs12"), expected: []string{"C++"}},

		// Vim
		{name: "TestGetLanguagesByModelineLinguist_15", filename: filepath.Join(modelinesDir, "seeplusplus"), expected: []string{"C++"}},
		{name: "TestGetLanguagesByModelineLinguist_16", filename: filepath.Join(modelinesDir, "iamjs.pl"), expected: []string{"JavaScript"}},
		{name: "TestGetLanguagesByModelineLinguist_17", filename: filepath.Join(modelinesDir, "iamjs2.pl"), expected: []string{"JavaScript"}},
		{name: "TestGetLanguagesByModelineLinguist_18", filename: filepath.Join(modelinesDir, "not_perl.pl"), expected: []string{"Prolog"}},
		{name: "TestGetLanguagesByModelineLinguist_19", filename: filepath.Join(modelinesDir, "ruby"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_20", filename: filepath.Join(modelinesDir, "ruby2"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_21", filename: filepath.Join(modelinesDir, "ruby3"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_22", filename: filepath.Join(modelinesDir, "ruby4"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_23", filename: filepath.Join(modelinesDir, "ruby5"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_24", filename: filepath.Join(modelinesDir, "ruby6"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_25", filename: filepath.Join(modelinesDir, "ruby7"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_26", filename: filepath.Join(modelinesDir, "ruby8"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_27", filename: filepath.Join(modelinesDir, "ruby9"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_28", filename: filepath.Join(modelinesDir, "ruby10"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_29", filename: filepath.Join(modelinesDir, "ruby11"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_30", filename: filepath.Join(modelinesDir, "ruby12"), expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByModelineLinguist_31", filename: filepath.Join(s.samplesDir, "C/main.c"), expected: nil},
		{name: "TestGetLanguagesByModelineLinguist_32", filename: "", expected: nil},
	}

	for _, test := range tests {
		var content []byte
		var err error

		if test.filename != "" {
			content, err = ioutil.ReadFile(test.filename)
			assert.NoError(s.T(), err)
		}

		languages := GetLanguagesByModeline(test.filename, content, test.candidates)
		assert.Equal(s.T(), test.expected, languages, fmt.Sprintf("%v: languages = %v, expected: %v", test.name, languages, test.expected))
	}
}

func (s *EnryTestSuite) TestGetLanguagesByModeline() {
	const (
		wrongVim  = `# vim: set syntax=ruby ft  =python filetype=perl :`
		rightVim  = `/* vim: set syntax=python ft   =python filetype=python */`
		noLangVim = `/* vim: set shiftwidth=4 softtabstop=0 cindent cinoptions={1s: */`
	)

	tests := []struct {
		name       string
		filename   string
		content    []byte
		candidates []string
		expected   []string
	}{
		{name: "TestGetLanguagesByModeline_1", content: []byte(wrongVim), expected: nil},
		{name: "TestGetLanguagesByModeline_2", content: []byte(rightVim), expected: []string{"Python"}},
		{name: "TestGetLanguagesByModeline_3", content: []byte(noLangVim), expected: nil},
		{name: "TestGetLanguagesByModeline_4", content: nil, expected: nil},
		{name: "TestGetLanguagesByModeline_5", content: []byte{}, expected: nil},
	}

	for _, test := range tests {
		languages := GetLanguagesByModeline(test.filename, test.content, test.candidates)
		assert.Equal(s.T(), test.expected, languages, fmt.Sprintf("%v: languages = %v, expected: %v", test.name, languages, test.expected))
	}
}

func (s *EnryTestSuite) TestGetLanguagesByFilename() {
	tests := []struct {
		name       string
		filename   string
		content    []byte
		candidates []string
		expected   []string
	}{
		{name: "TestGetLanguagesByFilename_1", filename: "unknown.interpreter", expected: nil},
		{name: "TestGetLanguagesByFilename_2", filename: ".bashrc", expected: []string{"Shell"}},
		{name: "TestGetLanguagesByFilename_3", filename: "Dockerfile", expected: []string{"Dockerfile"}},
		{name: "TestGetLanguagesByFilename_4", filename: "Makefile.frag", expected: []string{"Makefile"}},
		{name: "TestGetLanguagesByFilename_5", filename: "makefile", expected: []string{"Makefile"}},
		{name: "TestGetLanguagesByFilename_6", filename: "Vagrantfile", expected: []string{"Ruby"}},
		{name: "TestGetLanguagesByFilename_7", filename: "_vimrc", expected: []string{"Vim script"}},
		{name: "TestGetLanguagesByFilename_8", filename: "pom.xml", expected: []string{"Maven POM"}},
		{name: "TestGetLanguagesByFilename_9", filename: "", expected: nil},
	}

	for _, test := range tests {
		languages := GetLanguagesByFilename(test.filename, test.content, test.candidates)
		assert.Equal(s.T(), test.expected, languages, fmt.Sprintf("%v: languages = %v, expected: %v", test.name, languages, test.expected))
	}
}

func (s *EnryTestSuite) TestGetLanguagesByShebang() {
	const (
		multilineExecHack = `#!/bin/sh
# Next line is comment in Tcl, but not in sh... \
exec tclsh "$0" ${1+"$@"}`

		multilineNoExecHack = `#!/bin/sh
#<<<#
echo "A shell script in a zkl program ($0)"
echo "Now run zkl <this file> with Hello World as args"
zkl $0 Hello World!
exit
#<<<#
println("The shell script says ",vm.arglist.concat(" "));`
	)

	tests := []struct {
		name       string
		filename   string
		content    []byte
		candidates []string
		expected   []string
	}{
		{name: "TestGetLanguagesByShebang_1", content: []byte(`#!/unknown/interpreter`), expected: nil},
		{name: "TestGetLanguagesByShebang_2", content: []byte(`no shebang`), expected: nil},
		{name: "TestGetLanguagesByShebang_3", content: []byte(`#!/usr/bin/env`), expected: nil},
		{name: "TestGetLanguagesByShebang_4", content: []byte(`#!/usr/bin/python -tt`), expected: []string{"Python"}},
		{name: "TestGetLanguagesByShebang_5", content: []byte(`#!/usr/bin/env python2.6`), expected: []string{"Python"}},
		{name: "TestGetLanguagesByShebang_6", content: []byte(`#!/usr/bin/env perl`), expected: []string{"Perl", "Pod"}},
		{name: "TestGetLanguagesByShebang_7", content: []byte(`#!	/bin/sh`), expected: []string{"Shell"}},
		{name: "TestGetLanguagesByShebang_8", content: []byte(`#!bash`), expected: []string{"Shell"}},
		{name: "TestGetLanguagesByShebang_9", content: []byte(multilineExecHack), expected: []string{"Tcl"}},
		{name: "TestGetLanguagesByShebang_10", content: []byte(multilineNoExecHack), expected: []string{"Shell"}},
		{name: "TestGetLanguagesByShebang_11", content: []byte(`#!`), expected: nil},
	}

	for _, test := range tests {
		languages := GetLanguagesByShebang(test.filename, test.content, test.candidates)
		assert.Equal(s.T(), test.expected, languages, fmt.Sprintf("%v: languages = %v, expected: %v", test.name, languages, test.expected))
	}
}

func (s *EnryTestSuite) TestGetLanguagesByExtension() {
	tests := []struct {
		name       string
		filename   string
		content    []byte
		candidates []string
		expected   []string
	}{
		{name: "TestGetLanguagesByExtension_1", filename: "foo.foo", expected: nil},
		{name: "TestGetLanguagesByExtension_2", filename: "foo.go", expected: []string{"Go"}},
		{name: "TestGetLanguagesByExtension_3", filename: "foo.go.php", expected: []string{"Hack", "PHP"}},
		{name: "TestGetLanguagesByExtension_4", filename: "", expected: nil},
	}

	for _, test := range tests {
		languages := GetLanguagesByExtension(test.filename, test.content, test.candidates)
		assert.Equal(s.T(), test.expected, languages, fmt.Sprintf("%v: languages = %v, expected: %v", test.name, languages, test.expected))
	}
}

func (s *EnryTestSuite) TestGetLanguagesByClassifier() {
	test := []struct {
		name       string
		filename   string
		candidates []string
		expected   string
	}{
		{name: "TestGetLanguagesByClassifier_1", filename: filepath.Join(s.samplesDir, "C/blob.c"), candidates: []string{"python", "ruby", "c", "c++"}, expected: "C"},
		{name: "TestGetLanguagesByClassifier_2", filename: filepath.Join(s.samplesDir, "C/blob.c"), candidates: nil, expected: OtherLanguage},
		{name: "TestGetLanguagesByClassifier_3", filename: filepath.Join(s.samplesDir, "C/main.c"), candidates: []string{}, expected: OtherLanguage},
		{name: "TestGetLanguagesByClassifier_4", filename: filepath.Join(s.samplesDir, "C/blob.c"), candidates: []string{"python", "ruby", "c++"}, expected: "C++"},
		{name: "TestGetLanguagesByClassifier_5", filename: filepath.Join(s.samplesDir, "C/blob.c"), candidates: []string{"ruby"}, expected: "Ruby"},
		{name: "TestGetLanguagesByClassifier_6", filename: filepath.Join(s.samplesDir, "Python/django-models-base.py"), candidates: []string{"python", "ruby", "c", "c++"}, expected: "Python"},
		{name: "TestGetLanguagesByClassifier_7", filename: "", candidates: []string{"python"}, expected: OtherLanguage},
	}

	for _, test := range test {
		var content []byte
		var err error

		if test.filename != "" {
			content, err = ioutil.ReadFile(test.filename)
			assert.NoError(s.T(), err)
		}

		languages := GetLanguagesByClassifier(test.filename, content, test.candidates)
		var language string
		if len(languages) == 0 {
			language = OtherLanguage
		} else {
			language = languages[0]
		}

		assert.Equal(s.T(), test.expected, language, fmt.Sprintf("%v: language = %v, expected: %v", test.name, language, test.expected))
	}
}

func (s *EnryTestSuite) TestGetLanguagesBySpecificClassifier() {
	test := []struct {
		name       string
		filename   string
		candidates []string
		classifier Classifier
		expected   string
	}{
		{name: "TestGetLanguagesByClassifier_1", filename: filepath.Join(s.samplesDir, "C/blob.c"), candidates: []string{"python", "ruby", "c", "c++"}, classifier: DefaultClassifier, expected: "C"},
		{name: "TestGetLanguagesByClassifier_2", filename: filepath.Join(s.samplesDir, "C/blob.c"), candidates: nil, classifier: DefaultClassifier, expected: "C"},
		{name: "TestGetLanguagesByClassifier_3", filename: filepath.Join(s.samplesDir, "C/main.c"), candidates: []string{}, classifier: DefaultClassifier, expected: "C"},
		{name: "TestGetLanguagesByClassifier_4", filename: filepath.Join(s.samplesDir, "C/blob.c"), candidates: []string{"python", "ruby", "c++"}, classifier: DefaultClassifier, expected: "C++"},
		{name: "TestGetLanguagesByClassifier_5", filename: filepath.Join(s.samplesDir, "C/blob.c"), candidates: []string{"ruby"}, classifier: DefaultClassifier, expected: "Ruby"},
		{name: "TestGetLanguagesByClassifier_6", filename: filepath.Join(s.samplesDir, "Python/django-models-base.py"), candidates: []string{"python", "ruby", "c", "c++"}, classifier: DefaultClassifier, expected: "Python"},
		{name: "TestGetLanguagesByClassifier_7", filename: os.DevNull, candidates: nil, classifier: DefaultClassifier, expected: OtherLanguage},
	}

	for _, test := range test {
		content, err := ioutil.ReadFile(test.filename)
		assert.NoError(s.T(), err)

		languages := GetLanguagesBySpecificClassifier(content, test.candidates, test.classifier)
		var language string
		if len(languages) == 0 {
			language = OtherLanguage
		} else {
			language = languages[0]
		}

		assert.Equal(s.T(), test.expected, language, fmt.Sprintf("%v: language = %v, expected: %v", test.name, language, test.expected))
	}
}

func (s *EnryTestSuite) TestGetLanguageExtensions() {
	tests := []struct {
		name     string
		language string
		expected []string
	}{
		{name: "TestGetLanguageExtensions_1", language: "foo", expected: nil},
		{name: "TestGetLanguageExtensions_2", language: "COBOL", expected: []string{".cob", ".cbl", ".ccp", ".cobol", ".cpy"}},
		{name: "TestGetLanguageExtensions_3", language: "Maven POM", expected: nil},
	}

	for _, test := range tests {
		extensions := GetLanguageExtensions(test.language)
		assert.EqualValues(s.T(), test.expected, extensions, fmt.Sprintf("%v: extensions = %v, expected: %v", test.name, extensions, test.expected))
	}
}

func (s *EnryTestSuite) TestGetLanguageType() {
	tests := []struct {
		name     string
		language string
		expected Type
	}{
		{name: "TestGetLanguageType_1", language: "BestLanguageEver", expected: Unknown},
		{name: "TestGetLanguageType_2", language: "JSON", expected: Data},
		{name: "TestGetLanguageType_3", language: "COLLADA", expected: Data},
		{name: "TestGetLanguageType_4", language: "Go", expected: Programming},
		{name: "TestGetLanguageType_5", language: "Brainfuck", expected: Programming},
		{name: "TestGetLanguageType_6", language: "HTML", expected: Markup},
		{name: "TestGetLanguageType_7", language: "Sass", expected: Markup},
		{name: "TestGetLanguageType_8", language: "AsciiDoc", expected: Prose},
		{name: "TestGetLanguageType_9", language: "Textile", expected: Prose},
	}

	for _, test := range tests {
		langType := GetLanguageType(test.language)
		assert.Equal(s.T(), test.expected, langType, fmt.Sprintf("%v: langType = %v, expected: %v", test.name, langType, test.expected))
	}
}

func (s *EnryTestSuite) TestGetLanguageByAlias() {
	tests := []struct {
		name         string
		alias        string
		expectedLang string
		expectedOk   bool
	}{
		{name: "TestGetLanguageByAlias_1", alias: "BestLanguageEver", expectedLang: OtherLanguage, expectedOk: false},
		{name: "TestGetLanguageByAlias_2", alias: "aspx-vb", expectedLang: "ASP", expectedOk: true},
		{name: "TestGetLanguageByAlias_3", alias: "C++", expectedLang: "C++", expectedOk: true},
		{name: "TestGetLanguageByAlias_4", alias: "c++", expectedLang: "C++", expectedOk: true},
		{name: "TestGetLanguageByAlias_5", alias: "objc", expectedLang: "Objective-C", expectedOk: true},
		{name: "TestGetLanguageByAlias_6", alias: "golang", expectedLang: "Go", expectedOk: true},
		{name: "TestGetLanguageByAlias_7", alias: "GOLANG", expectedLang: "Go", expectedOk: true},
		{name: "TestGetLanguageByAlias_8", alias: "bsdmake", expectedLang: "Makefile", expectedOk: true},
		{name: "TestGetLanguageByAlias_9", alias: "xhTmL", expectedLang: "HTML", expectedOk: true},
		{name: "TestGetLanguageByAlias_10", alias: "python", expectedLang: "Python", expectedOk: true},
	}

	for _, test := range tests {
		lang, ok := GetLanguageByAlias(test.alias)
		assert.Equal(s.T(), test.expectedLang, lang, fmt.Sprintf("%v: lang = %v, expected: %v", test.name, lang, test.expectedLang))
		assert.Equal(s.T(), test.expectedOk, ok, fmt.Sprintf("%v: ok = %v, expected: %v", test.name, ok, test.expectedOk))
	}
}

func (s *EnryTestSuite) TestLinguistCorpus() {
	const filenamesDir = "filenames"
	var cornerCases = map[string]bool{
		"hello.ms": true,
	}

	var total, failed, ok, other int
	var expected string
	filepath.Walk(s.samplesDir, func(path string, f os.FileInfo, err error) error {
		if f.IsDir() {
			if f.Name() != filenamesDir {
				expected = f.Name()
			}

			return nil
		}

		filename := filepath.Base(path)
		content, _ := ioutil.ReadFile(path)

		total++
		obtained := GetLanguage(filename, content)
		if obtained == OtherLanguage {
			obtained = "Other"
			other++
		}

		var status string
		if expected == obtained {
			status = "ok"
			ok++
		} else {
			status = "failed"
			failed++

		}

		if _, ok := cornerCases[filename]; ok {
			fmt.Printf("\t\t[considered corner case] %s\texpected: %s\tobtained: %s\tstatus: %s\n", filename, expected, obtained, status)
		} else {
			assert.Equal(s.T(), expected, obtained, fmt.Sprintf("%s\texpected: %s\tobtained: %s\tstatus: %s\n", filename, expected, obtained, status))
		}

		return nil
	})

	fmt.Printf("\t\ttotal files: %d, ok: %d, failed: %d, other: %d\n", total, ok, failed, other)
}
