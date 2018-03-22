package generator

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	linguistURL = "https://github.com/github/linguist.git"
	linguistClonedEnvVar = "ENRY_TEST_REPO"
	commit        = "d5c8db3fb91963c4b2762ca2ea2ff7cfac109f68"
	samplesDir    = "samples"
	languagesFile = "lib/linguist/languages.yml"

	// Extensions test
	extensionGold         = "test_files/extension.gold"
	extensionTestTmplPath = "../assets/extension.go.tmpl"
	extensionTestTmplName = "extension.go.tmpl"

	// Heuristics test
	heuristicsTestFile  = "lib/linguist/heuristics.rb"
	contentGold         = "test_files/content.gold"
	contentTestTmplPath = "../assets/content.go.tmpl"
	contentTestTmplName = "content.go.tmpl"

	// Vendor test
	vendorTestFile     = "lib/linguist/vendor.yml"
	vendorGold         = "test_files/vendor.gold"
	vendorTestTmplPath = "../assets/vendor.go.tmpl"
	vendorTestTmplName = "vendor.go.tmpl"

	// Documentation test
	documentationTestFile     = "lib/linguist/documentation.yml"
	documentationGold         = "test_files/documentation.gold"
	documentationTestTmplPath = "../assets/documentation.go.tmpl"
	documentationTestTmplName = "documentation.go.tmpl"

	// Types test
	typeGold         = "test_files/type.gold"
	typeTestTmplPath = "../assets/type.go.tmpl"
	typeTestTmplName = "type.go.tmpl"

	// Interpreters test
	interpreterGold         = "test_files/interpreter.gold"
	interpreterTestTmplPath = "../assets/interpreter.go.tmpl"
	interpreterTestTmplName = "interpreter.go.tmpl"

	// Filenames test
	filenameGold         = "test_files/filename.gold"
	filenameTestTmplPath = "../assets/filename.go.tmpl"
	filenameTestTmplName = "filename.go.tmpl"

	// Aliases test
	aliasGold         = "test_files/alias.gold"
	aliasTestTmplPath = "../assets/alias.go.tmpl"
	aliasTestTmplName = "alias.go.tmpl"

	// Frequencies test
	frequenciesGold         = "test_files/frequencies.gold"
	frequenciesTestTmplPath = "../assets/frequencies.go.tmpl"
	frequenciesTestTmplName = "frequencies.go.tmpl"

	// commit test
	commitGold         = "test_files/commit.gold"
	commitTestTmplPath = "../assets/commit.go.tmpl"
	commitTestTmplName = "commit.go.tmpl"

	// mime test
	mimeTypeGold         = "test_files/mimeType.gold"
	mimeTypeTestTmplPath = "../assets/mimeType.go.tmpl"
	mimeTypeTestTmplName = "mimeType.go.tmpl"
)

type GeneratorTestSuite struct {
	suite.Suite
	tmpLinguist string
	cloned      bool
}

func TestGeneratorTestSuite(t *testing.T) {
	suite.Run(t, new(GeneratorTestSuite))
}

func (s *GeneratorTestSuite) SetupSuite() {
	var err error
	s.tmpLinguist = os.Getenv(linguistClonedEnvVar)
	s.cloned = s.tmpLinguist == ""
	if s.cloned {
		s.tmpLinguist, err = ioutil.TempDir("", "linguist-")
		assert.NoError(s.T(), err)
		cmd := exec.Command("git", "clone", linguistURL, s.tmpLinguist)
		err = cmd.Run()
		assert.NoError(s.T(), err)
	}

	cwd, err := os.Getwd()
	assert.NoError(s.T(), err)

	err = os.Chdir(s.tmpLinguist)
	assert.NoError(s.T(), err)

	cmd := exec.Command("git", "checkout", commit)
	err = cmd.Run()
	assert.NoError(s.T(), err)

	err = os.Chdir(cwd)
	assert.NoError(s.T(), err)
}

func (s *GeneratorTestSuite) TearDownSuite() {
	if s.cloned {
		err := os.RemoveAll(s.tmpLinguist)
		assert.NoError(s.T(), err)
	}
}

func (s *GeneratorTestSuite) TestGenerationFiles() {
	tests := []struct {
		name        string
		fileToParse string
		samplesDir  string
		tmplPath    string
		tmplName    string
		commit      string
		generate    File
		wantOut     string
	}{
		{
			name:        "Extensions()",
			fileToParse: filepath.Join(s.tmpLinguist, languagesFile),
			samplesDir:  "",
			tmplPath:    extensionTestTmplPath,
			tmplName:    extensionTestTmplName,
			commit:      commit,
			generate:    Extensions,
			wantOut:     extensionGold,
		},
		{
			name:        "Heuristics()",
			fileToParse: filepath.Join(s.tmpLinguist, heuristicsTestFile),
			samplesDir:  "",
			tmplPath:    contentTestTmplPath,
			tmplName:    contentTestTmplName,
			commit:      commit,
			generate:    Heuristics,
			wantOut:     contentGold,
		},
		{
			name:        "Vendor()",
			fileToParse: filepath.Join(s.tmpLinguist, vendorTestFile),
			samplesDir:  "",
			tmplPath:    vendorTestTmplPath,
			tmplName:    vendorTestTmplName,
			commit:      commit,
			generate:    Vendor,
			wantOut:     vendorGold,
		},
		{
			name:        "Documentation()",
			fileToParse: filepath.Join(s.tmpLinguist, documentationTestFile),
			samplesDir:  "",
			tmplPath:    documentationTestTmplPath,
			tmplName:    documentationTestTmplName,
			commit:      commit,
			generate:    Documentation,
			wantOut:     documentationGold,
		},
		{
			name:        "Types()",
			fileToParse: filepath.Join(s.tmpLinguist, languagesFile),
			samplesDir:  "",
			tmplPath:    typeTestTmplPath,
			tmplName:    typeTestTmplName,
			commit:      commit,
			generate:    Types,
			wantOut:     typeGold,
		},
		{
			name:        "Interpreters()",
			fileToParse: filepath.Join(s.tmpLinguist, languagesFile),
			samplesDir:  "",
			tmplPath:    interpreterTestTmplPath,
			tmplName:    interpreterTestTmplName,
			commit:      commit,
			generate:    Interpreters,
			wantOut:     interpreterGold,
		},
		{
			name:        "Filenames()",
			fileToParse: filepath.Join(s.tmpLinguist, languagesFile),
			samplesDir:  filepath.Join(s.tmpLinguist, samplesDir),
			tmplPath:    filenameTestTmplPath,
			tmplName:    filenameTestTmplName,
			commit:      commit,
			generate:    Filenames,
			wantOut:     filenameGold,
		},
		{
			name:        "Aliases()",
			fileToParse: filepath.Join(s.tmpLinguist, languagesFile),
			samplesDir:  "",
			tmplPath:    aliasTestTmplPath,
			tmplName:    aliasTestTmplName,
			commit:      commit,
			generate:    Aliases,
			wantOut:     aliasGold,
		},
		{
			name:       "Frequencies()",
			samplesDir: filepath.Join(s.tmpLinguist, samplesDir),
			tmplPath:   frequenciesTestTmplPath,
			tmplName:   frequenciesTestTmplName,
			commit:     commit,
			generate:   Frequencies,
			wantOut:    frequenciesGold,
		},
		{
			name:       "Commit()",
			samplesDir: "",
			tmplPath:   commitTestTmplPath,
			tmplName:   commitTestTmplName,
			commit:     commit,
			generate:   Commit,
			wantOut:    commitGold,
		},
		{
			name:        "MimeType()",
			fileToParse: filepath.Join(s.tmpLinguist, languagesFile),
			samplesDir:  "",
			tmplPath:    mimeTypeTestTmplPath,
			tmplName:    mimeTypeTestTmplName,
			commit:      commit,
			generate:    MimeType,
			wantOut:     mimeTypeGold,
		},
	}

	for _, test := range tests {
		gold, err := ioutil.ReadFile(test.wantOut)
		assert.NoError(s.T(), err)

		outPath, err := ioutil.TempFile("/tmp", "generator-test-")
		assert.NoError(s.T(), err)
		defer os.Remove(outPath.Name())
		err = test.generate(test.fileToParse, test.samplesDir, outPath.Name(), test.tmplPath, test.tmplName, test.commit)
		assert.NoError(s.T(), err)
		out, err := ioutil.ReadFile(outPath.Name())
		assert.NoError(s.T(), err)
		assert.EqualValues(s.T(), gold, out, fmt.Sprintf("%v: %v, expected: %v", test.name, string(out), string(gold)))
	}
}
