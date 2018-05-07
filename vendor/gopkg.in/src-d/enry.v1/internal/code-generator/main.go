package main

import (
	"io/ioutil"
	"log"

	"gopkg.in/src-d/enry.v1/internal/code-generator/generator"
)

const (
	// languages info file
	languagesYAML = ".linguist/lib/linguist/languages.yml"

	// linguist's samples directory
	samplesDir = ".linguist/samples"

	// extension.go generation
	extensionsFile     = "data/extension.go"
	extensionsTmplPath = "internal/code-generator/assets/extension.go.tmpl"
	extensionsTmpl     = "extension.go.tmpl"

	// content.go generation
	heuristicsRuby  = ".linguist/lib/linguist/heuristics.rb"
	contentFile     = "data/content.go"
	contentTmplPath = "internal/code-generator/assets/content.go.tmpl"
	contentTmpl     = "content.go.tmpl"

	// vendor.go generation
	vendorYAML     = ".linguist/lib/linguist/vendor.yml"
	vendorFile     = "data/vendor.go"
	vendorTmplPath = "internal/code-generator/assets/vendor.go.tmpl"
	vendorTmpl     = "vendor.go.tmpl"

	// documentation.go generation
	documentationYAML     = ".linguist/lib/linguist/documentation.yml"
	documentationFile     = "data/documentation.go"
	documentationTmplPath = "internal/code-generator/assets/documentation.go.tmpl"
	documentationTmpl     = "documentation.go.tmpl"

	// type.go generation
	typeFile     = "data/type.go"
	typeTmplPath = "internal/code-generator/assets/type.go.tmpl"
	typeTmpl     = "type.go.tmpl"

	// interpreter.go generation
	interpretersFile     = "data/interpreter.go"
	interpretersTmplPath = "internal/code-generator/assets/interpreter.go.tmpl"
	interpretersTmpl     = "interpreter.go.tmpl"

	// filename.go generation
	filenamesFile     = "data/filename.go"
	filenamesTmplPath = "internal/code-generator/assets/filename.go.tmpl"
	filenamesTmpl     = "filename.go.tmpl"

	// alias.go generation
	aliasesFile     = "data/alias.go"
	aliasesTmplPath = "internal/code-generator/assets/alias.go.tmpl"
	aliasesTmpl     = "alias.go.tmpl"

	// frequencies.go generation
	frequenciesFile     = "data/frequencies.go"
	frequenciesTmplPath = "internal/code-generator/assets/frequencies.go.tmpl"
	frequenciesTmpl     = "frequencies.go.tmpl"

	// commit.go generation
	commitFile     = "data/commit.go"
	commitTmplPath = "internal/code-generator/assets/commit.go.tmpl"
	commitTmpl     = "commit.go.tmpl"

	// mimeType.go generation
	mimeTypeFile     = "data/mimeType.go"
	mimeTypeTmplPath = "internal/code-generator/assets/mimeType.go.tmpl"
	mimeTypeTmpl     = "mimeType.go.tmpl"

	commitPath = ".linguist/.git/HEAD"
)

type generatorFiles struct {
	generate    generator.File
	fileToParse string
	samplesDir  string
	outPath     string
	tmplPath    string
	tmplName    string
	commit      string
}

func main() {
	commit, err := getCommit(commitPath)
	if err != nil {
		log.Printf("couldn't find commit: %v", err)
	}

	fileList := []*generatorFiles{
		{generator.Extensions, languagesYAML, "", extensionsFile, extensionsTmplPath, extensionsTmpl, commit},
		{generator.Heuristics, heuristicsRuby, "", contentFile, contentTmplPath, contentTmpl, commit},
		{generator.Vendor, vendorYAML, "", vendorFile, vendorTmplPath, vendorTmpl, commit},
		{generator.Documentation, documentationYAML, "", documentationFile, documentationTmplPath, documentationTmpl, commit},
		{generator.Types, languagesYAML, "", typeFile, typeTmplPath, typeTmpl, commit},
		{generator.Interpreters, languagesYAML, "", interpretersFile, interpretersTmplPath, interpretersTmpl, commit},
		{generator.Filenames, languagesYAML, samplesDir, filenamesFile, filenamesTmplPath, filenamesTmpl, commit},
		{generator.Aliases, languagesYAML, "", aliasesFile, aliasesTmplPath, aliasesTmpl, commit},
		{generator.Frequencies, "", samplesDir, frequenciesFile, frequenciesTmplPath, frequenciesTmpl, commit},
		{generator.Commit, "", "", commitFile, commitTmplPath, commitTmpl, commit},
		{generator.MimeType, languagesYAML, "", mimeTypeFile, mimeTypeTmplPath, mimeTypeTmpl, commit},
	}

	for _, file := range fileList {
		if err := file.generate(file.fileToParse, file.samplesDir, file.outPath, file.tmplPath, file.tmplName, file.commit); err != nil {
			log.Println(err)
		}
	}
}

func getCommit(path string) (string, error) {
	commit, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}

	if string(commit) == "ref: refs/heads/master\n" {
		path = ".linguist/.git/" + string(commit[5:len(commit)-1])
		commit, err = ioutil.ReadFile(path)
		if err != nil {
			return "", err
		}
	}

	return string(commit[:len(commit)-1]), nil
}
