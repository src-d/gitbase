package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/src-d/enry.v1"
	"gopkg.in/src-d/enry.v1/data"
)

var (
	version = "undefined"
	build   = "undefined"
	commit  = "undefined"
)

func main() {
	flag.Usage = usage
	breakdownFlag := flag.Bool("breakdown", false, "")
	jsonFlag := flag.Bool("json", false, "")
	showVersion := flag.Bool("version", false, "Show the enry version information")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	root, err := filepath.Abs(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}

	fileInfo, err := os.Stat(root)
	if err != nil {
		log.Fatal(err)
	}

	if fileInfo.Mode().IsRegular() {
		printFileAnalysis(root)
		return
	}

	out := make(map[string][]string, 0)
	err = filepath.Walk(root, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			log.Println(err)
			return filepath.SkipDir
		}

		if !f.Mode().IsDir() && !f.Mode().IsRegular() {
			return nil
		}

		relativePath, err := filepath.Rel(root, path)
		if err != nil {
			log.Println(err)
			return nil
		}

		if relativePath == "." {
			return nil
		}

		if f.IsDir() {
			relativePath = relativePath + "/"
		}

		if enry.IsVendor(relativePath) || enry.IsDotFile(relativePath) ||
			enry.IsDocumentation(relativePath) || enry.IsConfiguration(relativePath) {
			if f.IsDir() {
				return filepath.SkipDir
			}

			return nil
		}

		if f.IsDir() {
			return nil
		}

		language, ok := enry.GetLanguageByExtension(path)
		if !ok {
			if language, ok = enry.GetLanguageByFilename(path); !ok {
				content, err := ioutil.ReadFile(path)
				if err != nil {
					log.Println(err)
					return nil
				}

				language = enry.GetLanguage(filepath.Base(path), content)
				if language == enry.OtherLanguage {
					return nil
				}
			}
		}

		out[language] = append(out[language], relativePath)
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	var buff bytes.Buffer
	switch {
	case *jsonFlag && !*breakdownFlag:
		printJson(out, &buff)
	case *jsonFlag && *breakdownFlag:
		printBreakDown(out, &buff)
	case *breakdownFlag:
		printPercents(out, &buff)
		buff.WriteByte('\n')
		printBreakDown(out, &buff)
	default:
		printPercents(out, &buff)
	}

	fmt.Print(buff.String())
}

func usage() {
	fmt.Fprintf(
		os.Stderr,
		`  %[1]s %[2]s build: %[3]s commit: %[4]s, based on linguist commit: %[5]s
  %[1]s, A simple (and faster) implementation of github/linguist
  usage: %[1]s <path>
         %[1]s [-json] [-breakdown] <path>
         %[1]s [-json] [-breakdown]
         %[1]s [-version]
`,
		os.Args[0], version, build, commit, data.LinguistCommit[:7],
	)
}

func printBreakDown(out map[string][]string, buff *bytes.Buffer) {
	for name, language := range out {
		writeStringLn(name, buff)
		for _, file := range language {
			writeStringLn(file, buff)
		}

		writeStringLn("", buff)
	}
}

func printJson(out map[string][]string, buff *bytes.Buffer) {
	data, _ := json.Marshal(out)
	buff.Write(data)
	buff.WriteByte('\n')
}

func printPercents(out map[string][]string, buff *bytes.Buffer) {
	var fileCountList enry.FileCountList
	total := 0
	for name, language := range out {
		fc := enry.FileCount{Name: name, Count: len(language)}
		fileCountList = append(fileCountList, fc)
		total += len(language)
	}
	// Sort the fileCountList in descending order of their count value.
	sort.Sort(sort.Reverse(fileCountList))

	for _, fc := range fileCountList {
		percent := float32(fc.Count) / float32(total) * 100
		buff.WriteString(fmt.Sprintf("%.2f%%	%s\n", percent, fc.Name))
	}
}

func printFileAnalysis(file string) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println(err)
	}

	totalLines, nonBlank := getLines(file, string(content))
	fileType := getFileType(file, content)
	language := enry.GetLanguage(file, content)
	mimeType := enry.GetMimeType(file, language)

	fmt.Printf(
		`%s: %d lines (%d sloc)
  type:      %s
  mime_type: %s
  language:  %s
`,
		filepath.Base(file), totalLines, nonBlank, fileType, mimeType, language,
	)
}

func getLines(file string, content string) (int, int) {
	totalLines := strings.Count(content, "\n")
	nonBlank := totalLines - strings.Count(content, "\n\n")
	return totalLines, nonBlank
}

func getFileType(file string, content []byte) string {
	switch {
	case enry.IsImage(file):
		return "Image"
	case enry.IsBinary(content):
		return "Binary"
	default:
		return "Text"
	}
}

func writeStringLn(s string, buff *bytes.Buffer) {
	buff.WriteString(s)
	buff.WriteByte('\n')
}
