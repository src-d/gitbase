package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	onlyProg := flag.Bool("prog", false, "Only show programming file types in output")
	countMode := flag.String("mode", "file", "the method used to count file size. Available options are: file, line and byte")
	limitKB := flag.Int64("limit", 16*1024, "Analyse first N KB of the file (-1 means no limit)")
	flag.Parse()
	limit := (*limitKB) * 1024

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
		err = printFileAnalysis(root, limit, *jsonFlag)
		if err != nil {
			fmt.Println(err)
		}
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
				content, err := readFile(path, limit)
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

		// If we are displaying only prog. and language is not prog. skip it.
		if *onlyProg && enry.GetLanguageType(language) != enry.Programming {
			return nil
		}

		out[language] = append(out[language], relativePath)
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	var buf bytes.Buffer
	switch {
	case *jsonFlag && !*breakdownFlag:
		printJson(out, &buf)
	case *jsonFlag && *breakdownFlag:
		printBreakDown(out, &buf)
	case *breakdownFlag:
		printPercents(out, &buf, *countMode)
		buf.WriteByte('\n')
		printBreakDown(out, &buf)
	default:
		printPercents(out, &buf, *countMode)
	}

	fmt.Print(buf.String())
}

func usage() {
	fmt.Fprintf(
		os.Stderr,
		`  %[1]s %[2]s build: %[3]s commit: %[4]s, based on linguist commit: %[5]s
  %[1]s, A simple (and faster) implementation of github/linguist
  usage: %[1]s [-mode=(file|line|byte)] [-prog] <path>
         %[1]s [-mode=(file|line|byte)] [-prog] [-json] [-breakdown] <path>
         %[1]s [-mode=(file|line|byte)] [-prog] [-json] [-breakdown]
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

func printJson(out map[string][]string, buf *bytes.Buffer) {
	json.NewEncoder(buf).Encode(out)
}

// filelistError represents a failed operation that took place across multiple files.
type filelistError []string

func (e filelistError) Error() string {
	return fmt.Sprintf("Could not process the following files:\n%s", strings.Join(e, "\n"))
}

func printPercents(fSummary map[string][]string, buff *bytes.Buffer, mode string) {
	// Select the way we quantify 'amount' of code.
	var reducer func([]string) (float64, filelistError)
	switch mode {
	case "file":
		reducer = fileCountValues
	case "line":
		reducer = lineCountValues
	case "byte":
		reducer = byteCountValues
	default:
		reducer = fileCountValues
	}

	// Reduce the list of files to a quantity of file type.
	var (
		total           float64
		keys            []string
		unreadableFiles filelistError
		fileValues      = make(map[string]float64)
	)
	for fType, files := range fSummary {
		val, err := reducer(files)
		if err != nil {
			unreadableFiles = append(unreadableFiles, err...)
		}
		fileValues[fType] = val
		keys = append(keys, fType)
		total += val
	}

	// Slice the keys by their quantity (file count, line count, byte size, etc.).
	sort.Slice(keys, func(i, j int) bool {
		return fileValues[keys[i]] > fileValues[keys[j]]
	})

	// Calculate and write percentages of each file type.
	for _, fType := range keys {
		val := fileValues[fType]
		percent := val / total * 100.0
		buff.WriteString(fmt.Sprintf("%.2f%%\t%s\n", percent, fType))
		if unreadableFiles != nil {
			buff.WriteString(fmt.Sprintf("\n%s", unreadableFiles.Error()))
		}
	}
}

func fileCountValues(files []string) (float64, filelistError) {
	return float64(len(files)), nil
}

func lineCountValues(files []string) (float64, filelistError) {
	var filesErr filelistError
	var t float64
	for _, fName := range files {
		l, _ := getLines(fName, nil)
		t += float64(l)
	}
	return t, filesErr
}

func byteCountValues(files []string) (float64, filelistError) {
	var filesErr filelistError
	var t float64
	for _, fName := range files {
		f, err := os.Open(fName)
		if err != nil {
			filesErr = append(filesErr, fName)
			continue
		}
		fi, err := f.Stat()
		f.Close()
		if err != nil {
			filesErr = append(filesErr, fName)
			continue
		}
		t += float64(fi.Size())
	}
	return t, filesErr
}

func printFileAnalysis(file string, limit int64, isJSON bool) error {
	data, err := readFile(file, limit)
	if err != nil {
		return err
	}

	isSample := limit > 0 && len(data) == int(limit)

	full := data
	if isSample {
		// communicate to getLines that we don't have full contents
		full = nil
	}

	totalLines, nonBlank := getLines(file, full)

	// functions below can work on a sample
	fileType := getFileType(file, data)
	language := enry.GetLanguage(file, data)
	mimeType := enry.GetMimeType(file, language)

	if isJSON {
		return json.NewEncoder(os.Stdout).Encode(map[string]interface{}{
			"filename":    filepath.Base(file),
			"lines":       nonBlank,
			"total_lines": totalLines,
			"type":        fileType,
			"mime":        mimeType,
			"language":    language,
		})
	}

	fmt.Printf(
		`%s: %d lines (%d sloc)
  type:      %s
  mime_type: %s
  language:  %s
`,
		filepath.Base(file), totalLines, nonBlank, fileType, mimeType, language,
	)
	return nil
}

func readFile(path string, limit int64) ([]byte, error) {
	if limit <= 0 {
		return ioutil.ReadFile(path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if limit > 0 && size > limit {
		size = limit
	}
	buf := bytes.NewBuffer(nil)
	buf.Grow(int(size))
	_, err = io.Copy(buf, io.LimitReader(f, limit))
	return buf.Bytes(), err
}

func getLines(file string, content []byte) (total, blank int) {
	var r io.Reader
	if content != nil {
		r = bytes.NewReader(content)
	} else {
		// file not loaded to memory - stream it
		f, err := os.Open(file)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer f.Close()
		r = f
	}
	br := bufio.NewReader(r)
	lastBlank := true
	empty := true
	for {
		data, prefix, err := br.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println(err)
			break
		}
		if prefix {
			continue
		}
		empty = false
		total++
		lastBlank = len(data) == 0
		if lastBlank {
			blank++
		}
	}
	if !empty && lastBlank {
		total++
		blank++
	}
	nonBlank := total - blank
	return total, nonBlank
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
