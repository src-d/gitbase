package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

const (
	// functions benchmarked
	getLanguageFunc = "GetLanguage()"
	classifyFunc    = "Classify()"
	modelineFunc    = "GetLanguagesByModeline()"
	filenameFunc    = "GetLanguagesByFilename()"
	shebangFunc     = "GetLanguagesByShebang()"
	extensionFunc   = "GetLanguagesByExtension()"
	contentFunc     = "GetLanguagesByContent()"

	// benchmark's outputs
	enryTotalBench       = "enry_total.bench"
	enrySamplesBench     = "enry_samples.bench"
	linguistTotalBench   = "linguist_total.bench"
	linguistSamplesBench = "linguist_samples.bench"

	// files to generate
	enryTotalCSV       = "enry-total.csv"
	enrySamplesCSV     = "enry-samples.csv"
	linguistTotalCSV   = "linguist-total.csv"
	linguistSamplesCSV = "linguist-samples.csv"

	// files to generate with flag distribution
	enryDistributionCSV     = "enry-distribution.csv"
	linguistDistributionCSV = "linguist-distribution.csv"
)

var (
	// flags
	distribution bool
	outDir       string

	enryFunctions         = []string{getLanguageFunc, classifyFunc, modelineFunc, filenameFunc, shebangFunc, extensionFunc, contentFunc}
	distributionIntervals = []string{"1us-10us", "10us-100us", "100us-1ms", "1ms-10ms", "10ms-100ms"}
)

func main() {
	flag.BoolVar(&distribution, "distribution", false, "generate enry-distribuition.csv and linguist-distribution.csv")
	flag.StringVar(&outDir, "outdir", "", "path to leave csv files")
	flag.Parse()

	if distribution {
		generateDistributionCSV()
		return
	}

	generateCSV()
}

func generateDistributionCSV() {
	CSVFiles := []struct {
		in   string
		out  string
		tool string
	}{
		{in: enrySamplesCSV, out: enryDistributionCSV, tool: "enry"},
		{in: linguistSamplesCSV, out: linguistDistributionCSV, tool: "linguist"},
	}

	for _, CSVFile := range CSVFiles {
		f, err := os.Open(CSVFile.in)
		if err != nil {
			log.Println(err)
			continue
		}
		defer f.Close()

		r := csv.NewReader(f)
		CSVSamples, err := r.ReadAll()
		if err != nil {
			log.Println(err)
			continue
		}

		CSVDistribution, err := buildDistribution(CSVSamples[1:], CSVFile.tool)
		if err != nil {
			log.Println(err)
			continue
		}

		if err := writeCSV(CSVDistribution, filepath.Join(outDir, CSVFile.out)); err != nil {
			log.Println(err)
			continue
		}
	}
}

func buildDistribution(CSVSamples [][]string, tool string) ([][]string, error) {
	count := make(map[string]int, len(distributionIntervals))
	for _, row := range CSVSamples {
		if row[1] != getLanguageFunc {
			continue
		}

		num, err := strconv.ParseFloat(row[len(row)-1], 64)
		if err != nil {
			return nil, err
		}

		arrangeByTime(count, num)
	}

	CSVDistribution := make([][]string, 0, len(count)+1)
	firstLine := []string{"timeInterval", tool, "numberOfFiles"}
	CSVDistribution = append(CSVDistribution, firstLine)
	for _, interval := range distributionIntervals {
		number := strconv.FormatInt(int64(count[interval]), 10)
		row := []string{interval, tool, number}
		CSVDistribution = append(CSVDistribution, row)
	}

	printDistributionInfo(count, tool)
	return CSVDistribution, nil
}

func printDistributionInfo(count map[string]int, tool string) {
	total := 0
	for _, v := range count {
		total += v
	}

	fmt.Println(tool, "files", total)
	fmt.Println("Distribution")
	for _, interval := range distributionIntervals {
		fmt.Println("\t", interval, count[interval])
	}

	fmt.Println("Percentage")
	for _, interval := range distributionIntervals {
		p := (float64(count[interval]) / float64(total)) * 100.00
		fmt.Printf("\t %s %f%%\n", interval, p)
	}

	fmt.Printf("\n\n")
}

func arrangeByTime(count map[string]int, num float64) {
	switch {
	case num > 1000.00 && num <= 10000.00:
		count[distributionIntervals[0]]++
	case num > 10000.00 && num <= 100000.00:
		count[distributionIntervals[1]]++
	case num > 100000.00 && num <= 1000000.00:
		count[distributionIntervals[2]]++
	case num > 1000000.00 && num <= 10000000.00:
		count[distributionIntervals[3]]++
	case num > 10000000.00 && num <= 100000000.00:
		count[distributionIntervals[4]]++
	}
}

func writeCSV(CSVData [][]string, outPath string) error {
	out, err := os.Create(outPath)
	if err != nil {
		return err
	}

	w := csv.NewWriter(out)
	w.WriteAll(CSVData)

	if err := w.Error(); err != nil {
		return err
	}

	return nil
}

type parse func(data []byte, tool string) ([][]string, error)

func generateCSV() {
	bmFiles := []struct {
		in    string
		out   string
		tool  string
		parse parse
	}{
		{in: enryTotalBench, out: enryTotalCSV, tool: "enry", parse: parseTotal},
		{in: linguistTotalBench, out: linguistTotalCSV, tool: "linguist", parse: parseTotal},
		{in: enrySamplesBench, out: enrySamplesCSV, tool: "enry", parse: parseSamples},
		{in: linguistSamplesBench, out: linguistSamplesCSV, tool: "linguist", parse: parseSamples},
	}

	for _, bmFile := range bmFiles {
		buf, err := ioutil.ReadFile(bmFile.in)
		if err != nil {
			log.Println(err)
			continue
		}

		info, err := bmFile.parse(buf, bmFile.tool)
		if err != nil {
			log.Println(err)
			continue
		}

		if err := writeCSV(info, filepath.Join(outDir, bmFile.out)); err != nil {
			log.Println(err)
			continue
		}
	}
}

func parseTotal(data []byte, tool string) ([][]string, error) {
	const totalLine = "_TOTAL"
	parsedInfo := map[string][]string{}
	buf := bufio.NewScanner(bytes.NewReader(data))
	for buf.Scan() {
		line := buf.Text()
		if strings.Contains(line, totalLine) {
			split := strings.Fields(line)
			row, err := getRow(split, tool)
			if err != nil {
				return nil, err
			}

			parsedInfo[row[0]] = row
		}
	}

	if err := buf.Err(); err != nil {
		return nil, err
	}

	firstLine := []string{"function", "tool", "iterations", "ns/op"}
	return prepareInfoForCSV(parsedInfo, firstLine), nil
}

func getRow(line []string, tool string) ([]string, error) {
	row := make([]string, 0, 3)
	for _, function := range enryFunctions {
		if strings.Contains(line[0], function) {
			row = append(row, function)
			break
		}
	}

	row = append(row, tool)
	iterations := line[1]
	row = append(row, iterations)

	average, err := getAverage(line)
	if err != nil {
		return nil, err

	}

	row = append(row, average)
	return row, nil
}

func getAverage(line []string) (string, error) {
	average := line[len(line)-1]
	if !strings.HasSuffix(average, ")") {
		return line[2], nil
	}

	totalTime := strings.Trim(average, "() ")
	time, err := strconv.ParseFloat(totalTime, 64)
	if err != nil {
		return "", err
	}

	iterations := line[1]
	i, err := strconv.ParseFloat(iterations, 64)
	if err != nil {
		return "", err
	}

	avg := (time * math.Pow10(9)) / i
	return fmt.Sprintf("%d", int(avg)), nil
}

func prepareInfoForCSV(parsedInfo map[string][]string, firstLine []string) [][]string {
	info := createInfoWithFirstLine(firstLine, len(parsedInfo))
	for _, function := range enryFunctions {
		info = append(info, parsedInfo[function])
	}

	return info
}

func createInfoWithFirstLine(firstLine []string, sliceLength int) (info [][]string) {
	if len(firstLine) > 0 {
		info = make([][]string, 0, sliceLength+1)
		info = append(info, firstLine)
	} else {
		info = make([][]string, 0, sliceLength)
	}

	return
}

type enryFuncs map[string][]string

func newEnryFuncs() enryFuncs {
	return enryFuncs{
		getLanguageFunc: nil,
		classifyFunc:    nil,
		modelineFunc:    nil,
		filenameFunc:    nil,
		shebangFunc:     nil,
		extensionFunc:   nil,
		contentFunc:     nil,
	}
}

func parseSamples(data []byte, tool string) ([][]string, error) {
	const sampleLine = "SAMPLE_"
	parsedInfo := map[string]enryFuncs{}
	buf := bufio.NewScanner(bytes.NewReader(data))
	for buf.Scan() {
		line := buf.Text()
		if strings.Contains(line, sampleLine) {
			split := strings.Fields(line)
			name := getSampleName(split[0])
			if _, ok := parsedInfo[name]; !ok {
				parsedInfo[name] = newEnryFuncs()
			}

			row := make([]string, 0, 4)
			row = append(row, name)
			r, err := getRow(split, tool)
			if err != nil {
				return nil, err
			}

			row = append(row, r...)
			function := row[1]
			parsedInfo[name][function] = row
		}
	}

	if err := buf.Err(); err != nil {
		return nil, err
	}

	firstLine := []string{"file", "function", "tool", "iterations", "ns/op"}
	return prepareSamplesInfoForCSV(parsedInfo, firstLine), nil
}

func getSampleName(s string) string {
	start := strings.Index(s, "SAMPLE_") + len("SAMPLE_")
	suffix := fmt.Sprintf("-%d", runtime.GOMAXPROCS(-1))
	name := strings.TrimSuffix(s[start:], suffix)
	return name
}

func prepareSamplesInfoForCSV(parsedInfo map[string]enryFuncs, firstLine []string) [][]string {
	info := createInfoWithFirstLine(firstLine, len(parsedInfo)*len(enryFunctions))
	orderedKeys := sortKeys(parsedInfo)
	for _, path := range orderedKeys {
		sampleInfo := prepareInfoForCSV(parsedInfo[path], nil)
		info = append(info, sampleInfo...)
	}

	return info
}

func sortKeys(parsedInfo map[string]enryFuncs) []string {
	keys := make([]string, 0, len(parsedInfo))
	for key := range parsedInfo {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}
