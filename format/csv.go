package format

import (
	"encoding/csv"
	"fmt"
	"io"
)

type CsvFormat struct {
	cw *csv.Writer
}

func NewCsvFormat(w io.Writer) *CsvFormat {
	return &CsvFormat{
		cw: csv.NewWriter(w),
	}
}

func (cf *CsvFormat) WriteHeader(headers []string) error {
	return cf.cw.Write(headers)
}

func (cf *CsvFormat) Write(line []interface{}) error {
	rowStrings := []string{}
	for _, v := range line {
		rowStrings = append(rowStrings, fmt.Sprintf("%v", v))
	}

	return cf.cw.Write(rowStrings)
}

func (cf *CsvFormat) Close() error {
	cf.cw.Flush()

	return nil
}
