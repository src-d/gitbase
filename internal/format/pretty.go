package format

import (
	"fmt"
	"io"

	"github.com/olekukonko/tablewriter"
)

type PrettyFormat struct {
	tw *tablewriter.Table
}

func NewPrettyFormat(w io.Writer) *PrettyFormat {
	return &PrettyFormat{
		tw: tablewriter.NewWriter(w),
	}
}

func (pf *PrettyFormat) WriteHeader(headers []string) error {
	pf.tw.SetHeader(headers)

	return nil
}

func (pf *PrettyFormat) Write(line []interface{}) error {
	rowStrings := []string{}
	for _, v := range line {
		if tv, ok := v.(string); ok {
			v = fmt.Sprintf(`"%s"`, tv)
		} else if v == nil {
			v = "NULL"
		}

		rowStrings = append(rowStrings, fmt.Sprintf("%v", v))
	}
	pf.tw.Append(rowStrings)

	return nil
}

func (pf *PrettyFormat) Close() error {
	pf.tw.Render()

	return nil
}
