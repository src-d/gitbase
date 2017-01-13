package format

import (
	"fmt"
	"io"
)

type Format interface {
	WriteHeader(headers []string) error
	Write(line []interface{}) error
	Close() error
}

func NewFormat(id string, w io.Writer) (Format, error) {
	switch id {
	case "pretty":
		return NewPrettyFormat(w), nil
	case "csv":
		return NewCsvFormat(w), nil
	case "json":
		return NewJsonFormat(w), nil
	default:
		return nil, fmt.Errorf("format not supported: %v", id)
	}
}
