package format

import (
	"encoding/json"
	"io"
)

type JsonFormat struct {
	je   *json.Encoder
	keys []string
}

func NewJsonFormat(w io.Writer) *JsonFormat {
	return &JsonFormat{
		je: json.NewEncoder(w),
	}
}

func (cf *JsonFormat) WriteHeader(headers []string) error {
	cf.keys = headers

	return nil
}

func (cf *JsonFormat) Write(line []interface{}) error {
	j := make(map[string]interface{})
	for i, k := range cf.keys {
		j[k] = line[i]
	}

	return cf.je.Encode(j)
}

func (cf *JsonFormat) Close() error {
	return nil
}
