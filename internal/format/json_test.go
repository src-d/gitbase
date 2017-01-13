package format

import (
	"bytes"
	"testing"
)

func TestNewJsonFormat(t *testing.T) {
	w := bytes.NewBuffer(nil)
	testFormat(&formatSpec{
		Format:  NewJsonFormat(w),
		Result:  "{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\"}\n",
		Headers: []string{"a", "b", "c"},
		Lines: [][]interface{}{
			[]interface{}{
				"a1", "b1", "c1",
			},
		},
	}, w, t)
}


func TestNewFormat_Json(t *testing.T) {
	testNewFormat("json", t)
}