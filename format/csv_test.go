package format

import (
	"bytes"
	"testing"
)

func TestNewCsvFormat(t *testing.T) {
	w := bytes.NewBuffer(nil)
	testFormat(&formatSpec{
		Format:  NewCsvFormat(w),
		Result:  "a,b,c\na1,b1,c1\n",
		Headers: []string{"a", "b", "c"},
		Lines: [][]interface{}{
			[]interface{}{
				"a1", "b1", "c1",
			},
		},
	}, w, t)
}

func TestNewFormat_Csv(t *testing.T) {
	testNewFormat("csv", t)
}
