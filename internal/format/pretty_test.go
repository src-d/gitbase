package format

import (
	"bytes"
	"testing"
)

func TestNewPrettyFormat(t *testing.T) {
	w := bytes.NewBuffer(nil)
	testFormat(&formatSpec{
		Format: NewPrettyFormat(w),
		Result: "+----+----+----+\n| A  | B  | C  |" +
			"\n+----+----+----+\n| a1 | b1 | c1 |" +
			"\n+----+----+----+\n",
		Headers: []string{"a", "b", "c"},
		Lines: [][]interface{}{
			[]interface{}{
				"a1", "b1", "c1",
			},
		},
	}, w, t)
}

func TestNewFormat_Pretty(t *testing.T) {
	testNewFormat("pretty", t)
}
