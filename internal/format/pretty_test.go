package format

import (
	"bytes"
	"testing"
)

func TestNewPrettyFormat(t *testing.T) {
	w := bytes.NewBuffer(nil)
	testFormat(&formatSpec{
		Format: NewPrettyFormat(w),
		Result: `` +
			`+------+------+------+
|  A   |  B   |  C   |
+------+------+------+
| "a1" | "b1" | "c1" |
| "a1" | "b1" | NULL |
| "a1" | "b1" |    1 |
+------+------+------+
`,
		Headers: []string{"a", "b", "c"},
		Lines: [][]interface{}{
			{"a1", "b1", "c1"},
			{"a1", "b1", nil},
			{"a1", "b1", 1},
		},
	}, w, t)
}

func TestNewFormat_Pretty(t *testing.T) {
	testNewFormat("pretty", t)
}
