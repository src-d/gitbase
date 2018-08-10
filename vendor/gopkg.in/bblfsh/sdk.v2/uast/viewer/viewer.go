package viewer

import (
	"encoding/json"

	"gopkg.in/bblfsh/sdk.v2/protocol/v1"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/role"
)

// MarshalUAST writes a UAST file compatible with uast-viewer.
func MarshalUAST(lang, code string, ast nodes.Node) ([]byte, error) {
	nd, err := uast1.ToNode(ast)
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(map[string]interface{}{
		"code": code, "uast": nd,
		"lang": lang,
	})
	var o interface{}
	err = json.Unmarshal(data, &o)
	var fix func(interface{})
	fix = func(n interface{}) {
		switch n := n.(type) {
		case []interface{}:
			for _, s := range n {
				fix(s)
			}
		case map[string]interface{}:
			for k, v := range n {
				if k == "Roles" {
					var arr []string
					for _, r := range v.([]interface{}) {
						rl := role.Role(r.(float64))
						arr = append(arr, rl.String())
					}
					n[k] = arr
					continue
				} else if k == "StartPosition" || k == "EndPosition" {
					m := v.(map[string]interface{})
					rename := func(from, to string) {
						m[to] = m[from]
						delete(m, from)
					}
					rename("offset", "Offset")
					rename("line", "Line")
					rename("col", "Col")
					continue
				}
				fix(v)
			}
		}
	}
	fix(o)
	return json.Marshal(o)
}
