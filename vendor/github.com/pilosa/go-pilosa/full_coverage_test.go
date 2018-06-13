// +build fullcoverage

package pilosa

import "testing"

func TestMakeRequestData(t *testing.T) {
	q := make([]byte, 2<<30)
	q[0] = 'a'
	p := PQLBaseQuery{
		pql: string(q),
	}
	cli := DefaultClient()
	resp, err := cli.Query(&p, nil)
	if err == nil {
		t.Fatalf("expected err with too large query, but got %v", resp)
	}
}
