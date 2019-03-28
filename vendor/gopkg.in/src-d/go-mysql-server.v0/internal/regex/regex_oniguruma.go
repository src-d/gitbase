package regex

import (
	rubex "github.com/src-d/go-oniguruma"
)

// Oniguruma holds a rubex regular expression Matcher.
type Oniguruma struct {
	reg string
}

// Match implements Matcher interface.
func (r *Oniguruma) Match(s string) bool {
	var b1, b2 []byte
	if len(r.reg) == 0 {
		b1 = []byte{0}
	} else {
		b1 = []byte(r.reg)
	}

	if len(s) == 0 {
		b2 = []byte{0}
	} else {
		b2 = []byte(s)
	}

	return rubex.MatchString3(b1, b2)
}

// Dispose implements Disposer interface.
// The function releases resources for oniguruma's precompiled regex
func (r *Oniguruma) Dispose() {
	// r.reg.Free()
}

// NewOniguruma creates a new Matcher using oniguruma engine.
func NewOniguruma(re string) (Matcher, Disposer, error) {
	// reg, err := rubex.Compile2(re)
	// if err != nil {
	// 	return nil, nil, err
	// }

	r := Oniguruma{
		reg: re,
	}
	return &r, &r, nil
}

func init() {
	err := Register("oniguruma", NewOniguruma)
	if err != nil {
		panic(err.Error())
	}
}
