package transformer

import (
	"fmt"
	"strconv"
)

// Quote uses strconv.Quote/Unquote to wrap provided string value.
func Quote(op Op) Op {
	return StringConv(op, func(s string) (string, error) {
		ns, err := strconv.Unquote(s)
		if err != nil {
			return "", fmt.Errorf("%v (%s)", err, s)
		}
		return ns, nil
	}, func(s string) (string, error) {
		return strconv.Quote(s), nil
	})
}
