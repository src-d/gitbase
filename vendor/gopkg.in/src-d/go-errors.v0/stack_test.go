package errors

import (
	"fmt"
	"testing"

	"strings"

	"github.com/stretchr/testify/assert"
)

func TestCaller(t *testing.T) {
	s := NewStackTrace(0)
	assert.NotNil(t, s)

	o := fmt.Sprintf("%s", s)
	assert.Equal(t, strings.HasPrefix(o, "[stack_test.go testing.go"), true)
}

func TestCallerSkip(t *testing.T) {
	full := NewStackTrace(0)
	assert.NotNil(t, full)

	s := NewStackTrace(1)
	assert.Len(t, s.StackTrace, len(full.StackTrace)-1)
}
