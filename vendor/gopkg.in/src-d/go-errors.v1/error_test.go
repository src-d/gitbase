package errors

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	k := NewKind("foo")
	assert.Equal(t, k.Message, "foo")
}

func TestKindNew(t *testing.T) {
	k := NewKind("foo")
	err := k.New()
	assert.Equal(t, err.Error(), "foo")
	assert.NotNil(t, err.StackTrace())
}

func TestKindNewWithFormat(t *testing.T) {
	k := NewKind("foo %s")
	err := k.New("bar")
	assert.Equal(t, err.Error(), "foo bar")
}

func TestKindWrap(t *testing.T) {
	k := NewKind("foo")
	err := k.Wrap(io.EOF)
	assert.Equal(t, err.Error(), "foo: EOF")
	assert.Equal(t, err.Cause(), io.EOF)
	assert.NotNil(t, err.StackTrace())
}

func TestKindWrapWithFormat(t *testing.T) {
	k := NewKind("foo %s")
	err := k.Wrap(io.EOF, "bar")
	assert.Equal(t, err.Error(), "foo bar: EOF")
	assert.Equal(t, err.Cause(), io.EOF)
	assert.NotNil(t, err.StackTrace())
}

func TestKindIs(t *testing.T) {
	k := NewKind("foo")
	err := k.New("bar")
	assert.Equal(t, k.Is(err), true)
	assert.Equal(t, k.Is(io.EOF), false)
	assert.Equal(t, k.Is(nil), false)
}

func TestKindIsChildren(t *testing.T) {
	k := NewKind("foo")
	err := k.Wrap(io.EOF)
	assert.Equal(t, k.Is(err), true)
}

func TestError(t *testing.T) {
	err := NewKind("foo %s").New("bar")
	assert.Equal(t, err.Error(), "foo bar")
}

func TestErrorCause(t *testing.T) {
	err := NewKind("foo").Wrap(io.EOF)
	assert.Equal(t, err.Error(), "foo: EOF")
}

func TestFormat(t *testing.T) {
	err := NewKind("foo %s").New("bar")
	assert.Equal(t, fmt.Sprintf("%s", err), "foo bar")
}

func TestFormatQuoted(t *testing.T) {
	err := NewKind("foo %s").New("bar")
	assert.Equal(t, fmt.Sprintf("%q", err), `"foo bar"`)
}

func TestFormatExtendedVerbose(t *testing.T) {
	err := NewKind("foo %s").New("bar")

	lines := strings.Split(fmt.Sprintf("%+v", err), "\n")
	assert.Len(t, lines, 8)
	assert.Equal(t, lines[0], "foo bar")
	assert.Equal(t, lines[2], "gopkg.in/src-d/go-errors%2ev1.TestFormatExtendedVerbose")
}
