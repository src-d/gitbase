package jsonlines

import (
	"bufio"
	"encoding/json"
	"io"
)

const (
	// DefaultBufferSize is the default buffer size for decoding. It will
	// be used whenever the given reader is not buffered.
	DefaultBufferSize = 1024 * 1024 * 4
)

type lineReader interface {
	ReadBytes(delim byte) ([]byte, error)
}

// Decoder decodes JSON lines.
type Decoder interface {
	// Decode decodes the next JSON line into the given value.
	Decode(interface{}) error
}

type decoder struct {
	r lineReader
}

// NewDecoder creates a new decoder with the given reader. If the given reader
// is not buffered, it will be wrapped with a *bufio.Reader.
func NewDecoder(r io.Reader) Decoder {
	var lr lineReader
	if v, ok := r.(lineReader); ok {
		lr = v
	} else {
		lr = bufio.NewReaderSize(r, DefaultBufferSize)
	}

	return &decoder{r: lr}
}

// Decode decodes the next line in the reader.
// It does not check JSON for well-formedness before decoding, so in case of
// error, the structure might be half-filled.
func (d *decoder) Decode(v interface{}) error {
	line, err := d.r.ReadBytes('\n')
	if err != nil {
		return err
	}

	switch o := v.(type) {
	case json.Unmarshaler:
		return o.UnmarshalJSON(line)
	default:
		return json.Unmarshal(line, v)
	}
}
