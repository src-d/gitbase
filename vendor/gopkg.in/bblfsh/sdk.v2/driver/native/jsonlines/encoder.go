package jsonlines

import (
	"encoding/json"
	"io"
)

// Encoder encodes JSON lines.
type Encoder interface {
	// Encode encodes the next value into a JSON line.
	Encode(interface{}) error
}

type encoder struct {
	w io.Writer
}

// NewEncoder creates a new encoder using the given writer.
func NewEncoder(w io.Writer) Encoder {
	return &encoder{w}
}

func (e *encoder) Encode(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	_, err = e.w.Write(append(b, '\n'))
	return err
}
