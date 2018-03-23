package driver

import (
	"encoding/base64"
	"fmt"

	"gopkg.in/bblfsh/sdk.v1/protocol"
)

// Encode converts UTF8 string into specified encoding.
func (e Encoding) Encode(s string) (string, error) {
	switch protocol.Encoding(e) {
	case protocol.UTF8:
		return s, nil
	case protocol.Base64:
		s = base64.StdEncoding.EncodeToString([]byte(s))
		return s, nil
	default:
		return "", fmt.Errorf("invalid encoding: %v", e)
	}
}

// Decode converts specified encoding into UTF8.
func (e Encoding) Decode(s string) (string, error) {
	switch protocol.Encoding(e) {
	case protocol.UTF8:
		return s, nil
	case protocol.Base64:
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		return "", fmt.Errorf("invalid encoding: %v", e)
	}
}
