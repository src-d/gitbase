package log

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/x-cray/logrus-prefixed-formatter"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	// DefaultLevel is the level used by LoggerFactory when Level is omitted.
	DefaultLevel = "info"
	// DefaultFormat is the format used by LoggerFactory when Format is omitted.
	DefaultFormat = "text"
)

var (
	validLevels = map[string]bool{
		"info": true, "debug": true, "warning": true, "error": true,
	}
	validFormats = map[string]bool{
		"text": true, "json": true,
	}
)

// LoggerFactory is a logger factory used to instanciate new Loggers, from
// string configuration, mainly coming from console flags.
type LoggerFactory struct {
	// Level as string, values are "info", "debug", "warning" or "error".
	Level string
	// Format as string, values are "text" or "json", by default "text" is used.
	// when a terminal is not detected "json" is used instead.
	Format string
	// Fields in JSON format to be used by configured in the new Logger.
	Fields string
	// ForceFormat if true the fact of being in a terminal or not is ignored.
	ForceFormat bool
}

// New returns a new logger based on the LoggerFactory values.
func (f *LoggerFactory) New() (Logger, error) {
	l := logrus.New()
	if err := f.setLevel(l); err != nil {
		return nil, err
	}

	if err := f.setFormat(l); err != nil {
		return nil, err
	}

	return f.setFields(l)
}

// ApplyToLogrus configures the standard logrus Logger with the LoggerFactory
// values. Useful to propagate the configuration to third-party libraries using
// logrus.
func (f *LoggerFactory) ApplyToLogrus() error {
	if err := f.setLevel(logrus.StandardLogger()); err != nil {
		return err
	}

	return f.setFormat(logrus.StandardLogger())
}

func (f *LoggerFactory) setLevel(l *logrus.Logger) error {
	if err := f.setDefaultLevel(); err != nil {
		return err
	}

	level, err := logrus.ParseLevel(f.Level)
	if err != nil {
		return err
	}

	l.Level = level
	return nil
}

func (f *LoggerFactory) setDefaultLevel() error {
	if f.Level == "" {
		f.Level = DefaultLevel
	}

	f.Level = strings.ToLower(f.Level)
	if validLevels[f.Level] {
		return nil
	}

	return fmt.Errorf(
		"invalid level %s, valid levels are: %v",
		f.Level, getKeysFromMap(validLevels),
	)
}

func (f *LoggerFactory) setFormat(l *logrus.Logger) error {
	if err := f.setDefaultFormat(); err != nil {
		return err
	}

	switch f.Format {
	case "text":
		f := new(prefixed.TextFormatter)
		f.ForceColors = true
		f.FullTimestamp = true
		l.Formatter = f
	case "json":
		l.Formatter = new(logrus.JSONFormatter)
	}

	return nil
}

func (f *LoggerFactory) setDefaultFormat() error {
	if f.Format == "" {
		f.Format = DefaultFormat
	}

	f.Format = strings.ToLower(f.Format)
	if validFormats[f.Format] {
		return nil
	}

	if !f.ForceFormat && isTerminal() {
		f.Format = "json"
	}

	return fmt.Errorf(
		"invalid format %s, valid formats are: %v",
		f.Format, getKeysFromMap(validFormats),
	)
}

func (f *LoggerFactory) setFields(l *logrus.Logger) (Logger, error) {
	var fields logrus.Fields
	if f.Fields != "" {
		if err := json.Unmarshal([]byte(f.Fields), &fields); err != nil {
			return nil, err
		}
	}

	e := l.WithFields(fields)
	return &logger{*e}, nil
}

func getKeysFromMap(m map[string]bool) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

func isTerminal() bool {
	return terminal.IsTerminal(int(os.Stdout.Fd()))
}
