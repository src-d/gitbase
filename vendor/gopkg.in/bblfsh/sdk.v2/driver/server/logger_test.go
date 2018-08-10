package server

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

func TestLoggerFactoryNew_Text(t *testing.T) {
	require := require.New(t)

	f := &LoggerFactory{Format: "text", Level: "debug"}
	logger, err := f.New()
	require.NoError(err)

	llogrus, ok := logger.(*logrus.Logger)
	require.True(ok)
	require.IsType(&prefixed.TextFormatter{}, llogrus.Formatter)
	require.Equal(logrus.DebugLevel, llogrus.Level)
}

func TestLoggerFactoryNew_JSON(t *testing.T) {
	require := require.New(t)

	f := &LoggerFactory{Format: "json", Level: "info"}
	logger, err := f.New()
	require.NoError(err)

	llogrus, ok := logger.(*logrus.Logger)
	require.True(ok)
	require.IsType(&logrus.JSONFormatter{}, llogrus.Formatter)
	require.Equal(logrus.InfoLevel, llogrus.Level)
}

func TestLoggerFactoryNew_Fields(t *testing.T) {
	require := require.New(t)

	js := `{"foo":"bar"}`
	f := &LoggerFactory{Format: "text", Level: "debug", Fields: js}
	logger, err := f.New()
	require.NoError(err)

	entry, ok := logger.(*logrus.Entry)
	require.True(ok)
	require.IsType(&prefixed.TextFormatter{}, entry.Logger.Formatter)
	require.Equal(logrus.DebugLevel, entry.Logger.Level)
	require.Equal(logrus.Fields{"foo": "bar"}, entry.Data)
}

func TestLoggerFactoryNew_Error(t *testing.T) {
	require := require.New(t)

	// missing level
	f := &LoggerFactory{Format: "text"}
	_, err := f.New()
	require.Error(err)

	// invalid level
	f = &LoggerFactory{Level: "text"}
	_, err = f.New()
	require.Error(err)

	// missing format
	f = &LoggerFactory{Level: "info"}
	_, err = f.New()
	require.Error(err)

	// invalid format
	f = &LoggerFactory{Level: "info", Format: "qux"}
	_, err = f.New()
	require.Error(err)

	// invalid json
	f = &LoggerFactory{Level: "info", Format: "text", Fields: "qux"}
	_, err = f.New()
	require.Error(err)
}

func TestLoggerFactoryApply(t *testing.T) {
	require := require.New(t)

	f := &LoggerFactory{Format: "text", Level: "debug"}
	err := f.Apply()
	require.NoError(err)

	require.IsType(&prefixed.TextFormatter{}, logrus.StandardLogger().Formatter)
	require.Equal(logrus.DebugLevel, logrus.StandardLogger().Level)
}
