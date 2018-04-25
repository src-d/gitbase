package log

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/x-cray/logrus-prefixed-formatter"
)

func TestLoggerFactoryNew_TextWithForce(t *testing.T) {
	require := require.New(t)

	f := &LoggerFactory{Format: "text", ForceFormat: true}
	l, err := f.New()
	require.NoError(err)

	logger, ok := l.(*logger)
	require.True(ok)
	require.IsType(&prefixed.TextFormatter{}, logger.Entry.Logger.Formatter)
}

func TestLoggerFactoryNew_JSON(t *testing.T) {
	require := require.New(t)

	f := &LoggerFactory{Format: "json", Level: "info"}
	l, err := f.New()
	require.NoError(err)

	logger, ok := l.(*logger)
	require.True(ok)
	require.IsType(&logrus.JSONFormatter{}, logger.Entry.Logger.Formatter)
	require.Equal(logrus.InfoLevel, logger.Entry.Logger.Level)
}

func TestLoggerFactoryNew_Fields(t *testing.T) {
	require := require.New(t)

	js := `{"foo":"bar"}`
	f := &LoggerFactory{Format: "text", Level: "debug", Fields: js}
	l, err := f.New()
	require.NoError(err)

	logger, ok := l.(*logger)
	require.True(ok)
	require.Equal(logrus.DebugLevel, logger.Entry.Logger.Level)
	require.Equal(logrus.Fields{"foo": "bar"}, logger.Entry.Data)

}

func TestLoggerFactoryNew_Error(t *testing.T) {
	require := require.New(t)

	// invalid level
	f := &LoggerFactory{Level: "text"}
	_, err := f.New()
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

	f := &LoggerFactory{Format: "text", ForceFormat: true, Level: "debug"}
	err := f.ApplyToLogrus()
	require.NoError(err)

	require.IsType(&prefixed.TextFormatter{}, logrus.StandardLogger().Formatter)
	require.Equal(logrus.DebugLevel, logrus.StandardLogger().Level)
}
