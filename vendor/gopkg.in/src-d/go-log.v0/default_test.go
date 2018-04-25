package log

import (
	"fmt"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	require := require.New(t)

	os.Setenv("LOG_LEVEL", "DEBUG")

	l, err := New()
	require.NoError(err)

	logger, ok := l.(*logger)
	require.True(ok)
	require.Equal(logrus.DebugLevel, logger.Entry.Logger.Level)
}

func TestInfof_Lazy(t *testing.T) {
	require := require.New(t)

	Infof("foo")
	require.NotNil(DefaultLogger)
}

func TestInfof(t *testing.T) {
	require := require.New(t)

	m := NewMockLogger()
	DefaultLogger = m

	Infof("foo")
	require.Equal(m.calledMethods["Infof"], "foo")
}

func TestDebugf(t *testing.T) {
	require := require.New(t)

	m := NewMockLogger()
	DefaultLogger = m

	Debugf("foo")
	require.Equal(m.calledMethods["Debugf"], "foo")
}

func TestWarningf(t *testing.T) {
	require := require.New(t)

	m := NewMockLogger()
	DefaultLogger = m

	Warningf("foo")
	require.Equal(m.calledMethods["Warningf"], "foo")
}
func TestError(t *testing.T) {
	require := require.New(t)

	m := NewMockLogger()
	DefaultLogger = m

	Error(fmt.Errorf("foo"), "bar")
	require.Equal(m.calledMethods["Error"], "bar")
}

type MockLogger struct {
	calledMethods map[string]interface{}
}

func NewMockLogger() *MockLogger {
	return &MockLogger{
		calledMethods: make(map[string]interface{}, 0),
	}
}

func (l *MockLogger) New(f Fields) Logger {
	l.calledMethods["New"] = f
	return nil
}

func (l *MockLogger) Debugf(format string, args ...interface{}) {
	l.calledMethods["Debugf"] = format

}

func (l *MockLogger) Infof(format string, args ...interface{}) {
	l.calledMethods["Infof"] = format

}

func (l *MockLogger) Warningf(format string, args ...interface{}) {
	l.calledMethods["Warningf"] = format

}

func (l *MockLogger) Error(err error, format string, args ...interface{}) {
	l.calledMethods["Error"] = format

}
