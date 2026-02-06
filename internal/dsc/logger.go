package dsc

import (
	"encoding/json"
	"fmt"
	"os"
)

// Logger provides structured logging for DSC resources.
// Messages are written to stderr in JSON format, following the DSC v3 logging protocol.
var Logger = &dscLogger{}

type dscLogger struct{}

type infoMessage struct {
	Info string `json:"info"`
}

type warnMessage struct {
	Warn string `json:"warn"`
}

type errorMessage struct {
	Error string `json:"error"`
}

type traceMessage struct {
	Trace string `json:"trace"`
}

// Info writes an informational message to stderr in JSON format.
func (l *dscLogger) Info(msg string) {
	writeStderr(infoMessage{Info: msg})
}

// Infof writes a formatted informational message to stderr in JSON format.
func (l *dscLogger) Infof(format string, args ...any) {
	l.Info(fmt.Sprintf(format, args...))
}

// Warn writes a warning message to stderr in JSON format.
func (l *dscLogger) Warn(msg string) {
	writeStderr(warnMessage{Warn: msg})
}

// Warnf writes a formatted warning message to stderr in JSON format.
func (l *dscLogger) Warnf(format string, args ...any) {
	l.Warn(fmt.Sprintf(format, args...))
}

// Error writes an error message to stderr in JSON format.
func (l *dscLogger) Error(msg string) {
	writeStderr(errorMessage{Error: msg})
}

// Errorf writes a formatted error message to stderr in JSON format.
func (l *dscLogger) Errorf(format string, args ...any) {
	l.Error(fmt.Sprintf(format, args...))
}

// Trace writes a trace/debug message to stderr in JSON format.
func (l *dscLogger) Trace(msg string) {
	writeStderr(traceMessage{Trace: msg})
}

// Tracef writes a formatted trace/debug message to stderr in JSON format.
func (l *dscLogger) Tracef(format string, args ...any) {
	l.Trace(fmt.Sprintf(format, args...))
}

func writeStderr(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	fmt.Fprintln(os.Stderr, string(data))
}
