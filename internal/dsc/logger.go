package dsc

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// traceLevel represents the DSC trace level hierarchy.
// Higher values include all lower levels.
type traceLevel int

const (
	levelOff   traceLevel = 0
	levelError traceLevel = 1
	levelWarn  traceLevel = 2
	levelInfo  traceLevel = 3
	levelDebug traceLevel = 4
	levelTrace traceLevel = 5
)

// Logger provides structured logging for DSC resources.
// Messages are written to stderr in JSON format, following the DSC v3 logging protocol.
// The logger respects the DSC_TRACE_LEVEL environment variable to filter messages.
var Logger = &dscLogger{level: parseTraceLevel(os.Getenv("DSC_TRACE_LEVEL"))}

// parseTraceLevel converts a DSC_TRACE_LEVEL string to a traceLevel.
// Defaults to levelWarn when the variable is empty or unrecognized,
// matching the DSC default.
func parseTraceLevel(s string) traceLevel {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "ERROR":
		return levelError
	case "WARN", "WARNING":
		return levelWarn
	case "INFO":
		return levelInfo
	case "DEBUG":
		return levelDebug
	case "TRACE":
		return levelTrace
	default:
		return levelWarn
	}
}

type dscLogger struct {
	level traceLevel
}

type infoMessage struct {
	Info string `json:"info"`
}

type warnMessage struct {
	Warn string `json:"warn"`
}

type errorMessage struct {
	Error string `json:"error"`
}

type debugMessage struct {
	Debug string `json:"debug"`
}

type traceMessage struct {
	Trace string `json:"trace"`
}

// Error writes an error message to stderr in JSON format.
func (l *dscLogger) Error(msg string) {
	if l.level >= levelError {
		writeStderr(errorMessage{Error: msg})
	}
}

// Errorf writes a formatted error message to stderr in JSON format.
func (l *dscLogger) Errorf(format string, args ...any) {
	l.Error(fmt.Sprintf(format, args...))
}

// Warn writes a warning message to stderr in JSON format.
func (l *dscLogger) Warn(msg string) {
	if l.level >= levelWarn {
		writeStderr(warnMessage{Warn: msg})
	}
}

// Warnf writes a formatted warning message to stderr in JSON format.
func (l *dscLogger) Warnf(format string, args ...any) {
	l.Warn(fmt.Sprintf(format, args...))
}

// Info writes an informational message to stderr in JSON format.
func (l *dscLogger) Info(msg string) {
	if l.level >= levelInfo {
		writeStderr(infoMessage{Info: msg})
	}
}

// Infof writes a formatted informational message to stderr in JSON format.
func (l *dscLogger) Infof(format string, args ...any) {
	l.Info(fmt.Sprintf(format, args...))
}

// Debug writes a debug message to stderr in JSON format.
func (l *dscLogger) Debug(msg string) {
	if l.level >= levelDebug {
		writeStderr(debugMessage{Debug: msg})
	}
}

// Debugf writes a formatted debug message to stderr in JSON format.
func (l *dscLogger) Debugf(format string, args ...any) {
	l.Debug(fmt.Sprintf(format, args...))
}

// Trace writes a trace message to stderr in JSON format.
func (l *dscLogger) Trace(msg string) {
	if l.level >= levelTrace {
		writeStderr(traceMessage{Trace: msg})
	}
}

// Tracef writes a formatted trace message to stderr in JSON format.
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
