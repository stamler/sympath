package main

import (
	"io"
	"log"
)

type logLevel int

const (
	logLevelDebug logLevel = iota
	logLevelInfo
	logLevelWarn
	logLevelError
	logLevelSilent
)

type verboseLogger struct {
	logger   *log.Logger
	minLevel logLevel
	progress *progressOutput
}

func newLeveledLogger(w io.Writer, minLevel logLevel) verboseLogger {
	if w == nil {
		return verboseLogger{}
	}
	progress := newProgressOutput(w)
	return verboseLogger{
		logger:   log.New(progress, "", 0),
		minLevel: minLevel,
		progress: progress,
	}
}

func newScanLogger(w io.Writer, verbose bool) verboseLogger {
	if verbose {
		return newLeveledLogger(w, logLevelDebug)
	}
	return newLeveledLogger(w, logLevelInfo)
}

func newUILogger(w io.Writer) verboseLogger {
	return newLeveledLogger(w, logLevelInfo)
}

func newVerboseLogger(w io.Writer, enabled bool) verboseLogger {
	return newScanLogger(w, enabled)
}

func (l verboseLogger) Enabled(level logLevel) bool {
	return l.logger != nil && level >= l.minLevel
}

func (l verboseLogger) Debugf(format string, args ...any) {
	l.logf(logLevelDebug, "DEBUG", format, args...)
}

func (l verboseLogger) Infof(format string, args ...any) {
	l.logf(logLevelInfo, "INFO", format, args...)
}

func (l verboseLogger) Warnf(format string, args ...any) {
	l.logf(logLevelWarn, "WARN", format, args...)
}

func (l verboseLogger) Errorf(format string, args ...any) {
	l.logf(logLevelError, "ERROR", format, args...)
}

func (l verboseLogger) logf(level logLevel, label, format string, args ...any) {
	if !l.Enabled(level) {
		return
	}
	l.logger.Printf(label+": "+format, args...)
}
