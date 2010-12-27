package mongogo

import (
    "fmt"
    "os"
)

// ---------------------------------------------------------------------------
// Logging integration.

// Avoid importing the log type information unnecessarily.  There's a small cost
// associated with using an interface rather than the type.  Depending on how
// often the logger is plugged in, it would be worth using the type instead.
type log_Logger interface {
    Output(calldepth int, s string) os.Error
}

var globalLogger log_Logger
var globalDebug bool

// Specify the *log.Logger object where log messages should be send to.
func SetLogger(logger log_Logger) {
    globalLogger = logger
}

// Enable the delivery of debug messages to the logger.  Only meaningful
// if a logger is also set.
func SetDebug(debug bool) {
    globalDebug = debug
}

func log(v ...interface{}) {
    if globalLogger != nil {
        globalLogger.Output(2, fmt.Sprint(v...))
    }
}

func logln(v ...interface{}) {
    if globalLogger != nil {
        globalLogger.Output(2, fmt.Sprintln(v...))
    }
}

func logf(format string, v ...interface{}) {
    if globalLogger != nil {
        globalLogger.Output(2, fmt.Sprintf(format, v...))
    }
}

func debug(v ...interface{}) {
    if globalDebug && globalLogger != nil {
        globalLogger.Output(2, fmt.Sprint(v...))
    }
}

func debugln(v ...interface{}) {
    if globalDebug && globalLogger != nil {
        globalLogger.Output(2, fmt.Sprintln(v...))
    }
}

func debugf(format string, v ...interface{}) {
    if globalDebug && globalLogger != nil {
        globalLogger.Output(2, fmt.Sprintf(format, v...))
    }
}
