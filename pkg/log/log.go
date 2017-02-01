package log

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
)

// Logger specific flags
// see: init function for more information about each flag
var (
	debug bool
)

// Infof logs info messages in verbose mode (debug-only)
func Infof(format string, args ...interface{}) {
	if !debug {
		return // only in debug mode
	}

	log.Printf(message("INFO", format), args...)
}

// Warningf logs an error that can be recovered from (debug-only)
func Warningf(format string, args ...interface{}) {
	if !debug {
		return // only in debug mode
	}

	log.Printf(message("WARNING", format), args...)
}

// Errorf logs an error that can't be recovered from and thus is fatal
func Errorf(format string, args ...interface{}) {
	log.Fatalf(message("ERROR", format), args...)
}

// message creates a informational log message
func message(level, message string) string {
	// collect fileName and lineNumber of callee
	_, fn, ln, _ := runtime.Caller(2)
	// return formatted format string
	return fmt.Sprintf("[%s] {%s:%d} %s", level, fn, ln, message)
}

func init() {
	flag.BoolVar(&debug, "debug", os.Getenv("DEBUG") != "",
		"allow for verbose logging (DEBUG env can be used as well)")
}
