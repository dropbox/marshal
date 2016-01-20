/*
 * portal - marshal
 *
 * this package exists for the simple reason that go vet complains bitterly
 * that the go-logging package we use named its error printer 'Error' without
 * the trailing 'f', and yet it accepts a format string. blah.
 *
 */

package marshal

import (
	"fmt"
	"strings"

	"github.com/op/go-logging"
)

type logger struct {
	l *logging.Logger
}

func (l *logger) Errorf(format string, args ...interface{}) {
	l.l.Error(format, args...)
}

func (l *logger) Warningf(format string, args ...interface{}) {
	l.l.Warning(format, args...)
}

func (l *logger) Infof(format string, args ...interface{}) {
	l.l.Info(format, args...)
}

func (l *logger) Debugf(format string, args ...interface{}) {
	l.l.Debug(format, args...)
}

func (l *logger) Fatalf(format string, args ...interface{}) {
	l.l.Fatalf(format, args...)
}

// optiopayLoggerShim is a translation layer between what the optiopay library expects and what
// we provide so all the logging ends up in one nice place.
type optiopayLoggerShim struct {
	l *logger
}

// toString is because of the formatting that the optiopay library uses, this condenses it to a
// version that works with our logging.
func (l *optiopayLoggerShim) toString(msg string, args ...interface{}) string {
	if len(args)%2 != 0 {
		return fmt.Sprintf("%s: <count mismatch!> %s", msg, args)
	}

	var argset []string
	for idx := 0; idx < len(args)/2; idx++ {
		argset = append(argset, fmt.Sprintf("%v=%v", args[idx*2], args[idx*2+1]))
	}

	return fmt.Sprintf("%s: %s", msg, strings.Join(argset, ", "))
}

func (l *optiopayLoggerShim) Error(format string, args ...interface{}) {
	l.l.Errorf(l.toString(format, args...))
}

func (l *optiopayLoggerShim) Warn(format string, args ...interface{}) {
	l.l.Warningf(l.toString(format, args...))
}

func (l *optiopayLoggerShim) Info(format string, args ...interface{}) {
	l.l.Infof(l.toString(format, args...))
}

func (l *optiopayLoggerShim) Debug(format string, args ...interface{}) {
	l.l.Debugf(l.toString(format, args...))
}

var log *logger

func init() {
	log = &logger{l: logging.MustGetLogger("PortalMarshal")}
	logging.SetLevel(logging.INFO, "PortalMarshal")
}
