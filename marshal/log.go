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
	"sync"

	"github.com/op/go-logging"
)

var log *logging.Logger
var logMu = &sync.Mutex{}

func init() {
	logMu.Lock()
	defer logMu.Unlock()

	if log != nil {
		return
	}
	log = logging.MustGetLogger("KafkaMarshal")
	logging.SetLevel(logging.INFO, "KafkaMarshal")
}

// SetLogger can be called with a logging.Logger in order to overwrite our internal
// logger. Useful if you need to control the logging (such as in tests).
func SetLogger(l *logging.Logger) {
	logMu.Lock()
	defer logMu.Unlock()

	log = l
}
