package marshal

import (
	"sync"

	"github.com/dropbox/kafka"
	"github.com/dropbox/kafka/kafkatest"
	"github.com/op/go-logging"

	. "gopkg.in/check.v1"
)

type logTestBackend struct {
	c  *C
	mu *sync.Mutex
}

var logTest = &logTestBackend{mu: &sync.Mutex{}}

func init() {
	logMu.Lock()
	defer logMu.Unlock()

	leveledLogger := logging.AddModuleLevel(logTest)
	leveledLogger.SetLevel(logging.DEBUG, "KafkaMarshal")
	leveledLogger.SetLevel(logging.DEBUG, "KafkaClient")
	leveledLogger.SetLevel(logging.DEBUG, "KafkaTest")

	log = logging.MustGetLogger("KafkaMarshal")
	log.SetBackend(leveledLogger)

	kafkatest.SetLogger(log)
	kafka.SetLogger(log)
}

func (l *logTestBackend) SetC(c *C) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.c = c
}

func ResetTestLogger(c *C) {
	logTest.SetC(c)
}

func (l *logTestBackend) Log(lvl logging.Level, cd int, rec *logging.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.c.Log(rec.Formatted(cd))
	return nil
}
