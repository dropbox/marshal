/*
 * simple Marshal debug/timer utility
 *
 * To use: point this binary at a Kafka server and give it a topic and some consumer options.
 * It will then spin up a Marshaler and start to claim the topic. We time every operation and
 * will report some statistics about the state of the world.
 */

package main

import (
	"flag"
	"time"

	"github.com/op/go-logging"
	"github.com/zorkian/marshal/marshal"
)

var log = logging.MustGetLogger("MarshalDebug")

type timeableFunc func()

func timeIt(text string, tf timeableFunc) {
	start := time.Now()
	tf()
	elapsed := time.Now().Sub(start)
	log.Info("<%0.2f ms> %s", float64(elapsed.Nanoseconds())/1000000.0, text)
}

func main() {
	broker := flag.String("broker", "localhost:9092", "ip:port of a single broker")
	group := flag.String("group", "debug-group", "group ID to use")
	client := flag.String("client", "debug-client", "client ID to use")
	topic := flag.String("topic", "test64", "topic to test against")
	claimTopic := flag.Bool("claim-topic", false, "claim entire topic mode")
	greedyClaim := flag.Bool("greedy-claim", false, "turn on greedy claims")
	fastReclaim := flag.Bool("fast-reclaim", false, "enable fast reclaim mode")
	flag.Parse()

	// Raise marshal debugging level
	logging.SetLevel(logging.INFO, "PortalMarshal")

	// Construction timing
	var m *marshal.Marshaler
	timeIt("construct Marshaler", func() {
		var err error
		m, err = marshal.NewMarshaler(*client, *group, []string{*broker})
		if err != nil {
			log.Fatalf("Failed to construct Marshaler: %s", err)
		}
	})
	defer timeIt("terminate Marshaler", func() { m.Terminate() })

	// Ensure target topic exists
	partitions := m.Partitions(*topic)
	if partitions == 0 {
		log.Fatalf("Topic %s has no partitions/does not exist.", *topic)
	}
	log.Info("Topic %s has %d partitions.", *topic, partitions)

	// Set up consumption of the topic with the options they gave us
	options := marshal.NewConsumerOptions()
	options.GreedyClaims = *greedyClaim
	options.FastReclaim = *fastReclaim
	options.ClaimEntireTopic = *claimTopic

	var c *marshal.Consumer
	timeIt("construct Consumer", func() {
		var err error
		c, err = m.NewConsumer(*topic, options)
		if err != nil {
			log.Fatalf("Failed to construct consumer: %s", err)
		}
	})
	defer timeIt("terminate Consumer", func() { c.Terminate(true) })

	// Now wait for partitions to be claimed
	for {
		time.Sleep(2 * time.Second)
		log.Info("Marshal claims: %d", c.GetCurrentLoad())
	}
}
