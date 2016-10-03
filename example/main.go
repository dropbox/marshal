/*
 * simple Marshal example consumer
 */

package main

import (
	"github.com/dropbox/marshal/marshal"
	"github.com/op/go-logging"
)

func main() {
	log := logging.MustGetLogger("MarshalExample")

	// Construct the marshaler. There will be one of these globally, and it's thread safe so can
	// be used from any goroutine.
	marshaler, err := marshal.NewMarshaler(
		"marshal_example_client_id",
		"marshal_example_consumer_group_id",
		[]string{"127.0.0.1:9092"})
	if err != nil {
		log.Fatalf("Failed to construct marshaler: %s", err)
	}

	// Make sure to terminate the Marshaler. This ensures that we release all of the partition
	// locks we're holding so other consumers can pick them up.
	defer marshaler.Terminate()

	// Now we set up a basic consumer; and we enable GreedyClaims which is useful in low QPS
	// environments as it will cause the consumer to claim as many partitions as it can
	// up front. Of course, if you have a very busy topic with many partitions, you will
	// not want to use this.
	options := marshal.NewConsumerOptions()
	options.GreedyClaims = true

	consumer, err := marshaler.NewConsumer([]string{"some-topic"}, options)
	if err != nil {
		log.Fatalf("Failed to construct consumer: %s", err)
	}
	defer consumer.Terminate(true)

	// Now we can get the consumption channel. Messages will be available in this channel
	// and you can consume from it in many different goroutines if your message processing
	// is such that it takes a while.
	msgChan := consumer.ConsumeChannel()

	// You can spin up many goroutines to process messages; how many depends entirely on the type
	// of workload you have. See the docs.
	for i := 0; i < 10; i++ {
		i := i
		go func() {
			for {
				msg := <-msgChan
				log.Info("[%d] got message: %s", i, msg.Value)

				// Now we have to commit the message now that we're done with it. If you don't
				// commit, then Marshal will never record forward progress and will eventually
				// terminate.
				consumer.Commit(msg)
			}
		}()
	}
}
