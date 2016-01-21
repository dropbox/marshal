# Marshal - a Kafka consumer coordination library

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/dropbox/marshal/marshal)
[![Build Status](https://travis-ci.org/dropbox/marshal.svg)](https://travis-ci.org/dropbox/marshal)

Marshal is **in beta**. We have deployed it in a few places and are
working to ensure it's stable and fast. It is not 100% battle tested
yet, feedback is very welcome.

## Purpose

This project assumes you have some familiarity with Kafka. You should
know what a topic is and what partitions are.

In Kafka, the unit of scalability is the partition. If you have a
topic that is getting "too busy", you increase the partition count.
Consumption of data from those busy topics requires consumers to be
aware of these partitions and be able to coordinate their consumption
across all of the consumers.

Traditional setups use Zookeeper or some other system for coordinating
consumers. This works in many situations, but introduces a point of
failure that isn't necessary. It is possible to completely perform
consumer coordination using Kafka alone.

Additionally, getting consumer coordination correct is a rather taxing
exercise in development and, frankly, shouldn't need to be done for
every single project, company, etc. There should be an open source
system that handles it for you.

Marshal is a library that you can drop into your Go programs and use it
to coordinate the consumption of partitions across multiple processes,
servers, etc. It is implemented in terms of Kafka itself: zero extra
dependencies.

Marshal is designed for use in production environments where there are
many topics, each topic having hundreds of partitions, with potentially
thousands of consumers working in concert across the infrastructure to
consume them. Marshal is designed for big environments with critical
needs.

## Usage

This module is designed to be extremely simple to use. The basic logical
flow is that you create a Marshaler and then you use that to create as
many Consumers as you need topics to consume. Logically, you want one
Marshaler in your program, and you want a single Consumer per topic that
you need to consume from.

Here's the simplest example (but see a more complicated example in the
example directory):

```go
package main

import "fmt"
import "github.com/dropbox/marshal/marshal"

func main() {
    marshaler, _ := marshal.NewMarshaler(
        "clientid", "groupid", []string{"127.0.0.1:9092"})
    defer marshaler.Terminate()

    consumer, _ := marshaler.NewConsumer(
        []string{"some-topic"}, marshal.NewConsumerOptions())
    defer consumer.Terminate()

    msgChan := consumer.ConsumeChannel()

    for {
        msg := <-msgChan
        fmt.Printf("Consumed message: %s", msg.Value)
        consumer.Commit(msg)
    }
}
```

If you were to hypothetically run this against a cluster that contained
a topic named `some-topic` that had 8 partitions, it would begin
claiming those partitions one by one until it had them all. If you
started up a second copy of the program, it would only claim the
partitions that are not already claimed. If the first one dies, the
second one will pick up the dropped partitions within a few minutes.

In essence, Marshal takes all of the effort of consumer coordination out
of your software and puts it where it belongs: on Kafka.

## How Coordination Works

Please read this section to get a handle on how Kafka performs
coordination and the guarantees that it gives you. In particular, the
failure scenarios might be interesting.

If you want the gory details about the protocol
used internally, please see the [PROTOCOL
documentation](https://github.com/dropbox/marshal/blob/master/PROTOCOL.md).
You don't need to read and understand it, though, but it might be
useful.

### Basic Coordination

In essence, Marshal uses a special topic within Kafka to coordinate the
actions of many consumers anywhere in the infrastructure. As long as
the consumers can connect to the Kafka cluster you want to coordinate,
you can use Marshal. There is no language dependency either -- Marshal
the algorithm could be implemented in any language and consumers could
coordinate with each other.

We assume that you're familiar with the basics of Kafka -- notably that
each partition is effectively a write-ahead log that records an ordered
set of events, and that it's not possible (barring unclean leader
elections) for two consumers to see different event orderings. Marshal
takes advantage of that property to perform distributed coordination.

When a program using Marshal starts up, the first thing it does is read
the logs in the coordinating topic. These logs contain certain events,
such as: claim partition, heartbeat, and release partition to name a
few.

Using these events Marshal can know not only what consumers exist, but
what partitions they are currently working on and how far along they
are. Using that information the local program can decide such things as
"which partitions are unclaimed" and then take action to claim and begin
consuming those partitions.

### Groups and Clients

Coordination happens within "groups". When you create a `Marshaler` you
can specify the group that your consumer is part of. All claims are done
on a per-group basis, which means you can consume the same topic N times
-- as long as you have N groups. There is a one-to-one mapping between
"consumers that can claim a given partition" and "number of groups".

The "client ID" specified when you create a `Marshaler` is used to
identify a particular instance of a program. These should be unique per
instance of software, but they should be reasonably stable. At Dropbox
we use the name of the machine the software is running on, plus possibly
an instance ID if we run multiple copies on a single box.

### Consumption of Messages

The main engine of Marshal happens when you create a consumer and call
`consumer.Consume()`. This will possibly return a message from one
of the partitions you have claimed. You then do something with the
message... and consume the next one. You don't have to do anything else.

Behind the scenes, the act of consuming updates internal cursors and
timers and will possibly generate heartbeat messages into the Marshal
event log. These messages contain information about the last offset
consumed, allowing other consumers (and monitoring systems) to know
where you are within the partition. In case of failure, they can resume
at the last point you heartbeated.

Presently, all consumption within Marshal is **at least once**. In
case of most consumer failures, it is likely a block of messages (one
heartbeat interval) will be reprocessed by the next consumer.

### Message Ordering

Kafka guarantees the ordering of messages committed to a partition,
but does not guarantee any ordering across partitions. Marshal *can*
give you the same guarantees, but by default we make no guarantee about
message ordering whatsoever.

If ordering is important, set the `StrictOrdering` option when you
create your consumer. This option will (possibly drastically) reduce the
speed of consumption, however, since only a single uncommitted message
(per partition) can be in-flight at a time.

If you are having throughput problems you should increase the number of
partitions you have available so that Marshal can have more in-flight
messages.

## Failure Modes

This documents some of the failure modes and how Marshal handles them.
Please let us know about more questions and we can analyze and write
about them.

### Consumer Too Slow

In the case where a consumer is too slow -- i.e. it is consuming more
slowly from a partition than data is coming in -- Marshal will detect
this and internally it will start failing its health checks. When this
happens it will, after enough time has passed, decide that it is not
able to sustain the load and will voluntarily surrender partitions.

This is useful as a load balancing mechanism if you happen to have one
consumer that ends up with 8 claims while another has only a handful,
the former can shed load and the latter will pick it up.

However, it is worth noting that in the unbalanced scenario, as long
as the consumers are keeping up with the traffic they won't release
partitions. It is perfectly valid right now for Marshal consumers to end
up unbalanced -- as long as they're all pulling their weight.

### Consumer Death: Expected

If a consumer dies or shuts down in an expected (controlled) way,
Marshal will attempt to commit release partition events into the log. If
this happens successfully then other consumers will be able to pick up
the partitions within seconds and begin consuming exactly where the last
consumer left off.

No data is skipped or double-consumed in this mode and the downtime is
extremely minimal.

### Consumer Death: Unexpected

If a consumer dies unexpectedly, things are slightly worse off. Assuming
a hardware failure or other such issue (network split, etc), the
partition's claim will start to become stale. From the perspective of
the rest of the fleet, they will have to wait an appropriate interval
(two heartbeats) until they can claim the partition.

Data might be skipped or double-consumed, but the maximum amount is one
heartbeat's worth. Depending on the last time you heartbeated, at worst
you will see that many messages be double-consumed. The downtime of
consumption is also up to two heartbeat intervals at worst.

### Network Partitions

Since Kafka can only have a single leader for a partition, any consumers
that are on the side of the leader will be able to continue working.
Consumers that are on the other side will fail to heartbeat and will
stop being able to work -- even if they could otherwise reach the leader
for the topics they were consuming.

The consumers on the side of the Marshal coordination partitions will be
able to tell that the other consumers dropped off and will be able to
start working. (Of course, this may cause them to overload themselves
with too many claims, leading to consumer slowness.)

If the partition is between the consumer and Kafka, the consumers
will be unable to consume and will also fail their heartbeat. This is
effectively treated as Consumer Death: Unexpected. When the partition
heals, the consumers that lost their lock will know (assuming machine
time is synchronized) and will abandon their claims.

## Important Notes

This system assumes that timestamps are valid. If your machines are
not using NTP to synchronize their clocks, you will not be able to get
deterministic behavior. Sorry.

Marshal also relies on all actors being good actors. Malicious users can
cause the system to act unpredictably or at their choosing.

## Frequently Asked Questions

Here are some questions we've seen. For more, see us on IRC.

### My consumers are unbalanced; one has more partitions than the others.

This is a design property of Marshal's implementation. We start with the
premise that we can capably health check ourself and determine whether
or not we are keeping up with our current claims. If that's true, then
it doesn't matter how many partitions we have -- we'll be healthy.

This means that we can end up in a state where one consumer has several
partitions and another consumer has fewer (or none), but Marshal
guarantees that all of them will be healthy.

### My consumer isn't claiming any partitions.

This usually happens when you are reusing Client IDs and your consumer
has previously become unhealthy and released partitions. A sick consumer
will not reclaim partitions it has previously released.

Make sure you have multiple consumers with different Client IDs, or
make sure that in the single consumer use case you are using randomly
generated Client IDs every time your program starts.

## Bugs and Contact

There may be bugs. This is a new project. There are tests, however, and
we very much welcome the submission of bug reports, pull requests, etc.

Github: https://github.com/dropbox/marshal

IRC: #kafka-marshal on Freenode
