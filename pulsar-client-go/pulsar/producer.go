//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package pulsar

import (
	"context"
	"time"
)

type MessageRoutingMode int

const (
	// Publish messages across all partitions in round-robin.
	RoundRobinDistribution MessageRoutingMode = iota

	// The producer will chose one single partition and publish all the messages into that partition
	UseSinglePartition

	// Use custom message router implementation that will be called to determine the partition for a particular message.
	CustomPartition
)

type HashingScheme int

const (
	JavaStringHash HashingScheme = iota // Java String.hashCode() equivalent
	Murmur3_32Hash                      // Use Murmur3 hashing function
	BoostHash                           // C++ based boost::hash
)

type CompressionType int

const (
	NoCompression CompressionType = iota
	LZ4
	ZLib
	ZSTD
	SNAPPY
)

type TopicMetadata interface {
	// Get the number of partitions for the specific topic
	NumPartitions() int
}

type ProducerOptions struct {
	// Specify the topic this producer will be publishing on.
	// This argument is required when constructing the producer.
	Topic string

	// Specify a name for the producer
	// If not assigned, the system will generate a globally unique name which can be access with
	// Producer.ProducerName().
	// When specifying a name, it is up to the user to ensure that, for a given topic, the producer name is unique
	// across all Pulsar's clusters. Brokers will enforce that only a single producer a given name can be publishing on
	// a topic.
	Name string

	// Attach a set of application defined properties to the producer
	// This properties will be visible in the topic stats
	Properties map[string]string

	// Set the send timeout (default: 30 seconds)
	// If a message is not acknowledged by the server before the sendTimeout expires, an error will be reported.
	// Setting the timeout to -1, will set the timeout to infinity, which can be useful when using Pulsar's message
	// deduplication feature.
	SendTimeout time.Duration

	// Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
	// When the queue is full, by default, all calls to Producer.send() and Producer.sendAsync() will fail
	// unless `BlockIfQueueFull` is set to true. Use BlockIfQueueFull(boolean) to change the blocking behavior.
	MaxPendingMessages int

	// Set the number of max pending messages across all the partitions
	// This setting will be used to lower the max pending messages for each partition
	// `MaxPendingMessages(int)`, if the total exceeds the configured value.
	MaxPendingMessagesAcrossPartitions int

	// Set whether the `Producer.Send()` and `Producer.sendAsync()` operations should block when the outgoing
	// message queue is full. Default is `false`. If set to `false`, send operations will immediately fail with
	// `ProducerQueueIsFullError` when there is no space left in pending queue.
	BlockIfQueueFull bool

	// Set the message routing mode for the partitioned producer.
	// Default routing mode is round-robin routing.
	//
	// This logic is applied when the application is not setting a key ProducerMessage#setKey(String) on a
	// particular message.
	MessageRoutingMode

	// Change the `HashingScheme` used to chose the partition on where to publish a particular message.
	// Standard hashing functions available are:
	//
	//  - `JavaStringHash` : Java String.hashCode() equivalent
	//  - `Murmur3_32Hash` : Use Murmur3 hashing function.
	// 		https://en.wikipedia.org/wiki/MurmurHash">https://en.wikipedia.org/wiki/MurmurHash
	//  - `BoostHash`      : C++ based boost::hash
	//
	// Default is `JavaStringHash`.
	HashingScheme

	// Set the compression type for the producer.
	// By default, message payloads are not compressed. Supported compression types are:
	//  - LZ4
	//  - ZLIB
	//  - ZSTD
	//  - SNAPPY
	//
	// Note: ZSTD is supported since Pulsar 2.3. Consumers will need to be at least at that
	// release in order to be able to receive messages compressed with ZSTD.
	//
	// Note: SNAPPY is supported since Pulsar 2.4. Consumers will need to be at least at that
	// release in order to be able to receive messages compressed with SNAPPY.
	CompressionType

	// Set a custom message routing policy by passing an implementation of MessageRouter
	// The router is a function that given a particular message and the topic metadata, returns the
	// partition index where the message should be routed to
	MessageRouter func(Message, TopicMetadata) int

	// Control whether automatic batching of messages is enabled for the producer. Default: false [No batching]
	//
	// When batching is enabled, multiple calls to Producer.sendAsync can result in a single batch to be sent to the
	// broker, leading to better throughput, especially when publishing small messages. If compression is enabled,
	// messages will be compressed at the batch level, leading to a much better compression ratio for similar headers or
	// contents.
	//
	// When enabled default batch delay is set to 1 ms and default batch size is 1000 messages
	Batching bool

	// Set the time period within which the messages sent will be batched (default: 10ms) if batch messages are
	// enabled. If set to a non zero value, messages will be queued until this time interval or until
	BatchingMaxPublishDelay time.Duration

	// Set the maximum number of messages permitted in a batch. (default: 1000) If set to a value greater than 1,
	// messages will be queued until this threshold is reached or batch interval has elapsed
	BatchingMaxMessages uint
}

// The producer is used to publish messages on a topic
type Producer interface {
	// return the topic to which producer is publishing to
	Topic() string

	// return the producer name which could have been assigned by the system or specified by the client
	Name() string

	// Send a message
	// This call will be blocking until is successfully acknowledged by the Pulsar broker.
	// Example:
	// producer.Send(ctx, pulsar.ProducerMessage{ Payload: myPayload })
	// @Deprecated
	Send(context.Context, ProducerMessage) error

	// Send a message
	// This call will be blocking until is successfully acknowledged by the Pulsar broker.
	// Example:
	// msgID, err := producer.SendAndGetMsgID(ctx, pulsar.ProducerMessage{ Payload: myPayload })
	SendAndGetMsgID(context.Context, ProducerMessage) (MessageID, error)

	// Send a message in asynchronous mode
	// The callback will report back the message being published and
	// the eventual error in publishing
	// @Deprecated
	SendAsync(context.Context, ProducerMessage, func(ProducerMessage, error))

	// Send a message in asynchronous mode
	// The callback will report back the message being published and
	// the eventual error in publishing
	SendAndGetMsgIDAsync(context.Context, ProducerMessage, func(MessageID, error))

	// Get the last sequence id that was published by this producer.
	// This represent either the automatically assigned or custom sequence id (set on the ProducerMessage) that
	// was published and acknowledged by the broker.
	// After recreating a producer with the same producer name, this will return the last message that was
	// published in the previous producer session, or -1 if there no message was ever published.
	// return the last sequence id published by this producer.
	LastSequenceID() int64

	// Flush all the messages buffered in the client and wait until all messages have been successfully
	// persisted.
	Flush() error

	// Close the producer and releases resources allocated
	// No more writes will be accepted from this producer. Waits until all pending write request are persisted. In case
	// of errors, pending writes will not be retried.
	Close() error

	Schema() Schema
}
