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
	"time"
	"context"
)

// Pair of a Consumer and Message
type ConsumerMessage struct {
	Consumer
	Message
}

// Types of subscription supported by Pulsar
type SubscriptionType int

const (
	// There can be only 1 consumer on the same topic with the same subscription name
	Exclusive SubscriptionType = 0

	// Multiple consumer will be able to use the same subscription name and the messages will be dispatched according to
	// a round-robin rotation between the connected consumers
	Shared SubscriptionType = 1

	// Multiple consumer will be able to use the same subscription name but only 1 consumer will receive the messages.
	// If that consumer disconnects, one of the other connected consumers will start receiving messages.
	Failover SubscriptionType = 2
)

// ConsumerBuilder is used to configure and create instances of Consumer
type ConsumerOptions struct {
	// Specify the topic this consumer will subscribe on.
	// This argument is required when subscribing
	Topic string

	// Specify the subscription name for this consumer
	// This argument is required when subscribing
	SubscriptionName string

	// Set the timeout for unacked messages
	// Message not acknowledged within the give time, will be replayed by the broker to the same or a different consumer
	// Default is 0, which means message are not being replayed based on ack time
	AckTimeout time.Duration

	// Select the subscription type to be used when subscribing to the topic.
	// Default is `Exclusive`
	Type SubscriptionType

	// Sets a `MessageChannel` for the consumer
	// When a message is received, it will be pushed to the channel for consumption
	MessageChannel chan ConsumerMessage

	// Sets the size of the consumer receive queue.
	// The consumer receive queue controls how many messages can be accumulated by the `Consumer` before the
	// application calls `Consumer.receive()`. Using a higher value could potentially increase the consumer
	// throughput at the expense of bigger memory utilization.
	// Default value is `1000` messages and should be good for most use cases.
	// Set to -1 to disable prefetching in consumer
	ReceiverQueueSize int

	// Set the max total receiver queue size across partitions.
	// This setting will be used to reduce the receiver queue size for individual partitions
	// ReceiverQueueSize(int) if the total exceeds this value (default: 50000).
	MaxTotalReceiverQueueSizeAcrossPartitions int

	// Set the consumer name.
	Name string
}

// An interface that abstracts behavior of Pulsar's consumer
type Consumer interface {
	// Get the topic for the consumer
	Topic() string

	// Get a subscription for the consumer
	Subscription() string

	// Unsubscribe the consumer
	Unsubscribe() error

	// Receives a single message.
	// This calls blocks until a message is available.
	Receive(context.Context) (Message, error)

	//Ack the consumption of a single message
	Ack(Message) error

	// Ack the consumption of a single message, identified by its MessageID
	AckID(MessageID) error

	// Ack the reception of all the messages in the stream up to (and including) the provided message.
	// This method will block until the acknowledge has been sent to the broker. After that, the messages will not be
	// re-delivered to this consumer.
	//
	// Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
	//
	// It's equivalent to calling asyncAcknowledgeCumulative(Message) and waiting for the callback to be triggered.
	AckCumulative(Message) error

	// Ack the reception of all the messages in the stream up to (and including) the provided message.
	// This method will block until the acknowledge has been sent to the broker. After that, the messages will not be
	// re-delivered to this consumer.
	//
	// Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
	//
	// It's equivalent to calling asyncAcknowledgeCumulative(MessageID) and waiting for the callback to be triggered.
	AckCumulativeID(MessageID) error

	// Close the consumer and stop the broker to push more messages
	Close() error

	// Redelivers all the unacknowledged messages. In Failover mode, the request is ignored if the consumer is not
	// active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed across all
	// the connected consumers. This is a non blocking call and doesn't throw an exception. In case the connection
	// breaks, the messages are redelivered after reconnect.
	RedeliverUnackedMessages()
}
