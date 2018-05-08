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
type ConsumerBuilder interface {
	// Finalize the `Consumer` creation by subscribing to the topic.
	//
	// If the subscription does not exist, a new subscription will be created and all messages published after the
	// creation will be retained until acknowledged, even if the consumer is not connected
	Subscribe() (Consumer, error)

	// Finalize the Consumer creation by subscribing to the topic in asynchronous mode.
	//
	// If the subscription does not exist, a new subscription will be created and all messages published after the
	// creation will be retained until acknowledged, even if the consumer is not connected.
	SubscribeAsync(callback func(Consumer, error))

	// Specify the topic this consumer will subscribe on
	Topic(topic string) ConsumerBuilder

	// Specify the subscription name for this consumer
	// This argument is required when constructing the consumer
	SubscriptionName(subscriptionName string) ConsumerBuilder

	// Set the timeout for unacked messages
	// Message not acknowledged within the give time, will be replayed by the broker to the same or a different consumer
	AckTimeout(ackTimeoutMillis int64) ConsumerBuilder

	// Select the subscription type to be used when subscribing to the topic.
	// Default is `Exclusive`
	SubscriptionType(subscriptionType SubscriptionType) ConsumerBuilder

	// Sets a `MessageListener` for the consumer
	// When a message is received, it will be pushed to the channel for consumption
	// When a listener  is set, application will receive messages through it. Calls to
	// `Consumer.Receive()` will not be allowed.
	MessageListener(listener chan ConsumerMessage) ConsumerBuilder

	// Sets the size of the consumer receive queue.
	// The consumer receive queue controls how many messages can be accumulated by the `Consumer` before the
	// application calls `Consumer.receive()`. Using a higher value could potentially increase the consumer
	// throughput at the expense of bigger memory utilization.
	// Default value is `1000` messages and should be good for most use cases.
	ReceiverQueueSize(receiverQueueSize int) ConsumerBuilder

	// Set the max total receiver queue size across partitions.
	// This setting will be used to reduce the receiver queue size for individual partitions
	// ReceiverQueueSize(int) if the total exceeds this value (default: 50000).
	MaxTotalReceiverQueueSizeAcrossPartitions(maxTotalReceiverQueueSizeAcrossPartitions int) ConsumerBuilder

	// Set the consumer name.
	ConsumerName(consumerName string) ConsumerBuilder
}

// An interface that abstracts behavior of Pulsar's consumer
type Consumer interface {
	// Get the topic for the consumer
	Topic() string

	// Get a subscription for the consumer
	Subscription() string

	// Unsubscribe the consumer
	Unsubscribe() error

	// Asynchronously unsubscribe the consumer
	UnsubscribeAsync(callback Callback)

	// Receives a single message.
	// This calls blocks until a message is available.
	Receive() (Message, error)

	// Receive a single message
	// Retrieves a message, waiting up to the specified wait time if necessary.
	// timeout of 0 or less means immediate rather than infinite
	ReceiveWithTimeout(timeoutMillis int) (Message, error)

	//Acknowledge the consumption of a single message
	Acknowledge(message Message) error

	// Acknowledge the consumption of a single message, identified by its MessageId
	AcknowledgeId(messageId MessageId) error

	// Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
	// This method will block until the acknowledge has been sent to the broker. After that, the messages will not be
	// re-delivered to this consumer.
	//
	// Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
	//
	// It's equivalent to calling asyncAcknowledgeCumulative(Message) and waiting for the callback to be triggered.
	AcknowledgeCumulative(message Message) error

	// Acknowledge the reception of all the messages in the stream up to (and including) the provided message.
	// This method will block until the acknowledge has been sent to the broker. After that, the messages will not be
	// re-delivered to this consumer.
	//
	// Cumulative acknowledge cannot be used when the consumer type is set to ConsumerShared.
	//
	// It's equivalent to calling asyncAcknowledgeCumulative(MessageId) and waiting for the callback to be triggered.
	AcknowledgeCumulativeId(messageId MessageId) error

	// Close the consumer and stop the broker to push more messages
	Close() error

	// Asynchronously close the consumer and stop the broker to push more messages
	CloseAsync(callback Callback)

	// Redelivers all the unacknowledged messages. In Failover mode, the request is ignored if the consumer is not
	// active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed across all
	// the connected consumers. This is a non blocking call and doesn't throw an exception. In case the connection
	// breaks, the messages are redelivered after reconnect.
	RedeliverUnacknowledgedMessages()
}
