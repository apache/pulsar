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

// Creates a message builder to construct a new message for publishing
func NewMessage() MessageBuilder {
	return newMessage()
}

type MessageBuilder interface {
	// Build the message to be sent
	Build() Message

	//Sets the key of the message for routing policy
	Key(key string) MessageBuilder

	// Set the payload for the message
	Payload(payload []byte) MessageBuilder

	// Sets a new property on the message
	Property(name string, value string) MessageBuilder

	// Set the event time for a given message
	EventTime(timestamp uint64) MessageBuilder

	// Specify a custom sequence id for the message being published.
	// The sequence id can be used for deduplication purposes and it needs to follow these rules:
	//  - `sequenceId >= 0`
	//  - Sequence id for a message needs to be greater than sequence id for earlier messages:
	//    `sequenceId(N+1) > sequenceId(N)`
	//  - It's not necessary for sequence ids to be consecutive.There can be holes between messages. Eg.the
	//    `sequenceId` could represent an offset or a cumulative size.
	SequenceId(sequenceId int64) MessageBuilder

	// Override the replication clusters for this message.
	ReplicationClusters(clusters []string) MessageBuilder

	// Disable replication for this message.
	DisableReplication() MessageBuilder
}

type Message interface {
	// Return the properties attached to the message.
	// Properties are application defined key/value pairs that will be attached to the message
	Properties() map[string]string

	// Get the payload of the message
	Payload() []byte

	// Get the unique message ID associated with this message.
	// The message id can be used to univocally refer to a message without having the keep the entire payload in memory.
	Id() MessageId

	// Get the publish time of this message. The publish time is the timestamp that a client publish the message.
	PublishTime() uint64

	// Get the event time associated with this message. It is typically set by the applications via
	// `MessageBuilder.setEventTime(long)`.
	// If there isn't any event time associated with this event, it will return 0.
	EventTime() uint64

	// Get the key of the message, if any
	Key() string
}

// Identifier for a particular message
type MessageId interface {
	// Serialize the message id into a sequence of bytes that can be stored somewhere else
	Serialize() []byte
}

// Reconstruct a MessageId object from its serialized representation
func DeserializeMessageId(data []byte) MessageId {
	return deserializeMessageId(data)
}

var (
	// MessageId that points to the earliest message avaialable in a topic
	EarliestMessage MessageId = earliestMessageId()

	// MessageId that points to the latest message
	LatestMessage MessageId = latestMessageId()
)
