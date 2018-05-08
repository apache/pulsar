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

type ReaderMessage struct {
	Reader
	Message
}

// ReaderBuilder is used to configure and create instances of Reader.
type ReaderBuilder interface {
	// Finalize the creation of the Reader instance.
	// This method will block until the reader is created successfully.
	Create() (Reader, error)

	// Finalize the creation of the Reader instance in asynchronous mode.
	CreateAsync(callback func(Reader, error))

	// Specify the topic this consumer will subscribe on.
	// This argument is required when constructing the reader.
	Topic(topicName string) ReaderBuilder

	// Start reading from the earliest message available in the topic
	StartFromEarliest() ReaderBuilder

	// Start reading from the end topic, only getting messages published after the
	// reader was created. This is the default behavior
	StartFromLatest() ReaderBuilder

	// Start reading from a particular message id, the reader will position itself on that
	// specific position. The first message to be read will be the message next to the specified messageId
	StartMessageId(startMessageId MessageId) ReaderBuilder

	// Sets a channel listener for the reader
	// When a {@link ReaderListener} is set, application will receive messages through it. Calls to
	// Reader.ReadNext() will not be allowed.
	ReaderListener(readerListener chan ReaderMessage) ReaderBuilder

	// Sets the size of the consumer receive queue.
	// The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
	// application calls {@link Consumer#receive()}. Using a higher value could potentially increase the consumer
	// throughput at the expense of bigger memory utilization.
	//
	// Default value is {@code 1000} messages and should be good for most use cases.
	ReceiverQueueSize(receiverQueueSize int) ReaderBuilder

	// Set the reader name.
	ReaderName(readerName string) ReaderBuilder

	// Set the subscription role prefix. The default prefix is "reader".S
	SubscriptionRolePrefix(subscriptionRolePrefix string) ReaderBuilder
}

// A Reader can be used to scan through all the messages currently available in a topic.
type Reader interface {
	// The topic from which this reader is reading from
	Topic() string

	// Read the next message in the topic
	ReadNext() (Message, error)

	// Read the next message in the topic waiting for a maximum of timeout
	// time units. Returns null if no message is recieved in that time.
	ReadNextWithTimeout(timeoutMillis int) (Message, error)

	// Close the reader and stop the broker to push more messages
	Close() error

	// Asynchronously close the reader and stop the broker to push more messages
	CloseAsync(callback Callback)
}
