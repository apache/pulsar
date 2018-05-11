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

import "time"

func NewClient(options ClientOptions) (Client, error) {
	return newClient(options)
}

// Builder interface that is used to construct a Pulsar Client instance.
type ClientOptions struct {
	// Configure the service URL for the Pulsar service.
	// This parameter is required
	URL string

	// Number of threads to be used for handling connections to brokers (default: 1 thread)
	IoThreads int

	// Set the operation timeout (default: 30 seconds)
	// Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
	// operation will be maked as failed
	OperationTimeoutSeconds time.Duration

	// Set the number of threads to be used for message listeners (default: 1 thread)
	MessageListenerThreads int

	// Number of concurrent lookup-requests allowed to send on each broker-connection to prevent overload on broker.
	// (default: 5000) It should be configured with higher value only in case of it requires to produce/subscribe
	// on thousands of topic using created Pulsar Client
	ConcurrentLookupRequests int

	// Initialize the Log4cxx configuration
	LogConfFilePath string

	// Configure whether to use TLS encryption on the connection (default: false)
	EnableTls bool

	// Set the path to the trusted TLS certificate file
	TlsTrustCertsFilePath string

	// Configure whether the Pulsar client accept untrusted TLS certificate from broker (default: false)
	TlsAllowInsecureConnection bool

	// Set the interval between each stat info (default: 60 seconds). Stats will be activated with positive
	// statsIntervalSeconds It should be set to at least 1 second
	StatsIntervalInSeconds int
}

type Client interface {
	// Create the producer instance
	// This method will block until the producer is created successfully
	CreateProducer(options ProducerOptions) (Producer, error)

	// Create the producer instance in asynchronous mode. The callback
	// will be triggered once the operation is completed
	CreateProducerAsync(options ProducerOptions, callback func(Producer, error))

	// Create a `Consumer` by subscribing to a topic.
	//
	// If the subscription does not exist, a new subscription will be created and all messages published after the
	// creation will be retained until acknowledged, even if the consumer is not connected
	Subscribe(options ConsumerOptions) (Consumer, error)

	// Create a `Consumer` by subscribing to a topic in asynchronous mode.
	//
	// If the subscription does not exist, a new subscription will be created and all messages published after the
	// creation will be retained until acknowledged, even if the consumer is not connected
	SubscribeAsync(options ConsumerOptions, callback func(Consumer, error))

	// Create a Reader instance.
	// This method will block until the reader is created successfully.
	CreateReader(options ReaderOptions) (Reader, error)

	// Create a Reader instance. in asynchronous mode.
	CreateReaderAsync(options ReaderOptions, callback func(Reader, error))

	// Close the Client and free associated resources
	Close() error
}
