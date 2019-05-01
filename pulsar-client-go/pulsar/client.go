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

	log "github.com/apache/pulsar/pulsar-client-go/logutil"
)

func NewClient(options ClientOptions) (Client, error) {
	return newClient(options)
}

// Opaque interface that represents the authentication credentials
type Authentication interface{}

// Create new Authentication provider with specified auth token
func NewAuthenticationToken(token string) Authentication {
	return newAuthenticationToken(token)
}

// Create new Authentication provider with specified auth token supplier
func NewAuthenticationTokenSupplier(tokenSupplier func() string) Authentication {
	return newAuthenticationTokenSupplier(tokenSupplier)
}

// Create new Authentication provider with specified TLS certificate and private key
func NewAuthenticationTLS(certificatePath string, privateKeyPath string) Authentication {
	return newAuthenticationTLS(certificatePath, privateKeyPath)
}

// Create new Athenz Authentication provider with configuration in JSON form
func NewAuthenticationAthenz(authParams string) Authentication {
	return newAuthenticationAthenz(authParams)
}

// Builder interface that is used to construct a Pulsar Client instance.
type ClientOptions struct {
	// Configure the service URL for the Pulsar service.
	// This parameter is required
	URL string

	// Number of threads to be used for handling connections to brokers (default: 1 thread)
	IOThreads int

	// Set the operation timeout (default: 30 seconds)
	// Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
	// operation will be marked as failed
	OperationTimeoutSeconds time.Duration

	// Set the number of threads to be used for message listeners (default: 1 thread)
	MessageListenerThreads int

	// Number of concurrent lookup-requests allowed to send on each broker-connection to prevent overload on broker.
	// (default: 5000) It should be configured with higher value only in case of it requires to produce/subscribe
	// on thousands of topic using created Pulsar Client
	ConcurrentLookupRequests int

	// Provide a custom logger implementation where all Pulsar library info/warn/error messages will be routed
	// By default, log messages will be printed on standard output. By passing a logger function, application
	// can determine how to print logs. This function will be called each time the Pulsar client library wants
	// to write any logs.
	Logger func(level log.LoggerLevel, file string, line int, message string)

	// Set the path to the trusted TLS certificate file
	TLSTrustCertsFilePath string

	// Configure whether the Pulsar client accept untrusted TLS certificate from broker (default: false)
	TLSAllowInsecureConnection bool

	// Configure whether the Pulsar client verify the validity of the host name from broker (default: false)
	TLSValidateHostname bool

	// Configure the authentication provider. (default: no authentication)
	// Example: `Authentication: NewAuthenticationTLS("my-cert.pem", "my-key.pem")`
	Authentication

	// Set the interval between each stat info (default: 60 seconds). Stats will be activated with positive
	// statsIntervalSeconds It should be set to at least 1 second
	StatsIntervalInSeconds int
}

type Client interface {
	// Create the producer instance
	// This method will block until the producer is created successfully
	CreateProducer(ProducerOptions) (Producer, error)

	CreateProducerWithSchema(ProducerOptions, Schema) (Producer, error)

	// Create a `Consumer` by subscribing to a topic.
	//
	// If the subscription does not exist, a new subscription will be created and all messages published after the
	// creation will be retained until acknowledged, even if the consumer is not connected
	Subscribe(ConsumerOptions) (Consumer, error)

	SubscribeWithSchema(ConsumerOptions, Schema) (Consumer, error)

	// Create a Reader instance.
	// This method will block until the reader is created successfully.
	CreateReader(ReaderOptions) (Reader, error)

	CreateReaderWithSchema(ReaderOptions, Schema) (Reader, error)

	// Fetch the list of partitions for a given topic
	//
	// If the topic is partitioned, this will return a list of partition names.
	// If the topic is not partitioned, the returned list will contain the topic
	// name itself.
	//
	// This can be used to discover the partitions and create {@link Reader},
	// {@link Consumer} or {@link Producer} instances directly on a particular partition.
	TopicPartitions(topic string) ([]string, error)

	// Close the Client and free associated resources
	Close() error
}
