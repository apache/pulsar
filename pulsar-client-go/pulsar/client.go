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

func NewClient() ClientBuilder {
	return newClient()
}

// Builder interface that is used to construct a Pulsar Client instance.
type ClientBuilder interface {
	// Create the new Pulsar Client instance
	Build() Client

	// Configure the service URL for the Pulsar service.
	// This parameter is required
	ServiceUrl(serviceUrl string) ClientBuilder

	// Set the number of threads to be used for handling connections to brokers (default: 1 thread)
	IoThreads(ioThreads int) ClientBuilder

	// Set the operation timeout (default: 30 seconds)
	// Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
	// operation will be maked as failed
	OperationTimeoutSeconds(operationTimeoutSeconds int) ClientBuilder

	// Set the number of threads to be used for message listeners (default: 1 thread)
	MessageListenerThreads(messageListenerThreads int) ClientBuilder

	// Number of concurrent lookup-requests allowed to send on each broker-connection to prevent overload on broker.
	// (default: 5000) It should be configured with higher value only in case of it requires to produce/subscribe
	// on thousands of topic using created Pulsar Client
	ConcurrentLookupRequests(concurrentLookupRequests int) ClientBuilder

	// Initialize the Log4cxx configuration
	LogConfFilePath(logConfFilePath string) ClientBuilder

	//Configure whether to use TLS encryption on the connection (default: false)
	EnableTls(enableTls bool) ClientBuilder

	// Set the path to the trusted TLS certificate file
	TlsTrustCertsFilePath(tlsTrustCertsFilePath string) ClientBuilder

	// Configure whether the Pulsar client accept untrusted TLS certificate from broker (default: false)
	TlsAllowInsecureConnection(tlsAllowInsecureConnection bool) ClientBuilder

	// Set the interval between each stat info (default: 60 seconds). Stats will be activated with positive
	// statsIntervalSeconds It should be set to at least 1 second
	StatsIntervalInSeconds(statsIntervalInSeconds int) ClientBuilder
}

type Client interface {
	// Create a producer with default for publishing on a specific topic
	// Example:
	// producer := client.newProducer().Topic(myTopic).Create()
	NewProducer() ProducerBuilder

	// Create a consumer builder
	// Example:
	// consumer := client.newConsumer().Topic(myTopic).SubscriptionName(mySubscription).Subscribe()
	NewConsumer() ConsumerBuilder

	// Create a reader builder
	// Example:
	// reader := client.newReader().Topic(myTopic).StartFromEarliest().Create()
	NewReader() ReaderBuilder

	// Close the Client and free associated resources
	Close() error
}
