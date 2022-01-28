/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef PULSAR_CLIENT_HPP_
#define PULSAR_CLIENT_HPP_

#include <pulsar/defines.h>
#include <pulsar/Consumer.h>
#include <pulsar/Producer.h>
#include <pulsar/Reader.h>
#include <pulsar/Result.h>
#include <pulsar/Message.h>
#include <pulsar/MessageBuilder.h>
#include <pulsar/ClientConfiguration.h>
#include <pulsar/Schema.h>
#include <pulsar/ConsoleLoggerFactory.h>
#include <pulsar/FileLoggerFactory.h>
#include <string>

namespace pulsar {
typedef std::function<void(Result, Producer)> CreateProducerCallback;
typedef std::function<void(Result, Consumer)> SubscribeCallback;
typedef std::function<void(Result, Reader)> ReaderCallback;
typedef std::function<void(Result, const std::vector<std::string>&)> GetPartitionsCallback;
typedef std::function<void(Result)> CloseCallback;

class ClientImpl;
class PulsarFriend;
class PulsarWrapper;

class PULSAR_PUBLIC Client {
   public:
    /**
     * Create a Pulsar client object connecting to the specified cluster address and using the default
     * configuration.
     *
     * @param serviceUrl the Pulsar endpoint to use (eg: pulsar://localhost:6650)
     */
    Client(const std::string& serviceUrl);

    /**
     * Create a Pulsar client object connecting to the specified cluster address and using the specified
     * configuration.
     *
     * @param serviceUrl the Pulsar endpoint to use (eg:
     * http://brokerv2-pdev.messaging.corp.gq1.yahoo.com:4080 for Sandbox access)
     * @param clientConfiguration the client configuration to use
     */
    Client(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration);

    /**
     * Create a producer with default configuration
     *
     * @see createProducer(const std::string&, const ProducerConfiguration&, Producer&)
     *
     * @param topic the topic where the new producer will publish
     * @param producer a non-const reference where the new producer will be copied
     * @return ResultOk if the producer has been successfully created
     * @return ResultError if there was an error
     */
    Result createProducer(const std::string& topic, Producer& producer);

    /**
     * Create a producer with specified configuration
     *
     * @see createProducer(const std::string&, const ProducerConfiguration&, Producer&)
     *
     * @param topic the topic where the new producer will publish
     * @param conf the producer config to use
     * @param producer a non-const reference where the new producer will be copied
     * @return ResultOk if the producer has been successfully created
     * @return ResultError if there was an error
     */
    Result createProducer(const std::string& topic, const ProducerConfiguration& conf, Producer& producer);

    /**
     * Asynchronously create a producer with the default ProducerConfiguration for publishing on a specific
     * topic
     *
     * @param topic the name of the topic where to produce
     * @param callback the callback that is triggered when the producer is created successfully or not
     * @param callback Callback function that is invoked when the operation is completed
     */
    void createProducerAsync(const std::string& topic, CreateProducerCallback callback);

    /**
     * Asynchronously create a producer with the customized ProducerConfiguration for publishing on a specific
     * topic
     *
     * @param topic the name of the topic where to produce
     * @param conf the customized ProducerConfiguration
     */
    void createProducerAsync(const std::string& topic, ProducerConfiguration conf,
                             CreateProducerCallback callback);

    /**
     * Subscribe to a given topic and subscription combination with the default ConsumerConfiguration
     *
     * @param topic the topic name
     * @param subscriptionName the subscription name
     * @param[out] consumer the consumer instance to be returned
     * @return ResultOk if it subscribes to the topic successfully
     */
    Result subscribe(const std::string& topic, const std::string& subscriptionName, Consumer& consumer);

    /**
     * Subscribe to a given topic and subscription combination with the customized ConsumerConfiguration
     *
     * @param topic the topic name
     * @param subscriptionName the subscription name
     * @param[out] consumer the consumer instance to be returned
     * @return ResultOk if it subscribes to the topic successfully
     */
    Result subscribe(const std::string& topic, const std::string& subscriptionName,
                     const ConsumerConfiguration& conf, Consumer& consumer);

    /**
     * Asynchronously subscribe to a given topic and subscription combination with the default
     * ConsumerConfiguration
     *
     * @param topic the topic name
     * @param subscriptionName the subscription name
     * @param callback the callback that is triggered when a given topic and subscription combination with the
     * default ConsumerConfiguration are asynchronously subscribed successfully or not
     */
    void subscribeAsync(const std::string& topic, const std::string& subscriptionName,
                        SubscribeCallback callback);

    /**
     * Asynchronously subscribe to a given topic and subscription combination with the customized
     * ConsumerConfiguration
     *
     * @param topic the topic name
     * @param subscriptionName the subscription name
     * @param conf the customized ConsumerConfiguration
     * @param callback the callback that is triggered when a given topic and subscription combination with the
     * customized ConsumerConfiguration are asynchronously subscribed successfully or not
     */
    void subscribeAsync(const std::string& topic, const std::string& subscriptionName,
                        const ConsumerConfiguration& conf, SubscribeCallback callback);

    /**
     * Subscribe to multiple topics under the same namespace.
     *
     * @param topics a list of topic names to subscribe to
     * @param subscriptionName the subscription name
     * @param[out] consumer the consumer instance to be returned
     */
    Result subscribe(const std::vector<std::string>& topics, const std::string& subscriptionName,
                     Consumer& consumer);

    /**
     * Subscribe to multiple topics with the customized ConsumerConfiguration under the same namespace
     *
     * @param topics a list of topic names to subscribe to
     * @param subscriptionName the subscription name
     * @param conf the customized ConsumerConfiguration
     * @param[out] consumer the consumer instance to be returned
     */
    Result subscribe(const std::vector<std::string>& topics, const std::string& subscriptionName,
                     const ConsumerConfiguration& conf, Consumer& consumer);

    /**
     * Asynchronously subscribe to a list of topics and subscription combination using the default
     ConsumerConfiguration
     *
     * @param topics the topic list
     * @param subscriptionName the subscription name
     * @param callback the callback that is triggered when a list of topics and subscription combination using
     the default ConsumerConfiguration are asynchronously subscribed successfully or not

     */
    void subscribeAsync(const std::vector<std::string>& topics, const std::string& subscriptionName,
                        SubscribeCallback callback);

    /**
     * Asynchronously subscribe to a list of topics and subscription combination using the customized
     * ConsumerConfiguration
     *
     * @param topics the topic list
     * @param subscriptionName the subscription name
     * @param conf the customized ConsumerConfiguration
     * @param callback the callback that is triggered when a list of topics and subscription combination using
     * the customized ConsumerConfiguration are asynchronously subscribed successfully or not
     */
    void subscribeAsync(const std::vector<std::string>& topics, const std::string& subscriptionName,
                        const ConsumerConfiguration& conf, SubscribeCallback callback);

    /**
     * Subscribe to multiple topics, which match given regexPattern, under the same namespace.
     */
    Result subscribeWithRegex(const std::string& regexPattern, const std::string& subscriptionName,
                              Consumer& consumer);

    /**
     * Subscribe to multiple topics (which match given regexPatterns) with the customized
     * ConsumerConfiguration under the same namespace
     */
    Result subscribeWithRegex(const std::string& regexPattern, const std::string& subscriptionName,
                              const ConsumerConfiguration& conf, Consumer& consumer);

    /**
     * Asynchronously subscribe to multiple topics (which match given regexPatterns) with the default
     * ConsumerConfiguration under the same namespace
     *
     * @see subscribeWithRegexAsync(const std::string&, const std::string&, const ConsumerConfiguration&,
     * SubscribeCallback)
     */
    void subscribeWithRegexAsync(const std::string& regexPattern, const std::string& subscriptionName,
                                 SubscribeCallback callback);

    /**
     * Asynchronously subscribe to multiple topics (which match given regexPatterns) with the customized
     * ConsumerConfiguration under the same namespace
     *
     * @param regexPattern the regular expression for topics pattern
     * @param subscriptionName the subscription name
     * @param conf the ConsumerConfiguration
     * @param callback the callback that is triggered when multiple topics with the customized
     * ConsumerConfiguration under the same namespace are asynchronously subscribed successfully or not
     */
    void subscribeWithRegexAsync(const std::string& regexPattern, const std::string& subscriptionName,
                                 const ConsumerConfiguration& conf, SubscribeCallback callback);

    /**
     * Create a topic reader with given {@code ReaderConfiguration} for reading messages from the specified
     * topic.
     * <p>
     * The Reader provides a low-level abstraction that allows for manual positioning in the topic, without
     * using a
     * subscription. Reader can only work on non-partitioned topics.
     * <p>
     * The initial reader positioning is done by specifying a message id. The options are:
     * <ul>
     * <li><code>MessageId.earliest</code> : Start reading from the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Start reading from the end topic, only getting messages published
     * after the
     * reader was created
     * <li><code>MessageId</code> : When passing a particular message id, the reader will position itself on
     * that
     * specific position. The first message to be read will be the message next to the specified messageId.
     * </ul>
     *
     * @param topic
     *            The name of the topic where to read
     * @param startMessageId
     *            The message id where the reader will position itself. The first message returned will be the
     * one after
     *            the specified startMessageId
     * @param conf
     *            The {@code ReaderConfiguration} object
     * @return The {@code Reader} object
     */
    Result createReader(const std::string& topic, const MessageId& startMessageId,
                        const ReaderConfiguration& conf, Reader& reader);

    /**
     * Asynchronously create a topic reader with the customized ReaderConfiguration for reading messages from
     * the specified topic.
     *
     * The Reader provides a low-level abstraction that allows for manual positioning in the topic, without
     * using a
     * subscription. The reader can only work on non-partitioned topics.
     *
     * The initial reader positioning is done by specifying a message ID. The options are  as below:
     * <ul>
     * <li><code>MessageId.earliest</code> : start reading from the earliest message available in the topic
     * <li><code>MessageId.latest</code> : start reading from the latest topic, only getting messages
     * published after the reader was created <li><code>MessageId</code> : when passing a particular message
     * ID, the reader positions itself on that is the message next to the specified messageId.
     * </ul>
     *
     * @param topic
     *            the name of the topic where to read
     * @param startMessageId
     *            the message ID where the reader positions itself. The first message returned is the
     * one after
     *            the specified startMessageId
     * @param conf
     *            the ReaderConfiguration object
     * @return the Reader object
     */
    void createReaderAsync(const std::string& topic, const MessageId& startMessageId,
                           const ReaderConfiguration& conf, ReaderCallback callback);

    /**
     * Get the list of partitions for a given topic.
     *
     * If the topic is partitioned, this will return a list of partition names. If the topic is not
     * partitioned, the returned list will contain the topic name itself.
     *
     * This can be used to discover the partitions and create Reader, Consumer or Producer
     * instances directly on a particular partition.
     *
     * @param topic
     *            the topic name
     * @since 2.3.0
     */
    Result getPartitionsForTopic(const std::string& topic, std::vector<std::string>& partitions);

    /**
     * Get the list of partitions for a given topic in asynchronous mode.
     *
     * If the topic is partitioned, this will return a list of partition names. If the topic is not
     * partitioned, the returned list will contain the topic name itself.
     *
     * This can be used to discover the partitions and create Reader, Consumer or Producer
     * instances directly on a particular partition.
     *
     * @param topic
     *            the topic name
     * @param callback
     *            the callback that will be invoked when the list of partitions is available
     * @since 2.3.0
     */
    void getPartitionsForTopicAsync(const std::string& topic, GetPartitionsCallback callback);

    /**
     *
     * @return
     */
    Result close();

    /**
     * Asynchronously close the Pulsar client and release all resources.
     *
     * All producers, consumers, and readers are orderly closed. The client waits until all pending write
     * requests are persisted.
     *
     * @param callback the callback that is triggered when the Pulsar client is asynchronously closed
     * successfully or not
     */
    void closeAsync(CloseCallback callback);

    /**
     * Perform immediate shutdown of Pulsar client.
     *
     * Release all resources and close all producer, consumer, and readers without waiting
     * for ongoing operations to complete.
     */
    void shutdown();

    /**
     * @brief Get the number of alive producers on the current client.
     *
     * @return The number of alive producers on the  current client.
     */
    uint64_t getNumberOfProducers();

    /**
     * @brief Get the number of alive consumers on the current client.
     *
     * @return The number of alive consumers on the current client.
     */
    uint64_t getNumberOfConsumers();

   private:
    Client(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration,
           bool poolConnections);
    Client(const std::shared_ptr<ClientImpl>);

    friend class PulsarFriend;
    friend class PulsarWrapper;
    std::shared_ptr<ClientImpl> impl_;
};
}  // namespace pulsar

#endif /* PULSAR_CLIENT_HPP_ */
