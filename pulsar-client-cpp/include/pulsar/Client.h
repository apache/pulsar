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

#include <pulsar/Consumer.h>
#include <pulsar/Producer.h>
#include <pulsar/Reader.h>
#include <pulsar/Result.h>
#include <pulsar/Message.h>
#include <pulsar/MessageBuilder.h>
#include <pulsar/ClientConfiguration.h>
#include <string>

#pragma GCC visibility push(default)

namespace pulsar {
typedef boost::function<void(Result, Producer)> CreateProducerCallback;
typedef boost::function<void(Result, Consumer)> SubscribeCallback;
typedef boost::function<void(Result, Reader)> ReaderCallback;
typedef boost::function<void(Result)> CloseCallback;

class ClientImpl;
class PulsarFriend;
class PulsarWrapper;

class Client {
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

    void createProducerAsync(const std::string& topic, CreateProducerCallback callback);

    void createProducerAsync(const std::string& topic, ProducerConfiguration conf,
                             CreateProducerCallback callback);

    Result subscribe(const std::string& topic, const std::string& consumerName, Consumer& consumer);
    Result subscribe(const std::string& topic, const std::string& consumerName,
                     const ConsumerConfiguration& conf, Consumer& consumer);

    void subscribeAsync(const std::string& topic, const std::string& consumerName,
                        SubscribeCallback callback);
    void subscribeAsync(const std::string& topic, const std::string& consumerName,
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

    void createReaderAsync(const std::string& topic, const MessageId& startMessageId,
                           const ReaderConfiguration& conf, ReaderCallback callback);

    /**
     *
     * @return
     */
    Result close();

    void closeAsync(CloseCallback callback);

    void shutdown();

   private:
    Client(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration,
           bool poolConnections);
    Client(const boost::shared_ptr<ClientImpl>);

    friend class PulsarFriend;
    friend class PulsarWrapper;
    boost::shared_ptr<ClientImpl> impl_;
};
}  // namespace pulsar

#pragma GCC visibility pop

#endif /* PULSAR_CLIENT_HPP_ */
