/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef PULSAR_CLIENT_HPP_
#define PULSAR_CLIENT_HPP_

#include <pulsar/Auth.h>
#include <pulsar/Consumer.h>
#include <pulsar/Producer.h>
#include <pulsar/Result.h>
#include <pulsar/Message.h>
#include <pulsar/MessageBuilder.h>

#include <string>

#pragma GCC visibility push(default)

class PulsarFriend;

namespace pulsar {

typedef boost::function<void(Result, Producer)> CreateProducerCallback;
typedef boost::function<void(Result, Consumer)> SubscribeCallback;
typedef boost::function<void(Result)> CloseCallback;

class ClientConfiguration {
 public:

    ClientConfiguration();
    ~ClientConfiguration();
    ClientConfiguration(const ClientConfiguration&);
    ClientConfiguration& operator=(const ClientConfiguration&);

    /**
     * Set the authentication method to be used with the broker
     *
     * @param authentication the authentication data to use
     */
    ClientConfiguration& setAuthentication(const AuthenticationPtr& authentication);

    /**
     * @return the authentication data
     */
    const Authentication& getAuthentication() const;

    /**
     * Set timeout on client operations (subscribe, create producer, close, unsubscribe)
     * Default is 30 seconds.
     *
     * @param timeout the timeout after which the operation will be considered as failed
     */
    ClientConfiguration& setOperationTimeoutSeconds(int timeout);

    /**
     * @return the client operations timeout in seconds
     */
    int getOperationTimeoutSeconds() const;

    /**
     * Set the number of IO threads to be used by the Pulsar client. Default is 1
     * thread.
     *
     * @param threads number of threads
     */
    ClientConfiguration& setIOThreads(int threads);

    /**
     * @return the number of IO threads to use
     */
    int getIOThreads() const;

    /**
     * Set the number of threads to be used by the Pulsar client when delivering messages
     * through message listener. Default is 1 thread per Pulsar client.
     *
     * If using more than 1 thread, messages for distinct MessageListener will be
     * delivered in different threads, however a single MessageListener will always
     * be assigned to the same thread.
     *
     * @param threads number of threads
     */
    ClientConfiguration& setMessageListenerThreads(int threads);

    /**
     * @return the number of IO threads to use
     */
    int getMessageListenerThreads() const;

    /**
     * Initialize the log configuration
     *
     * @param logConfFilePath  path of the configuration file
     */
    ClientConfiguration& setLogConfFilePath(const std::string& logConfFilePath);

    /**
     * Get the path of log configuration file (log4cpp)
     */
    const std::string& getLogConfFilePath() const;

 private:
    const AuthenticationPtr& getAuthenticationPtr() const;

    struct Impl;
    boost::shared_ptr<Impl> impl_;
    friend class ClientImpl;
};

class ClientImpl;

class Client {
 public:
    /**
     * Create a Pulsar client object connecting to the specified cluster address and using the default configuration.
     *
     * @param serviceUrl the Pulsar endpoint to use (eg: http://brokerv2-pdev.messaging.corp.gq1.yahoo.com:4080 for Sandbox access)
     */
    Client(const std::string& serviceUrl);

    /**
     * Create a Pulsar client object connecting to the specified cluster address and using the specified configuration.
     *
     * @param serviceUrl the Pulsar endpoint to use (eg: http://brokerv2-pdev.messaging.corp.gq1.yahoo.com:4080 for Sandbox access)
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
    Result createProducer(const std::string& topic, const ProducerConfiguration& conf,
                          Producer& producer);

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
     *
     * @return
     */
    Result close();

    void closeAsync(CloseCallback callback);

    void shutdown();

 private:
    Client(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration, bool poolConnections);

    friend class PulsarFriend;
    boost::shared_ptr<ClientImpl> impl_;
};

}

#pragma GCC visibility pop

#endif /* PULSAR_CLIENT_HPP_ */
