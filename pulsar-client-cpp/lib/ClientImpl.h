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
#ifndef LIB_CLIENTIMPL_H_
#define LIB_CLIENTIMPL_H_

#include <pulsar/Client.h>
#include "ExecutorService.h"
#include "BinaryProtoLookupService.h"
#include "ConnectionPool.h"
#include "LookupDataResult.h"
#include <mutex>
#include <lib/TopicName.h>
#include "ProducerImplBase.h"
#include "ConsumerImplBase.h"

#include <vector>

namespace pulsar {

class ClientImpl;
class PulsarFriend;
typedef std::shared_ptr<ClientImpl> ClientImplPtr;
typedef std::weak_ptr<ClientImpl> ClientImplWeakPtr;

class ReaderImpl;
typedef std::shared_ptr<ReaderImpl> ReaderImplPtr;
typedef std::weak_ptr<ReaderImpl> ReaderImplWeakPtr;

const std::string generateRandomName();

class ClientImpl : public std::enable_shared_from_this<ClientImpl> {
   public:
    ClientImpl(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration,
               bool poolConnections);
    ~ClientImpl();

    void createProducerAsync(const std::string& topic, ProducerConfiguration conf,
                             CreateProducerCallback callback);

    void subscribeAsync(const std::string& topic, const std::string& consumerName,
                        const ConsumerConfiguration& conf, SubscribeCallback callback);

    void subscribeAsync(const std::vector<std::string>& topics, const std::string& consumerName,
                        const ConsumerConfiguration& conf, SubscribeCallback callback);

    void subscribeWithRegexAsync(const std::string& regexPattern, const std::string& consumerName,
                                 const ConsumerConfiguration& conf, SubscribeCallback callback);

    void createReaderAsync(const std::string& topic, const MessageId& startMessageId,
                           const ReaderConfiguration& conf, ReaderCallback callback);

    void getPartitionsForTopicAsync(const std::string& topic, GetPartitionsCallback callback);

    Future<Result, ClientConnectionWeakPtr> getConnection(const std::string& topic);
    void handleLookup(Result result, LookupDataResultPtr data,
                      Promise<Result, ClientConnectionWeakPtr> promise);
    void handleNewConnection(Result result, const ClientConnectionWeakPtr& conn,
                             Promise<Result, ClientConnectionWeakPtr> promise);

    void closeAsync(CloseCallback callback);
    void shutdown();

    uint64_t newProducerId();
    uint64_t newConsumerId();
    uint64_t newRequestId();

    const ClientConfiguration& getClientConfig() const;

    const ClientConfiguration& conf() const;
    ExecutorServiceProviderPtr getIOExecutorProvider();
    ExecutorServiceProviderPtr getListenerExecutorProvider();
    ExecutorServiceProviderPtr getPartitionListenerExecutorProvider();
    LookupServicePtr getLookup();
    friend class PulsarFriend;

   private:
    void handleCreateProducer(const Result result, const LookupDataResultPtr partitionMetadata,
                              TopicNamePtr topicName, ProducerConfiguration conf,
                              CreateProducerCallback callback);

    void handleSubscribe(const Result result, const LookupDataResultPtr partitionMetadata,
                         TopicNamePtr topicName, const std::string& consumerName, ConsumerConfiguration conf,
                         SubscribeCallback callback);

    void handleReaderMetadataLookup(const Result result, const LookupDataResultPtr partitionMetadata,
                                    TopicNamePtr topicName, MessageId startMessageId,
                                    ReaderConfiguration conf, ReaderCallback callback);

    void handleGetPartitions(const Result result, const LookupDataResultPtr partitionMetadata,
                             TopicNamePtr topicName, GetPartitionsCallback callback);

    void handleProducerCreated(Result result, ProducerImplBaseWeakPtr producerWeakPtr,
                               CreateProducerCallback callback, ProducerImplBasePtr producer);
    void handleConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerWeakPtr,
                               SubscribeCallback callback, ConsumerImplBasePtr consumer);

    typedef std::shared_ptr<int> SharedInt;

    void handleClose(Result result, SharedInt remaining, ResultCallback callback);

    void createPatternMultiTopicsConsumer(const Result result, const NamespaceTopicsPtr topics,
                                          const std::string& regexPattern, const std::string& consumerName,
                                          const ConsumerConfiguration& conf, SubscribeCallback callback);

    enum State
    {
        Open,
        Closing,
        Closed
    };

    std::mutex mutex_;

    State state_;
    std::string serviceUrl_;
    ClientConfiguration clientConfiguration_;

    ExecutorServiceProviderPtr ioExecutorProvider_;
    ExecutorServiceProviderPtr listenerExecutorProvider_;
    ExecutorServiceProviderPtr partitionListenerExecutorProvider_;

    LookupServicePtr lookupServicePtr_;
    ConnectionPool pool_;

    uint64_t producerIdGenerator_;
    uint64_t consumerIdGenerator_;
    uint64_t requestIdGenerator_;

    typedef std::vector<ProducerImplBaseWeakPtr> ProducersList;
    ProducersList producers_;

    typedef std::vector<ConsumerImplBaseWeakPtr> ConsumersList;
    ConsumersList consumers_;

    friend class Client;
};
} /* namespace pulsar */

#endif /* LIB_CLIENTIMPL_H_ */
