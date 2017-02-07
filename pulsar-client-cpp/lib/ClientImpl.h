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

#ifndef LIB_CLIENTIMPL_H_
#define LIB_CLIENTIMPL_H_

#include <pulsar/Client.h>
#include "ExecutorService.h"
#include "BinaryProtoLookupService.h"
#include "ConnectionPool.h"
#include "LookupDataResult.h"
#include "DestinationName.h"
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include "ProducerImplBase.h"
#include "ConsumerImplBase.h"

#include <vector>

namespace pulsar {

class ClientImpl;
class PulsarFriend;
typedef boost::shared_ptr<ClientImpl> ClientImplPtr;
typedef boost::weak_ptr<ClientImpl> ClientImplWeakPtr;

class ClientImpl : public boost::enable_shared_from_this<ClientImpl> {
 public:
    ClientImpl(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration,
               bool poolConnections);
    ~ClientImpl();

    void createProducerAsync(const std::string& topic, ProducerConfiguration conf,
                             CreateProducerCallback callback);

    void subscribeAsync(const std::string& topic, const std::string& consumerName,
                        const ConsumerConfiguration& conf, SubscribeCallback callback);

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

    const ClientConfiguration& conf() const;
    ExecutorServiceProviderPtr getIOExecutorProvider();
    ExecutorServiceProviderPtr getListenerExecutorProvider();
    ExecutorServiceProviderPtr getPartitionListenerExecutorProvider();
    friend class PulsarFriend;

 private:

    void handleCreateProducer(const Result result,
                              const LookupDataResultPtr partitionMetadata,
                              DestinationNamePtr dn,
                              ProducerConfiguration conf,
                              CreateProducerCallback callback);

    void handleSubscribe(const Result result,
                             const LookupDataResultPtr partitionMetadata,
                             DestinationNamePtr dn,
                             const std::string& consumerName,
                             ConsumerConfiguration conf,
                             SubscribeCallback callback);

    void handleProducerCreated(Result result, ProducerImplBaseWeakPtr producerWeakPtr,
                               CreateProducerCallback callback, ProducerImplBasePtr producer);
    void handleConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerWeakPtr,
                               SubscribeCallback callback, ConsumerImplBasePtr consumer);

    typedef boost::shared_ptr<int> SharedInt;

    void handleClose(Result result, SharedInt remaining, ResultCallback callback);

    enum State {
        Open,
        Closing,
        Closed
    };

    boost::mutex mutex_;

    State state_;
    std::string serviceUrl_;
    ClientConfiguration clientConfiguration_;

    ExecutorServiceProviderPtr ioExecutorProvider_;
    ExecutorServiceProviderPtr listenerExecutorProvider_;
    ExecutorServiceProviderPtr partitionListenerExecutorProvider_;

    BinaryProtoLookupService lookup_;
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

typedef boost::shared_ptr<ClientImpl> ClientImplPtr;

} /* namespace pulsar */

#endif /* LIB_CLIENTIMPL_H_ */
