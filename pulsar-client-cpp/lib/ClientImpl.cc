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

#include "ClientImpl.h"

#include "LogUtils.h"
#include "ConsumerImpl.h"
#include "ProducerImpl.h"
#include "DestinationName.h"
#include "PartitionedProducerImpl.h"
#include "PartitionedConsumerImpl.h"
#include <boost/bind.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <sstream>
#include <openssl/sha.h>
#include "boost/date_time/posix_time/posix_time.hpp"

DECLARE_LOG_OBJECT()

namespace pulsar {

    static const char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    const std::string generateRandomName() {
        unsigned char hash[SHA_DIGEST_LENGTH];  // == 20;
        boost::posix_time::ptime t(boost::posix_time::microsec_clock::universal_time());
        long nanoSeconds = t.time_of_day().total_nanoseconds();
        std::stringstream ss;
        ss << nanoSeconds;
        SHA1(reinterpret_cast<const unsigned char*>(ss.str().c_str()), ss.str().length(), hash);

        const int nameLength = 6;
        std::stringstream hexHash;
        for (int i = 0; i < nameLength / 2; i++) {
            hexHash << hexDigits[(hash[i] & 0xF0) >> 4];
            hexHash << hexDigits[hash[i] & 0x0F];
        }

        return hexHash.str();
    }
    typedef boost::unique_lock<boost::mutex> Lock;

    ClientImpl::ClientImpl(const std::string& serviceUrl,
                           const ClientConfiguration& clientConfiguration, bool poolConnections)
        : mutex_(),
          state_(Open),
          serviceUrl_(serviceUrl),
          clientConfiguration_(clientConfiguration),
          ioExecutorProvider_(boost::make_shared<ExecutorServiceProvider>(clientConfiguration.getIOThreads())),
          listenerExecutorProvider_(boost::make_shared<ExecutorServiceProvider>(clientConfiguration.getMessageListenerThreads())),
          partitionListenerExecutorProvider_(boost::make_shared<ExecutorServiceProvider>(clientConfiguration.getMessageListenerThreads())),
          pool_(clientConfiguration, ioExecutorProvider_, clientConfiguration.getAuthenticationPtr(), poolConnections),
          lookup_(pool_, serviceUrl),
          producerIdGenerator_(0),
          consumerIdGenerator_(0),
          requestIdGenerator_(0) {
        LogUtils::init(clientConfiguration.getLogConfFilePath());
    }

    ClientImpl::~ClientImpl() {
        shutdown();
    }

    const ClientConfiguration& ClientImpl::conf() const {
        return clientConfiguration_;
    }

    ExecutorServiceProviderPtr ClientImpl::getIOExecutorProvider() {
        return ioExecutorProvider_;
    }

    ExecutorServiceProviderPtr ClientImpl::getListenerExecutorProvider() {
        return listenerExecutorProvider_;
    }

    ExecutorServiceProviderPtr ClientImpl::getPartitionListenerExecutorProvider() {
        return partitionListenerExecutorProvider_;
    }
    void ClientImpl::createProducerAsync(const std::string& topic,
                                         ProducerConfiguration conf,
                                         CreateProducerCallback callback) {
        DestinationNamePtr dn;
        {
            Lock lock(mutex_);
            if (state_ != Open) {
                lock.unlock();
                callback(ResultAlreadyClosed, Producer());
                return;
            } else if (!(dn = DestinationName::get(topic))) {
                lock.unlock();
                callback(ResultInvalidTopicName, Producer());
                return;
            }
        }
        lookup_.getPartitionMetadataAsync(dn).addListener(boost::bind(&ClientImpl::handleCreateProducer,
                                    shared_from_this(), _1, _2, dn, conf, callback));
    }

    void ClientImpl::handleCreateProducer(const Result result,
            const LookupDataResultPtr partitionMetadata,
            DestinationNamePtr dn,
            ProducerConfiguration conf,
            CreateProducerCallback callback) {
        if (!result) {
            ProducerImplBasePtr producer;
            if (partitionMetadata->getPartitions() > 1) {
                producer = boost::make_shared<PartitionedProducerImpl>(shared_from_this(),
                                                                       dn, partitionMetadata->getPartitions(), conf);
            } else {
                producer = boost::make_shared<ProducerImpl>(shared_from_this(), dn->toString(), conf);
            }
            producer->getProducerCreatedFuture().addListener(boost::bind(&ClientImpl::handleProducerCreated,
                                                                         shared_from_this(), _1, _2, callback,
                                                                         producer));
            Lock lock(mutex_);
            producers_.push_back(producer);
            lock.unlock();
            producer->start();
        } else {
            LOG_ERROR("Error Checking/Getting Partition Metadata while creating producer on " << dn->toString() << " -- " << result );
            callback (result, Producer());
        }

    }

    void ClientImpl::handleProducerCreated(Result result, ProducerImplBaseWeakPtr producerBaseWeakPtr,
                                           CreateProducerCallback callback, ProducerImplBasePtr producer) {
        callback(result, Producer(producer));
    }

    void ClientImpl::subscribeAsync(const std::string& topic, const std::string& consumerName,
                                    const ConsumerConfiguration& conf, SubscribeCallback callback) {
        DestinationNamePtr dn;
        {
            Lock lock(mutex_);
            if (state_ != Open) {
                lock.unlock();
                callback(ResultAlreadyClosed, Consumer());
                return;
            } else if (!(dn = DestinationName::get(topic))) {
                lock.unlock();
                callback(ResultInvalidTopicName, Consumer());
                return;
            }
        }

        lookup_.getPartitionMetadataAsync(dn).addListener(boost::bind(&ClientImpl::handleSubscribe,
                                    shared_from_this(), _1, _2, dn, consumerName, conf, callback));
    }

    void ClientImpl::handleSubscribe(const Result result,
            const LookupDataResultPtr partitionMetadata,
            DestinationNamePtr dn,
            const std::string& consumerName,
            ConsumerConfiguration conf,
            SubscribeCallback callback) {
        if (!result) {
            // generate random name if not supplied by the customer.
            if(conf.getConsumerName().empty()) {
                conf.setConsumerName(generateRandomName());
            }
            ConsumerImplBasePtr consumer;
            if (partitionMetadata->getPartitions() > 1) {
                if (conf.getReceiverQueueSize() == 0) {
                    LOG_ERROR("Can't use partitioned topic if the queue size is 0.");
                    callback (ResultInvalidConfiguration, Consumer());
                    return;
                }
                consumer = boost::make_shared<PartitionedConsumerImpl>(shared_from_this(),
                                                                       consumerName,
                                                                       dn,
                                                                       partitionMetadata->getPartitions(),
                                                                       conf);
            } else  {
                consumer = boost::make_shared<ConsumerImpl>(shared_from_this(), dn->toString(),
                                                            consumerName, conf);
            }
            consumer->getConsumerCreatedFuture().addListener(boost::bind(&ClientImpl::handleConsumerCreated,
                                                                         shared_from_this(), _1, _2, callback,
                                                                         consumer));
            Lock lock(mutex_);
            consumers_.push_back(consumer);
            lock.unlock();
            consumer->start();
        } else {
            LOG_ERROR("Error Checking/Getting Partition Metadata while Subscribing- " <<  result );
            callback (result, Consumer());
        }

    }

    void ClientImpl::handleConsumerCreated(Result result,
                                           ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                           SubscribeCallback callback,
                                           ConsumerImplBasePtr consumer) {
        callback(result, Consumer(consumer));
    }

    Future<Result, ClientConnectionWeakPtr> ClientImpl::getConnection(const std::string& topic) {
        Promise<Result, ClientConnectionWeakPtr> promise;
        lookup_.lookupAsync(topic).addListener(boost::bind(&ClientImpl::handleLookup, this, _1, _2, promise));
        return promise.getFuture();
    }

    void ClientImpl::handleLookup(Result result, LookupDataResultPtr data,
                                      Promise<Result, ClientConnectionWeakPtr> promise) {
            if (data) {
                LOG_DEBUG("Getting connection to broker: " << data->getBrokerUrl());
                Future<Result, ClientConnectionWeakPtr> future = pool_.getConnectionAsync(
                                                                                          data->getBrokerUrl());
                future.addListener(boost::bind(&ClientImpl::handleNewConnection, this, _1, _2, promise));
            } else {
                promise.setFailed(result);
            }
        }

    void ClientImpl::handleNewConnection(Result result, const ClientConnectionWeakPtr& conn,
                                         Promise<Result, ClientConnectionWeakPtr> promise) {
        if (result == ResultOk) {
            promise.setValue(conn);
        } else {
            promise.setFailed(ResultConnectError);
        }
    }

    void ClientImpl::closeAsync(CloseCallback callback) {
        Lock lock(mutex_);
        ProducersList producers(producers_);
        ConsumersList consumers(consumers_);

        if (state_ != Open && callback) {
            lock.unlock();
            callback(ResultAlreadyClosed);
            return;
        }
        // Set the state to Closing so that no producers could get added
        state_ = Closing;
        lock.unlock();

        LOG_DEBUG("Closing Pulsar client");
        SharedInt numberOfOpenHandlers = boost::make_shared<int>(producers.size() + consumers.size());

        for (ProducersList::iterator it = producers.begin(); it != producers.end(); ++it) {
            ProducerImplBasePtr producer = it->lock();
            if (producer && !producer->isClosed()) {
                producer->closeAsync(
                                     boost::bind(&ClientImpl::handleClose, shared_from_this(), _1, numberOfOpenHandlers,
                                                 callback));
            } else {
                // Since the connection is already closed
                (*numberOfOpenHandlers)--;
            }
        }

        for (ConsumersList::iterator it = consumers.begin(); it != consumers.end(); ++it) {
            ConsumerImplBasePtr consumer = it->lock();
            if (consumer && !consumer->isClosed()) {
                consumer->closeAsync(
                                     boost::bind(&ClientImpl::handleClose, shared_from_this(), _1, numberOfOpenHandlers,
                                                 callback));
            } else {
                // Since the connection is already closed
                (*numberOfOpenHandlers)--;
            }
        }

        if (*numberOfOpenHandlers == 0 && callback) {
            callback(ResultOk);
        }
    }

    void ClientImpl::handleClose(Result result, SharedInt numberOfOpenHandlers, ResultCallback callback) {
        static bool errorClosing = false;
        static Result failResult = ResultOk;
        if (result != ResultOk) {
            errorClosing = true;
            failResult = result;
        }
        if(*numberOfOpenHandlers > 0) {
            --(*numberOfOpenHandlers);
        }
        if (*numberOfOpenHandlers == 0) {

            Lock lock(mutex_);
            state_ = Closed;
            lock.unlock();
            if (errorClosing) {
                LOG_DEBUG("Problem in closing client, could not close one or more consumers or producers");
                if (callback) {
                    callback(failResult);
                }
            }

            LOG_DEBUG("Shutting down producers and consumers for client");
            shutdown();
            if (callback) {
                callback(ResultOk);
            }
        }
    }

    void ClientImpl::shutdown() {
        Lock lock(mutex_);
        ProducersList producers;
        ConsumersList consumers;

        producers.swap(producers_);
        consumers.swap(consumers_);
        lock.unlock();

        for (ProducersList::iterator it = producers.begin(); it != producers.end(); ++it) {
            ProducerImplBasePtr producer = it->lock();
            if (producer) {
                producer->shutdown();
            }
        }

        for (ConsumersList::iterator it = consumers.begin(); it != consumers.end(); ++it) {
            ConsumerImplBasePtr consumer = it->lock();
            if (consumer) {
                consumer->shutdown();
            }
        }

        ioExecutorProvider_->close();
        listenerExecutorProvider_->close();
        partitionListenerExecutorProvider_->close();
    }

    uint64_t ClientImpl::newProducerId() {
        Lock lock(mutex_);
        return producerIdGenerator_++;
    }

    uint64_t ClientImpl::newConsumerId() {
        Lock lock(mutex_);
        return consumerIdGenerator_++;
    }

    uint64_t ClientImpl::newRequestId() {
        Lock lock(mutex_);
        return requestIdGenerator_++;
    }

} /* namespace pulsar */
