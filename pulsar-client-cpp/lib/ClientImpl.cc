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
#include "ClientImpl.h"
#include "ClientConfigurationImpl.h"
#include "LogUtils.h"
#include "ConsumerImpl.h"
#include "ProducerImpl.h"
#include "ReaderImpl.h"
#include "PartitionedProducerImpl.h"
#include "PartitionedConsumerImpl.h"
#include "MultiTopicsConsumerImpl.h"
#include "PatternMultiTopicsConsumerImpl.h"
#include <pulsar/ConsoleLoggerFactory.h>
#include <boost/algorithm/string/predicate.hpp>
#include <sstream>
#include <lib/HTTPLookupService.h>
#include <lib/TopicName.h>
#include <algorithm>
#include <random>
#include <mutex>
#ifdef USE_LOG4CXX
#include "Log4CxxLogger.h"
#endif

#ifdef PULSAR_USE_BOOST_REGEX
#include <boost/regex.hpp>
#define PULSAR_REGEX_NAMESPACE boost
#else
#include <regex>
#define PULSAR_REGEX_NAMESPACE std
#endif

DECLARE_LOG_OBJECT()

namespace pulsar {

static const char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                 '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
static std::uniform_int_distribution<> hexDigitsDist(0, sizeof(hexDigits) - 1);
static std::mt19937 randomEngine =
    std::mt19937(std::chrono::high_resolution_clock::now().time_since_epoch().count());

std::string generateRandomName() {
    const int randomNameLength = 10;

    std::string randomName;
    for (int i = 0; i < randomNameLength; ++i) {
        randomName += hexDigits[hexDigitsDist(randomEngine)];
    }
    return randomName;
}

typedef std::unique_lock<std::mutex> Lock;

typedef std::vector<std::string> StringList;

static const std::string https("https");
static const std::string pulsarSsl("pulsar+ssl");

static const ClientConfiguration detectTls(const std::string& serviceUrl,
                                           const ClientConfiguration& clientConfiguration) {
    ClientConfiguration conf(clientConfiguration);
    if (serviceUrl.compare(0, https.size(), https) == 0 ||
        serviceUrl.compare(0, pulsarSsl.size(), pulsarSsl) == 0) {
        conf.setUseTls(true);
    }
    return conf;
}

ClientImpl::ClientImpl(const std::string& serviceUrl, const ClientConfiguration& clientConfiguration,
                       bool poolConnections)
    : mutex_(),
      state_(Open),
      serviceUrl_(serviceUrl),
      clientConfiguration_(detectTls(serviceUrl, clientConfiguration)),
      memoryLimitController_(clientConfiguration.getMemoryLimit()),
      ioExecutorProvider_(std::make_shared<ExecutorServiceProvider>(clientConfiguration_.getIOThreads())),
      listenerExecutorProvider_(
          std::make_shared<ExecutorServiceProvider>(clientConfiguration_.getMessageListenerThreads())),
      partitionListenerExecutorProvider_(
          std::make_shared<ExecutorServiceProvider>(clientConfiguration_.getMessageListenerThreads())),
      pool_(clientConfiguration_, ioExecutorProvider_, clientConfiguration_.getAuthPtr(), poolConnections),
      producerIdGenerator_(0),
      consumerIdGenerator_(0),
      requestIdGenerator_(0),
      closingError(ResultOk) {
    std::unique_ptr<LoggerFactory> loggerFactory = clientConfiguration_.impl_->takeLogger();
    if (!loggerFactory) {
#ifdef USE_LOG4CXX
        if (!clientConfiguration_.getLogConfFilePath().empty()) {
            // A log4cxx log file was passed through deprecated parameter. Use that to configure Log4CXX
            loggerFactory = Log4CxxLoggerFactory::create(clientConfiguration_.getLogConfFilePath());
        } else {
            // Use default simple console logger
            loggerFactory.reset(new ConsoleLoggerFactory);
        }
#else
        // Use default simple console logger
        loggerFactory.reset(new ConsoleLoggerFactory);
#endif
    }
    LogUtils::setLoggerFactory(std::move(loggerFactory));

    if (serviceUrl_.compare(0, 4, "http") == 0) {
        LOG_DEBUG("Using HTTP Lookup");
        lookupServicePtr_ =
            std::make_shared<HTTPLookupService>(std::cref(serviceUrl_), std::cref(clientConfiguration_),
                                                std::cref(clientConfiguration_.getAuthPtr()));
    } else {
        LOG_DEBUG("Using Binary Lookup");
        lookupServicePtr_ = std::make_shared<BinaryProtoLookupService>(
            std::ref(pool_), std::ref(serviceUrl), std::cref(clientConfiguration_.getListenerName()));
    }
}

ClientImpl::~ClientImpl() { shutdown(); }

const ClientConfiguration& ClientImpl::conf() const { return clientConfiguration_; }

MemoryLimitController& ClientImpl::getMemoryLimitController() { return memoryLimitController_; }

ExecutorServiceProviderPtr ClientImpl::getIOExecutorProvider() { return ioExecutorProvider_; }

ExecutorServiceProviderPtr ClientImpl::getListenerExecutorProvider() { return listenerExecutorProvider_; }

ExecutorServiceProviderPtr ClientImpl::getPartitionListenerExecutorProvider() {
    return partitionListenerExecutorProvider_;
}

LookupServicePtr ClientImpl::getLookup() { return lookupServicePtr_; }

void ClientImpl::createProducerAsync(const std::string& topic, ProducerConfiguration conf,
                                     CreateProducerCallback callback) {
    TopicNamePtr topicName;
    {
        Lock lock(mutex_);
        if (state_ != Open) {
            lock.unlock();
            callback(ResultAlreadyClosed, Producer());
            return;
        } else if (!(topicName = TopicName::get(topic))) {
            lock.unlock();
            callback(ResultInvalidTopicName, Producer());
            return;
        }
    }
    lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
        std::bind(&ClientImpl::handleCreateProducer, shared_from_this(), std::placeholders::_1,
                  std::placeholders::_2, topicName, conf, callback));
}

void ClientImpl::handleCreateProducer(const Result result, const LookupDataResultPtr partitionMetadata,
                                      TopicNamePtr topicName, ProducerConfiguration conf,
                                      CreateProducerCallback callback) {
    if (!result) {
        ProducerImplBasePtr producer;
        if (partitionMetadata->getPartitions() > 0) {
            producer = std::make_shared<PartitionedProducerImpl>(shared_from_this(), topicName,
                                                                 partitionMetadata->getPartitions(), conf);
        } else {
            producer = std::make_shared<ProducerImpl>(shared_from_this(), topicName->toString(), conf);
        }
        producer->getProducerCreatedFuture().addListener(
            std::bind(&ClientImpl::handleProducerCreated, shared_from_this(), std::placeholders::_1,
                      std::placeholders::_2, callback, producer));
        Lock lock(mutex_);
        producers_.push_back(producer);
        lock.unlock();
        producer->start();
    } else {
        LOG_ERROR("Error Checking/Getting Partition Metadata while creating producer on "
                  << topicName->toString() << " -- " << result);
        callback(result, Producer());
    }
}

void ClientImpl::handleProducerCreated(Result result, ProducerImplBaseWeakPtr producerBaseWeakPtr,
                                       CreateProducerCallback callback, ProducerImplBasePtr producer) {
    callback(result, Producer(producer));
}

void ClientImpl::createReaderAsync(const std::string& topic, const MessageId& startMessageId,
                                   const ReaderConfiguration& conf, ReaderCallback callback) {
    TopicNamePtr topicName;
    {
        Lock lock(mutex_);
        if (state_ != Open) {
            lock.unlock();
            callback(ResultAlreadyClosed, Reader());
            return;
        } else if (!(topicName = TopicName::get(topic))) {
            lock.unlock();
            callback(ResultInvalidTopicName, Reader());
            return;
        }
    }

    MessageId msgId(startMessageId);
    lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
        std::bind(&ClientImpl::handleReaderMetadataLookup, shared_from_this(), std::placeholders::_1,
                  std::placeholders::_2, topicName, msgId, conf, callback));
}

void ClientImpl::handleReaderMetadataLookup(const Result result, const LookupDataResultPtr partitionMetadata,
                                            TopicNamePtr topicName, MessageId startMessageId,
                                            ReaderConfiguration conf, ReaderCallback callback) {
    if (result != ResultOk) {
        LOG_ERROR("Error Checking/Getting Partition Metadata while creating readeron "
                  << topicName->toString() << " -- " << result);
        callback(result, Reader());
        return;
    }

    if (partitionMetadata->getPartitions() > 0) {
        LOG_ERROR("Topic reader cannot be created on a partitioned topic: " << topicName->toString());
        callback(ResultOperationNotSupported, Reader());
        return;
    }

    ReaderImplPtr reader = std::make_shared<ReaderImpl>(shared_from_this(), topicName->toString(), conf,
                                                        getListenerExecutorProvider()->get(), callback);
    reader->start(startMessageId);

    Lock lock(mutex_);
    consumers_.push_back(reader->getConsumer());
}

void ClientImpl::subscribeWithRegexAsync(const std::string& regexPattern, const std::string& subscriptionName,
                                         const ConsumerConfiguration& conf, SubscribeCallback callback) {
    TopicNamePtr topicNamePtr = TopicName::get(regexPattern);

    Lock lock(mutex_);
    if (state_ != Open) {
        lock.unlock();
        callback(ResultAlreadyClosed, Consumer());
        return;
    } else {
        lock.unlock();
        if (!topicNamePtr) {
            LOG_ERROR("Topic pattern not valid: " << regexPattern);
            callback(ResultInvalidTopicName, Consumer());
            return;
        }
    }

    NamespaceNamePtr nsName = topicNamePtr->getNamespaceName();

    lookupServicePtr_->getTopicsOfNamespaceAsync(nsName).addListener(
        std::bind(&ClientImpl::createPatternMultiTopicsConsumer, shared_from_this(), std::placeholders::_1,
                  std::placeholders::_2, regexPattern, subscriptionName, conf, callback));
}

void ClientImpl::createPatternMultiTopicsConsumer(const Result result, const NamespaceTopicsPtr topics,
                                                  const std::string& regexPattern,
                                                  const std::string& subscriptionName,
                                                  const ConsumerConfiguration& conf,
                                                  SubscribeCallback callback) {
    if (result == ResultOk) {
        ConsumerImplBasePtr consumer;

        PULSAR_REGEX_NAMESPACE::regex pattern(regexPattern);

        NamespaceTopicsPtr matchTopics =
            PatternMultiTopicsConsumerImpl::topicsPatternFilter(*topics, pattern);

        consumer = std::make_shared<PatternMultiTopicsConsumerImpl>(
            shared_from_this(), regexPattern, *matchTopics, subscriptionName, conf, lookupServicePtr_);

        consumer->getConsumerCreatedFuture().addListener(
            std::bind(&ClientImpl::handleConsumerCreated, shared_from_this(), std::placeholders::_1,
                      std::placeholders::_2, callback, consumer));
        Lock lock(mutex_);
        consumers_.push_back(consumer);
        lock.unlock();
        consumer->start();
    } else {
        LOG_ERROR("Error Getting topicsOfNameSpace while createPatternMultiTopicsConsumer:  " << result);
        callback(result, Consumer());
    }
}

void ClientImpl::subscribeAsync(const std::vector<std::string>& topics, const std::string& subscriptionName,
                                const ConsumerConfiguration& conf, SubscribeCallback callback) {
    TopicNamePtr topicNamePtr;

    Lock lock(mutex_);
    if (state_ != Open) {
        lock.unlock();
        callback(ResultAlreadyClosed, Consumer());
        return;
    } else {
        if (!topics.empty() && !(topicNamePtr = MultiTopicsConsumerImpl::topicNamesValid(topics))) {
            lock.unlock();
            callback(ResultInvalidTopicName, Consumer());
            return;
        }
    }

    if (topicNamePtr) {
        std::string randomName = generateRandomName();
        std::stringstream consumerTopicNameStream;
        consumerTopicNameStream << topicNamePtr->toString() << "-TopicsConsumerFakeName-" << randomName;
        topicNamePtr = TopicName::get(consumerTopicNameStream.str());
    }

    ConsumerImplBasePtr consumer = std::make_shared<MultiTopicsConsumerImpl>(
        shared_from_this(), topics, subscriptionName, topicNamePtr, conf, lookupServicePtr_);

    consumer->getConsumerCreatedFuture().addListener(std::bind(&ClientImpl::handleConsumerCreated,
                                                               shared_from_this(), std::placeholders::_1,
                                                               std::placeholders::_2, callback, consumer));
    consumers_.push_back(consumer);
    lock.unlock();
    consumer->start();
}

void ClientImpl::subscribeAsync(const std::string& topic, const std::string& subscriptionName,
                                const ConsumerConfiguration& conf, SubscribeCallback callback) {
    TopicNamePtr topicName;
    {
        Lock lock(mutex_);
        if (state_ != Open) {
            lock.unlock();
            callback(ResultAlreadyClosed, Consumer());
            return;
        } else if (!(topicName = TopicName::get(topic))) {
            lock.unlock();
            callback(ResultInvalidTopicName, Consumer());
            return;
        } else if (conf.isReadCompacted() && (topicName->getDomain().compare("persistent") != 0 ||
                                              (conf.getConsumerType() != ConsumerExclusive &&
                                               conf.getConsumerType() != ConsumerFailover))) {
            lock.unlock();
            callback(ResultInvalidConfiguration, Consumer());
            return;
        }
    }

    lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
        std::bind(&ClientImpl::handleSubscribe, shared_from_this(), std::placeholders::_1,
                  std::placeholders::_2, topicName, subscriptionName, conf, callback));
}

void ClientImpl::handleSubscribe(const Result result, const LookupDataResultPtr partitionMetadata,
                                 TopicNamePtr topicName, const std::string& subscriptionName,
                                 ConsumerConfiguration conf, SubscribeCallback callback) {
    if (result == ResultOk) {
        // generate random name if not supplied by the customer.
        if (conf.getConsumerName().empty()) {
            conf.setConsumerName(generateRandomName());
        }
        ConsumerImplBasePtr consumer;
        if (partitionMetadata->getPartitions() > 0) {
            if (conf.getReceiverQueueSize() == 0) {
                LOG_ERROR("Can't use partitioned topic if the queue size is 0.");
                callback(ResultInvalidConfiguration, Consumer());
                return;
            }
            consumer = std::make_shared<PartitionedConsumerImpl>(
                shared_from_this(), subscriptionName, topicName, partitionMetadata->getPartitions(), conf);
        } else {
            auto consumerImpl = std::make_shared<ConsumerImpl>(shared_from_this(), topicName->toString(),
                                                               subscriptionName, conf);
            consumerImpl->setPartitionIndex(topicName->getPartitionIndex());
            consumer = consumerImpl;
        }
        consumer->getConsumerCreatedFuture().addListener(
            std::bind(&ClientImpl::handleConsumerCreated, shared_from_this(), std::placeholders::_1,
                      std::placeholders::_2, callback, consumer));
        Lock lock(mutex_);
        consumers_.push_back(consumer);
        lock.unlock();
        consumer->start();
    } else {
        LOG_ERROR("Error Checking/Getting Partition Metadata while Subscribing on " << topicName->toString()
                                                                                    << " -- " << result);
        callback(result, Consumer());
    }
}

void ClientImpl::handleConsumerCreated(Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
                                       SubscribeCallback callback, ConsumerImplBasePtr consumer) {
    callback(result, Consumer(consumer));
}

Future<Result, ClientConnectionWeakPtr> ClientImpl::getConnection(const std::string& topic) {
    Promise<Result, ClientConnectionWeakPtr> promise;
    lookupServicePtr_->lookupAsync(topic).addListener(std::bind(&ClientImpl::handleLookup, shared_from_this(),
                                                                std::placeholders::_1, std::placeholders::_2,
                                                                promise));
    return promise.getFuture();
}

void ClientImpl::handleLookup(Result result, LookupDataResultPtr data,
                              Promise<Result, ClientConnectionWeakPtr> promise) {
    if (data) {
        const std::string& logicalAddress =
            clientConfiguration_.isUseTls() ? data->getBrokerUrlTls() : data->getBrokerUrl();
        LOG_DEBUG("Getting connection to broker: " << logicalAddress);
        const std::string& physicalAddress =
            data->shouldProxyThroughServiceUrl() ? serviceUrl_ : logicalAddress;
        Future<Result, ClientConnectionWeakPtr> future =
            pool_.getConnectionAsync(logicalAddress, physicalAddress);
        future.addListener(std::bind(&ClientImpl::handleNewConnection, shared_from_this(),
                                     std::placeholders::_1, std::placeholders::_2, promise));
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

void ClientImpl::handleGetPartitions(const Result result, const LookupDataResultPtr partitionMetadata,
                                     TopicNamePtr topicName, GetPartitionsCallback callback) {
    if (result != ResultOk) {
        LOG_ERROR("Error getting topic partitions metadata: " << result);
        callback(result, StringList());
        return;
    }

    StringList partitions;

    if (partitionMetadata->getPartitions() > 0) {
        for (unsigned int i = 0; i < partitionMetadata->getPartitions(); i++) {
            partitions.push_back(topicName->getTopicPartitionName(i));
        }
    } else {
        partitions.push_back(topicName->toString());
    }

    callback(ResultOk, partitions);
}

void ClientImpl::getPartitionsForTopicAsync(const std::string& topic, GetPartitionsCallback callback) {
    TopicNamePtr topicName;
    {
        Lock lock(mutex_);
        if (state_ != Open) {
            lock.unlock();
            callback(ResultAlreadyClosed, StringList());
            return;
        } else if (!(topicName = TopicName::get(topic))) {
            lock.unlock();
            callback(ResultInvalidTopicName, StringList());
            return;
        }
    }
    lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
        std::bind(&ClientImpl::handleGetPartitions, shared_from_this(), std::placeholders::_1,
                  std::placeholders::_2, topicName, callback));
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

    SharedInt numberOfOpenHandlers = std::make_shared<int>(producers.size() + consumers.size());
    LOG_INFO("Closing Pulsar client with " << producers.size() << " producers and " << consumers.size()
                                           << " consumers");

    for (ProducersList::iterator it = producers.begin(); it != producers.end(); ++it) {
        ProducerImplBasePtr producer = it->lock();
        if (producer && !producer->isClosed()) {
            producer->closeAsync(std::bind(&ClientImpl::handleClose, shared_from_this(),
                                           std::placeholders::_1, numberOfOpenHandlers, callback));
        } else {
            // Since the connection is already closed
            (*numberOfOpenHandlers)--;
        }
    }

    for (ConsumersList::iterator it = consumers.begin(); it != consumers.end(); ++it) {
        ConsumerImplBasePtr consumer = it->lock();
        if (consumer && !consumer->isClosed()) {
            consumer->closeAsync(std::bind(&ClientImpl::handleClose, shared_from_this(),
                                           std::placeholders::_1, numberOfOpenHandlers, callback));
        } else {
            // Since the connection is already closed
            (*numberOfOpenHandlers)--;
        }
    }

    if (*numberOfOpenHandlers == 0 && callback) {
        handleClose(ResultOk, numberOfOpenHandlers, callback);
    }
}

void ClientImpl::handleClose(Result result, SharedInt numberOfOpenHandlers, ResultCallback callback) {
    Result expected = ResultOk;
    if (!closingError.compare_exchange_strong(expected, result)) {
        LOG_DEBUG("Tried to updated closingError, but already set to "
                  << expected << ". This means multiple errors have occurred while closing the client");
    }

    if (*numberOfOpenHandlers > 0) {
        --(*numberOfOpenHandlers);
    }
    if (*numberOfOpenHandlers == 0) {
        Lock lock(mutex_);
        state_ = Closed;
        lock.unlock();

        LOG_DEBUG("Shutting down producers and consumers for client");
        shutdown();
        if (callback) {
            if (closingError != ResultOk) {
                LOG_DEBUG("Problem in closing client, could not close one or more consumers or producers");
            }
            callback(closingError);
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

    if (producers.size() + consumers.size() > 0) {
        LOG_DEBUG(producers.size() << " producers and " << consumers.size()
                                   << " consumers have been shutdown.");
    }
    if (!pool_.close()) {
        // pool_ has already been closed. It means shutdown() has been called before.
        return;
    }
    LOG_DEBUG("ConnectionPool is closed");
    ioExecutorProvider_->close();
    LOG_DEBUG("ioExecutorProvider_ is closed");
    listenerExecutorProvider_->close();
    LOG_DEBUG("listenerExecutorProvider_ is closed");
    partitionListenerExecutorProvider_->close();
    LOG_DEBUG("partitionListenerExecutorProvider_ is closed");
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

uint64_t ClientImpl::getNumberOfProducers() {
    Lock lock(mutex_);
    uint64_t numberOfAliveProducers = 0;
    for (const auto& producer : producers_) {
        const auto& producerImpl = producer.lock();
        if (producerImpl) {
            numberOfAliveProducers += producerImpl->getNumberOfConnectedProducer();
        }
    }
    return numberOfAliveProducers;
}

uint64_t ClientImpl::getNumberOfConsumers() {
    Lock lock(mutex_);
    uint64_t numberOfAliveConsumers = 0;
    for (const auto& consumer : consumers_) {
        const auto consumerImpl = consumer.lock();
        if (consumerImpl) {
            numberOfAliveConsumers += consumerImpl->getNumberOfConnectedConsumer();
        }
    }
    return numberOfAliveConsumers;
}

const ClientConfiguration& ClientImpl::getClientConfig() const { return clientConfiguration_; }

} /* namespace pulsar */
