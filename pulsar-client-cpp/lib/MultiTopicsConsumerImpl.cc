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
#include "MultiTopicsConsumerImpl.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

MultiTopicsConsumerImpl::MultiTopicsConsumerImpl(ClientImplPtr client, const std::vector<std::string>& topics,
                                                 const std::string& subscriptionName, TopicNamePtr topicName,
                                                 const ConsumerConfiguration& conf,
                                                 const LookupServicePtr lookupServicePtr)
    : client_(client),
      subscriptionName_(subscriptionName),
      topic_(topicName ? topicName->toString() : "EmptyTopics"),
      conf_(conf),
      state_(Pending),
      messages_(1000),
      listenerExecutor_(client->getListenerExecutorProvider()->get()),
      messageListener_(conf.getMessageListener()),
      pendingReceives_(),
      namespaceName_(topicName ? topicName->getNamespaceName() : std::shared_ptr<NamespaceName>()),
      lookupServicePtr_(lookupServicePtr),
      numberTopicPartitions_(std::make_shared<std::atomic<int>>(0)),
      topics_(topics) {
    std::stringstream consumerStrStream;
    consumerStrStream << "[Muti Topics Consumer: "
                      << "TopicName - " << topic_ << " - Subscription - " << subscriptionName << "]";
    consumerStr_ = consumerStrStream.str();

    if (conf.getUnAckedMessagesTimeoutMs() != 0) {
        unAckedMessageTrackerPtr_.reset(
            new UnAckedMessageTrackerEnabled(conf.getUnAckedMessagesTimeoutMs(), client, *this));
    } else {
        unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerDisabled());
    }
}

void MultiTopicsConsumerImpl::start() {
    if (topics_.empty()) {
        if (compareAndSetState(Pending, Ready)) {
            LOG_DEBUG("No topics passed in when create MultiTopicsConsumer.");
            multiTopicsConsumerCreatedPromise_.setValue(shared_from_this());
            return;
        } else {
            LOG_ERROR("Consumer " << consumerStr_ << " in wrong state: " << state_);
            multiTopicsConsumerCreatedPromise_.setFailed(ResultUnknownError);
            return;
        }
    }

    // start call subscribeOneTopicAsync for each single topic
    int topicsNumber = topics_.size();
    std::shared_ptr<std::atomic<int>> topicsNeedCreate = std::make_shared<std::atomic<int>>(topicsNumber);
    // subscribe for each passed in topic
    for (std::vector<std::string>::const_iterator itr = topics_.begin(); itr != topics_.end(); itr++) {
        subscribeOneTopicAsync(*itr).addListener(std::bind(&MultiTopicsConsumerImpl::handleOneTopicSubscribed,
                                                           shared_from_this(), std::placeholders::_1,
                                                           std::placeholders::_2, *itr, topicsNeedCreate));
    }
}

void MultiTopicsConsumerImpl::handleOneTopicSubscribed(Result result, Consumer consumer,
                                                       const std::string& topic,
                                                       std::shared_ptr<std::atomic<int>> topicsNeedCreate) {
    int previous = topicsNeedCreate->fetch_sub(1);
    assert(previous > 0);

    if (result != ResultOk) {
        setState(Failed);
        LOG_ERROR("Failed when subscribed to topic " << topic << " in TopicsConsumer. Error - " << result);
    }

    LOG_DEBUG("Subscribed to topic " << topic << " in TopicsConsumer ");

    if (topicsNeedCreate->load() == 0) {
        if (compareAndSetState(Pending, Ready)) {
            LOG_INFO("Successfully Subscribed to Topics");
            if (!namespaceName_) {
                namespaceName_ = TopicName::get(topic)->getNamespaceName();
            }
            multiTopicsConsumerCreatedPromise_.setValue(shared_from_this());
        } else {
            LOG_ERROR("Unable to create Consumer - " << consumerStr_ << " Error - " << result);
            // unsubscribed all of the successfully subscribed partitioned consumers
            ResultCallback nullCallbackForCleanup = NULL;
            closeAsync(nullCallbackForCleanup);
            multiTopicsConsumerCreatedPromise_.setFailed(result);
            return;
        }
        return;
    }
}

// subscribe for passed in topic
Future<Result, Consumer> MultiTopicsConsumerImpl::subscribeOneTopicAsync(const std::string& topic) {
    TopicNamePtr topicName;
    ConsumerSubResultPromisePtr topicPromise = std::make_shared<Promise<Result, Consumer>>();
    if (!(topicName = TopicName::get(topic))) {
        LOG_ERROR("TopicName invalid: " << topic);
        topicPromise->setFailed(ResultInvalidTopicName);
        return topicPromise->getFuture();
    }

    if (namespaceName_ && !(*namespaceName_ == *(topicName->getNamespaceName()))) {
        LOG_ERROR("TopicName namespace not the same with topicsConsumer. wanted namespace: "
                  << namespaceName_->toString() << " this topic: " << topic);
        topicPromise->setFailed(ResultInvalidTopicName);
        return topicPromise->getFuture();
    }

    if (state_ == Closed || state_ == Closing) {
        LOG_ERROR("MultiTopicsConsumer already closed when subscribe.");
        topicPromise->setFailed(ResultAlreadyClosed);
        return topicPromise->getFuture();
    }

    // subscribe for each partition, when all partitions completed, complete promise
    lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(std::bind(
        &MultiTopicsConsumerImpl::subscribeTopicPartitions, shared_from_this(), std::placeholders::_1,
        std::placeholders::_2, topicName, subscriptionName_, conf_, topicPromise));
    return topicPromise->getFuture();
}

void MultiTopicsConsumerImpl::subscribeTopicPartitions(const Result result,
                                                       const LookupDataResultPtr partitionMetadata,
                                                       TopicNamePtr topicName,
                                                       const std::string& consumerName,
                                                       ConsumerConfiguration conf,
                                                       ConsumerSubResultPromisePtr topicSubResultPromise) {
    if (result != ResultOk) {
        LOG_ERROR("Error Checking/Getting Partition Metadata while MultiTopics Subscribing- "
                  << consumerStr_ << " result: " << result)
        topicSubResultPromise->setFailed(result);
        return;
    }

    std::shared_ptr<ConsumerImpl> consumer;
    ConsumerConfiguration config;
    ExecutorServicePtr internalListenerExecutor = client_->getPartitionListenerExecutorProvider()->get();

    // all the consumers should have same name.
    config.setConsumerName(conf_.getConsumerName());
    config.setConsumerType(conf_.getConsumerType());
    config.setBrokerConsumerStatsCacheTimeInMs(conf_.getBrokerConsumerStatsCacheTimeInMs());
    config.setMessageListener(std::bind(&MultiTopicsConsumerImpl::messageReceived, shared_from_this(),
                                        std::placeholders::_1, std::placeholders::_2));

    int numPartitions = partitionMetadata->getPartitions() >= 1 ? partitionMetadata->getPartitions() : 1;
    // Apply total limit of receiver queue size across partitions
    config.setReceiverQueueSize(
        std::min(conf_.getReceiverQueueSize(),
                 (int)(conf_.getMaxTotalReceiverQueueSizeAcrossPartitions() / numPartitions)));

    Lock lock(mutex_);
    topicsPartitions_.insert(std::make_pair(topicName->toString(), numPartitions));
    lock.unlock();
    numberTopicPartitions_->fetch_add(numPartitions);

    std::shared_ptr<std::atomic<int>> partitionsNeedCreate =
        std::make_shared<std::atomic<int>>(numPartitions);

    if (numPartitions == 1) {
        // We don't have to add partition-n suffix
        consumer = std::make_shared<ConsumerImpl>(client_, topicName->toString(), subscriptionName_, config,
                                                  internalListenerExecutor, NonPartitioned);
        consumer->getConsumerCreatedFuture().addListener(std::bind(
            &MultiTopicsConsumerImpl::handleSingleConsumerCreated, shared_from_this(), std::placeholders::_1,
            std::placeholders::_2, partitionsNeedCreate, topicSubResultPromise));
        consumers_.insert(std::make_pair(topicName->toString(), consumer));
        LOG_DEBUG("Creating Consumer for - " << topicName << " - " << consumerStr_);
        consumer->start();

    } else {
        for (int i = 0; i < numPartitions; i++) {
            std::string topicPartitionName = topicName->getTopicPartitionName(i);
            consumer = std::make_shared<ConsumerImpl>(client_, topicPartitionName, subscriptionName_, config,
                                                      internalListenerExecutor, Partitioned);
            consumer->getConsumerCreatedFuture().addListener(std::bind(
                &MultiTopicsConsumerImpl::handleSingleConsumerCreated, shared_from_this(),
                std::placeholders::_1, std::placeholders::_2, partitionsNeedCreate, topicSubResultPromise));
            consumer->setPartitionIndex(i);
            consumers_.insert(std::make_pair(topicPartitionName, consumer));
            LOG_DEBUG("Creating Consumer for - " << topicPartitionName << " - " << consumerStr_);
            consumer->start();
        }
    }
}

void MultiTopicsConsumerImpl::handleSingleConsumerCreated(
    Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr,
    std::shared_ptr<std::atomic<int>> partitionsNeedCreate,
    ConsumerSubResultPromisePtr topicSubResultPromise) {
    if (state_ == Failed) {
        // one of the consumer creation failed, and we are cleaning up
        topicSubResultPromise->setFailed(ResultAlreadyClosed);
        LOG_ERROR("Unable to create Consumer " << consumerStr_ << " state == Failed, result: " << result);
        return;
    }

    int previous = partitionsNeedCreate->fetch_sub(1);
    assert(previous > 0);

    if (result != ResultOk) {
        topicSubResultPromise->setFailed(result);
        LOG_ERROR("Unable to create Consumer - " << consumerStr_ << " Error - " << result);
        return;
    }

    LOG_DEBUG("Successfully Subscribed to a single partition of topic in TopicsConsumer. "
              << "Partitions need to create - " << previous - 1);

    if (partitionsNeedCreate->load() == 0) {
        topicSubResultPromise->setValue(Consumer(shared_from_this()));
    }
}

void MultiTopicsConsumerImpl::unsubscribeAsync(ResultCallback callback) {
    LOG_INFO("[ Topics Consumer " << topic_ << "," << subscriptionName_ << "] Unsubscribing");

    Lock lock(mutex_);
    if (state_ == Closing || state_ == Closed) {
        LOG_INFO(consumerStr_ << " already closed");
        lock.unlock();
        callback(ResultAlreadyClosed);
        return;
    }
    state_ = Closing;
    lock.unlock();

    if (consumers_.empty()) {
        // No need to unsubscribe, since the list matching the regex was empty
        callback(ResultOk);
        return;
    }

    std::shared_ptr<std::atomic<int>> consumerUnsubed = std::make_shared<std::atomic<int>>(0);

    for (ConsumerMap::const_iterator consumer = consumers_.begin(); consumer != consumers_.end();
         consumer++) {
        LOG_DEBUG("Unsubcribing Consumer - " << consumer->first);
        (consumer->second)
            ->unsubscribeAsync(std::bind(&MultiTopicsConsumerImpl::handleUnsubscribedAsync,
                                         shared_from_this(), std::placeholders::_1, consumerUnsubed,
                                         callback));
    }
}

void MultiTopicsConsumerImpl::handleUnsubscribedAsync(Result result,
                                                      std::shared_ptr<std::atomic<int>> consumerUnsubed,
                                                      ResultCallback callback) {
    int previous = consumerUnsubed->fetch_add(1);
    assert(previous < numberTopicPartitions_->load());

    if (result != ResultOk) {
        setState(Failed);
        LOG_ERROR("Error Closing one of the consumers in TopicsConsumer, result: "
                  << result << " subscription - " << subscriptionName_);
    }

    if (consumerUnsubed->load() == numberTopicPartitions_->load()) {
        LOG_DEBUG("Unsubscribed all of the partition consumer for TopicsConsumer.  - " << consumerStr_);
        consumers_.clear();
        topicsPartitions_.clear();
        unAckedMessageTrackerPtr_->clear();

        Result result1 = (state_ != Failed) ? ResultOk : ResultUnknownError;
        setState(Closed);
        callback(result1);
        return;
    }
}

void MultiTopicsConsumerImpl::unsubscribeOneTopicAsync(const std::string& topic, ResultCallback callback) {
    std::map<std::string, int>::iterator it = topicsPartitions_.find(topic);
    if (it == topicsPartitions_.end()) {
        LOG_ERROR("TopicsConsumer does not subscribe topic : " << topic << " subscription - "
                                                               << subscriptionName_);
        callback(ResultTopicNotFound);
        return;
    }

    if (state_ == Closing || state_ == Closed) {
        LOG_ERROR("TopicsConsumer already closed when unsubscribe topic: " << topic << " subscription - "
                                                                           << subscriptionName_);
        callback(ResultAlreadyClosed);
        return;
    }

    TopicNamePtr topicName;
    if (!(topicName = TopicName::get(topic))) {
        LOG_ERROR("TopicName invalid: " << topic);
        callback(ResultUnknownError);
    }
    int numberPartitions = it->second;
    std::shared_ptr<std::atomic<int>> consumerUnsubed = std::make_shared<std::atomic<int>>(0);

    for (int i = 0; i < numberPartitions; i++) {
        std::string topicPartitionName = topicName->getTopicPartitionName(i);
        std::map<std::string, ConsumerImplPtr>::iterator iterator = consumers_.find(topicPartitionName);

        if (consumers_.end() == iterator) {
            LOG_ERROR("TopicsConsumer not subscribed on topicPartitionName: " << topicPartitionName);
            callback(ResultUnknownError);
        }

        (iterator->second)
            ->unsubscribeAsync(std::bind(&MultiTopicsConsumerImpl::handleOneTopicUnsubscribedAsync,
                                         shared_from_this(), std::placeholders::_1, consumerUnsubed,
                                         numberPartitions, topicName, topicPartitionName, callback));
    }
}

void MultiTopicsConsumerImpl::handleOneTopicUnsubscribedAsync(
    Result result, std::shared_ptr<std::atomic<int>> consumerUnsubed, int numberPartitions,
    TopicNamePtr topicNamePtr, std::string& topicPartitionName, ResultCallback callback) {
    int previous = consumerUnsubed->fetch_add(1);
    assert(previous < numberPartitions);

    if (result != ResultOk) {
        setState(Failed);
        LOG_ERROR("Error Closing one of the consumers in TopicsConsumer, result: "
                  << result << " topicPartitionName - " << topicPartitionName);
    }

    LOG_DEBUG("Successfully Unsubscribed one Consumer. topicPartitionName - " << topicPartitionName);

    std::map<std::string, ConsumerImplPtr>::iterator iterator = consumers_.find(topicPartitionName);
    if (consumers_.end() != iterator) {
        iterator->second->pauseMessageListener();
        consumers_.erase(iterator);
    }

    if (consumerUnsubed->load() == numberPartitions) {
        LOG_DEBUG("Unsubscribed all of the partition consumer for TopicsConsumer.  - " << consumerStr_);
        std::map<std::string, int>::iterator it = topicsPartitions_.find(topicNamePtr->toString());
        if (it != topicsPartitions_.end()) {
            numberTopicPartitions_->fetch_sub(numberPartitions);
            Lock lock(mutex_);
            topicsPartitions_.erase(it);
            lock.unlock();
        }
        if (state_ != Failed) {
            callback(ResultOk);
        } else {
            callback(ResultUnknownError);
        }
        unAckedMessageTrackerPtr_->removeTopicMessage(topicNamePtr->toString());
        return;
    }
}

void MultiTopicsConsumerImpl::closeAsync(ResultCallback callback) {
    if (state_ == Closing || state_ == Closed) {
        LOG_ERROR("TopicsConsumer already closed "
                  << " topic" << topic_ << " consumer - " << consumerStr_);
        callback(ResultAlreadyClosed);
        return;
    }

    setState(Closing);

    if (consumers_.empty()) {
        LOG_ERROR("TopicsConsumer have no consumers to close "
                  << " topic" << topic_ << " subscription - " << subscriptionName_);
        setState(Closed);
        callback(ResultAlreadyClosed);
        return;
    }

    // close successfully subscribed consumers
    for (ConsumerMap::const_iterator consumer = consumers_.begin(); consumer != consumers_.end();
         consumer++) {
        std::string topicPartitionName = consumer->first;
        ConsumerImplPtr consumerPtr = consumer->second;

        consumerPtr->closeAsync(std::bind(&MultiTopicsConsumerImpl::handleSingleConsumerClose,
                                          shared_from_this(), std::placeholders::_1, topicPartitionName,
                                          callback));
    }

    // fail pending recieve
    failPendingReceiveCallback();
}

void MultiTopicsConsumerImpl::handleSingleConsumerClose(Result result, std::string& topicPartitionName,
                                                        CloseCallback callback) {
    std::map<std::string, ConsumerImplPtr>::iterator iterator = consumers_.find(topicPartitionName);
    if (consumers_.end() != iterator) {
        consumers_.erase(iterator);
    }

    LOG_DEBUG("Closing the consumer for partition - " << topicPartitionName << " numberTopicPartitions_ - "
                                                      << numberTopicPartitions_->load());

    assert(numberTopicPartitions_->load() > 0);
    numberTopicPartitions_->fetch_sub(1);

    if (result != ResultOk) {
        setState(Failed);
        LOG_ERROR("Closing the consumer failed for partition - " << topicPartitionName << " with error - "
                                                                 << result);
    }

    // closed all consumers
    if (numberTopicPartitions_->load() == 0) {
        messages_.clear();
        consumers_.clear();
        topicsPartitions_.clear();
        unAckedMessageTrackerPtr_->clear();

        if (state_ != Failed) {
            state_ = Closed;
        }

        multiTopicsConsumerCreatedPromise_.setFailed(ResultUnknownError);
        if (callback) {
            callback(result);
        }
        return;
    }
}

void MultiTopicsConsumerImpl::messageReceived(Consumer consumer, const Message& msg) {
    LOG_DEBUG("Received Message from one of the topic - " << consumer.getTopic()
                                                          << " message:" << msg.getDataAsString());
    const std::string& topicPartitionName = consumer.getTopic();
    msg.impl_->setTopicName(topicPartitionName);

    Lock lock(pendingReceiveMutex_);
    if (!pendingReceives_.empty()) {
        ReceiveCallback callback = pendingReceives_.front();
        pendingReceives_.pop();
        lock.unlock();
        unAckedMessageTrackerPtr_->add(msg.getMessageId());
        listenerExecutor_->postWork(std::bind(callback, ResultOk, msg));
    } else {
        if (messages_.full()) {
            lock.unlock();
        }
        messages_.push(msg);
        if (messageListener_) {
            unAckedMessageTrackerPtr_->add(msg.getMessageId());
            listenerExecutor_->postWork(
                std::bind(&MultiTopicsConsumerImpl::internalListener, shared_from_this(), consumer));
        }
    }
}

void MultiTopicsConsumerImpl::internalListener(Consumer consumer) {
    Message m;
    messages_.pop(m);

    try {
        messageListener_(Consumer(shared_from_this()), m);
    } catch (const std::exception& e) {
        LOG_ERROR("Exception thrown from listener of Partitioned Consumer" << e.what());
    }
}

Result MultiTopicsConsumerImpl::receive(Message& msg) {
    Lock lock(mutex_);
    if (state_ != Ready) {
        lock.unlock();
        return ResultAlreadyClosed;
    }

    if (messageListener_) {
        lock.unlock();
        LOG_ERROR("Can not receive when a listener has been set");
        return ResultInvalidConfiguration;
    }
    messages_.pop(msg);
    lock.unlock();

    unAckedMessageTrackerPtr_->add(msg.getMessageId());
    return ResultOk;
}

Result MultiTopicsConsumerImpl::receive(Message& msg, int timeout) {
    Lock lock(mutex_);
    if (state_ != Ready) {
        lock.unlock();
        return ResultAlreadyClosed;
    }

    if (messageListener_) {
        lock.unlock();
        LOG_ERROR("Can not receive when a listener has been set");
        return ResultInvalidConfiguration;
    }

    if (messages_.pop(msg, std::chrono::milliseconds(timeout))) {
        lock.unlock();
        unAckedMessageTrackerPtr_->add(msg.getMessageId());
        return ResultOk;
    } else {
        return ResultTimeout;
    }
}

void MultiTopicsConsumerImpl::receiveAsync(ReceiveCallback& callback) {
    Message msg;

    // fail the callback if consumer is closing or closed
    Lock stateLock(mutex_);
    if (state_ != Ready) {
        callback(ResultAlreadyClosed, msg);
        return;
    }
    stateLock.unlock();

    Lock lock(pendingReceiveMutex_);
    if (messages_.pop(msg, std::chrono::milliseconds(0))) {
        lock.unlock();
        unAckedMessageTrackerPtr_->add(msg.getMessageId());
        callback(ResultOk, msg);
    } else {
        pendingReceives_.push(callback);
    }
}

void MultiTopicsConsumerImpl::failPendingReceiveCallback() {
    Message msg;
    Lock lock(pendingReceiveMutex_);
    while (!pendingReceives_.empty()) {
        ReceiveCallback callback = pendingReceives_.front();
        pendingReceives_.pop();
        listenerExecutor_->postWork(std::bind(callback, ResultAlreadyClosed, msg));
    }
    lock.unlock();
}

void MultiTopicsConsumerImpl::acknowledgeAsync(const MessageId& msgId, ResultCallback callback) {
    if (state_ != Ready) {
        callback(ResultAlreadyClosed);
        return;
    }

    const std::string& topicPartitionName = msgId.getTopicName();
    std::map<std::string, ConsumerImplPtr>::iterator iterator = consumers_.find(topicPartitionName);

    if (consumers_.end() != iterator) {
        unAckedMessageTrackerPtr_->remove(msgId);
        iterator->second->acknowledgeAsync(msgId, callback);
    } else {
        LOG_ERROR("Message of topic: " << topicPartitionName << " not in unAckedMessageTracker");
        callback(ResultUnknownError);
        return;
    }
}

void MultiTopicsConsumerImpl::acknowledgeCumulativeAsync(const MessageId& msgId, ResultCallback callback) {
    callback(ResultOperationNotSupported);
}

void MultiTopicsConsumerImpl::negativeAcknowledge(const MessageId& msgId) {
    auto iterator = consumers_.find(msgId.getTopicName());

    if (consumers_.end() != iterator) {
        unAckedMessageTrackerPtr_->remove(msgId);
        iterator->second->negativeAcknowledge(msgId);
    }
}

MultiTopicsConsumerImpl::~MultiTopicsConsumerImpl() {}

Future<Result, ConsumerImplBaseWeakPtr> MultiTopicsConsumerImpl::getConsumerCreatedFuture() {
    return multiTopicsConsumerCreatedPromise_.getFuture();
}
const std::string& MultiTopicsConsumerImpl::getSubscriptionName() const { return subscriptionName_; }

const std::string& MultiTopicsConsumerImpl::getTopic() const { return topic_; }

const std::string& MultiTopicsConsumerImpl::getName() const { return consumerStr_; }

void MultiTopicsConsumerImpl::setState(const MultiTopicsConsumerState state) {
    Lock lock(mutex_);
    state_ = state;
}

bool MultiTopicsConsumerImpl::compareAndSetState(MultiTopicsConsumerState expect,
                                                 MultiTopicsConsumerState update) {
    Lock lock(mutex_);
    if (state_ == expect) {
        state_ = update;
        return true;
    } else {
        return false;
    }
}

void MultiTopicsConsumerImpl::shutdown() {}

bool MultiTopicsConsumerImpl::isClosed() { return state_ == Closed; }

bool MultiTopicsConsumerImpl::isOpen() {
    Lock lock(mutex_);
    return state_ == Ready;
}

void MultiTopicsConsumerImpl::receiveMessages() {
    for (ConsumerMap::const_iterator consumer = consumers_.begin(); consumer != consumers_.end();
         consumer++) {
        ConsumerImplPtr consumerPtr = consumer->second;
        consumerPtr->receiveMessages(consumerPtr->getCnx().lock(), conf_.getReceiverQueueSize());
        LOG_DEBUG("Sending FLOW command for consumer - " << consumerPtr->getConsumerId());
    }
}

Result MultiTopicsConsumerImpl::pauseMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }
    for (ConsumerMap::const_iterator consumer = consumers_.begin(); consumer != consumers_.end();
         consumer++) {
        (consumer->second)->pauseMessageListener();
    }
    return ResultOk;
}

Result MultiTopicsConsumerImpl::resumeMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }
    for (ConsumerMap::const_iterator consumer = consumers_.begin(); consumer != consumers_.end();
         consumer++) {
        (consumer->second)->resumeMessageListener();
    }
    return ResultOk;
}

void MultiTopicsConsumerImpl::redeliverUnacknowledgedMessages() {
    LOG_DEBUG("Sending RedeliverUnacknowledgedMessages command for partitioned consumer.");
    for (ConsumerMap::const_iterator consumer = consumers_.begin(); consumer != consumers_.end();
         consumer++) {
        (consumer->second)->redeliverUnacknowledgedMessages();
    }
}

int MultiTopicsConsumerImpl::getNumOfPrefetchedMessages() const { return messages_.size(); }

void MultiTopicsConsumerImpl::getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback) {
    Lock lock(mutex_);
    if (state_ != Ready) {
        lock.unlock();
        callback(ResultConsumerNotInitialized, BrokerConsumerStats());
        return;
    }
    MultiTopicsBrokerConsumerStatsPtr statsPtr =
        std::make_shared<MultiTopicsBrokerConsumerStatsImpl>(numberTopicPartitions_->load());
    LatchPtr latchPtr = std::make_shared<Latch>(numberTopicPartitions_->load());
    int size = consumers_.size();
    lock.unlock();

    ConsumerMap::const_iterator consumer = consumers_.begin();
    for (int i = 0; i < size; i++, consumer++) {
        consumer->second->getBrokerConsumerStatsAsync(
            std::bind(&MultiTopicsConsumerImpl::handleGetConsumerStats, shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2, latchPtr, statsPtr, i, callback));
    }
}

void MultiTopicsConsumerImpl::handleGetConsumerStats(Result res, BrokerConsumerStats brokerConsumerStats,
                                                     LatchPtr latchPtr,
                                                     MultiTopicsBrokerConsumerStatsPtr statsPtr, size_t index,
                                                     BrokerConsumerStatsCallback callback) {
    Lock lock(mutex_);
    if (res == ResultOk) {
        latchPtr->countdown();
        statsPtr->add(brokerConsumerStats, index);
    } else {
        lock.unlock();
        callback(res, BrokerConsumerStats());
        return;
    }
    if (latchPtr->getCount() == 0) {
        lock.unlock();
        callback(ResultOk, BrokerConsumerStats(statsPtr));
    }
}

std::shared_ptr<TopicName> MultiTopicsConsumerImpl::topicNamesValid(const std::vector<std::string>& topics) {
    TopicNamePtr topicNamePtr = std::shared_ptr<TopicName>();
    NamespaceNamePtr namespaceNamePtr = std::shared_ptr<NamespaceName>();

    // all topics name valid, and all topics have same namespace
    for (std::vector<std::string>::const_iterator itr = topics.begin(); itr != topics.end(); itr++) {
        // topic name valid
        if (!(topicNamePtr = TopicName::get(*itr))) {
            LOG_ERROR("Topic name invalid when init " << *itr);
            return std::shared_ptr<TopicName>();
        }

        // all contains same namespace part
        if (!namespaceNamePtr) {
            namespaceNamePtr = topicNamePtr->getNamespaceName();
        } else if (!(*namespaceNamePtr == *(topicNamePtr->getNamespaceName()))) {
            LOG_ERROR("Different namespace name. expected: " << namespaceNamePtr->toString() << " now:"
                                                             << topicNamePtr->getNamespaceName()->toString());
            return std::shared_ptr<TopicName>();
        }
    }

    return topicNamePtr;
}

void MultiTopicsConsumerImpl::seekAsync(const MessageId& msgId, ResultCallback callback) {
    callback(ResultOperationNotSupported);
}
