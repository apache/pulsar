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
#include "MultiResultCallback.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

MultiTopicsConsumerImpl::MultiTopicsConsumerImpl(ClientImplPtr client, const std::vector<std::string>& topics,
                                                 const std::string& subscriptionName, TopicNamePtr topicName,
                                                 const ConsumerConfiguration& conf,
                                                 LookupServicePtr lookupServicePtr)
    : client_(client),
      subscriptionName_(subscriptionName),
      topic_(topicName ? topicName->toString() : "EmptyTopics"),
      conf_(conf),
      messages_(conf.getReceiverQueueSize()),
      listenerExecutor_(client->getListenerExecutorProvider()->get()),
      messageListener_(conf.getMessageListener()),
      lookupServicePtr_(lookupServicePtr),
      numberTopicPartitions_(std::make_shared<std::atomic<int>>(0)),
      topics_(topics) {
    std::stringstream consumerStrStream;
    consumerStrStream << "[Muti Topics Consumer: "
                      << "TopicName - " << topic_ << " - Subscription - " << subscriptionName << "]";
    consumerStr_ = consumerStrStream.str();

    if (conf.getUnAckedMessagesTimeoutMs() != 0) {
        if (conf.getTickDurationInMs() > 0) {
            unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerEnabled(
                conf.getUnAckedMessagesTimeoutMs(), conf.getTickDurationInMs(), client, *this));
        } else {
            unAckedMessageTrackerPtr_.reset(
                new UnAckedMessageTrackerEnabled(conf.getUnAckedMessagesTimeoutMs(), client, *this));
        }
    } else {
        unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerDisabled());
    }
    auto partitionsUpdateInterval = static_cast<unsigned int>(client_->conf().getPartitionsUpdateInterval());
    if (partitionsUpdateInterval > 0) {
        partitionsUpdateTimer_ = listenerExecutor_->createDeadlineTimer();
        partitionsUpdateInterval_ = boost::posix_time::seconds(partitionsUpdateInterval);
        lookupServicePtr_ = client_->getLookup();
    }
}

void MultiTopicsConsumerImpl::start() {
    if (topics_.empty()) {
        MultiTopicsConsumerState state = Pending;
        if (state_.compare_exchange_strong(state, Ready)) {
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
    if (result != ResultOk) {
        state_ = Failed;
        // Use the first failed result
        auto expectedResult = ResultOk;
        failedResult.compare_exchange_strong(expectedResult, result);
        LOG_ERROR("Failed when subscribed to topic " << topic << " in TopicsConsumer. Error - " << result);
    } else {
        LOG_DEBUG("Subscribed to topic " << topic << " in TopicsConsumer ");
    }

    if (--(*topicsNeedCreate) == 0) {
        MultiTopicsConsumerState state = Pending;
        if (state_.compare_exchange_strong(state, Ready)) {
            LOG_INFO("Successfully Subscribed to Topics");
            multiTopicsConsumerCreatedPromise_.setValue(shared_from_this());
        } else {
            LOG_ERROR("Unable to create Consumer - " << consumerStr_ << " Error - " << result);
            // unsubscribed all of the successfully subscribed partitioned consumers
            // It's safe to capture only this here, because the callback can be called only when this is valid
            closeAsync(
                [this](Result result) { multiTopicsConsumerCreatedPromise_.setFailed(failedResult.load()); });
        }
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

    const auto state = state_.load();
    if (state == Closed || state == Closing) {
        LOG_ERROR("MultiTopicsConsumer already closed when subscribe.");
        topicPromise->setFailed(ResultAlreadyClosed);
        return topicPromise->getFuture();
    }

    // subscribe for each partition, when all partitions completed, complete promise
    Lock lock(mutex_);
    auto entry = topicsPartitions_.find(topic);
    if (entry == topicsPartitions_.end()) {
        lock.unlock();
        lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
            [this, topicName, topicPromise](Result result, const LookupDataResultPtr& lookupDataResult) {
                if (result != ResultOk) {
                    LOG_ERROR("Error Checking/Getting Partition Metadata while MultiTopics Subscribing- "
                              << consumerStr_ << " result: " << result)
                    topicPromise->setFailed(result);
                    return;
                }
                subscribeTopicPartitions(lookupDataResult->getPartitions(), topicName, subscriptionName_,
                                         topicPromise);
            });
    } else {
        auto numPartitions = entry->second;
        lock.unlock();
        subscribeTopicPartitions(numPartitions, topicName, subscriptionName_, topicPromise);
    }
    return topicPromise->getFuture();
}

void MultiTopicsConsumerImpl::subscribeTopicPartitions(int numPartitions, TopicNamePtr topicName,
                                                       const std::string& consumerName,
                                                       ConsumerSubResultPromisePtr topicSubResultPromise) {
    std::shared_ptr<ConsumerImpl> consumer;
    ConsumerConfiguration config = conf_.clone();
    ExecutorServicePtr internalListenerExecutor = client_->getPartitionListenerExecutorProvider()->get();

    config.setMessageListener(std::bind(&MultiTopicsConsumerImpl::messageReceived, shared_from_this(),
                                        std::placeholders::_1, std::placeholders::_2));

    int partitions = numPartitions == 0 ? 1 : numPartitions;

    // Apply total limit of receiver queue size across partitions
    config.setReceiverQueueSize(
        std::min(conf_.getReceiverQueueSize(),
                 (int)(conf_.getMaxTotalReceiverQueueSizeAcrossPartitions() / partitions)));

    Lock lock(mutex_);
    topicsPartitions_[topicName->toString()] = partitions;
    lock.unlock();
    numberTopicPartitions_->fetch_add(partitions);

    std::shared_ptr<std::atomic<int>> partitionsNeedCreate = std::make_shared<std::atomic<int>>(partitions);

    // non-partitioned topic
    if (numPartitions == 0) {
        // We don't have to add partition-n suffix
        consumer = std::make_shared<ConsumerImpl>(client_, topicName->toString(), subscriptionName_, config,
                                                  internalListenerExecutor, true, NonPartitioned);
        consumer->getConsumerCreatedFuture().addListener(std::bind(
            &MultiTopicsConsumerImpl::handleSingleConsumerCreated, shared_from_this(), std::placeholders::_1,
            std::placeholders::_2, partitionsNeedCreate, topicSubResultPromise));
        consumers_.emplace(topicName->toString(), consumer);
        LOG_DEBUG("Creating Consumer for - " << topicName << " - " << consumerStr_);
        consumer->start();

    } else {
        for (int i = 0; i < numPartitions; i++) {
            std::string topicPartitionName = topicName->getTopicPartitionName(i);
            consumer = std::make_shared<ConsumerImpl>(client_, topicPartitionName, subscriptionName_, config,
                                                      internalListenerExecutor, true, Partitioned);
            consumer->getConsumerCreatedFuture().addListener(std::bind(
                &MultiTopicsConsumerImpl::handleSingleConsumerCreated, shared_from_this(),
                std::placeholders::_1, std::placeholders::_2, partitionsNeedCreate, topicSubResultPromise));
            consumer->setPartitionIndex(i);
            consumers_.emplace(topicPartitionName, consumer);
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

    LOG_INFO("Successfully Subscribed to a single partition of topic in TopicsConsumer. "
             << "Partitions need to create : " << previous - 1);

    if (partitionsNeedCreate->load() == 0) {
        if (partitionsUpdateTimer_) {
            runPartitionUpdateTask();
        }
        topicSubResultPromise->setValue(Consumer(shared_from_this()));
    }
}

void MultiTopicsConsumerImpl::unsubscribeAsync(ResultCallback callback) {
    LOG_INFO("[ Topics Consumer " << topic_ << "," << subscriptionName_ << "] Unsubscribing");

    const auto state = state_.load();
    if (state == Closing || state == Closed) {
        LOG_INFO(consumerStr_ << " already closed");
        callback(ResultAlreadyClosed);
        return;
    }
    state_ = Closing;

    std::shared_ptr<std::atomic<int>> consumerUnsubed = std::make_shared<std::atomic<int>>(0);
    auto self = shared_from_this();
    int numConsumers = 0;
    consumers_.forEachValue(
        [&numConsumers, &consumerUnsubed, &self, callback](const ConsumerImplPtr& consumer) {
            numConsumers++;
            consumer->unsubscribeAsync([self, consumerUnsubed, callback](Result result) {
                self->handleUnsubscribedAsync(result, consumerUnsubed, callback);
            });
        });
    if (numConsumers == 0) {
        // No need to unsubscribe, since the list matching the regex was empty
        callback(ResultOk);
    }
}

void MultiTopicsConsumerImpl::handleUnsubscribedAsync(Result result,
                                                      std::shared_ptr<std::atomic<int>> consumerUnsubed,
                                                      ResultCallback callback) {
    (*consumerUnsubed)++;

    if (result != ResultOk) {
        state_ = Failed;
        LOG_ERROR("Error Closing one of the consumers in TopicsConsumer, result: "
                  << result << " subscription - " << subscriptionName_);
    }

    if (consumerUnsubed->load() == numberTopicPartitions_->load()) {
        LOG_DEBUG("Unsubscribed all of the partition consumer for TopicsConsumer.  - " << consumerStr_);
        consumers_.clear();
        topicsPartitions_.clear();
        unAckedMessageTrackerPtr_->clear();

        Result result1 = (state_ != Failed) ? ResultOk : ResultUnknownError;
        state_ = Closed;
        callback(result1);
        return;
    }
}

void MultiTopicsConsumerImpl::unsubscribeOneTopicAsync(const std::string& topic, ResultCallback callback) {
    Lock lock(mutex_);
    std::map<std::string, int>::iterator it = topicsPartitions_.find(topic);
    if (it == topicsPartitions_.end()) {
        lock.unlock();
        LOG_ERROR("TopicsConsumer does not subscribe topic : " << topic << " subscription - "
                                                               << subscriptionName_);
        callback(ResultTopicNotFound);
        return;
    }
    int numberPartitions = it->second;
    lock.unlock();

    const auto state = state_.load();
    if (state == Closing || state == Closed) {
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
    std::shared_ptr<std::atomic<int>> consumerUnsubed = std::make_shared<std::atomic<int>>(0);

    for (int i = 0; i < numberPartitions; i++) {
        std::string topicPartitionName = topicName->getTopicPartitionName(i);
        auto optConsumer = consumers_.find(topicPartitionName);
        if (optConsumer.is_empty()) {
            LOG_ERROR("TopicsConsumer not subscribed on topicPartitionName: " << topicPartitionName);
            callback(ResultUnknownError);
            continue;
        }

        optConsumer.value()->unsubscribeAsync(
            std::bind(&MultiTopicsConsumerImpl::handleOneTopicUnsubscribedAsync, shared_from_this(),
                      std::placeholders::_1, consumerUnsubed, numberPartitions, topicName, topicPartitionName,
                      callback));
    }
}

void MultiTopicsConsumerImpl::handleOneTopicUnsubscribedAsync(
    Result result, std::shared_ptr<std::atomic<int>> consumerUnsubed, int numberPartitions,
    TopicNamePtr topicNamePtr, std::string& topicPartitionName, ResultCallback callback) {
    (*consumerUnsubed)++;

    if (result != ResultOk) {
        state_ = Failed;
        LOG_ERROR("Error Closing one of the consumers in TopicsConsumer, result: "
                  << result << " topicPartitionName - " << topicPartitionName);
    }

    LOG_DEBUG("Successfully Unsubscribed one Consumer. topicPartitionName - " << topicPartitionName);

    auto optConsumer = consumers_.remove(topicPartitionName);
    if (optConsumer.is_present()) {
        optConsumer.value()->pauseMessageListener();
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
    const auto state = state_.load();
    if (state == Closing || state == Closed) {
        LOG_ERROR("TopicsConsumer already closed "
                  << " topic" << topic_ << " consumer - " << consumerStr_);
        if (callback) {
            callback(ResultAlreadyClosed);
        }
        return;
    }

    state_ = Closing;

    std::weak_ptr<MultiTopicsConsumerImpl> weakSelf{shared_from_this()};
    int numConsumers = 0;
    consumers_.clear(
        [this, weakSelf, &numConsumers, callback](const std::string& name, const ConsumerImplPtr& consumer) {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }
            numConsumers++;
            consumer->closeAsync([this, weakSelf, name, callback](Result result) {
                auto self = weakSelf.lock();
                if (!self) {
                    return;
                }
                LOG_DEBUG("Closing the consumer for partition - " << name << " numberTopicPartitions_ - "
                                                                  << numberTopicPartitions_->load());
                const int numConsumersLeft = --*numberTopicPartitions_;
                if (numConsumersLeft < 0) {
                    LOG_ERROR("[" << name << "] Unexpected number of left consumers: " << numConsumersLeft
                                  << " during close");
                    return;
                }
                if (result != ResultOk) {
                    state_ = Failed;
                    LOG_ERROR("Closing the consumer failed for partition - " << name << " with error - "
                                                                             << result);
                }
                // closed all consumers
                if (numConsumersLeft == 0) {
                    messages_.clear();
                    topicsPartitions_.clear();
                    unAckedMessageTrackerPtr_->clear();

                    if (state_ != Failed) {
                        state_ = Closed;
                    }

                    if (callback) {
                        callback(result);
                    }
                }
            });
        });
    if (numConsumers == 0) {
        LOG_DEBUG("TopicsConsumer have no consumers to close "
                  << " topic" << topic_ << " subscription - " << subscriptionName_);
        state_ = Closed;
        if (callback) {
            callback(ResultAlreadyClosed);
        }
        return;
    }

    // fail pending recieve
    failPendingReceiveCallback();
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
        listenerExecutor_->postWork(std::bind(&MultiTopicsConsumerImpl::notifyPendingReceivedCallback,
                                              shared_from_this(), ResultOk, msg, callback));
    } else {
        if (messages_.full()) {
            lock.unlock();
        }
        messages_.push(msg);
        if (messageListener_) {
            listenerExecutor_->postWork(
                std::bind(&MultiTopicsConsumerImpl::internalListener, shared_from_this(), consumer));
        }
    }
}

void MultiTopicsConsumerImpl::internalListener(Consumer consumer) {
    Message m;
    messages_.pop(m);
    unAckedMessageTrackerPtr_->add(m.getMessageId());
    try {
        messageListener_(Consumer(shared_from_this()), m);
    } catch (const std::exception& e) {
        LOG_ERROR("Exception thrown from listener of Partitioned Consumer" << e.what());
    }
}

Result MultiTopicsConsumerImpl::receive(Message& msg) {
    if (state_ != Ready) {
        return ResultAlreadyClosed;
    }

    if (messageListener_) {
        LOG_ERROR("Can not receive when a listener has been set");
        return ResultInvalidConfiguration;
    }
    messages_.pop(msg);

    unAckedMessageTrackerPtr_->add(msg.getMessageId());
    return ResultOk;
}

Result MultiTopicsConsumerImpl::receive(Message& msg, int timeout) {
    if (state_ != Ready) {
        return ResultAlreadyClosed;
    }

    if (messageListener_) {
        LOG_ERROR("Can not receive when a listener has been set");
        return ResultInvalidConfiguration;
    }

    if (messages_.pop(msg, std::chrono::milliseconds(timeout))) {
        unAckedMessageTrackerPtr_->add(msg.getMessageId());
        return ResultOk;
    } else {
        return ResultTimeout;
    }
}

void MultiTopicsConsumerImpl::receiveAsync(ReceiveCallback& callback) {
    Message msg;

    // fail the callback if consumer is closing or closed
    if (state_ != Ready) {
        callback(ResultAlreadyClosed, msg);
        return;
    }

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
        listenerExecutor_->postWork(std::bind(&MultiTopicsConsumerImpl::notifyPendingReceivedCallback,
                                              shared_from_this(), ResultAlreadyClosed, msg, callback));
    }
    lock.unlock();
}

void MultiTopicsConsumerImpl::notifyPendingReceivedCallback(Result result, Message& msg,
                                                            const ReceiveCallback& callback) {
    if (result == ResultOk) {
        unAckedMessageTrackerPtr_->add(msg.getMessageId());
    }
    callback(result, msg);
}

void MultiTopicsConsumerImpl::acknowledgeAsync(const MessageId& msgId, ResultCallback callback) {
    if (state_ != Ready) {
        callback(ResultAlreadyClosed);
        return;
    }

    const std::string& topicPartitionName = msgId.getTopicName();
    auto optConsumer = consumers_.find(topicPartitionName);

    if (optConsumer.is_present()) {
        unAckedMessageTrackerPtr_->remove(msgId);
        optConsumer.value()->acknowledgeAsync(msgId, callback);
    } else {
        LOG_ERROR("Message of topic: " << topicPartitionName << " not in unAckedMessageTracker");
        callback(ResultUnknownError);
    }
}

void MultiTopicsConsumerImpl::acknowledgeCumulativeAsync(const MessageId& msgId, ResultCallback callback) {
    callback(ResultOperationNotSupported);
}

void MultiTopicsConsumerImpl::negativeAcknowledge(const MessageId& msgId) {
    auto optConsumer = consumers_.find(msgId.getTopicName());

    if (optConsumer.is_present()) {
        unAckedMessageTrackerPtr_->remove(msgId);
        optConsumer.value()->negativeAcknowledge(msgId);
    }
}

MultiTopicsConsumerImpl::~MultiTopicsConsumerImpl() {}

Future<Result, ConsumerImplBaseWeakPtr> MultiTopicsConsumerImpl::getConsumerCreatedFuture() {
    return multiTopicsConsumerCreatedPromise_.getFuture();
}
const std::string& MultiTopicsConsumerImpl::getSubscriptionName() const { return subscriptionName_; }

const std::string& MultiTopicsConsumerImpl::getTopic() const { return topic_; }

const std::string& MultiTopicsConsumerImpl::getName() const { return consumerStr_; }

void MultiTopicsConsumerImpl::shutdown() {}

bool MultiTopicsConsumerImpl::isClosed() { return state_ == Closed; }

bool MultiTopicsConsumerImpl::isOpen() { return state_ == Ready; }

void MultiTopicsConsumerImpl::receiveMessages() {
    const auto receiverQueueSize = conf_.getReceiverQueueSize();
    consumers_.forEachValue([receiverQueueSize](const ConsumerImplPtr& consumer) {
        consumer->sendFlowPermitsToBroker(consumer->getCnx().lock(), receiverQueueSize);
        LOG_DEBUG("Sending FLOW command for consumer - " << consumer->getConsumerId());
    });
}

Result MultiTopicsConsumerImpl::pauseMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }
    consumers_.forEachValue([](const ConsumerImplPtr& consumer) { consumer->pauseMessageListener(); });
    return ResultOk;
}

Result MultiTopicsConsumerImpl::resumeMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }
    consumers_.forEachValue([](const ConsumerImplPtr& consumer) { consumer->resumeMessageListener(); });
    return ResultOk;
}

void MultiTopicsConsumerImpl::redeliverUnacknowledgedMessages() {
    LOG_DEBUG("Sending RedeliverUnacknowledgedMessages command for partitioned consumer.");
    consumers_.forEachValue(
        [](const ConsumerImplPtr& consumer) { consumer->redeliverUnacknowledgedMessages(); });
    unAckedMessageTrackerPtr_->clear();
}

void MultiTopicsConsumerImpl::redeliverUnacknowledgedMessages(const std::set<MessageId>& messageIds) {
    if (messageIds.empty()) {
        return;
    }
    if (conf_.getConsumerType() != ConsumerShared && conf_.getConsumerType() != ConsumerKeyShared) {
        redeliverUnacknowledgedMessages();
        return;
    }
    LOG_DEBUG("Sending RedeliverUnacknowledgedMessages command for partitioned consumer.");
    consumers_.forEachValue([&messageIds](const ConsumerImplPtr& consumer) {
        consumer->redeliverUnacknowledgedMessages(messageIds);
    });
}

int MultiTopicsConsumerImpl::getNumOfPrefetchedMessages() const { return messages_.size(); }

void MultiTopicsConsumerImpl::getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback) {
    if (state_ != Ready) {
        callback(ResultConsumerNotInitialized, BrokerConsumerStats());
        return;
    }
    Lock lock(mutex_);
    MultiTopicsBrokerConsumerStatsPtr statsPtr =
        std::make_shared<MultiTopicsBrokerConsumerStatsImpl>(numberTopicPartitions_->load());
    LatchPtr latchPtr = std::make_shared<Latch>(numberTopicPartitions_->load());
    lock.unlock();

    auto self = shared_from_this();
    size_t i = 0;
    consumers_.forEachValue([&self, &latchPtr, &statsPtr, &i, callback](const ConsumerImplPtr& consumer) {
        size_t index = i++;
        consumer->getBrokerConsumerStatsAsync(
            [self, latchPtr, statsPtr, index, callback](Result result, BrokerConsumerStats stats) {
                self->handleGetConsumerStats(result, stats, latchPtr, statsPtr, index, callback);
            });
    });
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

    // all topics name valid, and all topics have same namespace
    for (std::vector<std::string>::const_iterator itr = topics.begin(); itr != topics.end(); itr++) {
        // topic name valid
        if (!(topicNamePtr = TopicName::get(*itr))) {
            LOG_ERROR("Topic name invalid when init " << *itr);
            return std::shared_ptr<TopicName>();
        }
    }

    return topicNamePtr;
}

void MultiTopicsConsumerImpl::seekAsync(const MessageId& msgId, ResultCallback callback) {
    callback(ResultOperationNotSupported);
}

void MultiTopicsConsumerImpl::seekAsync(uint64_t timestamp, ResultCallback callback) {
    if (state_ != Ready) {
        callback(ResultAlreadyClosed);
        return;
    }

    MultiResultCallback multiResultCallback(callback, consumers_.size());
    consumers_.forEachValue([&timestamp, &multiResultCallback](ConsumerImplPtr consumer) {
        consumer->seekAsync(timestamp, multiResultCallback);
    });
}

void MultiTopicsConsumerImpl::setNegativeAcknowledgeEnabledForTesting(bool enabled) {
    consumers_.forEachValue([enabled](const ConsumerImplPtr& consumer) {
        consumer->setNegativeAcknowledgeEnabledForTesting(enabled);
    });
}

bool MultiTopicsConsumerImpl::isConnected() const {
    if (state_ != Ready) {
        return false;
    }

    return consumers_
        .findFirstValueIf([](const ConsumerImplPtr& consumer) { return !consumer->isConnected(); })
        .is_empty();
}

uint64_t MultiTopicsConsumerImpl::getNumberOfConnectedConsumer() {
    uint64_t numberOfConnectedConsumer = 0;
    consumers_.forEachValue([&numberOfConnectedConsumer](const ConsumerImplPtr& consumer) {
        if (consumer->isConnected()) {
            numberOfConnectedConsumer++;
        }
    });
    return numberOfConnectedConsumer;
}
void MultiTopicsConsumerImpl::runPartitionUpdateTask() {
    partitionsUpdateTimer_->expires_from_now(partitionsUpdateInterval_);
    std::weak_ptr<MultiTopicsConsumerImpl> weakSelf{shared_from_this()};
    partitionsUpdateTimer_->async_wait([weakSelf](const boost::system::error_code& ec) {
        // If two requests call runPartitionUpdateTask at the same time, the timer will fail, and it
        // cannot continue at this time, and the request needs to be ignored.
        auto self = weakSelf.lock();
        if (self && !ec) {
            self->topicPartitionUpdate();
        }
    });
}
void MultiTopicsConsumerImpl::topicPartitionUpdate() {
    using namespace std::placeholders;
    Lock lock(mutex_);
    auto topicsPartitions = topicsPartitions_;
    lock.unlock();
    for (const auto& item : topicsPartitions) {
        auto topicName = TopicName::get(item.first);
        auto currentNumPartitions = item.second;
        lookupServicePtr_->getPartitionMetadataAsync(topicName).addListener(
            std::bind(&MultiTopicsConsumerImpl::handleGetPartitions, shared_from_this(), topicName,
                      std::placeholders::_1, std::placeholders::_2, currentNumPartitions));
    }
}
void MultiTopicsConsumerImpl::handleGetPartitions(TopicNamePtr topicName, Result result,
                                                  const LookupDataResultPtr& lookupDataResult,
                                                  int currentNumPartitions) {
    if (state_ != Ready) {
        return;
    }
    if (!result) {
        const auto newNumPartitions = static_cast<unsigned int>(lookupDataResult->getPartitions());
        if (newNumPartitions > currentNumPartitions) {
            LOG_INFO("new partition count: " << newNumPartitions
                                             << " current partition count: " << currentNumPartitions);
            auto partitionsNeedCreate =
                std::make_shared<std::atomic<int>>(newNumPartitions - currentNumPartitions);
            ConsumerSubResultPromisePtr topicPromise = std::make_shared<Promise<Result, Consumer>>();
            Lock lock(mutex_);
            topicsPartitions_[topicName->toString()] = newNumPartitions;
            lock.unlock();
            numberTopicPartitions_->fetch_add(newNumPartitions - currentNumPartitions);
            for (unsigned int i = currentNumPartitions; i < newNumPartitions; i++) {
                subscribeSingleNewConsumer(newNumPartitions, topicName, i, topicPromise,
                                           partitionsNeedCreate);
            }
            // `runPartitionUpdateTask()` will be called in `handleSingleConsumerCreated()`
            return;
        }
    } else {
        LOG_WARN("Failed to getPartitionMetadata: " << strResult(result));
    }
    runPartitionUpdateTask();
}

void MultiTopicsConsumerImpl::subscribeSingleNewConsumer(
    int numPartitions, TopicNamePtr topicName, int partitionIndex,
    ConsumerSubResultPromisePtr topicSubResultPromise,
    std::shared_ptr<std::atomic<int>> partitionsNeedCreate) {
    ConsumerConfiguration config = conf_.clone();
    ExecutorServicePtr internalListenerExecutor = client_->getPartitionListenerExecutorProvider()->get();
    config.setMessageListener(std::bind(&MultiTopicsConsumerImpl::messageReceived, shared_from_this(),
                                        std::placeholders::_1, std::placeholders::_2));

    // Apply total limit of receiver queue size across partitions
    config.setReceiverQueueSize(
        std::min(conf_.getReceiverQueueSize(),
                 (int)(conf_.getMaxTotalReceiverQueueSizeAcrossPartitions() / numPartitions)));

    std::string topicPartitionName = topicName->getTopicPartitionName(partitionIndex);

    auto consumer = std::make_shared<ConsumerImpl>(client_, topicPartitionName, subscriptionName_, config,
                                                   internalListenerExecutor, true, Partitioned);
    consumer->getConsumerCreatedFuture().addListener(
        std::bind(&MultiTopicsConsumerImpl::handleSingleConsumerCreated, shared_from_this(),
                  std::placeholders::_1, std::placeholders::_2, partitionsNeedCreate, topicSubResultPromise));
    consumer->setPartitionIndex(partitionIndex);
    consumer->start();
    consumers_.emplace(topicPartitionName, consumer);
    LOG_INFO("Add Creating Consumer for - " << topicPartitionName << " - " << consumerStr_
                                            << " consumerSize: " << consumers_.size());
}
