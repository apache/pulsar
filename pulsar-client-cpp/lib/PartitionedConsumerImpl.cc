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
#include "PartitionedConsumerImpl.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

PartitionedConsumerImpl::PartitionedConsumerImpl(ClientImplPtr client, const std::string& subscriptionName,
                                                 const TopicNamePtr topicName,
                                                 const unsigned int numPartitions,
                                                 const ConsumerConfiguration& conf)
    : client_(client),
      subscriptionName_(subscriptionName),
      topicName_(topicName),
      numPartitions_(numPartitions),
      numConsumersCreated_(0),
      conf_(conf),
      state_(Pending),
      unsubscribedSoFar_(0),
      messages_(1000),
      listenerExecutor_(client->getListenerExecutorProvider()->get()),
      messageListener_(conf.getMessageListener()),
      pendingReceives_(),
      topic_(topicName->toString()) {
    std::stringstream consumerStrStream;
    consumerStrStream << "[Partitioned Consumer: " << topic_ << "," << subscriptionName << ","
                      << numPartitions << "]";
    if (conf.getUnAckedMessagesTimeoutMs() != 0) {
        unAckedMessageTrackerPtr_.reset(
            new UnAckedMessageTrackerEnabled(conf.getUnAckedMessagesTimeoutMs(), client, *this));
    } else {
        unAckedMessageTrackerPtr_.reset(new UnAckedMessageTrackerDisabled());
    }
}

PartitionedConsumerImpl::~PartitionedConsumerImpl() {}

Future<Result, ConsumerImplBaseWeakPtr> PartitionedConsumerImpl::getConsumerCreatedFuture() {
    return partitionedConsumerCreatedPromise_.getFuture();
}
const std::string& PartitionedConsumerImpl::getSubscriptionName() const { return subscriptionName_; }

const std::string& PartitionedConsumerImpl::getTopic() const { return topic_; }

Result PartitionedConsumerImpl::receive(Message& msg) {
    Lock lock(mutex_);
    if (state_ != Ready) {
        lock.unlock();
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

Result PartitionedConsumerImpl::receive(Message& msg, int timeout) {
    Lock lock(mutex_);
    if (state_ != Ready) {
        lock.unlock();
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

void PartitionedConsumerImpl::receiveAsync(ReceiveCallback& callback) {
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

void PartitionedConsumerImpl::unsubscribeAsync(ResultCallback callback) {
    LOG_INFO("[" << topicName_->toString() << "," << subscriptionName_ << "] Unsubscribing");
    // change state to Closing, so that no Ready state operation is permitted during unsubscribe
    setState(Closing);
    // do not accept un subscribe until we have subscribe to all of the partitions of a topic
    // it's a logical single topic so it should behave like a single topic, even if it's sharded
    Lock lock(mutex_);
    if (state_ != Ready) {
        lock.unlock();
        unsigned int index = 0;
        for (ConsumerList::const_iterator consumer = consumers_.begin(); consumer != consumers_.end();
             consumer++) {
            LOG_DEBUG("Unsubcribing Consumer - " << index << " for Subscription - " << subscriptionName_
                                                 << " for Topic - " << topicName_->toString());
            (*consumer)->unsubscribeAsync(std::bind(&PartitionedConsumerImpl::handleUnsubscribeAsync,
                                                    shared_from_this(), std::placeholders::_1, index++,
                                                    callback));
        }
    }
}

void PartitionedConsumerImpl::handleUnsubscribeAsync(Result result, unsigned int consumerIndex,
                                                     ResultCallback callback) {
    Lock lock(mutex_);
    if (state_ == Failed) {
        lock.unlock();
        // we have already informed the client that unsubcribe has failed so, ignore this callbacks
        // or do we still go ahead and check how many could we close successfully?
        LOG_DEBUG("handleUnsubscribeAsync callback received in Failed State for consumerIndex - "
                  << consumerIndex << "with Result - " << result << " for Subscription - "
                  << subscriptionName_ << " for Topic - " << topicName_->toString());
        return;
    }
    lock.unlock();
    if (result != ResultOk) {
        setState(Failed);
        LOG_ERROR("Error Closing one of the parition consumers, consumerIndex - " << consumerIndex);
        callback(ResultUnknownError);
        return;
    }
    assert(unsubscribedSoFar_ <= numPartitions_);
    assert(consumerIndex <= numPartitions_);
    // this means we have successfully closed this partition consumer and no unsubscribe has failed so far
    LOG_INFO("Successfully Unsubscribed Consumer - " << consumerIndex << " for Subscription - "
                                                     << subscriptionName_ << " for Topic - "
                                                     << topicName_->toString());
    unsubscribedSoFar_++;
    if (unsubscribedSoFar_ == numPartitions_) {
        LOG_DEBUG("Unsubscribed all of the partition consumer for subscription - " << subscriptionName_);
        setState(Closed);
        callback(ResultOk);
        return;
    }
}

void PartitionedConsumerImpl::acknowledgeAsync(const MessageId& msgId, ResultCallback callback) {
    int32_t partition = msgId.partition();
    assert(partition < numPartitions_ && partition >= 0 && consumers_.size() > partition);
    unAckedMessageTrackerPtr_->remove(msgId);
    consumers_[partition]->acknowledgeAsync(msgId, callback);
}

void PartitionedConsumerImpl::acknowledgeCumulativeAsync(const MessageId& msgId, ResultCallback callback) {
    callback(ResultOperationNotSupported);
}

void PartitionedConsumerImpl::negativeAcknowledge(const MessageId& msgId) {
    int32_t partition = msgId.partition();
    unAckedMessageTrackerPtr_->remove(msgId);
    consumers_[partition]->negativeAcknowledge(msgId);
}

void PartitionedConsumerImpl::start() {
    ExecutorServicePtr internalListenerExecutor = client_->getPartitionListenerExecutorProvider()->get();
    std::shared_ptr<ConsumerImpl> consumer;
    ConsumerConfiguration config;
    // all the partitioned-consumer belonging to one partitioned topic should have same name
    config.setConsumerName(conf_.getConsumerName());
    config.setConsumerType(conf_.getConsumerType());
    config.setBrokerConsumerStatsCacheTimeInMs(conf_.getBrokerConsumerStatsCacheTimeInMs());
    config.setMessageListener(std::bind(&PartitionedConsumerImpl::messageReceived, shared_from_this(),
                                        std::placeholders::_1, std::placeholders::_2));

    // Apply total limit of receiver queue size across partitions
    config.setReceiverQueueSize(
        std::min(conf_.getReceiverQueueSize(),
                 (int)(conf_.getMaxTotalReceiverQueueSizeAcrossPartitions() / numPartitions_)));

    // create consumer on each partition
    for (unsigned int i = 0; i < numPartitions_; i++) {
        std::string topicPartitionName = topicName_->getTopicPartitionName(i);
        consumer = std::make_shared<ConsumerImpl>(client_, topicPartitionName, subscriptionName_, config,
                                                  internalListenerExecutor, Partitioned);
        consumer->getConsumerCreatedFuture().addListener(
            std::bind(&PartitionedConsumerImpl::handleSinglePartitionConsumerCreated, shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2, i));
        consumer->setPartitionIndex(i);
        consumers_.push_back(consumer);

        LOG_DEBUG("Creating Consumer for single Partition - " << topicPartitionName << "SubName - "
                                                              << subscriptionName_);
    }
    for (ConsumerList::const_iterator consumer = consumers_.begin(); consumer != consumers_.end();
         consumer++) {
        (*consumer)->start();
    }
}

void PartitionedConsumerImpl::handleSinglePartitionConsumerCreated(
    Result result, ConsumerImplBaseWeakPtr consumerImplBaseWeakPtr, unsigned int partitionIndex) {
    ResultCallback nullCallbackForCleanup = NULL;
    Lock lock(mutex_);
    if (state_ == Failed) {
        // one of the consumer creation failed, and we are cleaning up
        return;
    }
    assert(numConsumersCreated_ < numPartitions_);

    if (result != ResultOk) {
        state_ = Failed;
        lock.unlock();
        partitionedConsumerCreatedPromise_.setFailed(result);
        // unsubscribed all of the successfully subscribed partitioned consumers
        closeAsync(nullCallbackForCleanup);
        LOG_DEBUG("Unable to create Consumer for partition - " << partitionIndex << " Error - " << result);
        return;
    }

    assert(partitionIndex < numPartitions_ && partitionIndex >= 0);
    numConsumersCreated_++;
    if (numConsumersCreated_ == numPartitions_) {
        LOG_INFO("Successfully Subscribed to Partitioned Topic - " << topicName_->toString() << " with - "
                                                                   << numPartitions_ << " Partitions.");
        state_ = Ready;
        lock.unlock();
        receiveMessages();
        partitionedConsumerCreatedPromise_.setValue(shared_from_this());
        return;
    }
}

void PartitionedConsumerImpl::handleSinglePartitionConsumerClose(Result result, unsigned int partitionIndex,
                                                                 CloseCallback callback) {
    Lock lock(mutex_);
    if (state_ == Failed) {
        // we should have already notified the client by callback
        return;
    }
    if (result != ResultOk) {
        state_ = Failed;
        LOG_ERROR("Closing the consumer failed for partition - " << partitionIndex);
        lock.unlock();
        partitionedConsumerCreatedPromise_.setFailed(result);
        if (callback) {
            callback(result);
        }
        return;
    }
    assert(partitionIndex < numPartitions_ && partitionIndex >= 0);
    if (numConsumersCreated_ > 0) {
        numConsumersCreated_--;
    }
    // closed all successfully
    if (!numConsumersCreated_) {
        state_ = Closed;
        lock.unlock();
        // set the producerCreatedPromise to failure
        partitionedConsumerCreatedPromise_.setFailed(ResultUnknownError);
        if (callback) {
            callback(result);
        }
        return;
    }
}
void PartitionedConsumerImpl::closeAsync(ResultCallback callback) {
    if (consumers_.empty()) {
        notifyResult(callback);
        return;
    }
    setState(Closed);
    int consumerIndex = 0;
    unsigned int consumerAlreadyClosed = 0;
    // close successfully subscribed consumers
    for (ConsumerList::const_iterator i = consumers_.begin(); i != consumers_.end(); i++) {
        ConsumerImplPtr consumer = *i;
        if (!consumer->isClosed()) {
            consumer->closeAsync(std::bind(&PartitionedConsumerImpl::handleSinglePartitionConsumerClose,
                                           shared_from_this(), std::placeholders::_1, consumerIndex,
                                           callback));
        } else {
            if (++consumerAlreadyClosed == consumers_.size()) {
                // everything is closed already. so we are good.
                notifyResult(callback);
                return;
            }
        }
    }

    // fail pending recieve
    failPendingReceiveCallback();
}

void PartitionedConsumerImpl::notifyResult(CloseCallback closeCallback) {
    if (closeCallback) {
        // this means client invoked the closeAsync with a valid callback
        setState(Closed);
        closeCallback(ResultOk);
    } else {
        // consumer create failed, closeAsync called to cleanup the successfully created producers
        setState(Failed);
        partitionedConsumerCreatedPromise_.setFailed(ResultUnknownError);
    }
}

void PartitionedConsumerImpl::setState(const PartitionedConsumerState state) {
    Lock lock(mutex_);
    state_ = state;
    lock.unlock();
}

void PartitionedConsumerImpl::shutdown() {}

bool PartitionedConsumerImpl::isClosed() { return state_ == Closed; }

bool PartitionedConsumerImpl::isOpen() {
    Lock lock(mutex_);
    return state_ == Ready;
}

void PartitionedConsumerImpl::messageReceived(Consumer consumer, const Message& msg) {
    LOG_DEBUG("Received Message from one of the partition - " << msg.impl_->messageId.partition());
    const std::string& topicPartitionName = consumer.getTopic();
    msg.impl_->setTopicName(topicPartitionName);
    // messages_ is a blocking queue: if queue is already full then no need of lock as receiveAsync already
    // gets available-msg and no need to put request in pendingReceives_
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
            listenerExecutor_->postWork(
                std::bind(&PartitionedConsumerImpl::internalListener, shared_from_this(), consumer));
        }
    }
}

void PartitionedConsumerImpl::failPendingReceiveCallback() {
    Message msg;
    Lock lock(pendingReceiveMutex_);
    while (!pendingReceives_.empty()) {
        ReceiveCallback callback = pendingReceives_.front();
        pendingReceives_.pop();
        listenerExecutor_->postWork(std::bind(callback, ResultAlreadyClosed, msg));
    }
    lock.unlock();
}

void PartitionedConsumerImpl::internalListener(Consumer consumer) {
    Message m;
    messages_.pop(m);
    try {
        messageListener_(Consumer(shared_from_this()), m);
    } catch (const std::exception& e) {
        LOG_ERROR("Exception thrown from listener of Partitioned Consumer" << e.what());
    }
}

void PartitionedConsumerImpl::receiveMessages() {
    for (ConsumerList::const_iterator i = consumers_.begin(); i != consumers_.end(); i++) {
        ConsumerImplPtr consumer = *i;
        consumer->receiveMessages(consumer->getCnx().lock(), conf_.getReceiverQueueSize());
        LOG_DEBUG("Sending FLOW command for consumer - " << consumer->getConsumerId());
    }
}

Result PartitionedConsumerImpl::pauseMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }
    for (ConsumerList::const_iterator i = consumers_.begin(); i != consumers_.end(); i++) {
        (*i)->pauseMessageListener();
    }
    return ResultOk;
}

Result PartitionedConsumerImpl::resumeMessageListener() {
    if (!messageListener_) {
        return ResultInvalidConfiguration;
    }
    for (ConsumerList::const_iterator i = consumers_.begin(); i != consumers_.end(); i++) {
        (*i)->resumeMessageListener();
    }
    return ResultOk;
}

void PartitionedConsumerImpl::redeliverUnacknowledgedMessages() {
    LOG_DEBUG("Sending RedeliverUnacknowledgedMessages command for partitioned consumer.");
    for (ConsumerList::const_iterator i = consumers_.begin(); i != consumers_.end(); i++) {
        (*i)->redeliverUnacknowledgedMessages();
    }
}

const std::string& PartitionedConsumerImpl::getName() const { return partitionStr_; }

int PartitionedConsumerImpl::getNumOfPrefetchedMessages() const { return messages_.size(); }

void PartitionedConsumerImpl::getBrokerConsumerStatsAsync(BrokerConsumerStatsCallback callback) {
    Lock lock(mutex_);
    if (state_ != Ready) {
        lock.unlock();
        callback(ResultConsumerNotInitialized, BrokerConsumerStats());
        return;
    }
    PartitionedBrokerConsumerStatsPtr statsPtr =
        std::make_shared<PartitionedBrokerConsumerStatsImpl>(numPartitions_);
    LatchPtr latchPtr = std::make_shared<Latch>(numPartitions_);
    ConsumerList consumerList = consumers_;
    lock.unlock();
    for (int i = 0; i < consumerList.size(); i++) {
        consumerList[i]->getBrokerConsumerStatsAsync(
            std::bind(&PartitionedConsumerImpl::handleGetConsumerStats, shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2, latchPtr, statsPtr, i, callback));
    }
}

void PartitionedConsumerImpl::handleGetConsumerStats(Result res, BrokerConsumerStats brokerConsumerStats,
                                                     LatchPtr latchPtr,
                                                     PartitionedBrokerConsumerStatsPtr statsPtr, size_t index,
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

void PartitionedConsumerImpl::seekAsync(const MessageId& msgId, ResultCallback callback) {
    callback(ResultOperationNotSupported);
}

}  // namespace pulsar
