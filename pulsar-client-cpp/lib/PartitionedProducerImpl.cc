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
#include "PartitionedProducerImpl.h"
#include "LogUtils.h"
#include <lib/TopicName.h>
#include <sstream>
#include "RoundRobinMessageRouter.h"
#include "SinglePartitionMessageRouter.h"
#include "TopicMetadataImpl.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

const std::string PartitionedProducerImpl::PARTITION_NAME_SUFFIX = "-partition-";

PartitionedProducerImpl::PartitionedProducerImpl(ClientImplPtr client, const TopicNamePtr topicName,
                                                 const unsigned int numPartitions,
                                                 const ProducerConfiguration& config)
    : client_(client),
      topicName_(topicName),
      topic_(topicName_->toString()),
      conf_(config),
      state_(Pending),
      topicMetadata_(new TopicMetadataImpl(numPartitions)),
      flushedPartitions_(0) {
    numProducersCreated_ = 0;
    cleanup_ = false;
    routerPolicy_ = getMessageRouter();

    int maxPendingMessagesPerPartition =
        std::min(config.getMaxPendingMessages(),
                 (int)(config.getMaxPendingMessagesAcrossPartitions() / numPartitions));
    conf_.setMaxPendingMessages(maxPendingMessagesPerPartition);

    auto partitionsUpdateInterval = static_cast<unsigned int>(client_->conf().getPartitionsUpdateInterval());
    if (partitionsUpdateInterval > 0) {
        listenerExecutor_ = client_->getListenerExecutorProvider()->get();
        partitionsUpdateTimer_ = listenerExecutor_->createDeadlineTimer();
        partitionsUpdateInterval_ = boost::posix_time::seconds(partitionsUpdateInterval);
        lookupServicePtr_ = client_->getLookup();
    }
}

MessageRoutingPolicyPtr PartitionedProducerImpl::getMessageRouter() {
    switch (conf_.getPartitionsRoutingMode()) {
        case ProducerConfiguration::RoundRobinDistribution:
            return std::make_shared<RoundRobinMessageRouter>(
                conf_.getHashingScheme(), conf_.getBatchingEnabled(), conf_.getBatchingMaxMessages(),
                conf_.getBatchingMaxAllowedSizeInBytes(),
                boost::posix_time::milliseconds(conf_.getBatchingMaxPublishDelayMs()));
        case ProducerConfiguration::CustomPartition:
            return conf_.getMessageRouterPtr();
        case ProducerConfiguration::UseSinglePartition:
        default:
            return std::make_shared<SinglePartitionMessageRouter>(getNumPartitions(),
                                                                  conf_.getHashingScheme());
    }
}

PartitionedProducerImpl::~PartitionedProducerImpl() {}
// override
const std::string& PartitionedProducerImpl::getTopic() const { return topic_; }

unsigned int PartitionedProducerImpl::getNumPartitions() const {
    return static_cast<unsigned int>(topicMetadata_->getNumPartitions());
}

unsigned int PartitionedProducerImpl::getNumPartitionsWithLock() const {
    Lock lock(producersMutex_);
    return getNumPartitions();
}

ProducerImplPtr PartitionedProducerImpl::newInternalProducer(unsigned int partition) const {
    using namespace std::placeholders;
    std::string topicPartitionName = topicName_->getTopicPartitionName(partition);
    auto producer = std::make_shared<ProducerImpl>(client_, topicPartitionName, conf_, partition);
    producer->getProducerCreatedFuture().addListener(
        std::bind(&PartitionedProducerImpl::handleSinglePartitionProducerCreated,
                  const_cast<PartitionedProducerImpl*>(this)->shared_from_this(), _1, _2, partition));

    LOG_DEBUG("Creating Producer for single Partition - " << topicPartitionName);
    return producer;
}

// override
void PartitionedProducerImpl::start() {
    // create producer per partition
    // Here we don't need `producersMutex` to protect `producers_`, because `producers_` can only be increased
    // when `state_` is Ready
    for (unsigned int i = 0; i < getNumPartitions(); i++) {
        producers_.push_back(newInternalProducer(i));
    }

    for (ProducerList::const_iterator prod = producers_.begin(); prod != producers_.end(); prod++) {
        (*prod)->start();
    }
}

void PartitionedProducerImpl::handleSinglePartitionProducerCreated(Result result,
                                                                   ProducerImplBaseWeakPtr producerWeakPtr,
                                                                   unsigned int partitionIndex) {
    // to indicate, we are doing cleanup using closeAsync after producer create
    // has failed and the invocation of closeAsync is not from client
    CloseCallback closeCallback = NULL;
    Lock lock(mutex_);
    if (state_ == Failed) {
        // Ignore, we have already informed client that producer creation failed
        return;
    }
    const auto numPartitions = getNumPartitionsWithLock();
    assert(numProducersCreated_ <= numPartitions);
    if (result != ResultOk) {
        state_ = Failed;
        lock.unlock();
        closeAsync(closeCallback);
        partitionedProducerCreatedPromise_.setFailed(result);
        LOG_ERROR("Unable to create Producer for partition - " << partitionIndex << " Error - " << result);
        return;
    }

    assert(partitionIndex <= numPartitions);
    numProducersCreated_++;
    if (numProducersCreated_ == numPartitions) {
        state_ = Ready;
        lock.unlock();
        if (partitionsUpdateTimer_) {
            runPartitionUpdateTask();
        }
        partitionedProducerCreatedPromise_.setValue(shared_from_this());
    }
}

// override
void PartitionedProducerImpl::sendAsync(const Message& msg, SendCallback callback) {
    // get partition for this message from router policy
    Lock producersLock(producersMutex_);
    short partition = (short)(routerPolicy_->getPartition(msg, *topicMetadata_));
    if (partition >= getNumPartitions() || partition >= producers_.size()) {
        LOG_ERROR("Got Invalid Partition for message from Router Policy, Partition - " << partition);
        // change me: abort or notify failure in callback?
        //          change to appropriate error if callback
        callback(ResultUnknownError, msg.getMessageId());
        return;
    }
    // find a producer for that partition, index should start from 0
    ProducerImplPtr producer = producers_[partition];
    producersLock.unlock();
    // send message on that partition
    producer->sendAsync(msg, callback);
}

// override
void PartitionedProducerImpl::shutdown() { setState(Closed); }

void PartitionedProducerImpl::setState(const PartitionedProducerState state) {
    Lock lock(mutex_);
    state_ = state;
    lock.unlock();
}

const std::string& PartitionedProducerImpl::getProducerName() const {
    Lock producersLock(producersMutex_);
    return producers_[0]->getProducerName();
}

const std::string& PartitionedProducerImpl::getSchemaVersion() const {
    Lock producersLock(producersMutex_);
    // Since the schema is atomically assigned on the partitioned-topic,
    // it's guaranteed that all the partitions will have the same schema version.
    return producers_[0]->getSchemaVersion();
}

int64_t PartitionedProducerImpl::getLastSequenceId() const {
    int64_t currentMax = -1L;
    Lock producersLock(producersMutex_);
    for (int i = 0; i < producers_.size(); i++) {
        currentMax = std::max(currentMax, producers_[i]->getLastSequenceId());
    }

    return currentMax;
}

/*
 * if createProducerCallback is set, it means the closeAsync is called from CreateProducer API which failed to
 * create one or many producers for partitions. So, we have to notify with ERROR on createProducerFailure
 */
void PartitionedProducerImpl::closeAsync(CloseCallback closeCallback) {
    setState(Closing);

    unsigned int producerAlreadyClosed = 0;

    // Here we don't need `producersMutex` to protect `producers_`, because `producers_` can only be increased
    // when `state_` is Ready
    for (auto& producer : producers_) {
        if (!producer->isClosed()) {
            auto self = shared_from_this();
            const auto partition = static_cast<unsigned int>(producer->partition());
            producer->closeAsync([this, self, partition, closeCallback](Result result) {
                handleSinglePartitionProducerClose(result, partition, closeCallback);
            });
        } else {
            producerAlreadyClosed++;
        }
    }
    const auto numProducers = producers_.size();

    /*
     * No need to set state since:-
     * a. If closeAsync before creation then state == Closed, since producers_.size() = producerAlreadyClosed
     * = 0
     * b. If closeAsync called after all sub partitioned producer connected -
     * handleSinglePartitionProducerClose handles the closing
     * c. If closeAsync called due to failure in creating just one sub producer then state is set by
     * handleSinglePartitionProducerCreated
     */
    if (producerAlreadyClosed == numProducers && closeCallback) {
        setState(Closed);
        closeCallback(ResultOk);
    }
}

void PartitionedProducerImpl::handleSinglePartitionProducerClose(Result result,
                                                                 const unsigned int partitionIndex,
                                                                 CloseCallback callback) {
    Lock lock(mutex_);
    if (state_ == Failed) {
        // we should have already notified the client by callback
        return;
    }
    if (result != ResultOk) {
        state_ = Failed;
        lock.unlock();
        LOG_ERROR("Closing the producer failed for partition - " << partitionIndex);
        if (callback) {
            callback(result);
        }
        return;
    }
    assert(partitionIndex < getNumPartitionsWithLock());
    if (numProducersCreated_ > 0) {
        numProducersCreated_--;
    }
    // closed all successfully
    if (!numProducersCreated_) {
        state_ = Closed;
        lock.unlock();
        // set the producerCreatedPromise to failure, if client called
        // closeAsync and it's not failure to create producer, the promise
        // is set second time here, first time it was successful. So check
        // if there's any adverse effect of setting it again. It should not
        // be but must check. MUSTCHECK changeme
        partitionedProducerCreatedPromise_.setFailed(ResultUnknownError);
        if (callback) {
            callback(result);
        }
        return;
    }
}

// override
Future<Result, ProducerImplBaseWeakPtr> PartitionedProducerImpl::getProducerCreatedFuture() {
    return partitionedProducerCreatedPromise_.getFuture();
}

// override
bool PartitionedProducerImpl::isClosed() { return state_ == Closed; }

void PartitionedProducerImpl::triggerFlush() {
    Lock producersLock(producersMutex_);
    for (ProducerList::const_iterator prod = producers_.begin(); prod != producers_.end(); prod++) {
        (*prod)->triggerFlush();
    }
}

void PartitionedProducerImpl::flushAsync(FlushCallback callback) {
    if (!flushPromise_ || flushPromise_->isComplete()) {
        flushPromise_ = std::make_shared<Promise<Result, bool_type>>();
    } else {
        // already in flushing, register a listener callback
        std::function<void(Result, bool)> listenerCallback = [this, callback](Result result, bool_type v) {
            if (v) {
                callback(ResultOk);
            } else {
                callback(ResultUnknownError);
            }
            return;
        };

        flushPromise_->getFuture().addListener(listenerCallback);
        return;
    }

    Lock producersLock(producersMutex_);
    const int numProducers = static_cast<int>(producers_.size());
    FlushCallback subFlushCallback = [this, callback, numProducers](Result result) {
        // We shouldn't lock `producersMutex_` here because `subFlushCallback` may be called in
        // `ProducerImpl::flushAsync`, and then deadlock occurs.
        int previous = flushedPartitions_.fetch_add(1);
        if (previous == numProducers - 1) {
            flushedPartitions_.store(0);
            flushPromise_->setValue(true);
            callback(result);
        }
        return;
    };

    for (ProducerList::const_iterator prod = producers_.begin(); prod != producers_.end(); prod++) {
        (*prod)->flushAsync(subFlushCallback);
    }
}

void PartitionedProducerImpl::runPartitionUpdateTask() {
    partitionsUpdateTimer_->expires_from_now(partitionsUpdateInterval_);
    partitionsUpdateTimer_->async_wait(
        std::bind(&PartitionedProducerImpl::getPartitionMetadata, shared_from_this()));
}

void PartitionedProducerImpl::getPartitionMetadata() {
    using namespace std::placeholders;
    lookupServicePtr_->getPartitionMetadataAsync(topicName_)
        .addListener(std::bind(&PartitionedProducerImpl::handleGetPartitions, shared_from_this(), _1, _2));
}

void PartitionedProducerImpl::handleGetPartitions(Result result,
                                                  const LookupDataResultPtr& lookupDataResult) {
    Lock stateLock(mutex_);
    if (state_ != Ready) {
        return;
    }

    if (!result) {
        const auto newNumPartitions = static_cast<unsigned int>(lookupDataResult->getPartitions());
        Lock producersLock(producersMutex_);
        const auto currentNumPartitions = getNumPartitions();
        assert(currentNumPartitions == producers_.size());
        if (newNumPartitions > currentNumPartitions) {
            LOG_INFO("new partition count: " << newNumPartitions);
            topicMetadata_.reset(new TopicMetadataImpl(newNumPartitions));

            for (unsigned int i = currentNumPartitions; i < newNumPartitions; i++) {
                auto producer = newInternalProducer(i);
                producer->start();
                producers_.push_back(producer);
            }
            // `runPartitionUpdateTask()` will be called in `handleSinglePartitionProducerCreated()`
            return;
        }
    } else {
        LOG_WARN("Failed to getPartitionMetadata: " << strResult(result));
    }

    runPartitionUpdateTask();
}

bool PartitionedProducerImpl::isConnected() const {
    Lock stateLock(mutex_);
    if (state_ != Ready) {
        return false;
    }
    stateLock.unlock();

    Lock producersLock(producersMutex_);
    const auto producers = producers_;
    producersLock.unlock();
    for (const auto& producer : producers_) {
        if (!producer->isConnected()) {
            return false;
        }
    }
    return true;
}

}  // namespace pulsar
