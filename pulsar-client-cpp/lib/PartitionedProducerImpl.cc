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

#include "PartitionedProducerImpl.h"
#include "LogUtils.h"
#include <boost/bind.hpp>
#include <sstream>
#include "RoundRobinMessageRouter.h"
#include "SinglePartitionMessageRouter.h"
#include "DestinationName.h"
#include "MessageImpl.h"

DECLARE_LOG_OBJECT()

namespace pulsar {

    const std::string PartitionedProducerImpl::PARTITION_NAME_SUFFIX = "-partition-";

    PartitionedProducerImpl::PartitionedProducerImpl(ClientImplPtr client,
                                                     const DestinationNamePtr destinationName,
                                                     const unsigned int numPartitions,
                                                     const ProducerConfiguration& config):client_(client),
                                                                                          destinationName_(destinationName),
                                                                                          topic_(destinationName_->toString()),
                                                                                          numPartitions_(numPartitions),
                                                                                          conf_(config),
                                                                                          state_(Pending)
    {
        numProducersCreated_ = 0;
        cleanup_ = false;
        if(config.getPartitionsRoutingMode() == ProducerConfiguration::RoundRobinDistribution) {
            routerPolicy_ = boost::make_shared<RoundRobinMessageRouter>(numPartitions);
        } else if (config.getPartitionsRoutingMode() == ProducerConfiguration::UseSinglePartition) {
            routerPolicy_ = boost::make_shared<SinglePartitionMessageRouter>(numPartitions);
        } else {
            routerPolicy_ = config.getMessageRouterPtr();
        }
    }

    PartitionedProducerImpl::~PartitionedProducerImpl() {
    }
    //override
    const std::string& PartitionedProducerImpl::getTopic() const {
        return topic_;
    }

    //override
    void PartitionedProducerImpl::start() {
        boost::shared_ptr<ProducerImpl> producer;
        // create producer per partition
        for (unsigned int i = 0; i < numPartitions_; i++) {
            std::string topicPartitionName = destinationName_->getTopicPartitionName(i);
            producer = boost::make_shared<ProducerImpl>(client_, topicPartitionName, conf_);
            producer->getProducerCreatedFuture().addListener(boost::bind(&PartitionedProducerImpl::handleSinglePartitionProducerCreated,
                                                                         shared_from_this(), _1, _2, i));
            producers_.push_back(producer);
            LOG_DEBUG("Creating Producer for single Partition - " << topicPartitionName);
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
        assert(numProducersCreated_ <= numPartitions_);
        if (result != ResultOk) {
            state_ = Failed;
            lock.unlock();
            closeAsync(closeCallback);
            partitionedProducerCreatedPromise_.setFailed(result);
            LOG_DEBUG("Unable to create Producer for partition - " << partitionIndex << " Error - " << result);
            return;
        }

        assert(partitionIndex <= numPartitions_);
        numProducersCreated_++;
        if(numProducersCreated_ == numPartitions_) {
            lock.unlock();
            partitionedProducerCreatedPromise_.setValue(shared_from_this());
        }
    }

    //override
    void PartitionedProducerImpl::sendAsync(const Message& msg, SendCallback callback) {
        //get partition for this message from router policy
        short partition = (short)(routerPolicy_->getPartition(msg));
        if (partition >= numPartitions_ || partition >= producers_.size()) {
            LOG_ERROR("Got Invalid Partition for message from Router Policy, Partition - " << partition);
            //change me: abort or notify failure in callback?
            //          change to appropriate error if callback
            callback(ResultUnknownError, msg);
            return;
        }
        //find a producer for that partition, index should start from 0
        ProducerImplPtr& producer = producers_[partition];
        msg.impl_->messageId.partition_ = partition;
        //send message on that partition
        producer->sendAsync(msg, callback);
    }

    //override
    void PartitionedProducerImpl::shutdown() {
        setState(Closed);
    }

    void PartitionedProducerImpl::setState(const PartitionedProducerState state) {
        Lock lock(mutex_);
        state_ = state;
        lock.unlock();
    }

    /*
     * if createProducerCallback is set, it means the closeAsync is called from CreateProducer API which failed to create
     * one or many producers for partitions. So, we have to notify with ERROR on createProducerFailure
     */
    void PartitionedProducerImpl::closeAsync(CloseCallback closeCallback) {
        int producerIndex = 0;
        unsigned int producerAlreadyClosed = 0;

        for (ProducerList::const_iterator i = producers_.begin(); i != producers_.end(); i++) {
            ProducerImplPtr prod = *i;
            if(!prod->isClosed()) {
                prod->closeAsync(boost::bind(&PartitionedProducerImpl::handleSinglePartitionProducerClose,
                                             shared_from_this(), _1, producerIndex, closeCallback));
            } else {
                producerAlreadyClosed++;
            }
        }

        /*
         * No need to set state since:-
         * a. If closeAsync before creation then state == Closed, since producers_.size() = producerAlreadyClosed = 0
         * b. If closeAsync called after all sub partitioned producer connected - handleSinglePartitionProducerClose handles the closing
         * c. If closeAsync called due to failure in creating just one sub producer then state is set by handleSinglePartitionProducerCreated
         */
        if (producerAlreadyClosed == producers_.size() && closeCallback) {
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
        assert (partitionIndex < numPartitions_);
        if(numProducersCreated_ > 0) {
            numProducersCreated_--;
        }
        // closed all successfully
        if(!numProducersCreated_) {
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

    //override
    Future<Result, ProducerImplBaseWeakPtr> PartitionedProducerImpl::getProducerCreatedFuture() {
        return partitionedProducerCreatedPromise_.getFuture();
    }

    //override
    bool PartitionedProducerImpl::isClosed() {
        return state_ == Closed;
    }
}
