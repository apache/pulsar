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
#include "PatternMultiTopicsConsumerImpl.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

PatternMultiTopicsConsumerImpl::PatternMultiTopicsConsumerImpl(ClientImplPtr client,
                                                               const std::string pattern,
                                                               const std::vector<std::string>& topics,
                                                               const std::string& subscriptionName,
                                                               const ConsumerConfiguration& conf,
                                                               const LookupServicePtr lookupServicePtr_)
    : MultiTopicsConsumerImpl(client, topics, subscriptionName, TopicName::get(pattern), conf,
                              lookupServicePtr_),
      patternString_(pattern),
      pattern_(std::regex(pattern)),
      autoDiscoveryTimer_(),
      autoDiscoveryRunning_(false) {}

const std::regex PatternMultiTopicsConsumerImpl::getPattern() { return pattern_; }

void PatternMultiTopicsConsumerImpl::resetAutoDiscoveryTimer() {
    autoDiscoveryRunning_ = false;
    autoDiscoveryTimer_->expires_from_now(seconds(conf_.getPatternAutoDiscoveryPeriod()));
    autoDiscoveryTimer_->async_wait(
        std::bind(&PatternMultiTopicsConsumerImpl::autoDiscoveryTimerTask, this, std::placeholders::_1));
}

void PatternMultiTopicsConsumerImpl::autoDiscoveryTimerTask(const boost::system::error_code& err) {
    if (err == boost::asio::error::operation_aborted) {
        LOG_DEBUG(getName() << "Timer cancelled: " << err.message());
        return;
    } else if (err) {
        LOG_ERROR(getName() << "Timer error: " << err.message());
        return;
    }

    if (state_ != Ready) {
        LOG_ERROR("Error in autoDiscoveryTimerTask consumer state not ready: " << state_);
        resetAutoDiscoveryTimer();
        return;
    }

    if (autoDiscoveryRunning_) {
        LOG_DEBUG("autoDiscoveryTimerTask still running, cancel this running. ");
        return;
    }

    autoDiscoveryRunning_ = true;

    // already get namespace from pattern.
    assert(namespaceName_);

    lookupServicePtr_->getTopicsOfNamespaceAsync(namespaceName_)
        .addListener(std::bind(&PatternMultiTopicsConsumerImpl::timerGetTopicsOfNamespace, this,
                               std::placeholders::_1, std::placeholders::_2));
}

void PatternMultiTopicsConsumerImpl::timerGetTopicsOfNamespace(const Result result,
                                                               const NamespaceTopicsPtr topics) {
    if (result != ResultOk) {
        LOG_ERROR("Error in Getting topicsOfNameSpace. result: " << result);
        resetAutoDiscoveryTimer();
        return;
    }

    NamespaceTopicsPtr newTopics = PatternMultiTopicsConsumerImpl::topicsPatternFilter(*topics, pattern_);
    // get old topics in consumer:
    NamespaceTopicsPtr oldTopics = std::make_shared<std::vector<std::string>>();
    for (std::map<std::string, int>::iterator it = topicsPartitions_.begin(); it != topicsPartitions_.end();
         it++) {
        oldTopics->push_back(it->first);
    }
    NamespaceTopicsPtr topicsAdded = topicsListsMinus(*newTopics, *oldTopics);
    NamespaceTopicsPtr topicsRemoved = topicsListsMinus(*oldTopics, *newTopics);

    // callback method when removed topics all un-subscribed.
    ResultCallback topicsRemovedCallback = [this](Result result) {
        if (result != ResultOk) {
            LOG_ERROR("Failed to unsubscribe topics: " << result);
        }
        resetAutoDiscoveryTimer();
    };

    // callback method when added topics all subscribed.
    ResultCallback topicsAddedCallback = [this, topicsRemoved, topicsRemovedCallback](Result result) {
        if (result == ResultOk) {
            // call to unsubscribe all removed topics.
            onTopicsRemoved(topicsRemoved, topicsRemovedCallback);
        } else {
            resetAutoDiscoveryTimer();
        }
    };

    // call to subscribe new added topics, then in its callback do unsubscribe
    onTopicsAdded(topicsAdded, topicsAddedCallback);
}

void PatternMultiTopicsConsumerImpl::onTopicsAdded(NamespaceTopicsPtr addedTopics, ResultCallback callback) {
    // start call subscribeOneTopicAsync for each single topic

    if (addedTopics->empty()) {
        LOG_DEBUG("no topics need subscribe");
        callback(ResultOk);
        return;
    }
    int topicsNumber = addedTopics->size();

    std::shared_ptr<std::atomic<int>> topicsNeedCreate = std::make_shared<std::atomic<int>>(topicsNumber);
    // subscribe for each passed in topic
    for (std::vector<std::string>::const_iterator itr = addedTopics->begin(); itr != addedTopics->end();
         itr++) {
        MultiTopicsConsumerImpl::subscribeOneTopicAsync(*itr).addListener(
            std::bind(&PatternMultiTopicsConsumerImpl::handleOneTopicAdded, this, std::placeholders::_1, *itr,
                      topicsNeedCreate, callback));
    }
}

void PatternMultiTopicsConsumerImpl::handleOneTopicAdded(const Result result, const std::string& topic,
                                                         std::shared_ptr<std::atomic<int>> topicsNeedCreate,
                                                         ResultCallback callback) {
    int previous = topicsNeedCreate->fetch_sub(1);
    assert(previous > 0);

    if (result != ResultOk) {
        LOG_ERROR("Failed when subscribed to topic " << topic << "  Error - " << result);
        callback(result);
        return;
    }

    if (topicsNeedCreate->load() == 0) {
        LOG_DEBUG("Subscribed all new added topics");
        callback(result);
    }
}

void PatternMultiTopicsConsumerImpl::onTopicsRemoved(NamespaceTopicsPtr removedTopics,
                                                     ResultCallback callback) {
    // start call subscribeOneTopicAsync for each single topic
    if (removedTopics->empty()) {
        LOG_DEBUG("no topics need unsubscribe");
        callback(ResultOk);
        return;
    }
    int topicsNumber = removedTopics->size();

    std::shared_ptr<std::atomic<int>> topicsNeedUnsub = std::make_shared<std::atomic<int>>(topicsNumber);
    ResultCallback oneTopicUnsubscribedCallback = [this, topicsNeedUnsub, callback](Result result) {
        int previous = topicsNeedUnsub->fetch_sub(1);
        assert(previous > 0);

        if (result != ResultOk) {
            LOG_ERROR("Failed when unsubscribe to one topic.  Error - " << result);
            callback(result);
            return;
        }

        if (topicsNeedUnsub->load() == 0) {
            LOG_DEBUG("unSubscribed all needed topics");
            callback(result);
        }
    };

    // unsubscribe for each passed in topic
    for (std::vector<std::string>::const_iterator itr = removedTopics->begin(); itr != removedTopics->end();
         itr++) {
        MultiTopicsConsumerImpl::unsubscribeOneTopicAsync(*itr, oneTopicUnsubscribedCallback);
    }
}

NamespaceTopicsPtr PatternMultiTopicsConsumerImpl::topicsPatternFilter(const std::vector<std::string>& topics,
                                                                       const std::regex& pattern) {
    NamespaceTopicsPtr topicsResultPtr = std::make_shared<std::vector<std::string>>();

    for (std::vector<std::string>::const_iterator itr = topics.begin(); itr != topics.end(); itr++) {
        if (std::regex_match(*itr, pattern)) {
            topicsResultPtr->push_back(*itr);
        }
    }
    return topicsResultPtr;
}

NamespaceTopicsPtr PatternMultiTopicsConsumerImpl::topicsListsMinus(std::vector<std::string>& list1,
                                                                    std::vector<std::string>& list2) {
    NamespaceTopicsPtr topicsResultPtr = std::make_shared<std::vector<std::string>>();
    std::remove_copy_if(list1.begin(), list1.end(), std::back_inserter(*topicsResultPtr),
                        [&list2](const std::string& arg) {
                            return (std::find(list2.begin(), list2.end(), arg) != list2.end());
                        });

    return topicsResultPtr;
}

void PatternMultiTopicsConsumerImpl::start() {
    MultiTopicsConsumerImpl::start();

    LOG_DEBUG("PatternMultiTopicsConsumerImpl start autoDiscoveryTimer_.");

    // Init autoDiscoveryTimer task only once, wait for the timeout to happen
    if (!autoDiscoveryTimer_ && conf_.getPatternAutoDiscoveryPeriod() > 0) {
        autoDiscoveryTimer_ = client_->getIOExecutorProvider()->get()->createDeadlineTimer();
        autoDiscoveryTimer_->expires_from_now(seconds(conf_.getPatternAutoDiscoveryPeriod()));
        autoDiscoveryTimer_->async_wait(
            std::bind(&PatternMultiTopicsConsumerImpl::autoDiscoveryTimerTask, this, std::placeholders::_1));
    }
}

void PatternMultiTopicsConsumerImpl::shutdown() {
    Lock lock(mutex_);
    state_ = Closed;
    autoDiscoveryTimer_->cancel();
    multiTopicsConsumerCreatedPromise_.setFailed(ResultAlreadyClosed);
}

void PatternMultiTopicsConsumerImpl::closeAsync(ResultCallback callback) {
    MultiTopicsConsumerImpl::closeAsync(callback);
    autoDiscoveryTimer_->cancel();
}
