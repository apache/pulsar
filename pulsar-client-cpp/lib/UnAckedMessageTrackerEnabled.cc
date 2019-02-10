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
#include "UnAckedMessageTrackerEnabled.h"

#include <functional>

DECLARE_LOG_OBJECT();

namespace pulsar {

void UnAckedMessageTrackerEnabled::timeoutHandler() {
    timeoutHandlerHelper();
    ExecutorServicePtr executorService = client_->getIOExecutorProvider()->get();
    timer_ = executorService->createDeadlineTimer();
    timer_->expires_from_now(boost::posix_time::milliseconds(timeoutMs_));
    timer_->async_wait([&](const boost::system::error_code& ec) {
        if (ec) {
            LOG_DEBUG("Ignoring timer cancelled event, code[" << ec << "]");
        } else {
            timeoutHandler();
        }
    });
}

void UnAckedMessageTrackerEnabled::timeoutHandlerHelper() {
    std::lock_guard<std::mutex> acquire(lock_);
    LOG_DEBUG("UnAckedMessageTrackerEnabled::timeoutHandlerHelper invoked for consumerPtr_ "
              << consumerReference_.getName().c_str());
    if (!oldSet_.empty()) {
        LOG_INFO(consumerReference_.getName().c_str()
                 << ": " << oldSet_.size() << " Messages were not acked within " << timeoutMs_ << " time");
        oldSet_.clear();
        currentSet_.clear();
        consumerReference_.redeliverUnacknowledgedMessages();
    }
    oldSet_.swap(currentSet_);
}

UnAckedMessageTrackerEnabled::UnAckedMessageTrackerEnabled(long timeoutMs, const ClientImplPtr client,
                                                           ConsumerImplBase& consumer)
    : consumerReference_(consumer) {
    timeoutMs_ = timeoutMs;
    client_ = client;
    timeoutHandler();
}

bool UnAckedMessageTrackerEnabled::add(const MessageId& m) {
    std::lock_guard<std::mutex> acquire(lock_);
    oldSet_.erase(m);
    return currentSet_.insert(m).second;
}

bool UnAckedMessageTrackerEnabled::isEmpty() {
    std::lock_guard<std::mutex> acquire(lock_);
    return oldSet_.empty() && currentSet_.empty();
}

bool UnAckedMessageTrackerEnabled::remove(const MessageId& m) {
    std::lock_guard<std::mutex> acquire(lock_);
    return oldSet_.erase(m) || currentSet_.erase(m);
}

long UnAckedMessageTrackerEnabled::size() {
    std::lock_guard<std::mutex> acquire(lock_);
    return oldSet_.size() + currentSet_.size();
}

void UnAckedMessageTrackerEnabled::removeMessagesTill(const MessageId& msgId) {
    std::lock_guard<std::mutex> acquire(lock_);
    for (std::set<MessageId>::iterator it = oldSet_.begin(); it != oldSet_.end();) {
        if (*it < msgId && it->partition() == msgId.partition()) {
            oldSet_.erase(it++);
        } else {
            it++;
        }
    }
    for (std::set<MessageId>::iterator it = currentSet_.begin(); it != currentSet_.end();) {
        if (*it < msgId && it->partition() == msgId.partition()) {
            currentSet_.erase(it++);
        } else {
            it++;
        }
    }
}

// this is only for MultiTopicsConsumerImpl, when un-subscribe a single topic, should remove all it's message.
void UnAckedMessageTrackerEnabled::removeTopicMessage(const std::string& topic) {
    for (std::set<MessageId>::iterator it = oldSet_.begin(); it != oldSet_.end();) {
        const std::string& topicPartitionName = it->getTopicName();
        if (topicPartitionName.find(topic) != std::string::npos) {
            oldSet_.erase(it++);
        } else {
            it++;
        }
    }
    for (std::set<MessageId>::iterator it = currentSet_.begin(); it != currentSet_.end();) {
        const std::string& topicPartitionName = it->getTopicName();
        if (topicPartitionName.find(topic) != std::string::npos) {
            currentSet_.erase(it++);
        } else {
            it++;
        }
    }
}

void UnAckedMessageTrackerEnabled::clear() {
    currentSet_.clear();
    oldSet_.clear();
}

UnAckedMessageTrackerEnabled::~UnAckedMessageTrackerEnabled() {
    if (timer_) {
        timer_->cancel();
    }
}
} /* namespace pulsar */
