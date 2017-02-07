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

#include "UnAckedMessageTrackerEnabled.h"

DECLARE_LOG_OBJECT();

namespace pulsar {

void UnAckedMessageTrackerEnabled::timeoutHandler(const boost::system::error_code& ec) {
    if (ec) {
        LOG_DEBUG("Ignoring timer cancelled event, code[" << ec <<"]");
    } else {
        timeoutHandler();
    }
}

void UnAckedMessageTrackerEnabled::timeoutHandler() {
    timeoutHandlerHelper();
    ExecutorServicePtr executorService = client_->getIOExecutorProvider()->get();
    timer_ = executorService->createDeadlineTimer();
    timer_->expires_from_now(boost::posix_time::milliseconds(timeoutMs_));
    timer_->async_wait(
            boost::bind(&pulsar::UnAckedMessageTrackerEnabled::timeoutHandler, this,
                        boost::asio::placeholders::error));
}

void UnAckedMessageTrackerEnabled::timeoutHandlerHelper() {
    boost::unique_lock<boost::mutex> acquire(lock_);
    LOG_DEBUG(
            "UnAckedMessageTrackerEnabled::timeoutHandlerHelper invoked for consumerPtr_ " << consumerReference_.getName().c_str());
    if (!oldSet_.empty()) {
        LOG_INFO(
                consumerReference_.getName().c_str() << ": " << oldSet_.size() << " Messages were not acked within "<< timeoutMs_ <<" time");
        oldSet_.clear();
        currentSet_.clear();
        consumerReference_.redeliverUnacknowledgedMessages();
    }
    oldSet_.swap(currentSet_);
}

UnAckedMessageTrackerEnabled::UnAckedMessageTrackerEnabled(long timeoutMs,
                                                           const ClientImplPtr client,
                                                           ConsumerImplBase& consumer)
        : consumerReference_(consumer) {
    timeoutMs_ = timeoutMs;
    client_ = client;
    timeoutHandler();
}

bool UnAckedMessageTrackerEnabled::add(const MessageId& m) {
    boost::unique_lock<boost::mutex> acquire(lock_);
    oldSet_.erase(m);
    return currentSet_.insert(m).second;
}

bool UnAckedMessageTrackerEnabled::isEmpty() {
    boost::unique_lock<boost::mutex> acquire(lock_);
    return oldSet_.empty() && currentSet_.empty();
}

bool UnAckedMessageTrackerEnabled::remove(const MessageId& m) {
    boost::unique_lock<boost::mutex> acquire(lock_);
    return oldSet_.erase(m) || currentSet_.erase(m);
}

long UnAckedMessageTrackerEnabled::size() {
    boost::unique_lock<boost::mutex> acquire(lock_);
    return oldSet_.size() + currentSet_.size();
}

void UnAckedMessageTrackerEnabled::removeMessagesTill(const MessageId& msgId) {
    boost::unique_lock<boost::mutex> acquire(lock_);
    for (std::set<MessageId>::iterator it = oldSet_.begin(); it != oldSet_.end();) {
        if (*it < msgId && it->partition_ == msgId.partition_) {
            oldSet_.erase(it++);
        } else {
            it++;
        }
    }
    for (std::set<MessageId>::iterator it = currentSet_.begin(); it != currentSet_.end();) {
        if (*it < msgId && it->partition_ == msgId.partition_) {
            currentSet_.erase(it++);
        } else {
            it++;
        }
    }
}

UnAckedMessageTrackerEnabled::~UnAckedMessageTrackerEnabled() {
    if (timer_) {
        timer_->cancel();
    }
}
} /* namespace pulsar */
