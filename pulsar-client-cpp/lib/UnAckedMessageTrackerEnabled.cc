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
    timer_->expires_from_now(boost::posix_time::milliseconds(tickDurationInMs_));
    timer_->async_wait([&](const boost::system::error_code& ec) {
        if (ec) {
            LOG_DEBUG("Ignoring timer cancelled event, code[" << ec << "]");
        } else {
            timeoutHandler();
        }
    });
}

void UnAckedMessageTrackerEnabled::timeoutHandlerHelper() {
    std::unique_lock<std::mutex> acquire(lock_);
    LOG_DEBUG("UnAckedMessageTrackerEnabled::timeoutHandlerHelper invoked for consumerPtr_ "
              << consumerReference_.getName().c_str());

    std::set<MessageId> headPartition = timePartitions.front();
    timePartitions.pop_front();

    std::set<MessageId> msgIdsToRedeliver;
    if (!headPartition.empty()) {
        LOG_INFO(consumerReference_.getName().c_str()
                 << ": " << headPartition.size() << " Messages were not acked within "
                 << timePartitions.size() * tickDurationInMs_ << " time");
        for (auto it = headPartition.begin(); it != headPartition.end(); it++) {
            msgIdsToRedeliver.insert(*it);
            messageIdPartitionMap.erase(*it);
        }
    }
    headPartition.clear();
    timePartitions.push_back(headPartition);

    if (msgIdsToRedeliver.size() > 0) {
        // redeliverUnacknowledgedMessages() may call clear() that acquire the lock again, so we should unlock
        // here to avoid deadlock
        acquire.unlock();
        consumerReference_.redeliverUnacknowledgedMessages(msgIdsToRedeliver);
    }
}

UnAckedMessageTrackerEnabled::UnAckedMessageTrackerEnabled(long timeoutMs, const ClientImplPtr client,
                                                           ConsumerImplBase& consumer)
    : UnAckedMessageTrackerEnabled(timeoutMs, timeoutMs, client, consumer) {}

UnAckedMessageTrackerEnabled::UnAckedMessageTrackerEnabled(long timeoutMs, long tickDurationInMs,
                                                           const ClientImplPtr client,
                                                           ConsumerImplBase& consumer)
    : consumerReference_(consumer),
      client_(client),
      timeoutMs_(timeoutMs),
      tickDurationInMs_(timeoutMs >= tickDurationInMs ? tickDurationInMs : timeoutMs) {
    const int blankPartitions =
        static_cast<int>(std::ceil(static_cast<double>(timeoutMs_) / tickDurationInMs_)) + 1;

    for (int i = 0; i < blankPartitions; i++) {
        std::set<MessageId> msgIds;
        timePartitions.push_back(msgIds);
    }

    timeoutHandler();
}

bool UnAckedMessageTrackerEnabled::add(const MessageId& msgId) {
    std::lock_guard<std::mutex> acquire(lock_);
    MessageId id(msgId.partition(), msgId.ledgerId(), msgId.entryId(), -1);
    if (messageIdPartitionMap.count(id) == 0) {
        std::set<MessageId>& partition = timePartitions.back();
        bool emplace = messageIdPartitionMap.emplace(id, partition).second;
        bool insert = partition.insert(id).second;
        return emplace && insert;
    }
    return false;
}

bool UnAckedMessageTrackerEnabled::isEmpty() {
    std::lock_guard<std::mutex> acquire(lock_);
    return messageIdPartitionMap.empty();
}

bool UnAckedMessageTrackerEnabled::remove(const MessageId& msgId) {
    std::lock_guard<std::mutex> acquire(lock_);
    MessageId id(msgId.partition(), msgId.ledgerId(), msgId.entryId(), -1);
    bool removed = false;

    std::map<MessageId, std::set<MessageId>&>::iterator exist = messageIdPartitionMap.find(id);
    if (exist != messageIdPartitionMap.end()) {
        removed = exist->second.erase(id);
        messageIdPartitionMap.erase(exist);
    }
    return removed;
}

long UnAckedMessageTrackerEnabled::size() {
    std::lock_guard<std::mutex> acquire(lock_);
    return messageIdPartitionMap.size();
}

void UnAckedMessageTrackerEnabled::removeMessagesTill(const MessageId& msgId) {
    std::lock_guard<std::mutex> acquire(lock_);
    for (auto it = messageIdPartitionMap.begin(); it != messageIdPartitionMap.end();) {
        MessageId msgIdInMap = it->first;
        if (msgIdInMap <= msgId) {
            it->second.erase(msgIdInMap);
            messageIdPartitionMap.erase(it++);
        } else {
            it++;
        }
    }
}

// this is only for MultiTopicsConsumerImpl, when un-subscribe a single topic, should remove all it's message.
void UnAckedMessageTrackerEnabled::removeTopicMessage(const std::string& topic) {
    std::lock_guard<std::mutex> acquire(lock_);
    for (auto it = messageIdPartitionMap.begin(); it != messageIdPartitionMap.end();) {
        MessageId msgIdInMap = it->first;
        if (msgIdInMap.getTopicName().compare(topic) == 0) {
            it->second.erase(msgIdInMap);
            messageIdPartitionMap.erase(it++);
        } else {
            it++;
        }
    }
}

void UnAckedMessageTrackerEnabled::clear() {
    std::lock_guard<std::mutex> acquire(lock_);
    messageIdPartitionMap.clear();
    for (auto it = timePartitions.begin(); it != timePartitions.end(); it++) {
        it->clear();
    }
}

UnAckedMessageTrackerEnabled::~UnAckedMessageTrackerEnabled() {
    if (timer_) {
        timer_->cancel();
    }
}
} /* namespace pulsar */
