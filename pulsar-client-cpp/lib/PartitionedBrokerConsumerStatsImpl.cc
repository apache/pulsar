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
#include <lib/PartitionedBrokerConsumerStatsImpl.h>
#include <boost/date_time/local_time/local_time.hpp>
#include <algorithm>
#include <numeric>

namespace pulsar {

const std::string PartitionedBrokerConsumerStatsImpl::DELIMITER = ";";

PartitionedBrokerConsumerStatsImpl::PartitionedBrokerConsumerStatsImpl(size_t size) {
    statsList_.resize(size);
}

bool PartitionedBrokerConsumerStatsImpl::isValid() const {
    bool isValid = true;
    for (int i = 0; i < statsList_.size(); i++) {
        isValid &= statsList_[i].isValid();
    }
    return isValid;
}

std::ostream& operator<<(std::ostream& os, const PartitionedBrokerConsumerStatsImpl& obj) {
    os << "\nPartitionedBrokerConsumerStatsImpl ["
       << "validTill_ = " << obj.isValid() << ", msgRateOut_ = " << obj.getMsgRateOut()
       << ", msgThroughputOut_ = " << obj.getMsgThroughputOut()
       << ", msgRateRedeliver_ = " << obj.getMsgRateRedeliver()
       << ", consumerName_ = " << obj.getConsumerName()
       << ", availablePermits_ = " << obj.getAvailablePermits()
       << ", unackedMessages_ = " << obj.getUnackedMessages()
       << ", blockedConsumerOnUnackedMsgs_ = " << obj.isBlockedConsumerOnUnackedMsgs()
       << ", address_ = " << obj.getAddress() << ", connectedSince_ = " << obj.getConnectedSince()
       << ", type_ = " << obj.getType() << ", msgRateExpired_ = " << obj.getMsgRateExpired()
       << ", msgBacklog_ = " << obj.getMsgBacklog() << "]";
    return os;
}

double PartitionedBrokerConsumerStatsImpl::getMsgRateOut() const {
    double sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getMsgRateOut();
    }
    return sum;
}

double PartitionedBrokerConsumerStatsImpl::getMsgThroughputOut() const {
    double sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getMsgThroughputOut();
    }
    return sum;
}

double PartitionedBrokerConsumerStatsImpl::getMsgRateRedeliver() const {
    double sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getMsgRateRedeliver();
    }
    return sum;
}

const std::string PartitionedBrokerConsumerStatsImpl::getConsumerName() const {
    std::string str;
    for (int i = 0; i < statsList_.size(); i++) {
        str += statsList_[i].getConsumerName() + DELIMITER;
    }
    return str;
}

uint64_t PartitionedBrokerConsumerStatsImpl::getAvailablePermits() const {
    uint64_t sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getAvailablePermits();
    }
    return sum;
}

uint64_t PartitionedBrokerConsumerStatsImpl::getUnackedMessages() const {
    uint64_t sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getUnackedMessages();
    }
    return sum;
}

bool PartitionedBrokerConsumerStatsImpl::isBlockedConsumerOnUnackedMsgs() const {
    if (statsList_.size() == 0) {
        return false;
    }

    bool isValid = true;
    for (int i = 0; i < statsList_.size(); i++) {
        isValid &= statsList_[i].isValid();
    }
    return isValid;
}

const std::string PartitionedBrokerConsumerStatsImpl::getAddress() const {
    std::string str;
    for (int i = 0; i < statsList_.size(); i++) {
        str += statsList_[i].getAddress() + DELIMITER;
    }
    return str;
}

const std::string PartitionedBrokerConsumerStatsImpl::getConnectedSince() const {
    std::string str;
    for (int i = 0; i < statsList_.size(); i++) {
        str += statsList_[i].getConnectedSince() + DELIMITER;
    }
    return str;
}

const ConsumerType PartitionedBrokerConsumerStatsImpl::getType() const {
    if (!statsList_.size()) {
        return ConsumerExclusive;
    }
    return statsList_[0].getType();
}

double PartitionedBrokerConsumerStatsImpl::getMsgRateExpired() const {
    double sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getMsgRateExpired();
    }
    return sum;
}

uint64_t PartitionedBrokerConsumerStatsImpl::getMsgBacklog() const {
    uint64_t sum = 0;
    for (int i = 0; i < statsList_.size(); i++) {
        sum += statsList_[i].getMsgBacklog();
    }
    return sum;
}

BrokerConsumerStats PartitionedBrokerConsumerStatsImpl::getBrokerConsumerStats(int index) {
    return statsList_[index];
}

void PartitionedBrokerConsumerStatsImpl::add(BrokerConsumerStats stats, int index) {
    statsList_[index] = stats;
}

void PartitionedBrokerConsumerStatsImpl::clear() { statsList_.clear(); }
}  // namespace pulsar
