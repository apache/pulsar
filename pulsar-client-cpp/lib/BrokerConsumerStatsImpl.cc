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
#include <lib/BrokerConsumerStatsImpl.h>
#include <boost/date_time/local_time/local_time.hpp>

namespace pulsar {
BrokerConsumerStatsImpl::BrokerConsumerStatsImpl()
    : validTill_(boost::posix_time::microsec_clock::universal_time()){};

BrokerConsumerStatsImpl::BrokerConsumerStatsImpl(double msgRateOut, double msgThroughputOut,
                                                 double msgRateRedeliver, std::string consumerName,
                                                 uint64_t availablePermits, uint64_t unackedMessages,
                                                 bool blockedConsumerOnUnackedMsgs, std::string address,
                                                 std::string connectedSince, const std::string& type,
                                                 double msgRateExpired, uint64_t msgBacklog)
    : msgRateOut_(msgRateOut),
      msgThroughputOut_(msgThroughputOut),
      msgRateRedeliver_(msgRateRedeliver),
      consumerName_(consumerName),
      availablePermits_(availablePermits),
      unackedMessages_(unackedMessages),
      blockedConsumerOnUnackedMsgs_(blockedConsumerOnUnackedMsgs),
      address_(address),
      connectedSince_(connectedSince),
      type_(convertStringToConsumerType(type)),
      msgRateExpired_(msgRateExpired),
      msgBacklog_(msgBacklog) {}

bool BrokerConsumerStatsImpl::isValid() const {
    return boost::posix_time::microsec_clock::universal_time() <= validTill_;
}

std::ostream& operator<<(std::ostream& os, const BrokerConsumerStatsImpl& obj) {
    os << "\nBrokerConsumerStatsImpl ["
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

double BrokerConsumerStatsImpl::getMsgRateOut() const { return msgRateOut_; }

double BrokerConsumerStatsImpl::getMsgThroughputOut() const { return msgThroughputOut_; }

double BrokerConsumerStatsImpl::getMsgRateRedeliver() const { return msgRateRedeliver_; }

const std::string BrokerConsumerStatsImpl::getConsumerName() const { return consumerName_; }

uint64_t BrokerConsumerStatsImpl::getAvailablePermits() const { return availablePermits_; }

uint64_t BrokerConsumerStatsImpl::getUnackedMessages() const { return unackedMessages_; }

bool BrokerConsumerStatsImpl::isBlockedConsumerOnUnackedMsgs() const { return blockedConsumerOnUnackedMsgs_; }

const std::string BrokerConsumerStatsImpl::getAddress() const { return address_; }

const std::string BrokerConsumerStatsImpl::getConnectedSince() const { return connectedSince_; }

const ConsumerType BrokerConsumerStatsImpl::getType() const { return type_; }

double BrokerConsumerStatsImpl::getMsgRateExpired() const { return msgRateExpired_; }

uint64_t BrokerConsumerStatsImpl::getMsgBacklog() const { return msgBacklog_; }

void BrokerConsumerStatsImpl::setCacheTime(uint64_t cacehTimeInMs) {
    validTill_ =
        boost::posix_time::microsec_clock::universal_time() + boost::posix_time::milliseconds(cacehTimeInMs);
}

ConsumerType BrokerConsumerStatsImpl::convertStringToConsumerType(const std::string& str) {
    if (str == "ConsumerFailover" || str == "Failover") {
        return ConsumerFailover;
    } else if (str == "ConsumerShared" || str == "Shared") {
        return ConsumerShared;
    } else if (str == "ConsumerKeyShared" || str == "KeyShared") {
        return ConsumerKeyShared;
    } else {
        return ConsumerExclusive;
    }
}
}  // namespace pulsar
