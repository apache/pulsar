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
#include <pulsar/defines.h>
#include <pulsar/BrokerConsumerStats.h>
#include <lib/BrokerConsumerStatsImplBase.h>

namespace pulsar {
BrokerConsumerStats::BrokerConsumerStats(std::shared_ptr<BrokerConsumerStatsImplBase> impl) : impl_(impl) {}

std::shared_ptr<BrokerConsumerStatsImplBase> BrokerConsumerStats::getImpl() const { return impl_; }

bool BrokerConsumerStats::isValid() const { return impl_->isValid(); }

PULSAR_PUBLIC std::ostream& operator<<(std::ostream& os, const BrokerConsumerStats& obj) {
    os << "\nBrokerConsumerStats ["
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

double BrokerConsumerStats::getMsgRateOut() const { return impl_->getMsgRateOut(); }

double BrokerConsumerStats::getMsgThroughputOut() const { return impl_->getMsgThroughputOut(); }

double BrokerConsumerStats::getMsgRateRedeliver() const { return impl_->getMsgRateRedeliver(); }

const std::string BrokerConsumerStats::getConsumerName() const { return impl_->getConsumerName(); }

uint64_t BrokerConsumerStats::getAvailablePermits() const { return impl_->getAvailablePermits(); }

uint64_t BrokerConsumerStats::getUnackedMessages() const { return impl_->getUnackedMessages(); }

bool BrokerConsumerStats::isBlockedConsumerOnUnackedMsgs() const {
    return impl_->isBlockedConsumerOnUnackedMsgs();
}

const std::string BrokerConsumerStats::getAddress() const { return impl_->getAddress(); }

const std::string BrokerConsumerStats::getConnectedSince() const { return impl_->getConnectedSince(); }

const ConsumerType BrokerConsumerStats::getType() const { return impl_->getType(); }

double BrokerConsumerStats::getMsgRateExpired() const { return impl_->getMsgRateExpired(); }

uint64_t BrokerConsumerStats::getMsgBacklog() const { return impl_->getMsgBacklog(); }
}  // namespace pulsar
