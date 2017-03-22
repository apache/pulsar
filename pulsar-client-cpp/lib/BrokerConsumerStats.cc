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

#include <pulsar/BrokerConsumerStats.h>
#include <lib/BrokerConsumerStatsImplBase.h>

namespace pulsar {
    BrokerConsumerStats::BrokerConsumerStats(boost::shared_ptr<BrokerConsumerStatsImplBase> impl) : impl_(impl) {}

    BrokerConsumerStats::BrokerConsumerStats() {}

    boost::shared_ptr<BrokerConsumerStatsImplBase> BrokerConsumerStats::getImpl() const {
        return impl_;
    }

    bool BrokerConsumerStats::isValid() const {
        if (impl_) {
            return impl_->isValid();
        }
        return false;
    }

    std::ostream& operator<<(std::ostream &os, const BrokerConsumerStats& obj) {
        os << "\nBrokerConsumerStats ["
           << "validTill_ = " << obj.isValid()
           << ", msgRateOut_ = " << obj.getMsgRateOut()
           << ", msgThroughputOut_ = " << obj.getMsgThroughputOut()
           << ", msgRateRedeliver_ = " << obj.getMsgRateRedeliver()
           << ", consumerName_ = " << obj.getConsumerName()
           << ", availablePermits_ = " << obj.getAvailablePermits()
           << ", unackedMessages_ = " << obj.getUnackedMessages()
           << ", blockedConsumerOnUnackedMsgs_ = " << obj.isBlockedConsumerOnUnackedMsgs()
           << ", address_ = " << obj.getAddress()
           << ", connectedSince_ = " << obj.getConnectedSince()
           << ", type_ = " << obj.getType()
           << ", msgRateExpired_ = " << obj.getMsgRateExpired()
           << ", msgBacklog_ = " << obj.getMsgBacklog()
           << "]";
        return os;
    }

    double BrokerConsumerStats::getMsgRateOut() const {
        if (impl_) {
            return impl_->getMsgRateOut();
        }
        return 0;
    }

    double BrokerConsumerStats::getMsgThroughputOut() const {
        if (impl_) {
            return impl_->getMsgThroughputOut();
        }
        return 0;
    }

    double BrokerConsumerStats::getMsgRateRedeliver() const {
        if (impl_) {
            return impl_->getMsgRateRedeliver();
        }
        return 0;
    }

    const std::string BrokerConsumerStats::getConsumerName() const {
        if (impl_) {
            return impl_->getConsumerName();
        }
        return "";
    }

    uint64_t BrokerConsumerStats::getAvailablePermits() const {
        if (impl_) {
            return impl_->getAvailablePermits();
        }
        return 0;
    }

    uint64_t BrokerConsumerStats::getUnackedMessages() const {
        if (impl_) {
            return impl_->getUnackedMessages();
        }
        return 0;
    }

    bool BrokerConsumerStats::isBlockedConsumerOnUnackedMsgs() const {
        if (impl_) {
            return impl_->isBlockedConsumerOnUnackedMsgs();
        }
        return false;
    }

    const std::string BrokerConsumerStats::getAddress() const {
        if (impl_) {
            return impl_->getAddress();
        }
        return "";
    }

    const std::string BrokerConsumerStats::getConnectedSince() const {
        if (impl_) {
            return impl_->getConnectedSince();
        }
        return "";
    }

    const ConsumerType BrokerConsumerStats::getType() const {
        if (impl_) {
            return impl_->getType();
        }
        return ConsumerExclusive;
    }

    double BrokerConsumerStats::getMsgRateExpired() const {
        if (impl_) {
            return impl_->getMsgRateExpired();
        }
        return 0;
    }

    uint64_t BrokerConsumerStats::getMsgBacklog() const {
        if (impl_) {
            return impl_->getMsgBacklog();
        }
        return 0;
    }
}