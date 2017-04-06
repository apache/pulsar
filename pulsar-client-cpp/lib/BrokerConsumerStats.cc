//
// Created by Jai Asher on 3/20/17.
//
#include <pulsar/BrokerConsumerStats.h>

namespace pulsar {
    bool BrokerConsumerStats::isValid() const {
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
        return 0;
    }

    double BrokerConsumerStats::getMsgThroughputOut() const {
        return 0;
    }

    double BrokerConsumerStats::getMsgRateRedeliver() const {
        return 0;
    }

    const std::string BrokerConsumerStats::getConsumerName() const {
        return "";
    }

    uint64_t BrokerConsumerStats::getAvailablePermits() const {
        return 0;
    }

    uint64_t BrokerConsumerStats::getUnackedMessages() const {
        return 0;
    }

    bool BrokerConsumerStats::isBlockedConsumerOnUnackedMsgs() const {
        return false;
    }

    const std::string BrokerConsumerStats::getAddress() const {
        return "";
    }

    const std::string BrokerConsumerStats::getConnectedSince() const {
        return "";
    }

    const ConsumerType BrokerConsumerStats::getType() const {
        return ConsumerExclusive;
    }

    double BrokerConsumerStats::getMsgRateExpired() const {
        return 0;
    }

    uint64_t BrokerConsumerStats::getMsgBacklog() const {
        return 0;
    }
}