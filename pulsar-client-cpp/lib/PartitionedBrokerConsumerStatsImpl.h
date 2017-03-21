//
// Created by Jai Asher on 3/20/17.
//

#ifndef PULSAR_CPP_PARTITIONEDBROKERCONSUMERSTATSIMPL_H
#define PULSAR_CPP_PARTITIONEDBROKERCONSUMERSTATSIMPL_H

#include <string.h>
#include <iostream>
#include <vector>
#include <pulsar/Result.h>
#include <boost/function.hpp>
#include <boost/date_time/microsec_time_clock.hpp>
#include <lib/BrokerConsumerStatsImpl.h>

namespace pulsar {
class PartitionedBrokerConsumerStatsImpl : public BrokerConsumerStats {
 private:
    std::vector<BrokerConsumerStatsImpl> statsList_;
    static const std::string DELIMITER;
 public:

    PartitionedBrokerConsumerStatsImpl(size_t size);

    /** Returns true if the Message is Expired **/
    virtual bool isValid() const;

    /** Returns the rate of messages delivered to the consumer. msg/s */
    virtual double getMsgRateOut() const;

    /** Returns the throughput delivered to the consumer. bytes/s */
    virtual double getMsgThroughputOut() const;

    /** Returns the rate of messages redelivered by this consumer. msg/s */
    virtual double getMsgRateRedeliver() const;

    /** Returns the Name of the consumer */
    virtual const std::string getConsumerName() const;

    /** Returns the Number of available message permits for the consumer */
    virtual uint64_t getAvailablePermits() const;

    /** Returns the Number of unacknowledged messages for the consumer */
    virtual uint64_t getUnackedMessages() const;

    /** Returns true if the consumer is blocked due to unacked messages.  */
    virtual bool isBlockedConsumerOnUnackedMsgs() const;

    /** Returns the Address of this consumer */
    virtual const std::string getAddress() const;

    /** Returns the Timestamp of connection */
    virtual const std::string getConnectedSince() const;

    /** Returns Whether this subscription is Exclusive or Shared or Failover */
    virtual const ConsumerType getType() const;

    /** Returns the rate of messages expired on this subscription. msg/s */
    virtual double getMsgRateExpired() const;

    /** Returns the Number of messages in the subscription backlog */
    virtual uint64_t getMsgBacklog() const;

    /** Returns the BrokerConsumerStatsImpl at of ith partition */
    BrokerConsumerStatsImpl getBrokerConsumerStats(int index);

    void add(BrokerConsumerStatsImpl stats, int index);

    void clear();

    friend std::ostream& operator<<(std::ostream &os, const PartitionedBrokerConsumerStatsImpl &obj);
};
}
#endif //PULSAR_CPP_BROKERCONSUMERSTATSIMPL_H
