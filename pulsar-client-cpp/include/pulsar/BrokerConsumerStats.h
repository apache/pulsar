//
// Created by Jai Asher on 3/20/17.
//

#ifndef PULSAR_CPP_BROKERCONSUMERSTATS_H
#define PULSAR_CPP_BROKERCONSUMERSTATS_H

#include <boost/date_time/posix_time/ptime.hpp>
#include <string.h>
#include <iostream>
#include <pulsar/Result.h>
#include <boost/function.hpp>
#include <pulsar/ConsumerType.h>

namespace pulsar {
class BrokerConsumerStats {
 public:
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

    friend std::ostream& operator<<(std::ostream &os, const BrokerConsumerStats &obj);
};
typedef boost::shared_ptr<BrokerConsumerStats> BrokerConsumerStatsPtr;
typedef boost::function<void(Result result, BrokerConsumerStats& brokerConsumerStats)> BrokerConsumerStatsCallback;

}
#endif //PULSAR_CPP_BROKERCONSUMERSTATS_H
