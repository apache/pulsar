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
#ifndef PULSAR_CPP_BROKERCONSUMERSTATSIMPL_H
#define PULSAR_CPP_BROKERCONSUMERSTATSIMPL_H

#include <string.h>
#include <iostream>
#include <pulsar/defines.h>
#include <pulsar/Result.h>
#include <functional>
#include <boost/date_time/microsec_time_clock.hpp>
#include <pulsar/BrokerConsumerStats.h>
#include <lib/BrokerConsumerStatsImplBase.h>

namespace pulsar {
class PULSAR_PUBLIC BrokerConsumerStatsImpl : public BrokerConsumerStatsImplBase {
   private:
    /** validTill_ - Stats will be valid till this time.*/
    boost::posix_time::ptime validTill_;

    /** Total rate of messages delivered to the consumer. msg/s */
    double msgRateOut_;

    /** Total throughput delivered to the consumer. bytes/s */
    double msgThroughputOut_;

    /** Total rate of messages redelivered by this consumer. msg/s */
    double msgRateRedeliver_;

    /** Name of the consumer */
    std::string consumerName_;

    /** Number of available message permits for the consumer */
    uint64_t availablePermits_;

    /** Number of unacknowledged messages for the consumer */
    uint64_t unackedMessages_;

    /** Flag to verify if consumer is blocked due to reaching threshold of unacked messages */
    bool blockedConsumerOnUnackedMsgs_;

    /** Address of this consumer */
    std::string address_;

    /** Timestamp of connection */
    std::string connectedSince_;

    /** Whether this subscription is Exclusive or Shared or Failover */
    ConsumerType type_;

    /** Total rate of messages expired on this subscription. msg/s */
    double msgRateExpired_;

    /** Number of messages in the subscription backlog */
    uint64_t msgBacklog_;

   public:
    BrokerConsumerStatsImpl();

    BrokerConsumerStatsImpl(double msgRateOut, double msgThroughputOut, double msgRateRedeliver,
                            std::string consumerName, uint64_t availablePermits, uint64_t unackedMessages,
                            bool blockedConsumerOnUnackedMsgs, std::string address,
                            std::string connectedSince, const std::string& type, double msgRateExpired,
                            uint64_t msgBacklog);

    /** Returns true if the Stats are still valid **/
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

    void setCacheTime(uint64_t cacehTimeInMs);

    friend std::ostream& operator<<(std::ostream& os, const BrokerConsumerStatsImpl& obj);

    static ConsumerType convertStringToConsumerType(const std::string& str);
};
}  // namespace pulsar

#endif  // PULSAR_CPP_BROKERCONSUMERSTATSIMPL_H
