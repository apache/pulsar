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
#ifndef PULSAR_CPP_PARTITIONEDBROKERCONSUMERSTATSIMPL_H
#define PULSAR_CPP_PARTITIONEDBROKERCONSUMERSTATSIMPL_H

#include <string.h>
#include <iostream>
#include <vector>
#include <pulsar/defines.h>
#include <pulsar/Result.h>
#include <functional>
#include <boost/date_time/microsec_time_clock.hpp>
#include <lib/BrokerConsumerStatsImpl.h>

namespace pulsar {
class PULSAR_PUBLIC PartitionedBrokerConsumerStatsImpl : public BrokerConsumerStatsImplBase {
   private:
    std::vector<BrokerConsumerStats> statsList_;
    static const std::string DELIMITER;

   public:
    PartitionedBrokerConsumerStatsImpl(size_t size);

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

    /** Returns the BrokerConsumerStatsImpl at of ith partition */
    BrokerConsumerStats getBrokerConsumerStats(int index);

    void add(BrokerConsumerStats stats, int index);

    void clear();

    friend std::ostream &operator<<(std::ostream &os, const PartitionedBrokerConsumerStatsImpl &obj);
};
typedef std::shared_ptr<PartitionedBrokerConsumerStatsImpl> PartitionedBrokerConsumerStatsPtr;
}  // namespace pulsar
#endif  // PULSAR_CPP_BROKERCONSUMERSTATSIMPL_H
