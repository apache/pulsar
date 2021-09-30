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
#ifndef PULSAR_CPP_BROKERCONSUMERSTATSIMPLBASE_H
#define PULSAR_CPP_BROKERCONSUMERSTATSIMPLBASE_H

#include <pulsar/BrokerConsumerStats.h>
#include <boost/date_time/posix_time/ptime.hpp>

namespace pulsar {
class BrokerConsumerStatsImplBase {
   public:
    virtual ~BrokerConsumerStatsImplBase() = default;
    /** Returns true if the Stats are still valid **/
    virtual bool isValid() const = 0;

    /** Returns the rate of messages delivered to the consumer. msg/s */
    virtual double getMsgRateOut() const = 0;

    /** Returns the throughput delivered to the consumer. bytes/s */
    virtual double getMsgThroughputOut() const = 0;

    /** Returns the rate of messages redelivered by this consumer. msg/s */
    virtual double getMsgRateRedeliver() const = 0;

    /** Returns the Name of the consumer */
    virtual const std::string getConsumerName() const = 0;

    /** Returns the Number of available message permits for the consumer */
    virtual uint64_t getAvailablePermits() const = 0;

    /** Returns the Number of unacknowledged messages for the consumer */
    virtual uint64_t getUnackedMessages() const = 0;

    /** Returns true if the consumer is blocked due to unacked messages.  */
    virtual bool isBlockedConsumerOnUnackedMsgs() const = 0;

    /** Returns the Address of this consumer */
    virtual const std::string getAddress() const = 0;

    /** Returns the Timestamp of connection */
    virtual const std::string getConnectedSince() const = 0;

    /** Returns Whether this subscription is Exclusive or Shared or Failover */
    virtual const ConsumerType getType() const = 0;

    /** Returns the rate of messages expired on this subscription. msg/s */
    virtual double getMsgRateExpired() const = 0;

    /** Returns the Number of messages in the subscription backlog */
    virtual uint64_t getMsgBacklog() const = 0;
};
typedef std::shared_ptr<BrokerConsumerStatsImplBase> BrokerConsumerStatsImplBasePtr;
}  // namespace pulsar

#endif  // PULSAR_CPP_BROKERCONSUMERSTATSIMPLBASE_H
