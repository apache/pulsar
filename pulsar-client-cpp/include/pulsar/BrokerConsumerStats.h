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
#ifndef PULSAR_CPP_BROKERCONSUMERSTATS_H
#define PULSAR_CPP_BROKERCONSUMERSTATS_H

#include <pulsar/defines.h>
#include <string.h>
#include <iostream>
#include <pulsar/Result.h>
#include <functional>
#include <memory>
#include <pulsar/ConsumerType.h>

namespace pulsar {
class BrokerConsumerStatsImplBase;
class PulsarWrapper;

/* @note: isValid() or getXXX() methods are not allowed on an invalid BrokerConsumerStats */
class PULSAR_PUBLIC BrokerConsumerStats {
   private:
    std::shared_ptr<BrokerConsumerStatsImplBase> impl_;

   public:
    BrokerConsumerStats() = default;
    explicit BrokerConsumerStats(std::shared_ptr<BrokerConsumerStatsImplBase> impl);

    virtual ~BrokerConsumerStats() = default;

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

    /** @deprecated */
    std::shared_ptr<BrokerConsumerStatsImplBase> getImpl() const;

    friend class PulsarWrapper;
    friend PULSAR_PUBLIC std::ostream &operator<<(std::ostream &os, const BrokerConsumerStats &obj);
};
typedef std::function<void(Result result, BrokerConsumerStats brokerConsumerStats)>
    BrokerConsumerStatsCallback;
}  // namespace pulsar

#endif  // PULSAR_CPP_BROKERCONSUMERSTATS_H
