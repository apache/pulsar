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
#ifndef PULSAR_CONSUMERCONFIGURATION_H_
#define PULSAR_CONSUMERCONFIGURATION_H_

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <pulsar/Result.h>
#include <pulsar/ConsumerType.h>
#include <pulsar/Message.h>

#pragma GCC visibility push(default)
namespace pulsar {

class Consumer;
class PulsarWrapper;

/// Callback definition for non-data operation
typedef boost::function<void(Result result)> ResultCallback;

/// Callback definition for MessageListener
typedef boost::function<void(Consumer consumer, const Message& msg)> MessageListener;

class ConsumerConfigurationImpl;

/**
 * Class specifying the configuration of a consumer.
 */
class ConsumerConfiguration {
 public:
    ConsumerConfiguration();
    ~ConsumerConfiguration();
    ConsumerConfiguration(const ConsumerConfiguration&);
    ConsumerConfiguration& operator=(const ConsumerConfiguration&);

    /**
     * Specify the consumer type. The consumer type enables
     * specifying the type of subscription. In Exclusive subscription,
     * only a single consumer is allowed to attach to the subscription. Other consumers
     * will get an error message. In Shared subscription, multiple consumers will be
     * able to use the same subscription name and the messages will be dispatched in a
     * round robin fashion. In Failover subscription, a master-slave subscription model
     * allows for multiple consumers to attach to a single subscription, though only one
     * of them will be “master” at a given time. Only the master consumer will receive
     * messages. When the master gets disconnected, one among the slaves will be promoted
     * to master and will start getting messages.
     */
    ConsumerConfiguration& setConsumerType(ConsumerType consumerType);
    ConsumerType getConsumerType() const;

    /**
     * A message listener enables your application to configure how to process
     * and acknowledge messages delivered. A listener will be called in order
     * for every message received.
     */
    ConsumerConfiguration& setMessageListener(MessageListener messageListener);
    MessageListener getMessageListener() const;
    bool hasMessageListener() const;

    /**
     * Sets the size of the consumer receive queue.
     *
     * The consumer receive queue controls how many messages can be accumulated by the Consumer before the
     * application calls receive(). Using a higher value could potentially increase the consumer throughput
     * at the expense of bigger memory utilization.
     *
     * Setting the consumer queue size as zero decreases the throughput of the consumer, by disabling pre-fetching of
     * messages. This approach improves the message distribution on shared subscription, by pushing messages only to
     * the consumers that are ready to process them. Neither receive with timeout nor Partitioned Topics can be
     * used if the consumer queue size is zero. The receive() function call should not be interrupted when
     * the consumer queue size is zero.
     *
     * Default value is 1000 messages and should be good for most use cases.
     *
     * @param size
     *            the new receiver queue size value
     */
    void setReceiverQueueSize(int size);
    int getReceiverQueueSize() const;

    void setConsumerName(const std::string&);
    const std::string& getConsumerName() const;

    /**
     * Set the timeout in milliseconds for unacknowledged messages, the timeout needs to be greater than
     * 10 seconds. An Exception is thrown if the given value is less than 10000 (10 seconds).
     * If a successful acknowledgement is not sent within the timeout all the unacknowledged messages are
     * redelivered.
     * @param timeout in milliseconds
     */
    void setUnAckedMessagesTimeoutMs(const uint64_t milliSeconds);

    /**
     * @return the configured timeout in milliseconds for unacked messages.
     */
    long getUnAckedMessagesTimeoutMs() const;

    /**
     * Set the time duration for which the broker side consumer stats will be cached in the client.
     * @param cacheTimeInMs in milliseconds
     */
    void setBrokerConsumerStatsCacheTimeInMs(const long cacheTimeInMs);

    /**
     * @return the configured timeout in milliseconds caching BrokerConsumerStats.
     */
    long getBrokerConsumerStatsCacheTimeInMs() const;
    friend class PulsarWrapper;

 private:
    boost::shared_ptr<ConsumerConfigurationImpl> impl_;
};

}
#pragma GCC visibility pop
#endif /* PULSAR_CONSUMERCONFIGURATION_H_ */

