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

#include <functional>
#include <memory>
#include <pulsar/defines.h>
#include <pulsar/Result.h>
#include <pulsar/ConsumerType.h>
#include <pulsar/Message.h>
#include <pulsar/Schema.h>
#include <pulsar/ConsumerCryptoFailureAction.h>
#include <pulsar/CryptoKeyReader.h>
#include <pulsar/InitialPosition.h>

namespace pulsar {

class Consumer;
class PulsarWrapper;

/// Callback definition for non-data operation
typedef std::function<void(Result result)> ResultCallback;
typedef std::function<void(Result, const Message& msg)> ReceiveCallback;

/// Callback definition for MessageListener
typedef std::function<void(Consumer consumer, const Message& msg)> MessageListener;

struct ConsumerConfigurationImpl;

/**
 * Class specifying the configuration of a consumer.
 */
class PULSAR_PUBLIC ConsumerConfiguration {
   public:
    ConsumerConfiguration();
    ~ConsumerConfiguration();
    ConsumerConfiguration(const ConsumerConfiguration&);
    ConsumerConfiguration& operator=(const ConsumerConfiguration&);

    /**
     * Declare the schema of the data that this consumer will be accepting.
     *
     * The schema will be checked against the schema of the topic, and the
     * consumer creation will fail if it's not compatible.
     *
     * @param schemaInfo the schema definition object
     */
    ConsumerConfiguration& setSchema(const SchemaInfo& schemaInfo);

    /**
     * @return the schema information declared for this consumer
     */
    const SchemaInfo& getSchema() const;

    /**
     * Specify the consumer type. The consumer type enables
     * specifying the type of subscription. In Exclusive subscription,
     * only a single consumer is allowed to attach to the subscription. Other consumers
     * will get an error message. In Shared subscription, multiple consumers will be
     * able to use the same subscription name and the messages will be dispatched in a
     * round robin fashion. In Failover subscription, a primary-failover subscription model
     * allows for multiple consumers to attach to a single subscription, though only one
     * of them will be “master” at a given time. Only the primary consumer will receive
     * messages. When the primary consumer gets disconnected, one among the failover
     * consumers will be promoted to primary and will start getting messages.
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
     * Setting the consumer queue size as zero decreases the throughput of the consumer, by disabling
     * pre-fetching of
     * messages. This approach improves the message distribution on shared subscription, by pushing messages
     * only to
     * the consumers that are ready to process them. Neither receive with timeout nor Partitioned Topics can
     * be
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

    /**
     * Set the max total receiver queue size across partitons.
     * <p>
     * This setting will be used to reduce the receiver queue size for individual partitions
     * {@link #setReceiverQueueSize(int)} if the total exceeds this value (default: 50000).
     *
     * @param maxTotalReceiverQueueSizeAcrossPartitions
     */
    void setMaxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions);

    /**
     * @return the configured max total receiver queue size across partitions
     */
    int getMaxTotalReceiverQueueSizeAcrossPartitions() const;

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
     * Set the delay to wait before re-delivering messages that have failed to be process.
     * <p>
     * When application uses {@link Consumer#negativeAcknowledge(Message)}, the failed message
     * will be redelivered after a fixed timeout. The default is 1 min.
     *
     * @param redeliveryDelay
     *            redelivery delay for failed messages
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     */
    void setNegativeAckRedeliveryDelayMs(long redeliveryDelayMillis);

    /**
     * Get the configured delay to wait before re-delivering messages that have failed to be process.
     *
     * @return redelivery delay for failed messages
     */
    long getNegativeAckRedeliveryDelayMs() const;

    /**
     * Set the time duration for which the broker side consumer stats will be cached in the client.
     * @param cacheTimeInMs in milliseconds
     */
    void setBrokerConsumerStatsCacheTimeInMs(const long cacheTimeInMs);

    /**
     * @return the configured timeout in milliseconds caching BrokerConsumerStats.
     */
    long getBrokerConsumerStatsCacheTimeInMs() const;

    bool isEncryptionEnabled() const;
    const CryptoKeyReaderPtr getCryptoKeyReader() const;
    ConsumerConfiguration& setCryptoKeyReader(CryptoKeyReaderPtr cryptoKeyReader);

    ConsumerCryptoFailureAction getCryptoFailureAction() const;
    ConsumerConfiguration& setCryptoFailureAction(ConsumerCryptoFailureAction action);

    bool isReadCompacted() const;
    void setReadCompacted(bool compacted);

    /**
     * Set the time duration in minutes, for which the PatternMultiTopicsConsumer will do a pattern auto
     * discovery.
     * The default value is 60 seconds. less than 0 will disable auto discovery.
     *
     * @param periodInSeconds       period in seconds to do an auto discovery
     */
    void setPatternAutoDiscoveryPeriod(int periodInSeconds);
    int getPatternAutoDiscoveryPeriod() const;

    void setSubscriptionInitialPosition(InitialPosition subscriptionInitialPosition);
    InitialPosition getSubscriptionInitialPosition() const;

    /**
     * Check whether the message has a specific property attached.
     *
     * @param name the name of the property to check
     * @return true if the message has the specified property
     * @return false if the property is not defined
     */
    bool hasProperty(const std::string& name) const;

    /**
     * Get the value of a specific property
     *
     * @param name the name of the property
     * @return the value of the property or null if the property was not defined
     */
    const std::string& getProperty(const std::string& name) const;

    /**
     * Get all the properties attached to this producer.
     */
    std::map<std::string, std::string>& getProperties() const;

    /**
     * Sets a new property on a message.
     * @param name   the name of the property
     * @param value  the associated value
     */
    ConsumerConfiguration& setProperty(const std::string& name, const std::string& value);

    /**
     * Add all the properties in the provided map
     */
    ConsumerConfiguration& setProperties(const std::map<std::string, std::string>& properties);

    friend class PulsarWrapper;

   private:
    std::shared_ptr<ConsumerConfigurationImpl> impl_;
};
}  // namespace pulsar
#endif /* PULSAR_CONSUMERCONFIGURATION_H_ */
