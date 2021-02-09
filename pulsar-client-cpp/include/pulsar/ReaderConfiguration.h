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
#ifndef PULSAR_READER_CONFIGURATION_H_
#define PULSAR_READER_CONFIGURATION_H_

#include <functional>
#include <memory>
#include <pulsar/defines.h>
#include <pulsar/Result.h>
#include <pulsar/Message.h>
#include <pulsar/Schema.h>
#include <pulsar/CryptoKeyReader.h>
#include <pulsar/ConsumerCryptoFailureAction.h>

namespace pulsar {

class Reader;
class PulsarWrapper;

/// Callback definition for non-data operation
typedef std::function<void(Result result)> ResultCallback;

/// Callback definition for MessageListener
typedef std::function<void(Reader reader, const Message& msg)> ReaderListener;

struct ReaderConfigurationImpl;

/**
 * Class specifying the configuration of a consumer.
 */
class PULSAR_PUBLIC ReaderConfiguration {
   public:
    ReaderConfiguration();
    ~ReaderConfiguration();
    ReaderConfiguration(const ReaderConfiguration&);
    ReaderConfiguration& operator=(const ReaderConfiguration&);

    /**
     * Declare the schema of the data that this reader will be accepting.
     *
     * The schema will be checked against the schema of the topic, and the
     * reader creation will fail if it's not compatible.
     *
     * @param schemaInfo the schema definition object
     */
    ReaderConfiguration& setSchema(const SchemaInfo& schemaInfo);

    /**
     * @return the schema information declared for this consumer
     */
    const SchemaInfo& getSchema() const;

    /**
     * A message listener enables your application to configure how to process
     * messages. A listener will be called in order for every message received.
     */
    ReaderConfiguration& setReaderListener(ReaderListener listener);
    ReaderListener getReaderListener() const;
    bool hasReaderListener() const;

    /**
     * Sets the size of the reader receive queue.
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

    void setReaderName(const std::string& readerName);
    const std::string& getReaderName() const;

    void setSubscriptionRolePrefix(const std::string& subscriptionRolePrefix);
    const std::string& getSubscriptionRolePrefix() const;

    void setReadCompacted(bool compacted);
    bool isReadCompacted() const;

    /**
     * Set the internal subscription name.
     * @param internal subscriptionName
     */
    void setInternalSubscriptionName(std::string internalSubscriptionName);
    const std::string& getInternalSubscriptionName() const;

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

    void setTickDurationInMs(const uint64_t milliSeconds);

    long getTickDurationInMs() const;

    /**
     * Set time window in milliseconds for grouping message ACK requests. An ACK request is not sent
     * to broker until the time window reaches its end, or the number of grouped messages reaches
     * limit. Default is 100 milliseconds. If it's set to a non-positive value, ACK requests will be
     * directly sent to broker without grouping.
     *
     * @param ackGroupMillis time of ACK grouping window in milliseconds.
     */
    void setAckGroupingTimeMs(long ackGroupingMillis);

    /**
     * Get grouping time window in milliseconds.
     *
     * @return grouping time window in milliseconds.
     */
    long getAckGroupingTimeMs() const;

    /**
     * Set max number of grouped messages within one grouping time window. If it's set to a
     * non-positive value, number of grouped messages is not limited. Default is 1000.
     *
     * @param maxGroupingSize max number of grouped messages with in one grouping time window.
     */
    void setAckGroupingMaxSize(long maxGroupingSize);

    /**
     * Get max number of grouped messages within one grouping time window.
     *
     * @return max number of grouped messages within one grouping time window.
     */
    long getAckGroupingMaxSize() const;

    bool isEncryptionEnabled() const;
    const CryptoKeyReaderPtr getCryptoKeyReader() const;
    ReaderConfiguration& setCryptoKeyReader(CryptoKeyReaderPtr cryptoKeyReader);

    ConsumerCryptoFailureAction getCryptoFailureAction() const;
    ReaderConfiguration& setCryptoFailureAction(ConsumerCryptoFailureAction action);

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
    ReaderConfiguration& setProperty(const std::string& name, const std::string& value);

    /**
     * Add all the properties in the provided map
     */
    ReaderConfiguration& setProperties(const std::map<std::string, std::string>& properties);

   private:
    std::shared_ptr<ReaderConfigurationImpl> impl_;
};
}  // namespace pulsar
#endif /* PULSAR_READER_CONFIGURATION_H_ */
