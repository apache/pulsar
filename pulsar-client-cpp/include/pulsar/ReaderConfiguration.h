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

   private:
    std::shared_ptr<ReaderConfigurationImpl> impl_;
};
}  // namespace pulsar
#endif /* PULSAR_READER_CONFIGURATION_H_ */
