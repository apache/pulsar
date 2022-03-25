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
#ifndef MESSAGE_BUILDER_H
#define MESSAGE_BUILDER_H

#include <chrono>
#include <string>
#include <vector>

#include <pulsar/Message.h>
#include <pulsar/defines.h>

namespace pulsar {
class PulsarWrapper;

class PULSAR_PUBLIC MessageBuilder {
   public:
    MessageBuilder();

    typedef std::map<std::string, std::string> StringMap;

    /**
     * Finalize the immutable message
     */
    Message build();

    /**
     * Set content of the message. The given data is copied into message.
     */
    MessageBuilder& setContent(const void* data, size_t size);

    /**
     * Set the content of the message
     *
     * @param data the content of the message.
     * @see setContent(const void*, size_t)
     */
    MessageBuilder& setContent(const std::string& data);

    /**
     * Set the content of the message
     *
     * @param data the content of the message. The given data is moved into message.
     */
    MessageBuilder& setContent(std::string&& data);

    /**
     * Set content of the message to a buffer already allocated by the caller. No copies of
     * this buffer will be made. The caller is responsible to ensure the memory buffer is
     * valid until the message has been persisted (or an error is returned).
     */
    MessageBuilder& setAllocatedContent(void* data, size_t size);

    /**
     * Sets a new property on a message.
     * @param name   the name of the property
     * @param value  the associated value
     */
    MessageBuilder& setProperty(const std::string& name, const std::string& value);

    /**
     * Add all the properties in the provided map
     */
    MessageBuilder& setProperties(const StringMap& properties);

    /**
     * set partition key for message routing and topic compaction
     * @param hash of this key is used to determine message's topic partition
     */
    MessageBuilder& setPartitionKey(const std::string& partitionKey);

    /**
     * set ordering key used for key_shared subscriptions
     * @param the ordering key for the message
     */
    MessageBuilder& setOrderingKey(const std::string& orderingKey);

    /**
     * Specify a delay for the delivery of the messages.
     *
     * @param delay the delay in milliseconds
     */
    MessageBuilder& setDeliverAfter(const std::chrono::milliseconds delay);

    /**
     * Specify the this message should not be delivered earlier than the
     * specified timestamp.
     *
     * @param deliveryTimestamp UTC based timestamp in milliseconds
     */
    MessageBuilder& setDeliverAt(uint64_t deliveryTimestamp);

    /**
     * Set the event timestamp for the message.
     */
    MessageBuilder& setEventTimestamp(uint64_t eventTimestamp);

    /**
     * Specify a custom sequence id for the message being published.
     * <p>
     * The sequence id can be used for deduplication purposes and it needs to follow these rules:
     * <ol>
     * <li><code>sequenceId >= 0</code>
     * <li>Sequence id for a message needs to be greater than sequence id for earlier messages:
     * <code>sequenceId(N+1) > sequenceId(N)</code>
     * <li>It's not necessary for sequence ids to be consecutive. There can be holes between
     * messages. Eg. the <code>sequenceId</code> could represent an offset or a cumulative size.
     * </ol>
     *
     * @param sequenceId
     *            the sequence id to assign to the current message
     * @since 1.20.0
     */
    MessageBuilder& setSequenceId(int64_t sequenceId);

    /**
     * override namespace replication clusters.  note that it is the
     * caller's responsibility to provide valid cluster names, and that
     * all clusters have been previously configured as topics.
     *
     * given an empty list, the message will replicate per the namespace
     * configuration.
     *
     * @param clusters where to send this message.
     */
    MessageBuilder& setReplicationClusters(const std::vector<std::string>& clusters);

    /**
     * Do not replicate this message
     * @param flag if true, disable replication, otherwise use default
     * replication
     */
    MessageBuilder& disableReplication(bool flag);

    /**
     * create a empty message, with no properties or data
     *
     */
    MessageBuilder& create();

   private:
    MessageBuilder(const MessageBuilder&);
    void checkMetadata();
    static std::shared_ptr<MessageImpl> createMessageImpl();
    Message::MessageImplPtr impl_;

    friend class PulsarWrapper;
};
}  // namespace pulsar

#endif
