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
#ifndef MESSAGE_HPP_
#define MESSAGE_HPP_

#include <map>
#include <string>

#include <memory>

#include <pulsar/defines.h>
#include "MessageId.h"

namespace pulsar {
namespace proto {
class CommandMessage;
class MessageMetadata;
class SingleMessageMetadata;
}  // namespace proto

class SharedBuffer;
class MessageBuilder;
class MessageImpl;
class PulsarWrapper;

class PULSAR_PUBLIC Message {
   public:
    typedef std::map<std::string, std::string> StringMap;

    Message();

    /**
     * Return the properties attached to the message.
     * Properties are application defined key/value pairs that will be attached to the message
     *
     * @return an unmodifiable view of the properties map
     */
    const StringMap& getProperties() const;

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
     * Get the content of the message
     *
     *
     * @return the pointer to the message payload
     */
    const void* getData() const;

    /**
     * Get the length of the message
     *
     * @return the length of the message payload
     */
    std::size_t getLength() const;

    /**
     * Get string representation of the message
     *
     * @return the string representation of the message payload
     */
    std::string getDataAsString() const;

    /**
     * Get the unique message ID associated with this message.
     *
     * The message id can be used to univocally refer to a message without having to keep the entire payload
     * in memory.
     *
     * Only messages received from the consumer will have a message id assigned.
     *
     */
    const MessageId& getMessageId() const;

    /**
     * Set the unique message ID.
     *
     */
    void setMessageId(const MessageId& messageId) const;

    /**
     * Get the partition key for this message
     * @return key string that is hashed to determine message's topic partition
     */
    const std::string& getPartitionKey() const;

    /**
     * @return true if the message has a partition key
     */
    bool hasPartitionKey() const;

    /**
     * Get the ordering key of the message
     *
     * @return the ordering key of the message
     */
    const std::string& getOrderingKey() const;

    /**
     * Check whether the message has a ordering key
     *
     * @return true if the ordering key was set while creating the message
     *         false if the ordering key was not set while creating the message
     */
    bool hasOrderingKey() const;

    /**
     * Get the UTC based timestamp in milliseconds referring to when the message was published by the client
     * producer
     */
    uint64_t getPublishTimestamp() const;

    /**
     * Get the event timestamp associated with this message. It is set by the client producer.
     */
    uint64_t getEventTimestamp() const;

    /**
     * Get the topic Name from which this message originated from
     */
    const std::string& getTopicName() const;

    /**
     * Get the redelivery count for this message
     */
    const int getRedeliveryCount() const;

    /**
     * Check if schema version exists
     */
    bool hasSchemaVersion() const;

    /**
     * Get the schema version
     */
    const std::string& getSchemaVersion() const;

    bool operator==(const Message& msg) const;

   private:
    typedef std::shared_ptr<MessageImpl> MessageImplPtr;
    MessageImplPtr impl_;

    Message(MessageImplPtr& impl);
    Message(const proto::CommandMessage& msg, proto::MessageMetadata& data, SharedBuffer& payload,
            int32_t partition);
    /// Used for Batch Messages
    Message(const MessageId& messageId, proto::MessageMetadata& metadata, SharedBuffer& payload,
            proto::SingleMessageMetadata& singleMetadata, const std::string& topicName);
    friend class PartitionedProducerImpl;
    friend class PartitionedConsumerImpl;
    friend class MultiTopicsConsumerImpl;
    friend class MessageBuilder;
    friend class ConsumerImpl;
    friend class ProducerImpl;
    friend class Commands;
    friend class BatchMessageContainerBase;
    friend class BatchAcknowledgementTracker;
    friend class PulsarWrapper;
    friend class MessageBatch;
    friend struct OpSendMsg;

    friend PULSAR_PUBLIC std::ostream& operator<<(std::ostream& s, const StringMap& map);
    friend PULSAR_PUBLIC std::ostream& operator<<(std::ostream& s, const Message& msg);
};
}  // namespace pulsar

#endif /* MESSAGE_HPP_ */
