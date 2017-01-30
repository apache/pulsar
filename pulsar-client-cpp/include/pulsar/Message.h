/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MESSAGE_HPP_
#define MESSAGE_HPP_

#include <map>
#include <string>

#include <boost/shared_ptr.hpp>
#include "BatchMessageId.h"

#pragma GCC visibility push(default)

namespace pulsar {
namespace proto {
class CommandMessage;
class MessageMetadata;
class SingleMessageMetadata;
}
}

using namespace pulsar;

namespace pulsar {

class SharedBuffer;
class MessageBuilder;
class MessageImpl;

class Message {
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
     * The message id can be used to univocally refer to a message without having to keep the entire payload in memory.
     *
     * Only messages received from the consumer will have a message id assigned.
     *
     */
    const MessageId& getMessageId() const;

    /**
     * Get the partition key for this message
     * @return key string that is hashed to determine message's destination partition
     */
    const std::string& getPartitionKey() const;
    bool hasPartitionKey() const;

    /**
     * Get the UTC based timestamp in milliseconds referring to when the message was published by the client producer
     */
    uint64_t getPublishTimestamp() const;

  private:
    typedef boost::shared_ptr<MessageImpl> MessageImplPtr;
    MessageImplPtr impl_;

    Message(MessageImplPtr& impl);
    Message(const proto::CommandMessage& msg, proto::MessageMetadata& data, SharedBuffer& payload);
    /// Used for Batch Messages
    Message(const BatchMessageId& messageID, proto::MessageMetadata& metadata, SharedBuffer& payload, proto::SingleMessageMetadata& singleMetadata);
    friend class PartitionedProducerImpl;
    friend class PartitionedConsumerImpl;
    friend class MessageBuilder;
    friend class ConsumerImpl;
    friend class ProducerImpl;
    friend class Commands;
    friend class BatchMessageContainer;
    friend class BatchAcknowledgementTracker;

    friend std::ostream& operator<<(std::ostream& s, const StringMap& map);
    friend std::ostream& operator<<(std::ostream& s, const Message& msg);
};

}

#pragma GCC visibility pop
#endif /* MESSAGE_HPP_ */
