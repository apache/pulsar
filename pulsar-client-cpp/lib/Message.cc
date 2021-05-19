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
#include <pulsar/defines.h>
#include <pulsar/Message.h>
#include <pulsar/MessageBuilder.h>

#include "PulsarApi.pb.h"

#include "MessageImpl.h"
#include "SharedBuffer.h"

#include <iostream>

using namespace pulsar;

namespace pulsar {

const static std::string emptyString;
const static MessageId invalidMessageId;

const Message::StringMap& Message::getProperties() const { return impl_->properties(); }

bool Message::hasProperty(const std::string& name) const {
    const StringMap& m = impl_->properties();
    return m.find(name) != m.end();
}

const std::string& Message::getProperty(const std::string& name) const {
    if (hasProperty(name)) {
        const StringMap& m = impl_->properties();
        return m.at(name);
    } else {
        return emptyString;
    }
}

const void* Message::getData() const { return impl_->payload.data(); }

std::size_t Message::getLength() const { return impl_->payload.readableBytes(); }

std::string Message::getDataAsString() const { return std::string((const char*)getData(), getLength()); }

Message::Message() : impl_() {}

Message::Message(MessageImplPtr& impl) : impl_(impl) {}

Message::Message(const proto::CommandMessage& msg, proto::MessageMetadata& metadata, SharedBuffer& payload,
                 int32_t partition)
    : impl_(std::make_shared<MessageImpl>()) {
    impl_->messageId =
        MessageId(partition, msg.message_id().ledgerid(), msg.message_id().entryid(), /* batchId */
                  -1);
    impl_->metadata = metadata;
    impl_->payload = payload;
}

Message::Message(const MessageId& messageID, proto::MessageMetadata& metadata, SharedBuffer& payload,
                 proto::SingleMessageMetadata& singleMetadata, const std::string& topicName)
    : impl_(std::make_shared<MessageImpl>()) {
    impl_->messageId = messageID;
    impl_->metadata = metadata;
    impl_->payload = payload;
    impl_->metadata.mutable_properties()->CopyFrom(singleMetadata.properties());
    impl_->topicName_ = &topicName;

    if (singleMetadata.has_partition_key()) {
        impl_->metadata.set_partition_key(singleMetadata.partition_key());
    }

    if (singleMetadata.has_event_time()) {
        impl_->metadata.set_event_time(singleMetadata.event_time());
    }
}

const MessageId& Message::getMessageId() const {
    if (!impl_) {
        return invalidMessageId;
    } else {
        return impl_->messageId;
    }
}

void Message::setMessageId(const MessageId& messageID) const {
    if (impl_) {
        impl_->messageId = messageID;
    }
    return;
}

bool Message::hasPartitionKey() const {
    if (impl_) {
        return impl_->hasPartitionKey();
    }
    return false;
}

const std::string& Message::getPartitionKey() const {
    if (!impl_) {
        return emptyString;
    }
    return impl_->getPartitionKey();
}

bool Message::hasOrderingKey() const {
    if (impl_) {
        return impl_->hasOrderingKey();
    }
    return false;
}

const std::string& Message::getOrderingKey() const {
    if (!impl_) {
        return emptyString;
    }
    return impl_->getOrderingKey();
}

const std::string& Message::getTopicName() const {
    if (!impl_) {
        return emptyString;
    }
    return impl_->getTopicName();
}

const int Message::getRedeliveryCount() const {
    if (!impl_) {
        return 0;
    }
    return impl_->getRedeliveryCount();
}

bool Message::hasSchemaVersion() const {
    if (impl_) {
        return impl_->hasSchemaVersion();
    }
    return false;
}

const std::string& Message::getSchemaVersion() const {
    if (!impl_) {
        return emptyString;
    }
    return impl_->getSchemaVersion();
}

uint64_t Message::getPublishTimestamp() const { return impl_ ? impl_->getPublishTimestamp() : 0ull; }

uint64_t Message::getEventTimestamp() const { return impl_ ? impl_->getEventTimestamp() : 0ull; }

bool Message::operator==(const Message& msg) const { return getMessageId() == msg.getMessageId(); }

PULSAR_PUBLIC std::ostream& operator<<(std::ostream& s, const Message::StringMap& map) {
    // Output at most 10 elements -- appropriate if used for logging.
    s << '{';

    Message::StringMap::const_iterator begin = map.begin();
    Message::StringMap::const_iterator end = map.end();
    for (int i = 0; begin != end && i < 10; ++i, ++begin) {
        if (i > 0) {
            s << ", ";
        }

        s << "'" << begin->first << "':'" << begin->second << "'";
    }

    if (begin != end) {
        s << " ...";
    }

    s << '}';
    return s;
}

PULSAR_PUBLIC std::ostream& operator<<(std::ostream& s, const Message& msg) {
    assert(msg.impl_.get());
    assert(msg.impl_->metadata.has_sequence_id());
    assert(msg.impl_->metadata.has_publish_time());
    s << "Message(prod=" << msg.impl_->metadata.producer_name()
      << ", seq=" << msg.impl_->metadata.sequence_id()
      << ", publish_time=" << msg.impl_->metadata.publish_time() << ", payload_size=" << msg.getLength()
      << ", msg_id=" << msg.getMessageId() << ", props=" << msg.getProperties() << ')';
    return s;
}

}  // namespace pulsar
