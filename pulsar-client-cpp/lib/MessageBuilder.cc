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
#include <pulsar/MessageBuilder.h>

#include <memory>
#include <stdexcept>

#include "MessageImpl.h"
#include "SharedBuffer.h"
#include "LogUtils.h"
#include "PulsarApi.pb.h"

DECLARE_LOG_OBJECT()

#include "ObjectPool.h"
#include "TimeUtils.h"

using namespace pulsar;

namespace pulsar {

ObjectPool<MessageImpl, 100000> messagePool;

std::shared_ptr<MessageImpl> MessageBuilder::createMessageImpl() { return messagePool.create(); }

MessageBuilder::MessageBuilder() { impl_ = createMessageImpl(); }

MessageBuilder& MessageBuilder::create() {
    impl_ = createMessageImpl();
    return *this;
}

Message MessageBuilder::build() { return Message(impl_); }

void MessageBuilder::checkMetadata() {
    if (!impl_.get()) {
        LOG_ERROR("Cannot reuse the same message builder to build a message");
        abort();
    }
}

MessageBuilder& MessageBuilder::setContent(const void* data, size_t size) {
    checkMetadata();
    impl_->payload = SharedBuffer::copy((char*)data, size);
    return *this;
}

MessageBuilder& MessageBuilder::setAllocatedContent(void* data, size_t size) {
    checkMetadata();
    impl_->payload = SharedBuffer::wrap((char*)data, size);
    return *this;
}

MessageBuilder& MessageBuilder::setContent(const std::string& data) {
    checkMetadata();
    impl_->payload = SharedBuffer::copy((char*)data.c_str(), data.length());
    return *this;
}

MessageBuilder& MessageBuilder::setProperty(const std::string& name, const std::string& value) {
    checkMetadata();
    proto::KeyValue* keyValue = proto::KeyValue().New();
    keyValue->set_key(name);
    keyValue->set_value(value);
    impl_->metadata.mutable_properties()->AddAllocated(keyValue);
    return *this;
}

MessageBuilder& MessageBuilder::setProperties(const StringMap& properties) {
    checkMetadata();
    for (StringMap::const_iterator it = properties.begin(); it != properties.end(); it++) {
        setProperty(it->first, it->second);
    }
    return *this;
}

MessageBuilder& MessageBuilder::setPartitionKey(const std::string& partitionKey) {
    checkMetadata();
    impl_->metadata.set_partition_key(partitionKey);
    return *this;
}

MessageBuilder& MessageBuilder::setOrderingKey(const std::string& orderingKey) {
    checkMetadata();
    impl_->metadata.set_ordering_key(orderingKey);
    return *this;
}

MessageBuilder& MessageBuilder::setEventTimestamp(uint64_t eventTimestamp) {
    checkMetadata();
    impl_->metadata.set_event_time(eventTimestamp);
    return *this;
}

MessageBuilder& MessageBuilder::setSequenceId(int64_t sequenceId) {
    if (sequenceId < 0) {
        throw std::invalid_argument("sequenceId needs to be >= 0");
    }
    checkMetadata();
    impl_->metadata.set_sequence_id(sequenceId);
    return *this;
}

MessageBuilder& MessageBuilder::setDeliverAfter(std::chrono::milliseconds delay) {
    return setDeliverAt(TimeUtils::currentTimeMillis() + delay.count());
}

MessageBuilder& MessageBuilder::setDeliverAt(uint64_t deliveryTimestamp) {
    checkMetadata();
    impl_->metadata.set_deliver_at_time(deliveryTimestamp);
    return *this;
}

MessageBuilder& MessageBuilder::setReplicationClusters(const std::vector<std::string>& clusters) {
    checkMetadata();
    google::protobuf::RepeatedPtrField<std::string> r(clusters.begin(), clusters.end());
    r.Swap(impl_->metadata.mutable_replicate_to());
    return *this;
}

MessageBuilder& MessageBuilder::disableReplication(bool flag) {
    checkMetadata();
    google::protobuf::RepeatedPtrField<std::string> r;
    if (flag) {
        r.AddAllocated(new std::string("__local__"));
    }
    r.Swap(impl_->metadata.mutable_replicate_to());
    return *this;
}
}  // namespace pulsar
