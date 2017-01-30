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

#include <pulsar/MessageBuilder.h>

#include <boost/make_shared.hpp>

#include "MessageImpl.h"
#include "SharedBuffer.h"
#include "LogUtils.h"
#include "PulsarApi.pb.h"

DECLARE_LOG_OBJECT()

#include "ObjectPool.h"

using namespace pulsar;

namespace pulsar {

ObjectPool<MessageImpl, 100000> messagePool;

MessageBuilder::MessageBuilder() {
    impl_ = messagePool.create();
}

MessageBuilder& MessageBuilder::create() {
    impl_ = messagePool.create();
    return *this;
}

Message MessageBuilder::build() {
    return Message(impl_);
}

void MessageBuilder::checkMetadata() {
    if (!impl_.get()) {
        LOG_FATAL("Cannot reuse the same message builder to build a message");
        abort();
    }
}

MessageBuilder& MessageBuilder::setContent(const void* data, size_t size) {
    checkMetadata();
    impl_->payload = SharedBuffer::copy((char *) data, size);
    return *this;
}

MessageBuilder& MessageBuilder::setAllocatedContent(void* data, size_t size) {
    checkMetadata();
    impl_->payload = SharedBuffer::wrap((char *) data, size);
    return *this;
}

MessageBuilder& MessageBuilder::setContent(const std::string& data) {
    checkMetadata();
    impl_->payload = SharedBuffer::copy((char *) data.c_str(), data.length());
    return *this;
}

MessageBuilder& MessageBuilder::setProperty(const std::string& name, const std::string& value) {
    checkMetadata();
    proto::KeyValue *keyValue = proto::KeyValue().New();
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

}
