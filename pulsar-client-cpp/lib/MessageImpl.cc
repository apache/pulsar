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
#include "MessageImpl.h"

namespace pulsar {

MessageImpl::MessageImpl() : metadata(), payload(), messageId(), cnx_(0), topicName_(), redeliveryCount_() {}

const Message::StringMap& MessageImpl::properties() {
    if (properties_.size() == 0) {
        for (int i = 0; i < metadata.properties_size(); i++) {
            const std::string& key = metadata.properties(i).key();
            const std::string& value = metadata.properties(i).value();
            properties_.insert(std::make_pair(key, value));
        }
    }
    return properties_;
}

const std::string& MessageImpl::getPartitionKey() const { return metadata.partition_key(); }

bool MessageImpl::hasPartitionKey() const { return metadata.has_partition_key(); }

const std::string& MessageImpl::getOrderingKey() const { return metadata.ordering_key(); }

bool MessageImpl::hasOrderingKey() const { return metadata.has_ordering_key(); }

uint64_t MessageImpl::getPublishTimestamp() const {
    if (metadata.has_publish_time()) {
        return metadata.publish_time();
    } else {
        return 0ull;
    }
}

uint64_t MessageImpl::getEventTimestamp() const {
    if (metadata.has_event_time()) {
        return metadata.event_time();
    } else {
        return 0ull;
    }
}

void MessageImpl::setReplicationClusters(const std::vector<std::string>& clusters) {
    google::protobuf::RepeatedPtrField<std::string> r(clusters.begin(), clusters.end());
    r.Swap(metadata.mutable_replicate_to());
}

void MessageImpl::disableReplication(bool flag) {
    google::protobuf::RepeatedPtrField<std::string> r;
    if (flag) {
        r.AddAllocated(new std::string("__local__"));
    }
    r.Swap(metadata.mutable_replicate_to());
}

void MessageImpl::setProperty(const std::string& name, const std::string& value) {
    proto::KeyValue* keyValue = proto::KeyValue().New();
    keyValue->set_key(name);
    keyValue->set_value(value);
    metadata.mutable_properties()->AddAllocated(keyValue);
}

void MessageImpl::setPartitionKey(const std::string& partitionKey) {
    metadata.set_partition_key(partitionKey);
}

void MessageImpl::setOrderingKey(const std::string& orderingKey) { metadata.set_ordering_key(orderingKey); }

void MessageImpl::setEventTimestamp(uint64_t eventTimestamp) { metadata.set_event_time(eventTimestamp); }

void MessageImpl::setTopicName(const std::string& topicName) {
    topicName_ = &topicName;
    messageId.setTopicName(topicName);
}

const std::string& MessageImpl::getTopicName() { return *topicName_; }

int MessageImpl::getRedeliveryCount() { return redeliveryCount_; }

void MessageImpl::setRedeliveryCount(int count) { redeliveryCount_ = count; }

bool MessageImpl::hasSchemaVersion() const { return metadata.has_schema_version(); }

void MessageImpl::setSchemaVersion(const std::string& schemaVersion) { schemaVersion_ = &schemaVersion; }

const std::string& MessageImpl::getSchemaVersion() const { return metadata.schema_version(); }

}  // namespace pulsar
