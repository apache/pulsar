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
#ifndef LIB_MESSAGEIMPL_H_
#define LIB_MESSAGEIMPL_H_

#include <pulsar/Message.h>
#include <pulsar/MessageId.h>
#include "SharedBuffer.h"
#include "PulsarApi.pb.h"

using namespace pulsar;
namespace pulsar {

class PulsarWrapper;
class ClientConnection;
class BatchMessageContainer;

class MessageImpl {
   public:
    MessageImpl();

    const Message::StringMap& properties();

    proto::MessageMetadata metadata;
    SharedBuffer payload;
    MessageId messageId;
    ClientConnection* cnx_;
    const std::string* topicName_;

    const std::string& getPartitionKey() const;
    bool hasPartitionKey() const;

    const std::string& getOrderingKey() const;
    bool hasOrderingKey() const;

    uint64_t getPublishTimestamp() const;
    uint64_t getEventTimestamp() const;

    /**
     * Get the topic Name from which this message originated from
     */
    const std::string& getTopicName();

    /**
     * Set a valid topicName
     */
    void setTopicName(const std::string& topicName);

    friend class PulsarWrapper;
    friend class MessageBuilder;

   private:
    void setReplicationClusters(const std::vector<std::string>& clusters);
    void setProperty(const std::string& name, const std::string& value);
    void disableReplication(bool flag);
    void setPartitionKey(const std::string& partitionKey);
    void setOrderingKey(const std::string& orderingKey);
    void setEventTimestamp(uint64_t eventTimestamp);
    Message::StringMap properties_;
};
}  // namespace pulsar

#endif /* LIB_MESSAGEIMPL_H_ */
