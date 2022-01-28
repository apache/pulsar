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
#ifndef PULSAR_MESSAGE_ROUTING_POLICY_HEADER_
#define PULSAR_MESSAGE_ROUTING_POLICY_HEADER_

#include <pulsar/defines.h>
#include <pulsar/DeprecatedException.h>
#include <pulsar/Message.h>
#include <pulsar/TopicMetadata.h>
#include <memory>

/*
 * Implement this interface to define custom policy giving message to
 * partition mapping.
 */
namespace pulsar {

class PULSAR_PUBLIC MessageRoutingPolicy {
   public:
    virtual ~MessageRoutingPolicy() {}

    /** @deprecated
       Use int getPartition(const Message& msg, const TopicMetadata& topicMetadata)
    */
    virtual int getPartition(const Message& msg) {
        throw DeprecatedException(
            "Use int getPartition(const Message& msg,"
            " const TopicMetadata& topicMetadata)");
    }

    /**
     * Choose the partition from the message and topic metadata
     *
     * @param message the Message
     * @param topicMetadata the TopicMetadata that contains the partition number
     * @return the partition number
     */
    virtual int getPartition(const Message& msg, const TopicMetadata& topicMetadata) {
        return getPartition(msg);
    }
};

typedef std::shared_ptr<MessageRoutingPolicy> MessageRoutingPolicyPtr;
}  // namespace pulsar

#endif  // PULSAR_MESSAGE_ROUTING_POLICY_HEADER_
