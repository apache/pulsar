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
#ifndef MESSAGE_ID_H
#define MESSAGE_ID_H

#include <iosfwd>
#include <stdint.h>
#include <boost/shared_ptr.hpp>

#pragma GCC visibility push(default)

namespace pulsar {

class ConsumerImpl;
class UnAckedMessageTrackerEnabled;
class PulsarWrapper;

class MessageId {
 public:
    MessageId& operator=(const MessageId&);
    MessageId();
    virtual ~MessageId() {}

    /**
     * MessageId representing the "earliest" or "oldest available" message stored in the topic
     */
    static const MessageId& earliest();

    /**
     * MessageId representing the "latest" or "last published" message in the topic
     */
    static const MessageId& latest();

    /**
     * Serialize the message id into a binary string for storing
     */
    virtual void serialize(std::string& result) const;

    /**
     * Deserialize a message id from a binary string
     */
    static boost::shared_ptr<MessageId> deserialize(const std::string& serializedMessageId);

    // These functions compare the message order as stored in bookkeeper
    bool operator<(const MessageId& other) const;
    bool operator==(const MessageId& other) const;

 protected:

    virtual int64_t getBatchIndex() const;
    friend class ConsumerImpl;
    friend class Message;
    friend class MessageImpl;
    friend class Commands;
    friend class BatchMessageId;
    friend class PartitionedProducerImpl;
    friend class PartitionedConsumerImpl;
    friend class UnAckedMessageTrackerEnabled;
    friend class BatchAcknowledgementTracker;
    friend class PulsarWrapper;
    MessageId(int64_t, int64_t);
    friend std::ostream& operator<<(std::ostream& s, const MessageId& messageId);
    int64_t ledgerId_;
    int64_t entryId_ :48;
    short partition_ :16;
};


}

#pragma GCC visibility pop

#endif //MESSAGE_ID_H
