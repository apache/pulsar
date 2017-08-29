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
#ifndef LIB_BATCHMESSAGEID_H_
#define LIB_BATCHMESSAGEID_H_

#include <pulsar/MessageId.h>
#include <iosfwd>

#pragma GCC visibility push(default)

namespace pulsar {

class PulsarWrapper;

class BatchMessageId : public MessageId {
 public:
    BatchMessageId(int64_t ledgerId, int64_t entryId, int batchIndex = -1)
            : MessageId(ledgerId, entryId),
              batchIndex_(batchIndex) {
    }

    BatchMessageId(const MessageId& msgId);

    BatchMessageId()
            : batchIndex_(-1) {
    }

    virtual void serialize(std::string& result) const;

    // These functions compare the message order as stored in bookkeeper
    bool operator<(const BatchMessageId& other) const;
    bool operator<=(const BatchMessageId& other) const;
    bool operator==(const BatchMessageId& other) const;

  protected:
    virtual int64_t getBatchIndex() const;

    friend class Commands;
    friend class ConsumerImpl;
    friend class ReaderImpl;
    friend class Message;
    friend class MessageImpl;
    friend class PartitionedProducerImpl;
    friend class PartitionedConsumerImpl;
    friend class BatchAcknowledgementTracker;
    friend class PulsarWrapper;
    friend class PulsarFriend;
    int64_t batchIndex_;

    friend std::ostream& operator<<(std::ostream& s, const BatchMessageId& messageId);
};

}
#pragma GCC visibility pop

#endif /* LIB_BATCHMESSAGEID_H_ */
