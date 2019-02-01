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
/*
 * \class BatchMessageContainer
 *
 * \brief This class is a container for holding individual messages being published until they are batched and
 * sent to broker.
 *
 * \note This class is not thread safe.
 */

#ifndef LIB_BATCHMESSAGECONTAINER_H_
#define LIB_BATCHMESSAGECONTAINER_H_
#include <string>
#include <vector>
#include <utility>
#include <pulsar/MessageBuilder.h>
#include "MessageImpl.h"
#include "CompressionCodec.h"
#include "Commands.h"
#include "LogUtils.h"
#include "ObjectPool.h"
#include "ExecutorService.h"
#include <boost/asio.hpp>
#include "ProducerImpl.h"

namespace pulsar {

class BatchMessageContainer {
   public:
    struct MessageContainer {
        MessageContainer(Message message, SendCallback sendCallback)
            : message_(message), sendCallback_(sendCallback) {}
        Message message_;
        SendCallback sendCallback_;
    };
    typedef std::vector<MessageContainer> MessageContainerList;
    typedef std::shared_ptr<MessageContainerList> MessageContainerListPtr;

    BatchMessageContainer(ProducerImpl& producer);

    ~BatchMessageContainer();

    void add(const Message& msg, SendCallback sendCallback, bool disableCheck = false);

    SharedBuffer getBatchedPayload();

    void clear();

    static void batchMessageCallBack(Result r, MessageContainerListPtr messages, FlushCallback callback);

    friend inline std::ostream& operator<<(std::ostream& os,
                                           const BatchMessageContainer& batchMessageContainer);
    friend class ProducerImpl;

   private:
    const CompressionType compressionType_;

    const unsigned int maxAllowedNumMessagesInBatch_;
    const unsigned long maxAllowedMessageBatchSizeInBytes_;
    unsigned long batchSizeInBytes_;

    /// Topic Name is used for creating descriptors in log messages
    const std::string topicName_;

    /// Producer Name is used for creating descriptors in log messages
    std::string producerName_;

    Message::MessageImplPtr impl_;

    // This copy (to vector) is needed since OpSendMsg no long holds the individual message and w/o a
    // container
    // the impl_ Shared Pointer will delete the data.
    MessageContainerListPtr messagesContainerListPtr_;

    ProducerImpl& producer_;

    DeadlineTimerPtr timer_;

    unsigned long numberOfBatchesSent_;

    double averageBatchSize_;

    void compressPayLoad();

    inline bool isEmpty() const;

    inline bool isFull() const;

    inline bool hasSpaceInBatch(const Message& msg) const;

    void startTimer();

    void sendMessage(FlushCallback callback);
};

bool BatchMessageContainer::hasSpaceInBatch(const Message& msg) const {
    return (msg.impl_->payload.readableBytes() + this->batchSizeInBytes_ <=
            this->maxAllowedMessageBatchSizeInBytes_) &&
           (this->messagesContainerListPtr_->size() < this->maxAllowedNumMessagesInBatch_);
}

bool BatchMessageContainer::isEmpty() const { return this->messagesContainerListPtr_->empty(); }

bool BatchMessageContainer::isFull() const {
    return (this->batchSizeInBytes_ >= this->maxAllowedMessageBatchSizeInBytes_ ||
            this->messagesContainerListPtr_->size() >= this->maxAllowedNumMessagesInBatch_);
}

std::ostream& operator<<(std::ostream& os, const BatchMessageContainer& b) {
    os << "{ BatchContainer [size = " << b.messagesContainerListPtr_->size()
       << "] [batchSizeInBytes_ = " << b.batchSizeInBytes_
       << "] [maxAllowedMessageBatchSizeInBytes_ = " << b.maxAllowedMessageBatchSizeInBytes_
       << "] [maxAllowedNumMessagesInBatch_ = " << b.maxAllowedNumMessagesInBatch_
       << "] [topicName = " << b.topicName_ << "] [producerName_ = " << b.producerName_
       << "] [batchSizeInBytes_ = " << b.batchSizeInBytes_
       << "] [numberOfBatchesSent = " << b.numberOfBatchesSent_
       << "] [averageBatchSize = " << b.averageBatchSize_ << "]}";
    return os;
}
}  // namespace pulsar
#endif /* LIB_BATCHMESSAGECONTAINER_H_ */
