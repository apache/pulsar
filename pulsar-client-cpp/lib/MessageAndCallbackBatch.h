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
#ifndef LIB_MESSAGEANDCALLBACK_BATCH_H_
#define LIB_MESSAGEANDCALLBACK_BATCH_H_

#include <atomic>
#include <vector>

#include <pulsar/Message.h>
#include <pulsar/ProducerConfiguration.h>

#include <boost/noncopyable.hpp>

namespace pulsar {

class MessageImpl;
using MessageImplPtr = std::shared_ptr<MessageImpl>;

class MessageAndCallbackBatch : public boost::noncopyable {
   public:
    // Wrapper methods of STL container
    bool empty() const noexcept { return callbacks_.empty(); }
    size_t size() const noexcept { return callbacks_.size(); }

    /**
     * Add a message and the associated send callback to the batch
     *
     * @param message
     * @callback the associated send callback
     */
    void add(const Message& msg, const SendCallback& callback);

    /**
     * Clear the internal stats
     */
    void clear();

    /**
     * Complete all the callbacks with given parameters
     *
     * @param result this batch's send result
     * @param id this batch's message id
     */
    void complete(Result result, const MessageId& id) const;

    /**
     * Create a single callback to trigger all the internal callbacks in order
     * It's used when you want to clear and add new messages and callbacks but current callbacks need to be
     * triggered later.
     *
     * @return the merged send callback
     */
    SendCallback createSendCallback() const;

    const MessageImplPtr& msgImpl() const { return msgImpl_; }
    uint64_t sequenceId() const noexcept { return sequenceId_; }

    uint32_t messagesCount() const { return messagesCount_; }
    uint64_t messagesSize() const { return messagesSize_; }

   private:
    MessageImplPtr msgImpl_;
    std::vector<SendCallback> callbacks_;
    std::atomic<uint64_t> sequenceId_{static_cast<uint64_t>(-1L)};

    uint32_t messagesCount_{0};
    uint64_t messagesSize_{0ull};
};

}  // namespace pulsar

#endif
