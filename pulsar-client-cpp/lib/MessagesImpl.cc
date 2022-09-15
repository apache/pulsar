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
#include "MessagesImpl.h"
#include "stdexcept"

MessagesImpl::MessagesImpl(int maxNumberOfMessages, long maxSizeOfMessages)
    : maxNumberOfMessages_(maxNumberOfMessages),
      maxSizeOfMessages_(maxSizeOfMessages),
      currentNumberOfMessages_(0),
      currentSizeOfMessages_(0) {
    messageList_ = std::vector<Message>();
}

const std::vector<Message>& MessagesImpl::getMessageList() const { return messageList_; }

bool MessagesImpl::canAdd(const Message& message) const {
    if (currentNumberOfMessages_ == 0) {
        return true;
    }

    if (maxNumberOfMessages_ > 0 && currentNumberOfMessages_ + 1 > maxNumberOfMessages_) {
        return false;
    }

    if (maxSizeOfMessages_ > 0 && currentSizeOfMessages_ + message.getLength() > maxSizeOfMessages_) {
        return false;
    }

    return true;
}

void MessagesImpl::add(const Message& message) {
    if (!canAdd(message)) {
        throw std::invalid_argument("No more space to add messages.");
    }
    currentNumberOfMessages_++;
    currentSizeOfMessages_ += message.getLength();
    messageList_.emplace_back(message);
}

int MessagesImpl::size() const { return messageList_.size(); }

void MessagesImpl::clear() {
    currentNumberOfMessages_ = 0;
    currentSizeOfMessages_ = 0;
    messageList_.clear();
}
