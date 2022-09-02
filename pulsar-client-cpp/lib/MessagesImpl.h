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
#ifndef PULSAR_CPP_MESSAGESIMPL_H
#define PULSAR_CPP_MESSAGESIMPL_H

#include <vector>
#include <pulsar/Message.h>
#include <pulsar/Messages.h>

using namespace pulsar;

namespace pulsar {

class MessagesImpl {
   public:
    MessagesImpl(const int maxNumberOfMessages, const long maxSizeOfMessages);
    const std::vector<Message>& getMessageList() const;
    bool canAdd(const Message& message) const;
    void add(const Message& message);
    int size() const;
    void clear();

   private:
    std::vector<Message> messageList_;
    const int maxNumberOfMessages_;
    const long maxSizeOfMessages_;
    int currentNumberOfMessages_;
    long currentSizeOfMessages_;
};

}  // namespace pulsar
#endif  // PULSAR_CPP_MESSAGESIMPL_H
