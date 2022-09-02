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
#ifndef MESSAGES_HPP_
#define MESSAGES_HPP_

#include <pulsar/defines.h>
#include <vector>
#include <memory>

namespace pulsar {

class Message;
class MessagesImpl;

class PULSAR_PUBLIC Messages {
   public:
    Messages();

    /**
     * Get message list.
     *
     * @return message list.
     */
    std::vector<Message> getMessageList() const;

   private:
    typedef std::shared_ptr<MessagesImpl> MessagesImplPtr;
    MessagesImplPtr impl_;
    Messages(MessagesImplPtr msgsPtr);
    friend class ConsumerImpl;
    friend class MultiTopicsConsumerImpl;
};
}  // namespace pulsar

#endif /* MESSAGES_HPP_ */
