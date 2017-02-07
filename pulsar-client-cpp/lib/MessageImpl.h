/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LIB_MESSAGEIMPL_H_
#define LIB_MESSAGEIMPL_H_

#include <pulsar/Message.h>
#include <pulsar/MessageId.h>
#include "pulsar/BatchMessageId.h"
#include "SharedBuffer.h"
#include "PulsarApi.pb.h"

#include <boost/scoped_ptr.hpp>

using namespace pulsar;
namespace pulsar {

class ClientConnection;
class BatchMessageContainer;

class MessageImpl {
 public:
    MessageImpl();

    const Message::StringMap& properties();

    proto::MessageMetadata metadata;
    SharedBuffer payload;
    BatchMessageId messageId;
    ClientConnection* cnx_;

    const std::string& getPartitionKey() const;
    bool hasPartitionKey() const;

    uint64_t getPublishTimestamp() const;
 private:
    Message::StringMap properties_;
};

}

#endif /* LIB_MESSAGEIMPL_H_ */
