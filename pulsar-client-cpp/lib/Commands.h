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

#ifndef LIB_COMMANDS_H_
#define LIB_COMMANDS_H_

#include <pulsar/Auth.h>
#include <pulsar/Message.h>

#include "PulsarApi.pb.h"
#include "SharedBuffer.h"

using namespace pulsar;

namespace pulsar {

typedef boost::shared_ptr<proto::MessageMetadata> MessageMetadataPtr;

/**
 * Construct buffers ready to send for Pulsar client commands.
 *
 * Buffer are already including the 4 byte size at the beginning
 */
class Commands {
 public:

    enum ChecksumType {
        Crc32c,
        None
    };
    enum WireFormatConstant {
        MaxMessageSize = (5 * 1024 * 1024 - (10 * 1024)),
        MaxFrameSize = (5 * 1024 * 1024)
    };

    const static uint16_t magicCrc32c = 0x0e01;
    const static int checksumSize = 4;

    static SharedBuffer newConnect(const AuthenticationPtr& authentication);

    static SharedBuffer newPartitionMetadataRequest(proto::BaseCommand& cmd, const std::string& topic, uint64_t requestId);

    static SharedBuffer newLookup(proto::BaseCommand& cmd, const std::string& topic, const bool authoritative,
                                  uint64_t requestId);

    static PairSharedBuffer newSend(SharedBuffer& headers, proto::BaseCommand& cmd,
                                    uint64_t producerId, uint64_t sequenceId, ChecksumType checksumType, const Message& msg);

    static SharedBuffer newSubscribe(const std::string& topic, const std::string&subscription,
                                     uint64_t consumerId, uint64_t requestId,
                                     proto::CommandSubscribe_SubType subType,
                                     const std::string& consumerName);

    static SharedBuffer newUnsubscribe(uint64_t consumerId, uint64_t requestId);

    static SharedBuffer newProducer(const std::string& topic, uint64_t producerId,
                                    const std::string& producerName, uint64_t requestId);

    static SharedBuffer newAck(uint64_t consumerId, const proto::MessageIdData& messageId,
                               proto::CommandAck_AckType ackType, int validationError);

    static SharedBuffer newFlow(uint64_t consumerId, uint32_t messagePermits);

    static SharedBuffer newCloseProducer(uint64_t producerId, uint64_t requestId);

    static SharedBuffer newCloseConsumer(uint64_t consumerId, uint64_t requestId);

    static SharedBuffer newPing();
    static SharedBuffer newPong();

    static SharedBuffer newRedeliverUnacknowledgedMessages(uint64_t consumerId);

    static std::string messageType(proto::BaseCommand::Type type);

    static void initBatchMessageMetadata(const Message &msg, pulsar::proto::MessageMetadata &batchMetadata);

    static void serializeSingleMessageInBatchWithPayload(const Message &msg, SharedBuffer& batchPayLoad, const unsigned long& maxMessageSizeInBytes);

    static Message deSerializeSingleMessageInBatch(Message& batchedMessage);

 private:
    Commands();

    static SharedBuffer writeMessageWithSize(const proto::BaseCommand& cmd);

};

} /* namespace pulsar */

#endif /* LIB_COMMANDS_H_ */
