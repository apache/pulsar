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
#ifndef LIB_PRODUCERIMPL_H_
#define LIB_PRODUCERIMPL_H_

#include <mutex>
#include <boost/date_time/posix_time/ptime.hpp>

#include "ClientImpl.h"
#include "BlockingQueue.h"
#include "HandlerBase.h"
#include "SharedBuffer.h"
#include "CompressionCodec.h"
#include "MessageCrypto.h"
#include "stats/ProducerStatsDisabled.h"
#include "stats/ProducerStatsImpl.h"

using namespace pulsar;

namespace pulsar {
typedef bool bool_type;

class BatchMessageContainer;

typedef std::shared_ptr<BatchMessageContainer> BatchMessageContainerPtr;
typedef std::shared_ptr<MessageCrypto> MessageCryptoPtr;

class PulsarFriend;

struct OpSendMsg {
    Message msg_;
    SendCallback sendCallback_;
    uint64_t producerId_;
    uint64_t sequenceId_;
    boost::posix_time::ptime timeout_;

    OpSendMsg();
    OpSendMsg(uint64_t producerId, uint64_t sequenceId, const Message& msg, const SendCallback& sendCallback,
              const ProducerConfiguration& conf);
};

class ProducerImpl : public HandlerBase,
                     public std::enable_shared_from_this<ProducerImpl>,
                     public ProducerImplBase {
   public:
    ProducerImpl(ClientImplPtr client, const std::string& topic,
                 const ProducerConfiguration& producerConfiguration);
    ~ProducerImpl();

    int keepMaxMessageSize_;

    virtual const std::string& getTopic() const;

    virtual void sendAsync(const Message& msg, SendCallback callback);

    virtual void closeAsync(CloseCallback callback);

    virtual Future<Result, ProducerImplBaseWeakPtr> getProducerCreatedFuture();

    bool removeCorruptMessage(uint64_t sequenceId);

    bool ackReceived(uint64_t sequenceId);

    virtual void disconnectProducer();

    const std::string& getProducerName() const;

    int64_t getLastSequenceId() const;

    const std::string& getSchemaVersion() const;

    uint64_t getProducerId() const;

    virtual void start();

    virtual void shutdown();

    virtual bool isClosed();

    virtual void triggerFlush();

    virtual void flushAsync(FlushCallback callback);

   protected:
    ProducerStatsBasePtr producerStatsBasePtr_;

    typedef BlockingQueue<OpSendMsg> MessageQueue;

    void setMessageMetadata(const Message& msg, const uint64_t& sequenceId, const uint32_t& uncompressedSize);

    void sendMessage(const Message& msg, SendCallback callback);

    void batchMessageTimeoutHandler(const boost::system::error_code& ec);

    friend class PulsarFriend;

    friend class BatchMessageContainer;

    virtual void connectionOpened(const ClientConnectionPtr& connection);
    virtual void connectionFailed(Result result);

    virtual HandlerBaseWeakPtr get_weak_from_this() { return shared_from_this(); }

    const std::string& getName() const;

   private:
    void printStats();

    void handleCreateProducer(const ClientConnectionPtr& cnx, Result result,
                              const ResponseData& responseData);

    void statsCallBackHandler(Result, const Message&, SendCallback, boost::posix_time::ptime);

    void handleClose(Result result, ResultCallback callback);

    void resendMessages(ClientConnectionPtr cnx);

    void refreshEncryptionKey(const boost::system::error_code& ec);
    bool encryptMessage(proto::MessageMetadata& metadata, SharedBuffer& payload,
                        SharedBuffer& encryptedPayload);

    typedef std::unique_lock<std::mutex> Lock;

    ProducerConfiguration conf_;

    ExecutorServicePtr executor_;

    MessageQueue pendingMessagesQueue_;

    std::string producerName_;
    std::string producerStr_;
    uint64_t producerId_;
    int64_t msgSequenceGenerator_;
    proto::BaseCommand cmd_;
    BatchMessageContainerPtr batchMessageContainer;

    volatile int64_t lastSequenceIdPublished_;
    std::string schemaVersion_;

    typedef std::shared_ptr<boost::asio::deadline_timer> TimerPtr;
    TimerPtr sendTimer_;
    void handleSendTimeout(const boost::system::error_code& err);

    Promise<Result, ProducerImplBaseWeakPtr> producerCreatedPromise_;

    void failPendingMessages(Result result);

    MessageCryptoPtr msgCrypto_;
    DeadlineTimerPtr dataKeyGenTImer_;
    uint32_t dataKeyGenIntervalSec_;
    std::shared_ptr<Promise<Result, bool_type>> flushPromise_;
};

struct ProducerImplCmp {
    bool operator()(const ProducerImplPtr& a, const ProducerImplPtr& b) const;
};

} /* namespace pulsar */

#endif /* LIB_PRODUCERIMPL_H_ */
