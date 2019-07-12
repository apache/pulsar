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
#ifndef _PULSAR_CLIENT_CONNECTION_HEADER_
#define _PULSAR_CLIENT_CONNECTION_HEADER_

#include <pulsar/defines.h>
#include <pulsar/Result.h>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/any.hpp>
#include <mutex>
#include <functional>
#include <string>
#include <vector>
#include <deque>

#include "ExecutorService.h"
#include "Future.h"
#include "PulsarApi.pb.h"
#include <pulsar/Result.h>
#include "SharedBuffer.h"
#include "Backoff.h"
#include "Commands.h"
#include "LookupDataResult.h"
#include "UtilAllocator.h"
#include <pulsar/Client.h>
#include <set>
#include <lib/BrokerConsumerStatsImpl.h>

using namespace pulsar;

namespace pulsar {

class PulsarFriend;

class ExecutorService;

class ClientConnection;
typedef std::shared_ptr<ClientConnection> ClientConnectionPtr;
typedef std::weak_ptr<ClientConnection> ClientConnectionWeakPtr;

class ProducerImpl;
typedef std::shared_ptr<ProducerImpl> ProducerImplPtr;
typedef std::weak_ptr<ProducerImpl> ProducerImplWeakPtr;

class ConsumerImpl;
typedef std::shared_ptr<ConsumerImpl> ConsumerImplPtr;
typedef std::weak_ptr<ConsumerImpl> ConsumerImplWeakPtr;

class LookupDataResult;

struct OpSendMsg;

// Data returned on the request operation. Mostly used on create-producer command
struct ResponseData {
    std::string producerName;
    int64_t lastSequenceId;
    std::string schemaVersion;
};

typedef std::shared_ptr<std::vector<std::string>> NamespaceTopicsPtr;

class PULSAR_PUBLIC ClientConnection : public std::enable_shared_from_this<ClientConnection> {
    enum State
    {
        Pending,
        TcpConnected,
        Ready,
        Disconnected
    };

   public:
    typedef std::shared_ptr<boost::asio::ip::tcp::socket> SocketPtr;
    typedef std::shared_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>> TlsSocketPtr;
    typedef std::shared_ptr<ClientConnection> ConnectionPtr;
    typedef std::function<void(const boost::system::error_code&, ConnectionPtr)> ConnectionListener;
    typedef std::vector<ConnectionListener>::iterator ListenerIterator;

    /*
     *  logicalAddress -  url of the service, for ex. pulsar://localhost:6650
     *  physicalAddress - the address to connect to, it could be different from the logical address if proxy
     * comes into play connected -  set when tcp connection is established
     *
     */
    ClientConnection(const std::string& logicalAddress, const std::string& physicalAddress,
                     ExecutorServicePtr executor, const ClientConfiguration& clientConfiguration,
                     const AuthenticationPtr& authentication);
    ~ClientConnection();

    /*
     * starts tcp connect_async
     * @return future<ConnectionPtr> which is not yet set
     */
    void tcpConnectAsync();

    void close();

    bool isClosed() const;

    Future<Result, ClientConnectionWeakPtr> getConnectFuture();

    Future<Result, ClientConnectionWeakPtr> getCloseFuture();

    void newTopicLookup(const std::string& topicName, bool authoritative, const uint64_t requestId,
                        LookupDataResultPromisePtr promise);

    void newPartitionedMetadataLookup(const std::string& topicName, const uint64_t requestId,
                                      LookupDataResultPromisePtr promise);

    void sendCommand(const SharedBuffer& cmd);
    void sendMessage(const OpSendMsg& opSend);

    void registerProducer(int producerId, ProducerImplPtr producer);
    void registerConsumer(int consumerId, ConsumerImplPtr consumer);

    void removeProducer(int producerId);
    void removeConsumer(int consumerId);

    /**
     * Send a request with a specific Id over the connection. The future will be
     * triggered when the response for this request is received
     */
    Future<Result, ResponseData> sendRequestWithId(SharedBuffer cmd, int requestId);

    const std::string& brokerAddress() const;

    const std::string& cnxString() const;

    int getServerProtocolVersion() const;

    int getMaxMessageSize() const;

    Commands::ChecksumType getChecksumType() const;

    Future<Result, BrokerConsumerStatsImpl> newConsumerStats(uint64_t consumerId, uint64_t requestId);

    Future<Result, MessageId> newGetLastMessageId(uint64_t consumerId, uint64_t requestId);

    Future<Result, NamespaceTopicsPtr> newGetTopicsOfNamespace(const std::string& nsName, uint64_t requestId);

   private:
    struct PendingRequestData {
        Promise<Result, ResponseData> promise;
        DeadlineTimerPtr timer;
    };

    struct LookupRequestData {
        LookupDataResultPromisePtr promise;
        DeadlineTimerPtr timer;
    };

    /*
     * handler for connectAsync
     * creates a ConnectionPtr which has a valid ClientConnection object
     * although not usable at this point, since this is just tcp connection
     * Pulsar - Connect/Connected has yet to happen
     */
    void handleTcpConnected(const boost::system::error_code& err,
                            boost::asio::ip::tcp::resolver::iterator endpointIterator);

    void handleHandshake(const boost::system::error_code& err);

    void handleSentPulsarConnect(const boost::system::error_code& err, const SharedBuffer& buffer);

    void readNextCommand();

    void handleRead(const boost::system::error_code& err, size_t bytesTransferred, uint32_t minReadSize);

    void processIncomingBuffer();
    bool verifyChecksum(SharedBuffer& incomingBuffer_, uint32_t& remainingBytes,
                        proto::BaseCommand& incomingCmd_);

    void handleIncomingCommand();
    void handleIncomingMessage(const proto::CommandMessage& msg, bool isChecksumValid,
                               proto::MessageMetadata& msgMetadata, SharedBuffer& payload);

    void handlePulsarConnected(const proto::CommandConnected& cmdConnected);

    void handleResolve(const boost::system::error_code& err,
                       boost::asio::ip::tcp::resolver::iterator endpointIterator);

    void handleSend(const boost::system::error_code& err, const SharedBuffer& cmd);
    void handleSendPair(const boost::system::error_code& err);
    void sendPendingCommands();
    void newLookup(const SharedBuffer& cmd, const uint64_t requestId, LookupDataResultPromisePtr promise);

    void handleRequestTimeout(const boost::system::error_code& ec, PendingRequestData pendingRequestData);

    void handleLookupTimeout(const boost::system::error_code&, LookupRequestData);

    void handleKeepAliveTimeout();

    template <typename Handler>
    inline AllocHandler<Handler> customAllocReadHandler(Handler h) {
        return AllocHandler<Handler>(readHandlerAllocator_, h);
    }

    template <typename Handler>
    inline AllocHandler<Handler> customAllocWriteHandler(Handler h) {
        return AllocHandler<Handler>(writeHandlerAllocator_, h);
    }

    template <typename ConstBufferSequence, typename WriteHandler>
    inline void asyncWrite(const ConstBufferSequence& buffers, WriteHandler handler) {
        if (tlsSocket_) {
            boost::asio::async_write(*tlsSocket_, buffers, handler);
        } else {
            boost::asio::async_write(*socket_, buffers, handler);
        }
    }

    template <typename MutableBufferSequence, typename ReadHandler>
    inline void asyncReceive(const MutableBufferSequence& buffers, ReadHandler handler) {
        if (tlsSocket_) {
            tlsSocket_->async_read_some(buffers, handler);
        } else {
            socket_->async_receive(buffers, handler);
        }
    }

    State state_;
    TimeDuration operationsTimeout_;
    AuthenticationPtr authentication_;
    int serverProtocolVersion_;
    int maxMessageSize_;

    ExecutorServicePtr executor_;

    TcpResolverPtr resolver_;

    /*
     *  tcp connection socket to the pulsar broker
     */
    SocketPtr socket_;
    TlsSocketPtr tlsSocket_;

    const std::string logicalAddress_;

    /*
     *  stores address of the service, for ex. pulsar://localhost:6650
     */
    const std::string physicalAddress_;

    // Represent both endpoint of the tcp connection. eg: [client:1234 -> server:6650]
    std::string cnxString_;

    /*
     *  indicates if async connection establishment failed
     */
    boost::system::error_code error_;

    SharedBuffer incomingBuffer_;
    proto::BaseCommand incomingCmd_;

    Promise<Result, ClientConnectionWeakPtr> connectPromise_;

    typedef std::map<long, PendingRequestData> PendingRequestsMap;
    PendingRequestsMap pendingRequests_;

    typedef std::map<long, LookupRequestData> PendingLookupRequestsMap;
    PendingLookupRequestsMap pendingLookupRequests_;

    typedef std::map<long, ProducerImplWeakPtr> ProducersMap;
    ProducersMap producers_;

    typedef std::map<long, ConsumerImplWeakPtr> ConsumersMap;
    ConsumersMap consumers_;

    typedef std::map<uint64_t, Promise<Result, BrokerConsumerStatsImpl>> PendingConsumerStatsMap;
    PendingConsumerStatsMap pendingConsumerStatsMap_;

    typedef std::map<long, Promise<Result, MessageId>> PendingGetLastMessageIdRequestsMap;
    PendingGetLastMessageIdRequestsMap pendingGetLastMessageIdRequests_;

    typedef std::map<long, Promise<Result, NamespaceTopicsPtr>> PendingGetNamespaceTopicsMap;
    PendingGetNamespaceTopicsMap pendingGetNamespaceTopicsRequests_;

    std::mutex mutex_;
    typedef std::unique_lock<std::mutex> Lock;

    // Pending buffers to write on the socket
    std::deque<boost::any> pendingWriteBuffers_;
    int pendingWriteOperations_;

    SharedBuffer outgoingBuffer_;
    proto::BaseCommand outgoingCmd_;

    HandlerAllocator readHandlerAllocator_;
    HandlerAllocator writeHandlerAllocator_;

    // Signals whether we're waiting for a response from broker
    bool havePendingPingRequest_;
    DeadlineTimerPtr keepAliveTimer_;
    DeadlineTimerPtr consumerStatsRequestTimer_;

    void handleConsumerStatsTimeout(const boost::system::error_code& ec,
                                    std::vector<uint64_t> consumerStatsRequests);

    void startConsumerStatsTimer(std::vector<uint64_t> consumerStatsRequests);
    uint32_t maxPendingLookupRequest_;
    uint32_t numOfPendingLookupRequest_;
    friend class PulsarFriend;

    bool isTlsAllowInsecureConnection_;
};
}  // namespace pulsar

#endif  //_PULSAR_CLIENT_CONNECTION_HEADER_
