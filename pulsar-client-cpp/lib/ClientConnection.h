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

#ifndef _PULSAR_CLIENT_CONNECTION_HEADER_
#define _PULSAR_CLIENT_CONNECTION_HEADER_

#include <pulsar/Result.h>

#include <boost/asio.hpp>
#include <boost/any.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/function.hpp>
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

using namespace pulsar;

namespace pulsar {

class PulsarFriend;

class ExecutorService;

class ClientConnection;
typedef boost::shared_ptr<ClientConnection> ClientConnectionPtr;
typedef boost::weak_ptr<ClientConnection> ClientConnectionWeakPtr;

class ProducerImpl;
typedef boost::shared_ptr<ProducerImpl> ProducerImplPtr;
typedef boost::weak_ptr<ProducerImpl> ProducerImplWeakPtr;

class ConsumerImpl;
typedef boost::shared_ptr<ConsumerImpl> ConsumerImplPtr;
typedef boost::weak_ptr<ConsumerImpl> ConsumerImplWeakPtr;

class LookupDataResult;

struct OpSendMsg;

class ClientConnection : public boost::enable_shared_from_this<ClientConnection> {
    enum State {
        Pending,
        TcpConnected,
        Ready,
        Disconnected
    };

 public:
    typedef boost::shared_ptr<boost::asio::ip::tcp::socket> SocketPtr;
    typedef boost::shared_ptr<ClientConnection> ConnectionPtr;
    typedef boost::function<void(const boost::system::error_code&, ConnectionPtr)> ConnectionListener;
    typedef std::vector<ConnectionListener>::iterator ListenerIterator;

    /*
     *  endpoint  -  url of the service, for ex. pulsar://bm92.corp.yahoo.com:8880
     *  connected -  set when tcp connection is established
     *
     */
    ClientConnection(const std::string& endpoint, ExecutorServicePtr executor,
                     const ClientConfiguration& clientConfiguration, const AuthenticationPtr& authentication);
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

    void newTopicLookup(const std::string& destinationName, bool authoritative, const uint64_t requestId,
                   LookupDataResultPromisePtr promise);

    void newPartitionedMetadataLookup(const std::string& destinationName, const uint64_t requestId,
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
    Future<Result, std::string> sendRequestWithId(SharedBuffer cmd, int requestId);

    const std::string& brokerAddress() const;

    const std::string& cnxString() const;

    int getServerProtocolVersion() const;

    Commands::ChecksumType getChecksumType() const;

 private:

    struct PendingRequestData {
        Promise<Result, std::string> promise;
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

    void handleSentPulsarConnect(const boost::system::error_code& err, const SharedBuffer& buffer);

    void readNextCommand();

    void handleRead(const boost::system::error_code& err, size_t bytesTransferred, uint32_t minReadSize);

    void processIncomingBuffer();
    bool verifyChecksum(SharedBuffer& incomingBuffer_, proto::BaseCommand& incomingCmd_);

    void handleIncomingCommand();
    void handleIncomingMessage(const proto::CommandMessage& msg, bool isChecksumValid,
                               proto::MessageMetadata& msgMetadata, SharedBuffer& payload);

    void handlePulsarConnected(const proto::CommandConnected& cmdConnected);

    void handleResolve(const boost::system::error_code& err,
                       boost::asio::ip::tcp::resolver::iterator endpointIterator);

    void handleSend(const boost::system::error_code& err, const SharedBuffer& cmd);
    void handleSendPair(const boost::system::error_code& err);
    void sendPendingCommands();
    void newLookup(const SharedBuffer& cmd, const uint64_t requestId,
                   LookupDataResultPromisePtr promise);


    void handleRequestTimeout(const boost::system::error_code& ec, PendingRequestData pendingRequestData);

    void handleKeepAliveTimeout();

    template<typename Handler>
    inline AllocHandler<Handler> customAllocReadHandler(Handler h) {
        return AllocHandler<Handler>(readHandlerAllocator_, h);
    }

    template<typename Handler>
    inline AllocHandler<Handler> customAllocWriteHandler(Handler h) {
        return AllocHandler<Handler>(writeHandlerAllocator_, h);
    }

    State state_;
    TimeDuration operationsTimeout_;
    AuthenticationPtr authentication_;
    int serverProtocolVersion_;

    ExecutorServicePtr executor_;

    TcpResolverPtr resolver_;

    /*
     *  tcp connection socket to the pulsar broker
     */
    SocketPtr socket_;
    /*
     *  stores address of the service, for ex. pulsar://pulsar.corp.yahoo.com:8880
     */
    const std::string address_;

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

    typedef std::map<long, LookupDataResultPromisePtr> PendingLookupRequestsMap;
    PendingLookupRequestsMap pendingLookupRequests_;

    typedef std::map<long, ProducerImplWeakPtr> ProducersMap;
    ProducersMap producers_;

    typedef std::map<long, ConsumerImplWeakPtr> ConsumersMap;
    ConsumersMap consumers_;

    boost::mutex mutex_;
    typedef boost::unique_lock<boost::mutex> Lock;

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

    friend class PulsarFriend;
};

}

#endif//_PULSAR_CLIENT_CONNECTION_HEADER_
