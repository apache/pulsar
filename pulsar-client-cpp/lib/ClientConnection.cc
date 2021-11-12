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
#include "ClientConnection.h"

#include "PulsarApi.pb.h"

#include <iostream>
#include <algorithm>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <climits>

#include "ExecutorService.h"
#include "Commands.h"
#include "LogUtils.h"
#include "Url.h"

#include <functional>
#include <string>

#include "ProducerImpl.h"
#include "ConsumerImpl.h"
#include "checksum/ChecksumProvider.h"

DECLARE_LOG_OBJECT()

using namespace pulsar::proto;
using namespace boost::asio::ip;

namespace pulsar {

static const uint32_t DefaultBufferSize = 64 * 1024;

static const int KeepAliveIntervalInSeconds = 30;

// Convert error codes from protobuf to client API Result
static Result getResult(ServerError serverError) {
    switch (serverError) {
        case UnknownError:
            return ResultUnknownError;

        case MetadataError:
            return ResultBrokerMetadataError;

        case ChecksumError:
            return ResultChecksumError;

        case PersistenceError:
            return ResultBrokerPersistenceError;

        case AuthenticationError:
            return ResultAuthenticationError;

        case AuthorizationError:
            return ResultAuthorizationError;

        case ConsumerBusy:
            return ResultConsumerBusy;

        case ServiceNotReady:
            return ResultServiceUnitNotReady;

        case ProducerBlockedQuotaExceededError:
            return ResultProducerBlockedQuotaExceededError;

        case ProducerBlockedQuotaExceededException:
            return ResultProducerBlockedQuotaExceededException;

        case TopicNotFound:
            return ResultTopicNotFound;

        case SubscriptionNotFound:
            return ResultSubscriptionNotFound;

        case ConsumerNotFound:
            return ResultConsumerNotFound;

        case UnsupportedVersionError:
            return ResultUnsupportedVersionError;

        case TooManyRequests:
            return ResultTooManyLookupRequestException;

        case TopicTerminatedError:
            return ResultTopicTerminated;

        case ProducerBusy:
            return ResultProducerBusy;

        case InvalidTopicName:
            return ResultInvalidTopicName;

        case IncompatibleSchema:
            return ResultIncompatibleSchema;

        case ConsumerAssignError:
            return ResultConsumerAssignError;

        case TransactionCoordinatorNotFound:
            return ResultTransactionCoordinatorNotFoundError;

        case InvalidTxnStatus:
            return ResultInvalidTxnStatusError;

        case NotAllowedError:
            return ResultNotAllowedError;

        case TransactionConflict:
            return ResultTransactionConflict;

        case TransactionNotFound:
            return ResultTransactionNotFound;

        case ProducerFenced:
            return ResultProducerFenced;
    }
    // NOTE : Do not add default case in the switch above. In future if we get new cases for
    // ServerError and miss them in the switch above we would like to get notified. Adding
    // return here to make the compiler happy.
    return ResultUnknownError;
}

inline std::ostream& operator<<(std::ostream& os, ServerError error) {
    os << getResult(error);
    return os;
}

static bool file_exists(const std::string& path) {
    if (path.empty()) {
        return false;
    }
    std::ifstream f(path);
    return f.good();
}

std::atomic<int32_t> ClientConnection::maxMessageSize_{Commands::DefaultMaxMessageSize};

ClientConnection::ClientConnection(const std::string& logicalAddress, const std::string& physicalAddress,
                                   ExecutorServicePtr executor,
                                   const ClientConfiguration& clientConfiguration,
                                   const AuthenticationPtr& authentication)
    : operationsTimeout_(seconds(clientConfiguration.getOperationTimeoutSeconds())),
      authentication_(authentication),
      serverProtocolVersion_(ProtocolVersion_MIN),
      executor_(executor),
      resolver_(executor_->createTcpResolver()),
      socket_(executor_->createSocket()),
#if BOOST_VERSION >= 107000
      strand_(boost::asio::make_strand(executor_->getIOService().get_executor())),
#elif BOOST_VERSION >= 106600
      strand_(executor_->getIOService().get_executor()),
#else
      strand_(executor_->getIOService()),
#endif
      logicalAddress_(logicalAddress),
      physicalAddress_(physicalAddress),
      cnxString_("[<none> -> " + physicalAddress + "] "),
      incomingBuffer_(SharedBuffer::allocate(DefaultBufferSize)),
      connectTimeoutTask_(std::make_shared<PeriodicTask>(executor_->getIOService(),
                                                         clientConfiguration.getConnectionTimeout())),
      outgoingBuffer_(SharedBuffer::allocate(DefaultBufferSize)),
      consumerStatsRequestTimer_(executor_->createDeadlineTimer()),
      maxPendingLookupRequest_(clientConfiguration.getConcurrentLookupRequest()) {

    LOG_INFO(cnxString_ << "Create ClientConnection, timeout=" << clientConfiguration.getConnectionTimeout());
    if (clientConfiguration.isUseTls()) {
#if BOOST_VERSION >= 105400
        boost::asio::ssl::context ctx(boost::asio::ssl::context::tlsv12_client);
#else
        boost::asio::ssl::context ctx(executor_->getIOService(), boost::asio::ssl::context::tlsv1_client);
#endif
        Url serviceUrl;
        Url::parse(physicalAddress, serviceUrl);
        if (clientConfiguration.isTlsAllowInsecureConnection()) {
            ctx.set_verify_mode(boost::asio::ssl::context::verify_none);
            isTlsAllowInsecureConnection_ = true;
        } else {
            ctx.set_verify_mode(boost::asio::ssl::context::verify_peer);

            if (clientConfiguration.isValidateHostName()) {
                LOG_DEBUG("Validating hostname for " << serviceUrl.host() << ":" << serviceUrl.port());
                ctx.set_verify_callback(boost::asio::ssl::rfc2818_verification(physicalAddress));
            }

            std::string trustCertFilePath = clientConfiguration.getTlsTrustCertsFilePath();
            if (!trustCertFilePath.empty()) {
                if (file_exists(trustCertFilePath)) {
                    ctx.load_verify_file(trustCertFilePath);
                } else {
                    LOG_ERROR(trustCertFilePath << ": No such trustCertFile");
                    close();
                    return;
                }
            } else {
                ctx.set_default_verify_paths();
            }
        }

        if (!authentication_) {
            LOG_ERROR("Invalid authentication plugin");
            close();
            return;
        }

        AuthenticationDataPtr authData;
        if (authentication_->getAuthData(authData) == ResultOk && authData->hasDataForTls()) {
            std::string tlsCertificates = authData->getTlsCertificates();
            std::string tlsPrivateKey = authData->getTlsPrivateKey();

            if (file_exists(tlsCertificates)) {
                ctx.use_certificate_file(tlsCertificates, boost::asio::ssl::context::pem);
            } else {
                LOG_ERROR(tlsCertificates << ": No such tlsCertificates");
                close();
                return;
            }

            if (file_exists(tlsPrivateKey)) {
                ctx.use_private_key_file(tlsPrivateKey, boost::asio::ssl::context::pem);
            } else {
                LOG_ERROR(tlsPrivateKey << ": No such tlsPrivateKey");
                close();
                return;
            }
        }

        tlsSocket_ = ExecutorService::createTlsSocket(socket_, ctx);

        LOG_DEBUG("TLS SNI Host: " << serviceUrl.host());
        if (!SSL_set_tlsext_host_name(tlsSocket_->native_handle(), serviceUrl.host().c_str())) {
            boost::system::error_code ec{static_cast<int>(::ERR_get_error()),
                                         boost::asio::error::get_ssl_category()};
            LOG_ERROR(boost::system::system_error{ec}.what() << ": Error while setting TLS SNI");
            return;
        }
    }
}

ClientConnection::~ClientConnection() { LOG_INFO(cnxString_ << "Destroyed connection"); }

void ClientConnection::handlePulsarConnected(const CommandConnected& cmdConnected) {
    if (!cmdConnected.has_server_version()) {
        LOG_ERROR(cnxString_ << "Server version is not set");
        close();
        return;
    }

    if (cmdConnected.has_max_message_size()) {
        LOG_DEBUG("Connection has max message size setting: " << cmdConnected.max_message_size());
        maxMessageSize_.store(cmdConnected.max_message_size(), std::memory_order_release);
        LOG_DEBUG("Current max message size is: " << maxMessageSize_);
    }

    state_ = Ready;
    connectTimeoutTask_->stop();
    serverProtocolVersion_ = cmdConnected.protocol_version();
    connectPromise_.setValue(shared_from_this());

    if (serverProtocolVersion_ >= v1) {
        // Only send keep-alive probes if the broker supports it
        keepAliveTimer_ = executor_->createDeadlineTimer();
        Lock lock(mutex_);
        if (keepAliveTimer_) {
            keepAliveTimer_->expires_from_now(boost::posix_time::seconds(KeepAliveIntervalInSeconds));
            keepAliveTimer_->async_wait(
                std::bind(&ClientConnection::handleKeepAliveTimeout, shared_from_this()));
        }
        lock.unlock();
    }

    if (serverProtocolVersion_ >= v8) {
        startConsumerStatsTimer(std::vector<uint64_t>());
    }
}

void ClientConnection::startConsumerStatsTimer(std::vector<uint64_t> consumerStatsRequests) {
    std::vector<Promise<Result, BrokerConsumerStatsImpl>> consumerStatsPromises;
    Lock lock(mutex_);

    for (int i = 0; i < consumerStatsRequests.size(); i++) {
        PendingConsumerStatsMap::iterator it = pendingConsumerStatsMap_.find(consumerStatsRequests[i]);
        if (it != pendingConsumerStatsMap_.end()) {
            LOG_DEBUG(cnxString_ << " removing request_id " << it->first
                                 << " from the pendingConsumerStatsMap_");
            consumerStatsPromises.push_back(it->second);
            pendingConsumerStatsMap_.erase(it);
        } else {
            LOG_DEBUG(cnxString_ << "request_id " << it->first << " already fulfilled - not removing it");
        }
    }

    consumerStatsRequests.clear();
    for (PendingConsumerStatsMap::iterator it = pendingConsumerStatsMap_.begin();
         it != pendingConsumerStatsMap_.end(); ++it) {
        consumerStatsRequests.push_back(it->first);
    }

    // If the close operation has reset the consumerStatsRequestTimer_ then the use_count will be zero
    // Check if we have a timer still before we set the request timer to pop again.
    if (consumerStatsRequestTimer_) {
        consumerStatsRequestTimer_->expires_from_now(operationsTimeout_);
        consumerStatsRequestTimer_->async_wait(std::bind(&ClientConnection::handleConsumerStatsTimeout,
                                                         shared_from_this(), std::placeholders::_1,
                                                         consumerStatsRequests));
    }
    lock.unlock();
    // Complex logic since promises need to be fulfilled outside the lock
    for (int i = 0; i < consumerStatsPromises.size(); i++) {
        consumerStatsPromises[i].setFailed(ResultTimeout);
        LOG_WARN(cnxString_ << " Operation timedout, didn't get response from broker");
    }
}

/// The number of unacknowledged probes to send before considering the connection dead and notifying the
/// application layer
typedef boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_KEEPCNT> tcp_keep_alive_count;

/// The interval between subsequential keepalive probes, regardless of what the connection has exchanged in
/// the meantime
typedef boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_KEEPINTVL> tcp_keep_alive_interval;

/// The interval between the last data packet sent (simple ACKs are not considered data) and the first
/// keepalive
/// probe; after the connection is marked to need keepalive, this counter is not used any further
#ifdef __APPLE__
typedef boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_KEEPALIVE> tcp_keep_alive_idle;
#else
typedef boost::asio::detail::socket_option::integer<IPPROTO_TCP, TCP_KEEPIDLE> tcp_keep_alive_idle;
#endif

/*
 *  TCP Connect handler
 *
 *  if async_connect without any error, connected_ would be set to true
 *  at this point the connection is deemed valid to be used by clients of this class
 */
void ClientConnection::handleTcpConnected(const boost::system::error_code& err,
                                          tcp::resolver::iterator endpointIterator) {
    if (!err) {
        std::stringstream cnxStringStream;
        try {
            cnxStringStream << "[" << socket_->local_endpoint() << " -> " << socket_->remote_endpoint()
                            << "] ";
            cnxString_ = cnxStringStream.str();
        } catch (const boost::system::system_error& e) {
            LOG_ERROR("Failed to get endpoint: " << e.what());
            close();
            return;
        }
        if (logicalAddress_ == physicalAddress_) {
            LOG_INFO(cnxString_ << "Connected to broker");
        } else {
            LOG_INFO(cnxString_ << "Connected to broker through proxy. Logical broker: " << logicalAddress_);
        }
        state_ = TcpConnected;

        boost::system::error_code error;
        socket_->set_option(tcp::no_delay(true), error);
        if (error) {
            LOG_WARN(cnxString_ << "Socket failed to set tcp::no_delay: " << error.message());
        }

        socket_->set_option(tcp::socket::keep_alive(true), error);
        if (error) {
            LOG_WARN(cnxString_ << "Socket failed to set tcp::socket::keep_alive: " << error.message());
        }

        // Start TCP keep-alive probes after connection has been idle after 1 minute. Ideally this
        // should never happen, given that we're sending our own keep-alive probes (within the TCP
        // connection) every 30 seconds
        socket_->set_option(tcp_keep_alive_idle(1 * 60), error);
        if (error) {
            LOG_DEBUG(cnxString_ << "Socket failed to set tcp_keep_alive_idle: " << error.message());
        }

        // Send up to 10 probes before declaring the connection broken
        socket_->set_option(tcp_keep_alive_count(10), error);
        if (error) {
            LOG_DEBUG(cnxString_ << "Socket failed to set tcp_keep_alive_count: " << error.message());
        }

        // Interval between probes: 6 seconds
        socket_->set_option(tcp_keep_alive_interval(6), error);
        if (error) {
            LOG_DEBUG(cnxString_ << "Socket failed to set tcp_keep_alive_interval: " << error.message());
        }

        if (tlsSocket_) {
            if (!isTlsAllowInsecureConnection_) {
                boost::system::error_code err;
                Url service_url;
                if (!Url::parse(physicalAddress_, service_url)) {
                    LOG_ERROR(cnxString_ << "Invalid Url, unable to parse: " << err << " " << err.message());
                    close();
                    return;
                }
            }
#if BOOST_VERSION >= 106600
            tlsSocket_->async_handshake(
                boost::asio::ssl::stream<tcp::socket>::client,
                boost::asio::bind_executor(strand_, std::bind(&ClientConnection::handleHandshake,
                                                              shared_from_this(), std::placeholders::_1)));
#else
            tlsSocket_->async_handshake(boost::asio::ssl::stream<tcp::socket>::client,
                                        strand_.wrap(std::bind(&ClientConnection::handleHandshake,
                                                               shared_from_this(), std::placeholders::_1)));
#endif
        } else {
            handleHandshake(boost::system::errc::make_error_code(boost::system::errc::success));
        }
    } else if (endpointIterator != tcp::resolver::iterator()) {
        LOG_WARN(cnxString_ << "Failed to establish connection: " << err.message());
        // The connection failed. Try the next endpoint in the list.
        boost::system::error_code closeError;
        socket_->close(closeError);  // ignore the error of close
        if (closeError) {
            LOG_WARN(cnxString_ << "Failed to close socket: " << err.message());
        }
        connectTimeoutTask_->stop();
        ++endpointIterator;
        if (endpointIterator != tcp::resolver::iterator()) {
            LOG_DEBUG(cnxString_ << "Connecting to " << endpointIterator->endpoint() << "...");
            connectTimeoutTask_->start();
            tcp::endpoint endpoint = *endpointIterator;
            socket_->async_connect(endpoint,
                                   std::bind(&ClientConnection::handleTcpConnected, shared_from_this(),
                                             std::placeholders::_1, ++endpointIterator));
        } else {
            close();
        }
    } else {
        LOG_ERROR(cnxString_ << "Failed to establish connection: " << err.message());
        close();
    }
}

void ClientConnection::handleHandshake(const boost::system::error_code& err) {
    if (err) {
        LOG_ERROR(cnxString_ << "Handshake failed: " << err.message());
        close();
        return;
    }

    bool connectingThroughProxy = logicalAddress_ != physicalAddress_;
    Result result = ResultOk;
    SharedBuffer buffer =
        Commands::newConnect(authentication_, logicalAddress_, connectingThroughProxy, result);
    if (result != ResultOk) {
        LOG_ERROR(cnxString_ << "Failed to establish connection: " << result);
        close(result);
        return;
    }
    // Send CONNECT command to broker
    asyncWrite(buffer.const_asio_buffer(), std::bind(&ClientConnection::handleSentPulsarConnect,
                                                     shared_from_this(), std::placeholders::_1, buffer));
}

void ClientConnection::handleSentPulsarConnect(const boost::system::error_code& err,
                                               const SharedBuffer& buffer) {
    if (err) {
        LOG_ERROR(cnxString_ << "Failed to establish connection: " << err.message());
        close();
        return;
    }

    // Schedule the reading of CONNECTED command from broker
    readNextCommand();
}

void ClientConnection::handleSentAuthResponse(const boost::system::error_code& err,
                                              const SharedBuffer& buffer) {
    if (err) {
        LOG_WARN(cnxString_ << "Failed to send auth response: " << err.message());
        close();
        return;
    }
}

/*
 * Async method to establish TCP connection with broker
 *
 * tcpConnectCompletionHandler is notified when the result of this call is available.
 *
 */
void ClientConnection::tcpConnectAsync() {
    if (isClosed()) {
        return;
    }

    boost::system::error_code err;
    Url service_url;
    if (!Url::parse(physicalAddress_, service_url)) {
        LOG_ERROR(cnxString_ << "Invalid Url, unable to parse: " << err << " " << err.message());
        close();
        return;
    }

    if (service_url.protocol() != "pulsar" && service_url.protocol() != "pulsar+ssl") {
        LOG_ERROR(cnxString_ << "Invalid Url protocol '" << service_url.protocol()
                             << "'. Valid values are 'pulsar' and 'pulsar+ssl'");
        close();
        return;
    }

    LOG_DEBUG(cnxString_ << "Resolving " << service_url.host() << ":" << service_url.port());
    tcp::resolver::query query(service_url.host(), std::to_string(service_url.port()));
    resolver_->async_resolve(query, std::bind(&ClientConnection::handleResolve, shared_from_this(),
                                              std::placeholders::_1, std::placeholders::_2));
}

void ClientConnection::handleResolve(const boost::system::error_code& err,
                                     tcp::resolver::iterator endpointIterator) {
    if (err) {
        LOG_ERROR(cnxString_ << "Resolve error: " << err << " : " << err.message());
        close();
        return;
    }

    auto self = ClientConnectionWeakPtr(shared_from_this());

    connectTimeoutTask_->setCallback([self](const PeriodicTask::ErrorCode& ec) {
        ClientConnectionPtr ptr = self.lock();
        if (!ptr) {
            // Connection was already destroyed
            return;
        }

        if (ptr->state_ != Ready) {
            LOG_ERROR(ptr->cnxString_ << "Connection was not established in "
                                      << ptr->connectTimeoutTask_->getPeriodMs() << " ms, close the socket");
            PeriodicTask::ErrorCode err;
            ptr->socket_->close(err);
            if (err) {
                LOG_WARN(ptr->cnxString_ << "Failed to close socket: " << err.message());
            }
        }
        ptr->connectTimeoutTask_->stop();
    });

    LOG_DEBUG(cnxString_ << "Connecting to " << endpointIterator->endpoint() << "...");
    connectTimeoutTask_->start();
    if (endpointIterator != tcp::resolver::iterator()) {
        LOG_DEBUG(cnxString_ << "Resolved hostname " << endpointIterator->host_name()  //
                             << " to " << endpointIterator->endpoint());
        socket_->async_connect(*endpointIterator,
                               std::bind(&ClientConnection::handleTcpConnected, shared_from_this(),
                                         std::placeholders::_1, endpointIterator));
    } else {
        LOG_WARN(cnxString_ << "No IP address found");
        close();
        return;
    }
}

void ClientConnection::readNextCommand() {
    const static uint32_t minReadSize = sizeof(uint32_t);
    asyncReceive(
        incomingBuffer_.asio_buffer(),
        customAllocReadHandler(std::bind(&ClientConnection::handleRead, shared_from_this(),
                                         std::placeholders::_1, std::placeholders::_2, minReadSize)));
}

void ClientConnection::handleRead(const boost::system::error_code& err, size_t bytesTransferred,
                                  uint32_t minReadSize) {
    // Update buffer write idx with new data
    incomingBuffer_.bytesWritten(bytesTransferred);

    if (err || bytesTransferred == 0) {
        if (err) {
            if (err == boost::asio::error::operation_aborted) {
                LOG_DEBUG(cnxString_ << "Read operation was canceled: " << err.message());
            } else {
                LOG_ERROR(cnxString_ << "Read operation failed: " << err.message());
            }
        }  // else: bytesTransferred == 0, which means server has closed the connection
        close();
    } else if (bytesTransferred < minReadSize) {
        // Read the remaining part, use a slice of buffer to write on the next
        // region
        SharedBuffer buffer = incomingBuffer_.slice(bytesTransferred);
        asyncReceive(buffer.asio_buffer(),
                     customAllocReadHandler(std::bind(&ClientConnection::handleRead, shared_from_this(),
                                                      std::placeholders::_1, std::placeholders::_2,
                                                      minReadSize - bytesTransferred)));
    } else {
        processIncomingBuffer();
    }
}

void ClientConnection::processIncomingBuffer() {
    // Process all the available frames from the incoming buffer
    while (incomingBuffer_.readableBytes() >= sizeof(uint32_t)) {
        // Extract message frames from incoming buffer
        // At this point we have at least 4 bytes in the buffer
        uint32_t frameSize = incomingBuffer_.readUnsignedInt();

        if (frameSize > incomingBuffer_.readableBytes()) {
            // We don't have the entire frame yet
            const uint32_t bytesToReceive = frameSize - incomingBuffer_.readableBytes();

            // Rollback the reading of frameSize (when the frame will be complete,
            // we'll read it again
            incomingBuffer_.rollback(sizeof(uint32_t));

            if (bytesToReceive <= incomingBuffer_.writableBytes()) {
                // The rest of the frame still fits in the current buffer
                asyncReceive(incomingBuffer_.asio_buffer(),
                             customAllocReadHandler(std::bind(&ClientConnection::handleRead,
                                                              shared_from_this(), std::placeholders::_1,
                                                              std::placeholders::_2, bytesToReceive)));
                return;
            } else {
                // Need to allocate a buffer big enough for the frame
                uint32_t newBufferSize = std::max<uint32_t>(DefaultBufferSize, frameSize + sizeof(uint32_t));
                incomingBuffer_ = SharedBuffer::copyFrom(incomingBuffer_, newBufferSize);

                asyncReceive(incomingBuffer_.asio_buffer(),
                             customAllocReadHandler(std::bind(&ClientConnection::handleRead,
                                                              shared_from_this(), std::placeholders::_1,
                                                              std::placeholders::_2, bytesToReceive)));
                return;
            }
        }

        // At this point,  we have at least one complete frame available in the buffer
        uint32_t cmdSize = incomingBuffer_.readUnsignedInt();
        if (!incomingCmd_.ParseFromArray(incomingBuffer_.data(), cmdSize)) {
            LOG_ERROR(cnxString_ << "Error parsing protocol buffer command");
            close();
            return;
        }

        incomingBuffer_.consume(cmdSize);

        if (incomingCmd_.type() == BaseCommand::MESSAGE) {
            // Parse message metadata and extract payload
            MessageMetadata msgMetadata;

            // read checksum
            uint32_t remainingBytes = frameSize - (cmdSize + 4);
            bool isChecksumValid = verifyChecksum(incomingBuffer_, remainingBytes, incomingCmd_);

            uint32_t metadataSize = incomingBuffer_.readUnsignedInt();
            if (!msgMetadata.ParseFromArray(incomingBuffer_.data(), metadataSize)) {
                LOG_ERROR(cnxString_ << "[consumer id " << incomingCmd_.message().consumer_id()  //
                                     << ", message ledger id "
                                     << incomingCmd_.message().message_id().ledgerid()  //
                                     << ", entry id " << incomingCmd_.message().message_id().entryid()
                                     << "] Error parsing message metadata");
                close();
                return;
            }

            incomingBuffer_.consume(metadataSize);
            remainingBytes -= (4 + metadataSize);

            uint32_t payloadSize = remainingBytes;
            SharedBuffer payload = SharedBuffer::copy(incomingBuffer_.data(), payloadSize);
            incomingBuffer_.consume(payloadSize);
            handleIncomingMessage(incomingCmd_.message(), isChecksumValid, msgMetadata, payload);
        } else {
            handleIncomingCommand();
        }
    }
    if (incomingBuffer_.readableBytes() > 0) {
        // We still have 1 to 3 bytes from the next frame
        assert(incomingBuffer_.readableBytes() < sizeof(uint32_t));

        // Restart with a new buffer and copy the the few bytes at the beginning
        incomingBuffer_ = SharedBuffer::copyFrom(incomingBuffer_, DefaultBufferSize);

        // At least we need to read 4 bytes to have the complete frame size
        uint32_t minReadSize = sizeof(uint32_t) - incomingBuffer_.readableBytes();

        asyncReceive(
            incomingBuffer_.asio_buffer(),
            customAllocReadHandler(std::bind(&ClientConnection::handleRead, shared_from_this(),
                                             std::placeholders::_1, std::placeholders::_2, minReadSize)));
        return;
    }

    // We have read everything we had in the buffer
    // Rollback the indexes to reuse the same buffer
    incomingBuffer_.reset();

    readNextCommand();
}

bool ClientConnection::verifyChecksum(SharedBuffer& incomingBuffer_, uint32_t& remainingBytes,
                                      proto::BaseCommand& incomingCmd_) {
    int readerIndex = incomingBuffer_.readerIndex();
    bool isChecksumValid = true;

    if (incomingBuffer_.readUnsignedShort() == Commands::magicCrc32c) {
        uint32_t storedChecksum = incomingBuffer_.readUnsignedInt();
        remainingBytes -= (2 + 4) /* subtract size of checksum itself */;

        // compute metadata-payload checksum
        int metadataPayloadSize = remainingBytes;
        uint32_t computedChecksum = computeChecksum(0, incomingBuffer_.data(), metadataPayloadSize);
        // verify checksum
        isChecksumValid = (storedChecksum == computedChecksum);

        if (!isChecksumValid) {
            LOG_ERROR("[consumer id "
                      << incomingCmd_.message().consumer_id()                                           //
                      << ", message ledger id " << incomingCmd_.message().message_id().ledgerid()       //
                      << ", entry id " << incomingCmd_.message().message_id().entryid()                 //
                      << "stored-checksum" << storedChecksum << "computedChecksum" << computedChecksum  //
                      << "] Checksum verification failed");
        }
    } else {
        incomingBuffer_.setReaderIndex(readerIndex);
    }
    return isChecksumValid;
}

void ClientConnection::handleActiveConsumerChange(const proto::CommandActiveConsumerChange& change) {
    Lock lock(mutex_);
    ConsumersMap::iterator it = consumers_.find(change.consumer_id());
    if (it != consumers_.end()) {
        ConsumerImplPtr consumer = it->second.lock();

        if (consumer) {
            lock.unlock();
            consumer->activeConsumerChanged(change.is_active());
        } else {
            consumers_.erase(change.consumer_id());
            LOG_DEBUG(cnxString_ << "Ignoring incoming message for already destroyed consumer "
                                 << change.consumer_id());
        }
    } else {
        LOG_DEBUG(cnxString_ << "Got invalid consumer Id in " << change.consumer_id()
                             << " -- isActive: " << change.is_active());
    }
}

void ClientConnection::handleIncomingMessage(const proto::CommandMessage& msg, bool isChecksumValid,
                                             proto::MessageMetadata& msgMetadata, SharedBuffer& payload) {
    LOG_DEBUG(cnxString_ << "Received a message from the server for consumer: " << msg.consumer_id());

    Lock lock(mutex_);
    ConsumersMap::iterator it = consumers_.find(msg.consumer_id());
    if (it != consumers_.end()) {
        ConsumerImplPtr consumer = it->second.lock();

        if (consumer) {
            // Unlock the mutex before notifying the consumer of the
            // new received message
            lock.unlock();
            consumer->messageReceived(shared_from_this(), msg, isChecksumValid, msgMetadata, payload);
        } else {
            consumers_.erase(msg.consumer_id());
            LOG_DEBUG(cnxString_ << "Ignoring incoming message for already destroyed consumer "
                                 << msg.consumer_id());
        }
    } else {
        LOG_DEBUG(cnxString_ << "Got invalid consumer Id in "  //
                             << msg.consumer_id() << " -- msg: " << msgMetadata.sequence_id());
    }
}

void ClientConnection::handleIncomingCommand() {
    LOG_DEBUG(cnxString_ << "Handling incoming command: " << Commands::messageType(incomingCmd_.type()));

    switch (state_) {
        case Pending: {
            LOG_ERROR(cnxString_ << "Connection is not ready yet");
            break;
        }

        case TcpConnected: {
            // Handle Pulsar Connected
            if (incomingCmd_.type() != BaseCommand::CONNECTED) {
                // Wrong cmd
                close();
            } else {
                handlePulsarConnected(incomingCmd_.connected());
            }
            break;
        }

        case Disconnected: {
            LOG_ERROR(cnxString_ << "Connection already disconnected");
            break;
        }

        case Ready: {
            // Handle normal commands
            switch (incomingCmd_.type()) {
                case BaseCommand::SEND_RECEIPT: {
                    const CommandSendReceipt& sendReceipt = incomingCmd_.send_receipt();
                    int producerId = sendReceipt.producer_id();
                    uint64_t sequenceId = sendReceipt.sequence_id();
                    const proto::MessageIdData& messageIdData = sendReceipt.message_id();
                    MessageId messageId = MessageId(messageIdData.partition(), messageIdData.ledgerid(),
                                                    messageIdData.entryid(), messageIdData.batch_index());

                    LOG_DEBUG(cnxString_ << "Got receipt for producer: " << producerId
                                         << " -- msg: " << sequenceId << "-- message id: " << messageId);

                    Lock lock(mutex_);
                    ProducersMap::iterator it = producers_.find(producerId);
                    if (it != producers_.end()) {
                        ProducerImplPtr producer = it->second.lock();
                        lock.unlock();

                        if (producer) {
                            if (!producer->ackReceived(sequenceId, messageId)) {
                                // If the producer fails to process the ack, we need to close the connection
                                // to give it a chance to recover from there
                                close();
                            }
                        }
                    } else {
                        LOG_ERROR(cnxString_ << "Got invalid producer Id in SendReceipt: "  //
                                             << producerId << " -- msg: " << sequenceId);
                    }

                    break;
                }

                case BaseCommand::SEND_ERROR: {
                    const CommandSendError& error = incomingCmd_.send_error();
                    LOG_WARN(cnxString_ << "Received send error from server: " << error.message());
                    if (ChecksumError == error.error()) {
                        long producerId = error.producer_id();
                        long sequenceId = error.sequence_id();
                        Lock lock(mutex_);
                        ProducersMap::iterator it = producers_.find(producerId);
                        if (it != producers_.end()) {
                            ProducerImplPtr producer = it->second.lock();
                            lock.unlock();

                            if (producer) {
                                if (!producer->removeCorruptMessage(sequenceId)) {
                                    // If the producer fails to remove corrupt msg, we need to close the
                                    // connection to give it a chance to recover from there
                                    close();
                                }
                            }
                        }
                    } else {
                        close();
                    }
                    break;
                }

                case BaseCommand::SUCCESS: {
                    const CommandSuccess& success = incomingCmd_.success();
                    LOG_DEBUG(cnxString_ << "Received success response from server. req_id: "
                                         << success.request_id());

                    Lock lock(mutex_);
                    PendingRequestsMap::iterator it = pendingRequests_.find(success.request_id());
                    if (it != pendingRequests_.end()) {
                        PendingRequestData requestData = it->second;
                        pendingRequests_.erase(it);
                        lock.unlock();

                        requestData.promise.setValue({});
                        requestData.timer->cancel();
                    }
                    break;
                }

                case BaseCommand::PARTITIONED_METADATA_RESPONSE: {
                    const CommandPartitionedTopicMetadataResponse& partitionMetadataResponse =
                        incomingCmd_.partitionmetadataresponse();
                    LOG_DEBUG(cnxString_ << "Received partition-metadata response from server. req_id: "
                                         << partitionMetadataResponse.request_id());

                    Lock lock(mutex_);
                    PendingLookupRequestsMap::iterator it =
                        pendingLookupRequests_.find(partitionMetadataResponse.request_id());
                    if (it != pendingLookupRequests_.end()) {
                        it->second.timer->cancel();
                        LookupDataResultPromisePtr lookupDataPromise = it->second.promise;
                        pendingLookupRequests_.erase(it);
                        numOfPendingLookupRequest_--;
                        lock.unlock();

                        if (!partitionMetadataResponse.has_response() ||
                            (partitionMetadataResponse.response() ==
                             CommandPartitionedTopicMetadataResponse::Failed)) {
                            if (partitionMetadataResponse.has_error()) {
                                LOG_ERROR(cnxString_ << "Failed partition-metadata lookup req_id: "
                                                     << partitionMetadataResponse.request_id()
                                                     << " error: " << partitionMetadataResponse.error());
                            } else {
                                LOG_ERROR(cnxString_ << "Failed partition-metadata lookup req_id: "
                                                     << partitionMetadataResponse.request_id()
                                                     << " with empty response: ");
                            }
                            lookupDataPromise->setFailed(ResultConnectError);
                        } else {
                            LookupDataResultPtr lookupResultPtr = std::make_shared<LookupDataResult>();
                            lookupResultPtr->setPartitions(partitionMetadataResponse.partitions());
                            lookupDataPromise->setValue(lookupResultPtr);
                        }

                    } else {
                        LOG_WARN("Received unknown request id from server: "
                                 << partitionMetadataResponse.request_id());
                    }
                    break;
                }

                case BaseCommand::CONSUMER_STATS_RESPONSE: {
                    const CommandConsumerStatsResponse& consumerStatsResponse =
                        incomingCmd_.consumerstatsresponse();
                    LOG_DEBUG(cnxString_ << "ConsumerStatsResponse command - Received consumer stats "
                                            "response from server. req_id: "
                                         << consumerStatsResponse.request_id());
                    Lock lock(mutex_);
                    PendingConsumerStatsMap::iterator it =
                        pendingConsumerStatsMap_.find(consumerStatsResponse.request_id());
                    if (it != pendingConsumerStatsMap_.end()) {
                        Promise<Result, BrokerConsumerStatsImpl> consumerStatsPromise = it->second;
                        pendingConsumerStatsMap_.erase(it);
                        lock.unlock();

                        if (consumerStatsResponse.has_error_code()) {
                            if (consumerStatsResponse.has_error_message()) {
                                LOG_ERROR(cnxString_ << " Failed to get consumer stats - "
                                                     << consumerStatsResponse.error_message());
                            }
                            consumerStatsPromise.setFailed(getResult(consumerStatsResponse.error_code()));
                        } else {
                            LOG_DEBUG(cnxString_ << "ConsumerStatsResponse command - Received consumer stats "
                                                    "response from server. req_id: "
                                                 << consumerStatsResponse.request_id() << " Stats: ");
                            BrokerConsumerStatsImpl brokerStats(
                                consumerStatsResponse.msgrateout(), consumerStatsResponse.msgthroughputout(),
                                consumerStatsResponse.msgrateredeliver(),
                                consumerStatsResponse.consumername(),
                                consumerStatsResponse.availablepermits(),
                                consumerStatsResponse.unackedmessages(),
                                consumerStatsResponse.blockedconsumeronunackedmsgs(),
                                consumerStatsResponse.address(), consumerStatsResponse.connectedsince(),
                                consumerStatsResponse.type(), consumerStatsResponse.msgrateexpired(),
                                consumerStatsResponse.msgbacklog());
                            consumerStatsPromise.setValue(brokerStats);
                        }
                    } else {
                        LOG_WARN("ConsumerStatsResponse command - Received unknown request id from server: "
                                 << consumerStatsResponse.request_id());
                    }
                    break;
                }

                case BaseCommand::LOOKUP_RESPONSE: {
                    const CommandLookupTopicResponse& lookupTopicResponse =
                        incomingCmd_.lookuptopicresponse();
                    LOG_DEBUG(cnxString_ << "Received lookup response from server. req_id: "
                                         << lookupTopicResponse.request_id());

                    Lock lock(mutex_);
                    PendingLookupRequestsMap::iterator it =
                        pendingLookupRequests_.find(lookupTopicResponse.request_id());
                    if (it != pendingLookupRequests_.end()) {
                        it->second.timer->cancel();
                        LookupDataResultPromisePtr lookupDataPromise = it->second.promise;
                        pendingLookupRequests_.erase(it);
                        numOfPendingLookupRequest_--;
                        lock.unlock();

                        if (!lookupTopicResponse.has_response() ||
                            (lookupTopicResponse.response() == CommandLookupTopicResponse::Failed)) {
                            if (lookupTopicResponse.has_error()) {
                                LOG_ERROR(cnxString_
                                          << "Failed lookup req_id: " << lookupTopicResponse.request_id()
                                          << " error: " << lookupTopicResponse.error());
                            } else {
                                LOG_ERROR(cnxString_
                                          << "Failed lookup req_id: " << lookupTopicResponse.request_id()
                                          << " with empty response: ");
                            }
                            lookupDataPromise->setFailed(ResultConnectError);
                        } else {
                            LOG_DEBUG(cnxString_
                                      << "Received lookup response from server. req_id: "
                                      << lookupTopicResponse.request_id()  //
                                      << " -- broker-url: " << lookupTopicResponse.brokerserviceurl()
                                      << " -- broker-tls-url: "  //
                                      << lookupTopicResponse.brokerserviceurltls()
                                      << " authoritative: " << lookupTopicResponse.authoritative()  //
                                      << " redirect: " << lookupTopicResponse.response());
                            LookupDataResultPtr lookupResultPtr = std::make_shared<LookupDataResult>();

                            if (tlsSocket_) {
                                lookupResultPtr->setBrokerUrl(lookupTopicResponse.brokerserviceurltls());
                            } else {
                                lookupResultPtr->setBrokerUrl(lookupTopicResponse.brokerserviceurl());
                            }

                            lookupResultPtr->setBrokerUrlTls(lookupTopicResponse.brokerserviceurltls());
                            lookupResultPtr->setAuthoritative(lookupTopicResponse.authoritative());
                            lookupResultPtr->setRedirect(lookupTopicResponse.response() ==
                                                         CommandLookupTopicResponse::Redirect);
                            lookupResultPtr->setShouldProxyThroughServiceUrl(
                                lookupTopicResponse.proxy_through_service_url());
                            lookupDataPromise->setValue(lookupResultPtr);
                        }

                    } else {
                        LOG_WARN(
                            "Received unknown request id from server: " << lookupTopicResponse.request_id());
                    }
                    break;
                }

                case BaseCommand::PRODUCER_SUCCESS: {
                    const CommandProducerSuccess& producerSuccess = incomingCmd_.producer_success();
                    LOG_DEBUG(cnxString_ << "Received success producer response from server. req_id: "
                                         << producerSuccess.request_id()  //
                                         << " -- producer name: " << producerSuccess.producer_name());

                    Lock lock(mutex_);
                    PendingRequestsMap::iterator it = pendingRequests_.find(producerSuccess.request_id());
                    if (it != pendingRequests_.end()) {
                        PendingRequestData requestData = it->second;
                        pendingRequests_.erase(it);
                        lock.unlock();

                        ResponseData data;
                        data.producerName = producerSuccess.producer_name();
                        data.lastSequenceId = producerSuccess.last_sequence_id();
                        if (producerSuccess.has_schema_version()) {
                            data.schemaVersion = producerSuccess.schema_version();
                        }
                        requestData.promise.setValue(data);
                        requestData.timer->cancel();
                    }
                    break;
                }

                case BaseCommand::ERROR: {
                    const CommandError& error = incomingCmd_.error();
                    Result result = getResult(error.error());
                    LOG_WARN(cnxString_ << "Received error response from server: " << result
                                        << (error.has_message() ? (" (" + error.message() + ")") : "")
                                        << " -- req_id: " << error.request_id());

                    Lock lock(mutex_);

                    PendingRequestsMap::iterator it = pendingRequests_.find(error.request_id());
                    if (it != pendingRequests_.end()) {
                        PendingRequestData requestData = it->second;
                        pendingRequests_.erase(it);
                        lock.unlock();

                        requestData.promise.setFailed(getResult(error.error()));
                        requestData.timer->cancel();
                    } else {
                        PendingGetLastMessageIdRequestsMap::iterator it =
                            pendingGetLastMessageIdRequests_.find(error.request_id());
                        if (it != pendingGetLastMessageIdRequests_.end()) {
                            Promise<Result, MessageId> getLastMessageIdPromise = it->second;
                            pendingGetLastMessageIdRequests_.erase(it);
                            lock.unlock();

                            getLastMessageIdPromise.setFailed(getResult(error.error()));
                        } else {
                            PendingGetNamespaceTopicsMap::iterator it =
                                pendingGetNamespaceTopicsRequests_.find(error.request_id());
                            if (it != pendingGetNamespaceTopicsRequests_.end()) {
                                Promise<Result, NamespaceTopicsPtr> getNamespaceTopicsPromise = it->second;
                                pendingGetNamespaceTopicsRequests_.erase(it);
                                lock.unlock();

                                getNamespaceTopicsPromise.setFailed(getResult(error.error()));
                            } else {
                                lock.unlock();
                            }
                        }
                    }
                    break;
                }

                case BaseCommand::CLOSE_PRODUCER: {
                    const CommandCloseProducer& closeProducer = incomingCmd_.close_producer();
                    int producerId = closeProducer.producer_id();

                    LOG_DEBUG("Broker notification of Closed producer: " << producerId);

                    Lock lock(mutex_);
                    ProducersMap::iterator it = producers_.find(producerId);
                    if (it != producers_.end()) {
                        ProducerImplPtr producer = it->second.lock();
                        producers_.erase(it);
                        lock.unlock();

                        if (producer) {
                            producer->disconnectProducer();
                        }
                    } else {
                        LOG_ERROR(cnxString_ << "Got invalid producer Id in closeProducer command: "
                                             << producerId);
                    }

                    break;
                }

                case BaseCommand::CLOSE_CONSUMER: {
                    const CommandCloseConsumer& closeconsumer = incomingCmd_.close_consumer();
                    int consumerId = closeconsumer.consumer_id();

                    LOG_DEBUG("Broker notification of Closed consumer: " << consumerId);

                    Lock lock(mutex_);
                    ConsumersMap::iterator it = consumers_.find(consumerId);
                    if (it != consumers_.end()) {
                        ConsumerImplPtr consumer = it->second.lock();
                        consumers_.erase(it);
                        lock.unlock();

                        if (consumer) {
                            consumer->disconnectConsumer();
                        }
                    } else {
                        LOG_ERROR(cnxString_ << "Got invalid consumer Id in closeConsumer command: "
                                             << consumerId);
                    }

                    break;
                }

                case BaseCommand::PING: {
                    // Respond to ping request
                    LOG_DEBUG(cnxString_ << "Replying to ping command");
                    sendCommand(Commands::newPong());
                    break;
                }

                case BaseCommand::PONG: {
                    LOG_DEBUG(cnxString_ << "Received response to ping message");
                    havePendingPingRequest_ = false;
                    break;
                }

                case BaseCommand::AUTH_CHALLENGE: {
                    LOG_DEBUG(cnxString_ << "Received auth challenge from broker");

                    Result result;
                    SharedBuffer buffer = Commands::newAuthResponse(authentication_, result);
                    if (result != ResultOk) {
                        LOG_ERROR(cnxString_ << "Failed to send auth response: " << result);
                        close(result);
                        break;
                    }
                    asyncWrite(buffer.const_asio_buffer(),
                               std::bind(&ClientConnection::handleSentAuthResponse, shared_from_this(),
                                         std::placeholders::_1, buffer));
                    break;
                }

                case BaseCommand::ACTIVE_CONSUMER_CHANGE: {
                    const CommandActiveConsumerChange& change = incomingCmd_.active_consumer_change();
                    LOG_DEBUG(cnxString_
                              << "Received notification about active consumer change, consumer_id: "
                              << change.consumer_id() << " isActive: " << change.is_active());
                    handleActiveConsumerChange(change);
                    break;
                }

                case BaseCommand::GET_LAST_MESSAGE_ID_RESPONSE: {
                    const CommandGetLastMessageIdResponse& getLastMessageIdResponse =
                        incomingCmd_.getlastmessageidresponse();
                    LOG_DEBUG(cnxString_ << "Received getLastMessageIdResponse from server. req_id: "
                                         << getLastMessageIdResponse.request_id());

                    Lock lock(mutex_);
                    PendingGetLastMessageIdRequestsMap::iterator it =
                        pendingGetLastMessageIdRequests_.find(getLastMessageIdResponse.request_id());

                    if (it != pendingGetLastMessageIdRequests_.end()) {
                        Promise<Result, MessageId> getLastMessageIdPromise = it->second;
                        pendingGetLastMessageIdRequests_.erase(it);
                        lock.unlock();

                        MessageIdData messageIdData = getLastMessageIdResponse.last_message_id();
                        MessageId messageId = MessageId(messageIdData.partition(), messageIdData.ledgerid(),
                                                        messageIdData.entryid(), messageIdData.batch_index());

                        getLastMessageIdPromise.setValue(messageId);
                    } else {
                        lock.unlock();
                        LOG_WARN(
                            "getLastMessageIdResponse command - Received unknown request id from server: "
                            << getLastMessageIdResponse.request_id());
                    }
                    break;
                }

                case BaseCommand::GET_TOPICS_OF_NAMESPACE_RESPONSE: {
                    const CommandGetTopicsOfNamespaceResponse& response =
                        incomingCmd_.gettopicsofnamespaceresponse();

                    LOG_DEBUG(cnxString_ << "Received GetTopicsOfNamespaceResponse from server. req_id: "
                                         << response.request_id() << " topicsSize" << response.topics_size());

                    Lock lock(mutex_);
                    PendingGetNamespaceTopicsMap::iterator it =
                        pendingGetNamespaceTopicsRequests_.find(response.request_id());

                    if (it != pendingGetNamespaceTopicsRequests_.end()) {
                        Promise<Result, NamespaceTopicsPtr> getTopicsPromise = it->second;
                        pendingGetNamespaceTopicsRequests_.erase(it);
                        lock.unlock();

                        int numTopics = response.topics_size();
                        std::set<std::string> topicSet;
                        // get all topics
                        for (int i = 0; i < numTopics; i++) {
                            // remove partition part
                            const std::string& topicName = response.topics(i);
                            int pos = topicName.find("-partition-");
                            std::string filteredName = topicName.substr(0, pos);

                            // filter duped topic name
                            if (topicSet.find(filteredName) == topicSet.end()) {
                                topicSet.insert(filteredName);
                            }
                        }

                        NamespaceTopicsPtr topicsPtr =
                            std::make_shared<std::vector<std::string>>(topicSet.begin(), topicSet.end());

                        getTopicsPromise.setValue(topicsPtr);
                    } else {
                        lock.unlock();
                        LOG_WARN(
                            "GetTopicsOfNamespaceResponse command - Received unknown request id from "
                            "server: "
                            << response.request_id());
                    }
                    break;
                }

                default: {
                    LOG_WARN(cnxString_ << "Received invalid message from server");
                    close();
                    break;
                }
            }
        }
    }
}

Future<Result, BrokerConsumerStatsImpl> ClientConnection::newConsumerStats(uint64_t consumerId,
                                                                           uint64_t requestId) {
    Lock lock(mutex_);
    Promise<Result, BrokerConsumerStatsImpl> promise;
    if (isClosed()) {
        lock.unlock();
        LOG_ERROR(cnxString_ << " Client is not connected to the broker");
        promise.setFailed(ResultNotConnected);
    }
    pendingConsumerStatsMap_.insert(std::make_pair(requestId, promise));
    lock.unlock();
    sendCommand(Commands::newConsumerStats(consumerId, requestId));
    return promise.getFuture();
}

void ClientConnection::newTopicLookup(const std::string& topicName, bool authoritative,
                                      const std::string& listenerName, const uint64_t requestId,
                                      LookupDataResultPromisePtr promise) {
    newLookup(Commands::newLookup(topicName, authoritative, requestId, listenerName), requestId, promise);
}

void ClientConnection::newPartitionedMetadataLookup(const std::string& topicName, const uint64_t requestId,
                                                    LookupDataResultPromisePtr promise) {
    newLookup(Commands::newPartitionMetadataRequest(topicName, requestId), requestId, promise);
}

void ClientConnection::newLookup(const SharedBuffer& cmd, const uint64_t requestId,
                                 LookupDataResultPromisePtr promise) {
    Lock lock(mutex_);
    std::shared_ptr<LookupDataResultPtr> lookupDataResult;
    lookupDataResult = std::make_shared<LookupDataResultPtr>();
    if (isClosed()) {
        lock.unlock();
        promise->setFailed(ResultNotConnected);
        return;
    } else if (numOfPendingLookupRequest_ >= maxPendingLookupRequest_) {
        lock.unlock();
        promise->setFailed(ResultTooManyLookupRequestException);
        return;
    }
    LookupRequestData requestData;
    requestData.promise = promise;
    requestData.timer = executor_->createDeadlineTimer();
    requestData.timer->expires_from_now(operationsTimeout_);
    requestData.timer->async_wait(std::bind(&ClientConnection::handleLookupTimeout, shared_from_this(),
                                            std::placeholders::_1, requestData));

    pendingLookupRequests_.insert(std::make_pair(requestId, requestData));
    numOfPendingLookupRequest_++;
    lock.unlock();
    sendCommand(cmd);
}

void ClientConnection::sendCommand(const SharedBuffer& cmd) {
    Lock lock(mutex_);

    if (pendingWriteOperations_++ == 0) {
        // Write immediately to socket
        if (tlsSocket_) {
#if BOOST_VERSION >= 106600
            boost::asio::post(strand_,
                              std::bind(&ClientConnection::sendCommandInternal, shared_from_this(), cmd));
#else
            strand_.post(std::bind(&ClientConnection::sendCommandInternal, shared_from_this(), cmd));
#endif
        } else {
            sendCommandInternal(cmd);
        }
    } else {
        // Queue to send later
        pendingWriteBuffers_.push_back(cmd);
    }
}

void ClientConnection::sendCommandInternal(const SharedBuffer& cmd) {
    asyncWrite(cmd.const_asio_buffer(),
               customAllocWriteHandler(
                   std::bind(&ClientConnection::handleSend, shared_from_this(), std::placeholders::_1, cmd)));
}

void ClientConnection::sendMessage(const OpSendMsg& opSend) {
    Lock lock(mutex_);

    if (pendingWriteOperations_++ == 0) {
        // Write immediately to socket
        if (tlsSocket_) {
#if BOOST_VERSION >= 106600
            boost::asio::post(strand_,
                              std::bind(&ClientConnection::sendMessageInternal, shared_from_this(), opSend));
#else
            strand_.post(std::bind(&ClientConnection::sendMessageInternal, shared_from_this(), opSend));
#endif
        } else {
            sendMessageInternal(opSend);
        }
    } else {
        // Queue to send later
        pendingWriteBuffers_.push_back(opSend);
    }
}

void ClientConnection::sendMessageInternal(const OpSendMsg& opSend) {
    PairSharedBuffer buffer = Commands::newSend(outgoingBuffer_, outgoingCmd_, opSend.producerId_,
                                                opSend.sequenceId_, getChecksumType(), opSend.msg_);

    asyncWrite(buffer, customAllocWriteHandler(std::bind(&ClientConnection::handleSendPair,
                                                         shared_from_this(), std::placeholders::_1)));
}

void ClientConnection::handleSend(const boost::system::error_code& err, const SharedBuffer&) {
    if (err) {
        LOG_WARN(cnxString_ << "Could not send message on connection: " << err << " " << err.message());
        close();
    } else {
        sendPendingCommands();
    }
}

void ClientConnection::handleSendPair(const boost::system::error_code& err) {
    if (err) {
        LOG_WARN(cnxString_ << "Could not send pair message on connection: " << err << " " << err.message());
        close();
    } else {
        sendPendingCommands();
    }
}

void ClientConnection::sendPendingCommands() {
    Lock lock(mutex_);

    if (--pendingWriteOperations_ > 0) {
        assert(!pendingWriteBuffers_.empty());
        boost::any any = pendingWriteBuffers_.front();
        pendingWriteBuffers_.pop_front();

        if (any.type() == typeid(SharedBuffer)) {
            SharedBuffer buffer = boost::any_cast<SharedBuffer>(any);
            asyncWrite(buffer.const_asio_buffer(),
                       customAllocWriteHandler(std::bind(&ClientConnection::handleSend, shared_from_this(),
                                                         std::placeholders::_1, buffer)));
        } else {
            assert(any.type() == typeid(OpSendMsg));

            const OpSendMsg& op = boost::any_cast<const OpSendMsg&>(any);
            PairSharedBuffer buffer = Commands::newSend(outgoingBuffer_, outgoingCmd_, op.producerId_,
                                                        op.sequenceId_, getChecksumType(), op.msg_);

            asyncWrite(buffer, customAllocWriteHandler(std::bind(&ClientConnection::handleSendPair,
                                                                 shared_from_this(), std::placeholders::_1)));
        }
    } else {
        // No more pending writes
        outgoingBuffer_.reset();
    }
}

Future<Result, ResponseData> ClientConnection::sendRequestWithId(SharedBuffer cmd, int requestId) {
    Lock lock(mutex_);

    if (isClosed()) {
        lock.unlock();
        Promise<Result, ResponseData> promise;
        promise.setFailed(ResultNotConnected);
        return promise.getFuture();
    }

    PendingRequestData requestData;
    requestData.timer = executor_->createDeadlineTimer();
    requestData.timer->expires_from_now(operationsTimeout_);
    requestData.timer->async_wait(std::bind(&ClientConnection::handleRequestTimeout, shared_from_this(),
                                            std::placeholders::_1, requestData));

    pendingRequests_.insert(std::make_pair(requestId, requestData));
    lock.unlock();

    sendCommand(cmd);
    return requestData.promise.getFuture();
}

void ClientConnection::handleRequestTimeout(const boost::system::error_code& ec,
                                            PendingRequestData pendingRequestData) {
    if (!ec) {
        pendingRequestData.promise.setFailed(ResultTimeout);
    }
}

void ClientConnection::handleLookupTimeout(const boost::system::error_code& ec,
                                           LookupRequestData pendingRequestData) {
    if (!ec) {
        pendingRequestData.promise->setFailed(ResultTimeout);
    }
}

void ClientConnection::handleKeepAliveTimeout() {
    if (isClosed()) {
        return;
    }

    if (havePendingPingRequest_) {
        LOG_WARN(cnxString_ << "Forcing connection to close after keep-alive timeout");
        close();
    } else {
        // Send keep alive probe to peer
        LOG_DEBUG(cnxString_ << "Sending ping message");
        havePendingPingRequest_ = true;
        sendCommand(Commands::newPing());

        // If the close operation has already called the keepAliveTimer_.reset() then the use_count will be
        // zero And we do not attempt to dereference the pointer.
        Lock lock(mutex_);
        if (keepAliveTimer_) {
            keepAliveTimer_->expires_from_now(boost::posix_time::seconds(KeepAliveIntervalInSeconds));
            keepAliveTimer_->async_wait(
                std::bind(&ClientConnection::handleKeepAliveTimeout, shared_from_this()));
        }
        lock.unlock();
    }
}

void ClientConnection::handleConsumerStatsTimeout(const boost::system::error_code& ec,
                                                  std::vector<uint64_t> consumerStatsRequests) {
    if (ec) {
        LOG_DEBUG(cnxString_ << " Ignoring timer cancelled event, code[" << ec << "]");
        return;
    }
    startConsumerStatsTimer(consumerStatsRequests);
}

void ClientConnection::close(Result result) {
    Lock lock(mutex_);
    if (isClosed()) {
        return;
    }
    state_ = Disconnected;
    boost::system::error_code err;
    socket_->close(err);
    if (err) {
        LOG_WARN(cnxString_ << "Failed to close socket: " << err.message());
    }

    if (tlsSocket_) {
        tlsSocket_->lowest_layer().close(err);
        if (err) {
            LOG_WARN(cnxString_ << "Failed to close TLS socket: " << err.message());
        }
    }

    if (executor_) {
        executor_.reset();
    }

    // Move the internal fields to process them after `mutex_` was unlocked
    auto consumers = std::move(consumers_);
    auto producers = std::move(producers_);
    auto pendingRequests = std::move(pendingRequests_);
    auto pendingLookupRequests = std::move(pendingLookupRequests_);
    auto pendingConsumerStatsMap = std::move(pendingConsumerStatsMap_);
    auto pendingGetLastMessageIdRequests = std::move(pendingGetLastMessageIdRequests_);
    auto pendingGetNamespaceTopicsRequests = std::move(pendingGetNamespaceTopicsRequests_);

    numOfPendingLookupRequest_ = 0;

    if (keepAliveTimer_) {
        keepAliveTimer_->cancel();
        keepAliveTimer_.reset();
    }

    if (consumerStatsRequestTimer_) {
        consumerStatsRequestTimer_->cancel();
        consumerStatsRequestTimer_.reset();
    }

    connectTimeoutTask_->stop();

    lock.unlock();
    LOG_INFO(cnxString_ << "Connection closed");

    for (ProducersMap::iterator it = producers.begin(); it != producers.end(); ++it) {
        HandlerBase::handleDisconnection(ResultConnectError, shared_from_this(), it->second);
    }

    for (ConsumersMap::iterator it = consumers.begin(); it != consumers.end(); ++it) {
        HandlerBase::handleDisconnection(ResultConnectError, shared_from_this(), it->second);
    }

    connectPromise_.setFailed(result);

    // Fail all pending requests, all these type are map whose value type contains the Promise object
    for (auto& kv : pendingRequests) {
        kv.second.promise.setFailed(ResultConnectError);
    }
    for (auto& kv : pendingLookupRequests) {
        kv.second.promise->setFailed(ResultConnectError);
    }
    for (auto& kv : pendingConsumerStatsMap) {
        LOG_ERROR(cnxString_ << " Closing Client Connection, please try again later");
        kv.second.setFailed(ResultConnectError);
    }
    for (auto& kv : pendingGetLastMessageIdRequests) {
        kv.second.setFailed(ResultConnectError);
    }
    for (auto& kv : pendingGetNamespaceTopicsRequests) {
        kv.second.setFailed(ResultConnectError);
    }
}

bool ClientConnection::isClosed() const { return state_ == Disconnected; }

Future<Result, ClientConnectionWeakPtr> ClientConnection::getConnectFuture() {
    return connectPromise_.getFuture();
}

void ClientConnection::registerProducer(int producerId, ProducerImplPtr producer) {
    Lock lock(mutex_);
    producers_.insert(std::make_pair(producerId, producer));
}

void ClientConnection::registerConsumer(int consumerId, ConsumerImplPtr consumer) {
    Lock lock(mutex_);
    consumers_.insert(std::make_pair(consumerId, consumer));
}

void ClientConnection::removeProducer(int producerId) {
    Lock lock(mutex_);
    producers_.erase(producerId);
}

void ClientConnection::removeConsumer(int consumerId) {
    Lock lock(mutex_);
    consumers_.erase(consumerId);
}

const std::string& ClientConnection::brokerAddress() const { return physicalAddress_; }

const std::string& ClientConnection::cnxString() const { return cnxString_; }

int ClientConnection::getServerProtocolVersion() const { return serverProtocolVersion_; }

int32_t ClientConnection::getMaxMessageSize() { return maxMessageSize_.load(std::memory_order_acquire); }

Commands::ChecksumType ClientConnection::getChecksumType() const {
    return getServerProtocolVersion() >= proto::v6 ? Commands::Crc32c : Commands::None;
}

Future<Result, MessageId> ClientConnection::newGetLastMessageId(uint64_t consumerId, uint64_t requestId) {
    Lock lock(mutex_);
    Promise<Result, MessageId> promise;
    if (isClosed()) {
        lock.unlock();
        LOG_ERROR(cnxString_ << " Client is not connected to the broker");
        promise.setFailed(ResultNotConnected);
        return promise.getFuture();
    }

    pendingGetLastMessageIdRequests_.insert(std::make_pair(requestId, promise));
    lock.unlock();
    sendRequestWithId(Commands::newGetLastMessageId(consumerId, requestId), requestId)
        .addListener([promise](Result result, const ResponseData& data) {
            if (result != ResultOk) {
                promise.setFailed(result);
            }
        });
    return promise.getFuture();
}

Future<Result, NamespaceTopicsPtr> ClientConnection::newGetTopicsOfNamespace(const std::string& nsName,
                                                                             uint64_t requestId) {
    Lock lock(mutex_);
    Promise<Result, NamespaceTopicsPtr> promise;
    if (isClosed()) {
        lock.unlock();
        LOG_ERROR(cnxString_ << "Client is not connected to the broker");
        promise.setFailed(ResultNotConnected);
        return promise.getFuture();
    }

    pendingGetNamespaceTopicsRequests_.insert(std::make_pair(requestId, promise));
    lock.unlock();
    sendCommand(Commands::newGetTopicsOfNamespace(nsName, requestId));
    return promise.getFuture();
}

}  // namespace pulsar
