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

#include "ClientConnection.h"

#include "PulsarApi.pb.h"

#include <boost/shared_ptr.hpp>
#include <boost/array.hpp>
#include <iostream>
#include <algorithm>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <climits>

#include "ExecutorService.h"
#include "Commands.h"
#include "LogUtils.h"
#include "Url.h"

#include <boost/bind.hpp>

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
    }
    // NOTE : Do not add default case in the switch above. In future if we get new cases for
    // ServerError and miss them in the switch above we would like to get notified. Adding
    // return here to make the compiler happy.
    return ResultUnknownError;
}

ClientConnection::ClientConnection(const std::string& endpoint, ExecutorServicePtr executor,
                                   const ClientConfiguration& clientConfiguration, const AuthenticationPtr& authentication)
        : state_(Pending),
          operationsTimeout_(seconds(clientConfiguration.getOperationTimeoutSeconds())),
          authentication_(authentication),
          serverProtocolVersion_(ProtocolVersion_MIN),
          executor_(executor),
          resolver_(executor->createTcpResolver()),
          socket_(executor->createSocket()),
          address_(endpoint),
          cnxString_("[<none> -> " + endpoint + "] "),
          error_(boost::system::error_code()),
          incomingBuffer_(SharedBuffer::allocate(DefaultBufferSize)),
          incomingCmd_(),
          pendingWriteBuffers_(),
          pendingWriteOperations_(0),
          outgoingBuffer_(SharedBuffer::allocate(DefaultBufferSize)),
          outgoingCmd_(),
          havePendingPingRequest_(false),
          keepAliveTimer_() {
}

ClientConnection::~ClientConnection() {
    LOG_INFO(cnxString_ << "Destroyed connection");
}

void ClientConnection::handlePulsarConnected(const CommandConnected& cmdConnected) {
    if (!cmdConnected.has_server_version()) {
        LOG_ERROR(cnxString_ << "Server version is not set");
        close();
        return;
    }

    state_ = Ready;
    serverProtocolVersion_ = cmdConnected.protocol_version();
    connectPromise_.setValue(shared_from_this());

    if (serverProtocolVersion_ >= v1) {
        // Only send keep-alive probes if the broker supports it
        keepAliveTimer_ = executor_->createDeadlineTimer();
        keepAliveTimer_->expires_from_now(boost::posix_time::seconds(KeepAliveIntervalInSeconds));
        keepAliveTimer_->async_wait(
                boost::bind(&ClientConnection::handleKeepAliveTimeout, shared_from_this()));
    }
}

/// The number of unacknowledged probes to send before considering the connection dead and notifying the application layer
typedef boost::asio::detail::socket_option::integer<BOOST_ASIO_OS_DEF(IPPROTO_TCP), TCP_KEEPCNT> tcp_keep_alive_count;

/// The interval between subsequential keepalive probes, regardless of what the connection has exchanged in the meantime
typedef boost::asio::detail::socket_option::integer<BOOST_ASIO_OS_DEF(IPPROTO_TCP), TCP_KEEPINTVL> tcp_keep_alive_interval;

/// The interval between the last data packet sent (simple ACKs are not considered data) and the first keepalive
/// probe; after the connection is marked to need keepalive, this counter is not used any further
#ifdef __APPLE__
  typedef boost::asio::detail::socket_option::integer<BOOST_ASIO_OS_DEF(IPPROTO_TCP), TCP_KEEPALIVE> tcp_keep_alive_idle;
#else
  typedef boost::asio::detail::socket_option::integer<BOOST_ASIO_OS_DEF(IPPROTO_TCP), TCP_KEEPIDLE> tcp_keep_alive_idle;
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
        cnxStringStream << "[" << socket_->local_endpoint() << " -> " << socket_->remote_endpoint() << "] ";
        cnxString_ = cnxStringStream.str();

        LOG_INFO(cnxString_ << "Connected to broker");
        state_ = TcpConnected;
        socket_->set_option(tcp::no_delay(true));

        socket_->set_option(tcp::socket::keep_alive(true));

        // Start TCP keep-alive probes after connection has been idle after 1 minute. Ideally this
        // should never happen, given that we're sending our own keep-alive probes (within the TCP
        // connection) every 30 seconds
        socket_->set_option(tcp_keep_alive_idle(1 * 60));

        // Send up to 10 probes before declaring the connection broken
        socket_->set_option(tcp_keep_alive_count(10));

        // Interval between probes: 6 seconds
        socket_->set_option(tcp_keep_alive_interval(6));

        SharedBuffer buffer = Commands::newConnect(authentication_);
        // Send CONNECT command to broker
        boost::asio::async_write(
                *socket_,
                buffer.const_asio_buffer(),
                boost::bind(&ClientConnection::handleSentPulsarConnect, shared_from_this(),
                            boost::asio::placeholders::error, buffer));
    } else if (endpointIterator != tcp::resolver::iterator()) {
        // The connection failed. Try the next endpoint in the list.
        socket_->close();
        tcp::endpoint endpoint = *endpointIterator;
        socket_->async_connect(
                endpoint,
                boost::bind(&ClientConnection::handleTcpConnected, shared_from_this(),
                            boost::asio::placeholders::error, ++endpointIterator));
    } else {
        LOG_ERROR(cnxString_ << "Failed to establish connection: " << err.message());
        close();
        return;

    }

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

/*
 * Async method to establish TCP connection with broker
 *
 * tcpConnectCompletionHandler is notified when the result of this call is available.
 *
 */
void ClientConnection::tcpConnectAsync() {
    boost::system::error_code err;
    Url service_url;
    if (!Url::parse(address_, service_url)) {
        LOG_ERROR(cnxString_ << "Invalid Url, unable to parse: " << err << " " << err.message());
        close();
        return;
    }

    if (service_url.protocol() != "pulsar" && service_url.protocol() != "pulsar+ssl") {
        LOG_ERROR(cnxString_ << "Invalid Url protocol '" <<  service_url.protocol() << "'. Valid values are 'pulsar' and 'pulsar+ssl'");
        close();
        return;
    }

    LOG_DEBUG(cnxString_ << "Connecting to " << service_url.host() << ":" << service_url.port());
    tcp::resolver::query query(service_url.host(),
                               boost::lexical_cast<std::string>(service_url.port()));
    resolver_->async_resolve(
            query,
            boost::bind(&ClientConnection::handleResolve, shared_from_this(),
                        boost::asio::placeholders::error, boost::asio::placeholders::iterator));
}

void ClientConnection::handleResolve(const boost::system::error_code& err,
                                     tcp::resolver::iterator endpointIterator) {
    if (err) {
        LOG_ERROR(cnxString_ << "Resolve error: " << err << " : " << err.message());
        close();
        return;
    }

    if (endpointIterator != tcp::resolver::iterator()) {
        LOG_DEBUG(cnxString_ << "Resolved hostname " << endpointIterator->host_name()  //
                << " to " << endpointIterator->endpoint());
        socket_->async_connect(
                *endpointIterator++,
                boost::bind(&ClientConnection::handleTcpConnected, shared_from_this(),
                            boost::asio::placeholders::error, endpointIterator));
    } else {
        LOG_WARN(cnxString_ << "No IP address found");
        close();
        return;
    }
}

void ClientConnection::readNextCommand() {
    const static uint32_t minReadSize = sizeof(uint32_t);
    socket_->async_receive(
            incomingBuffer_.asio_buffer(),
            customAllocReadHandler(
                    boost::bind(&ClientConnection::handleRead, shared_from_this(), _1, _2,
                                minReadSize)));
}

void ClientConnection::handleRead(const boost::system::error_code& err, size_t bytesTransferred,
                                  uint32_t minReadSize) {
    // Update buffer write idx with new data
    incomingBuffer_.bytesWritten(bytesTransferred);

    if (err || bytesTransferred == 0) {
        close();
    } else if (bytesTransferred < minReadSize) {
        // Read the remaining part, use a slice of buffer to write on the next
        // region
        SharedBuffer buffer = incomingBuffer_.slice(bytesTransferred);
        socket_->async_receive(
                buffer.asio_buffer(),
                customAllocReadHandler(
                        boost::bind(&ClientConnection::handleRead, shared_from_this(), _1, _2,
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
                socket_->async_receive(
                        incomingBuffer_.asio_buffer(),
                        customAllocReadHandler(
                                boost::bind(&ClientConnection::handleRead, shared_from_this(), _1,
                                            _2, bytesToReceive)));
                return;
            } else {
                // Need to allocate a buffer big enough for the frame
                uint32_t newBufferSize = std::max<uint32_t>(DefaultBufferSize,
                                                            frameSize + sizeof(uint32_t));
                incomingBuffer_ = SharedBuffer::copyFrom(incomingBuffer_, newBufferSize);

                socket_->async_receive(
                        incomingBuffer_.asio_buffer(),
                        customAllocReadHandler(
                                boost::bind(&ClientConnection::handleRead, shared_from_this(), _1,
                                            _2, bytesToReceive)));
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

            //read checksum
            bool isChecksumValid = verifyChecksum(incomingBuffer_, incomingCmd_);

            uint32_t metadataSize = incomingBuffer_.readUnsignedInt();
            if (!msgMetadata.ParseFromArray(incomingBuffer_.data(), metadataSize)) {
                LOG_ERROR(cnxString_ << "[consumer id " << incomingCmd_.message().consumer_id()  //
                        << ", message ledger id " << incomingCmd_.message().message_id().ledgerid() //
                        << ", entry id " << incomingCmd_.message().message_id().entryid() << "] Error parsing message metadata");
                close();
                return;
            }

            incomingBuffer_.consume(metadataSize);

            uint32_t payloadSize = frameSize - (cmdSize + 4) - (metadataSize + 4);
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

        socket_->async_receive(
                incomingBuffer_.asio_buffer(),
                customAllocReadHandler(
                        boost::bind(&ClientConnection::handleRead, shared_from_this(), _1, _2,
                                    minReadSize)));
        return;
    }

    // We have read everything we had in the buffer
    // Rollback the indexes to reuse the same buffer
    incomingBuffer_.reset();

    readNextCommand();
}

bool ClientConnection::verifyChecksum(SharedBuffer& incomingBuffer_, proto::BaseCommand& incomingCmd_) {
    int readerIndex = incomingBuffer_.readerIndex();
    bool isChecksumValid = true;
    if (incomingBuffer_.readUnsignedShort() == Commands::magicCrc32c) {
        uint32_t storedChecksum = incomingBuffer_.readUnsignedInt();
        // compute metadata-payload checksum
        int metadataPayloadSize = incomingBuffer_.readableBytes();
        uint32_t computedChecksum = computeChecksum(0, incomingBuffer_.data(), metadataPayloadSize);
        // verify checksum
        isChecksumValid = (storedChecksum == computedChecksum);

        if (!isChecksumValid) {
            LOG_ERROR("[consumer id " << incomingCmd_.message().consumer_id()  //
                    << ", message ledger id " << incomingCmd_.message().message_id().ledgerid()//
                    << ", entry id " << incomingCmd_.message().message_id().entryid()//
                    << "stored-checksum" << storedChecksum << "computedChecksum" << computedChecksum//
                    << "] Checksum verification failed");
        }
    } else {
        incomingBuffer_.setReaderIndex(readerIndex);
    }
    return isChecksumValid;
}

void ClientConnection::handleIncomingMessage(const proto::CommandMessage& msg, bool isChecksumValid,
                                             proto::MessageMetadata& msgMetadata,
                                             SharedBuffer& payload) {
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
            LOG_DEBUG(
                    cnxString_ << "Ignoring incoming message for already destroyed consumer " << msg.consumer_id());
        }
    } else {
        LOG_DEBUG(cnxString_ << "Got invalid consumer Id in "            //
                << msg.consumer_id() << " -- msg: "<< msgMetadata.sequence_id());
    }
}

void ClientConnection::handleIncomingCommand() {
    LOG_DEBUG(
            cnxString_ << "Handling incoming command: " << Commands::messageType(incomingCmd_.type()));

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

                    LOG_DEBUG(
                            cnxString_ << "Got receipt for producer: " << producerId << " -- msg: "<< sequenceId);

                    Lock lock(mutex_);
                    ProducersMap::iterator it = producers_.find(producerId);
                    if (it != producers_.end()) {
                        ProducerImplPtr producer = it->second.lock();
                        lock.unlock();

                        if (producer) {
                            if (!producer->ackReceived(sequenceId)) {
                                // If the producer fails to process the ack, we need to close the connection to give it a chance to recover from there
                                close();
                            }
                        }
                    } else {
                        LOG_ERROR(cnxString_ << "Got invalid producer Id in SendReceipt: "  //
                                << producerId << " -- msg: "<< sequenceId);
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
                                    // If the producer fails to remove corrupt msg, we need to close the connection to give it a chance to recover from there
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
                    LOG_DEBUG(
                            cnxString_ << "Received success response from server. req_id: " << success.request_id());

                    Lock lock(mutex_);
                    PendingRequestsMap::iterator it = pendingRequests_.find(success.request_id());
                    if (it != pendingRequests_.end()) {
                        PendingRequestData requestData = it->second;
                        pendingRequests_.erase(it);
                        lock.unlock();

                        requestData.promise.setValue("");
                        requestData.timer->cancel();
                    }
                    break;
                }

                case BaseCommand::PARTITIONED_METADATA_RESPONSE: {
                    const CommandPartitionedTopicMetadataResponse& partitionMetadataResponse =
                            incomingCmd_.partitionmetadataresponse();
                    LOG_DEBUG(
                            cnxString_ << "Received partition-metadata response from server. req_id: " << partitionMetadataResponse.request_id());

                    Lock lock(mutex_);
                    PendingLookupRequestsMap::iterator it = pendingLookupRequests_.find(
                            partitionMetadataResponse.request_id());
                    if (it != pendingLookupRequests_.end()) {
                        LookupDataResultPromisePtr lookupDataPromise = it->second;
                        pendingLookupRequests_.erase(it);
                        lock.unlock();

                        if (!partitionMetadataResponse.has_response()
                                || (partitionMetadataResponse.response()
                                        == CommandPartitionedTopicMetadataResponse::Failed)) {
                            if (partitionMetadataResponse.has_error()) {
                                LOG_ERROR(
                                        cnxString_ << "Failed partition-metadata lookup req_id: " << partitionMetadataResponse.request_id() << " error: " << partitionMetadataResponse.error());
                            } else {
                                LOG_ERROR(
                                        cnxString_ << "Failed partition-metadata lookup req_id: " << partitionMetadataResponse.request_id() << " with empty response: ");
                            }
                            lookupDataPromise->setFailed(ResultConnectError);
                        } else {
                            LookupDataResultPtr lookupResultPtr = boost::make_shared<
                                    LookupDataResult>();
                            lookupResultPtr->setPartitions(partitionMetadataResponse.partitions());
                            lookupDataPromise->setValue(lookupResultPtr);
                        }

                    } else {
                        LOG_WARN(
                                "Received unknown request id from server: " << partitionMetadataResponse.request_id());
                    }
                    break;
                }

                case BaseCommand::LOOKUP_RESPONSE: {
                    const CommandLookupTopicResponse& lookupTopicResponse = incomingCmd_.lookuptopicresponse();
                    LOG_DEBUG(
                            cnxString_ << "Received lookup response from server. req_id: " << lookupTopicResponse.request_id());

                    Lock lock(mutex_);
                    PendingLookupRequestsMap::iterator it = pendingLookupRequests_.find(
                            lookupTopicResponse.request_id());
                    if (it != pendingLookupRequests_.end()) {
                        LookupDataResultPromisePtr lookupDataPromise = it->second;
                        pendingLookupRequests_.erase(it);
                        lock.unlock();

                        if (!lookupTopicResponse.has_response()
                                || (lookupTopicResponse.response()
                                        == CommandLookupTopicResponse::Failed)) {
                            if (lookupTopicResponse.has_error()) {
                                LOG_ERROR(
                                        cnxString_ << "Failed lookup req_id: " << lookupTopicResponse.request_id() << " error: " << lookupTopicResponse.error());
                            } else {
                                LOG_ERROR(
                                        cnxString_ << "Failed lookup req_id: " << lookupTopicResponse.request_id() << " with empty response: ");
                            }
                            lookupDataPromise -> setFailed(ResultConnectError);
                        } else {
                            LOG_DEBUG(
                                    cnxString_ << "Received lookup response from server. req_id: " << lookupTopicResponse.request_id()  //
                                    << " -- broker-url: " << lookupTopicResponse.brokerserviceurl() << " -- broker-tls-url: "//
                                    << lookupTopicResponse.brokerserviceurltls() << " authoritative: " << lookupTopicResponse.authoritative()//
                                    << " redirect: " << lookupTopicResponse.response());
                            LookupDataResultPtr lookupResultPtr =
                                                            boost::make_shared<LookupDataResult>();
                            lookupResultPtr->setBrokerUrl(lookupTopicResponse.brokerserviceurl());
                            lookupResultPtr->setBrokerUrlSsl(
                                    lookupTopicResponse.brokerserviceurltls());
                            lookupResultPtr->setAuthoritative(lookupTopicResponse.authoritative());
                            lookupResultPtr->setRedirect(
                                    lookupTopicResponse.response()
                                            == CommandLookupTopicResponse::Redirect);
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
                    LOG_DEBUG(
                            cnxString_ << "Received success producer response from server. req_id: " << producerSuccess.request_id()  //
                            << " -- producer name: " << producerSuccess.producer_name());

                    Lock lock(mutex_);
                    PendingRequestsMap::iterator it = pendingRequests_.find(
                            producerSuccess.request_id());
                    if (it != pendingRequests_.end()) {
                        PendingRequestData requestData = it->second;
                        pendingRequests_.erase(it);
                        lock.unlock();

                        requestData.promise.setValue(producerSuccess.producer_name());
                        requestData.timer->cancel();
                    }
                    break;
                }

                case BaseCommand::ERROR: {
                    const CommandError& error = incomingCmd_.error();
                    Result result = getResult(error.error());
                    LOG_WARN(
                            cnxString_ << "Received error response from server: " << result << " -- req_id: "<< error.request_id());

                    Lock lock(mutex_);
                    PendingRequestsMap::iterator it = pendingRequests_.find(error.request_id());
                    if (it != pendingRequests_.end()) {
                        PendingRequestData requestData = it->second;
                        pendingRequests_.erase(it);
                        lock.unlock();

                        requestData.promise.setFailed(getResult(error.error()));
                        requestData.timer->cancel();
                    } else {
                        lock.unlock();
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
                        lock.unlock();

                        if (producer) {
                            producer->disconnectProducer();
                        }
                    } else {
                        LOG_ERROR(
                                cnxString_ << "Got invalid producer Id in closeProducer command: "<< producerId);
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
                        lock.unlock();

                        if (consumer) {
                            consumer->disconnectConsumer();
                        }
                    } else {
                        LOG_ERROR(
                                cnxString_ << "Got invalid consumer Id in closeConsumer command: "<< consumerId);
                    }

                    break;
                }

                case BaseCommand::PING: {
                    // Respond to ping request
                    LOG_DEBUG(cnxString_  << "Replying to ping command");
                    sendCommand(Commands::newPong());
                    break;
                }

                case BaseCommand::PONG: {
                    LOG_DEBUG(cnxString_ << "Received response to ping message");
                    havePendingPingRequest_ = false;
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

void ClientConnection::newTopicLookup(const std::string& destinationName, bool authoritative,
                                      const uint64_t requestId,
                                      LookupDataResultPromisePtr promise) {
    newLookup(Commands::newLookup(outgoingCmd_, destinationName, authoritative, requestId),
              requestId, promise);
}

void ClientConnection::newPartitionedMetadataLookup(const std::string& destinationName,
                                                    const uint64_t requestId,
                                                    LookupDataResultPromisePtr promise) {
    newLookup(Commands::newPartitionMetadataRequest(outgoingCmd_, destinationName, requestId), requestId,
              promise);
}

void ClientConnection::newLookup(const SharedBuffer& cmd, const uint64_t requestId,
                                 LookupDataResultPromisePtr promise) {
    Lock lock(mutex_);
    boost::shared_ptr<LookupDataResultPtr> lookupDataResult;
    lookupDataResult = boost::make_shared<LookupDataResultPtr>();
    if (isClosed()) {
        lock.unlock();
        promise->setFailed(ResultNotConnected);
    }
    pendingLookupRequests_.insert(std::make_pair(requestId, promise));
    lock.unlock();
    sendCommand(cmd);
}

void ClientConnection::sendCommand(const SharedBuffer& cmd) {
    Lock lock(mutex_);

    if (pendingWriteOperations_++ == 0) {

        // Write immediately to socket
        boost::asio::async_write(
                *socket_,
                cmd.const_asio_buffer(),
                customAllocWriteHandler(
                        boost::bind(&ClientConnection::handleSend, shared_from_this(), _1, cmd)));
    } else {
        // Queue to send later
        pendingWriteBuffers_.push_back(cmd);
    }
}

void ClientConnection::sendMessage(const OpSendMsg& opSend) {
    Lock lock(mutex_);

    if (pendingWriteOperations_++ == 0) {

        PairSharedBuffer buffer = Commands::newSend(outgoingBuffer_, outgoingCmd_,
                                                    opSend.producerId_, opSend.sequenceId_,
                                                    getChecksumType(), opSend.msg_);

        // Write immediately to socket
        boost::asio::async_write(
                *socket_,
                buffer,
                customAllocWriteHandler(
                        boost::bind(&ClientConnection::handleSendPair, shared_from_this(), _1)));
    } else {
        // Queue to send later
        pendingWriteBuffers_.push_back(opSend);
    }
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
            boost::asio::async_write(
                    *socket_,
                    buffer.const_asio_buffer(),
                    customAllocWriteHandler(
                            boost::bind(&ClientConnection::handleSend, shared_from_this(), _1,
                                        buffer)));
        } else {
            assert(any.type() == typeid(OpSendMsg));

            const OpSendMsg& op = boost::any_cast<const OpSendMsg&>(any);
            PairSharedBuffer buffer = Commands::newSend(outgoingBuffer_, outgoingCmd_,
                                                        op.producerId_, op.sequenceId_,
                                                        getChecksumType(), op.msg_);

            boost::asio::async_write(
                    *socket_,
                    buffer,
                    customAllocWriteHandler(
                            boost::bind(&ClientConnection::handleSendPair, shared_from_this(),
                                        _1)));
        }
    } else {
        // No more pending writes
        outgoingBuffer_.reset();
    }
}

Future<Result, std::string> ClientConnection::sendRequestWithId(SharedBuffer cmd, int requestId) {
    Lock lock(mutex_);

    if (isClosed()) {
        lock.unlock();
        Promise<Result, std::string> promise;
        promise.setFailed(ResultNotConnected);
        return promise.getFuture();
    }

    PendingRequestData requestData;
    requestData.timer = executor_->createDeadlineTimer();
    requestData.timer->expires_from_now(operationsTimeout_);
    requestData.timer->async_wait(
            boost::bind(&ClientConnection::handleRequestTimeout, shared_from_this(), _1,
                        requestData));

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

        keepAliveTimer_->expires_from_now(boost::posix_time::seconds(KeepAliveIntervalInSeconds));
        keepAliveTimer_->async_wait(
                boost::bind(&ClientConnection::handleKeepAliveTimeout, shared_from_this()));
    }
}

void ClientConnection::close() {
    Lock lock(mutex_);
    state_ = Disconnected;
    boost::system::error_code err;
    socket_->close(err);
    lock.unlock();

    LOG_INFO(cnxString_ << "Connection closed");

    if (keepAliveTimer_) {
        keepAliveTimer_->cancel();
    }

    for (ProducersMap::iterator it = producers_.begin(); it != producers_.end(); ++it ) {
    	HandlerBase::handleDisconnection(ResultConnectError, shared_from_this(), it->second);
    }

    for (ConsumersMap::iterator it = consumers_.begin(); it != consumers_.end(); ++it ) {
        HandlerBase::handleDisconnection(ResultConnectError, shared_from_this(), it->second);
    }

    connectPromise_.setFailed(ResultConnectError);

    // Fail all pending operations on the connection
    for (PendingRequestsMap::iterator it = pendingRequests_.begin(); it != pendingRequests_.end(); ++it) {
        it->second.promise.setFailed(ResultConnectError);
    }

    // Fail all pending lookup-requests on the connection
    for (PendingLookupRequestsMap::iterator it = pendingLookupRequests_.begin(); it != pendingLookupRequests_.end(); ++it) {
        it->second->setFailed(ResultConnectError);
    }
}

bool ClientConnection::isClosed() const {
    return state_ == Disconnected;
}

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

const std::string& ClientConnection::brokerAddress() const {
    return address_;
}

const std::string& ClientConnection::cnxString() const {
    return cnxString_;
}

int ClientConnection::getServerProtocolVersion() const {
    return serverProtocolVersion_;
}

Commands::ChecksumType ClientConnection::getChecksumType() const {
    return getServerProtocolVersion() >= proto::v6 ?
            Commands::Crc32c : Commands::None;
}

}
