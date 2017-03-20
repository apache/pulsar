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
package com.yahoo.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkArgument;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.impl.BinaryProtoLookupService.LookupDataResult;
import com.yahoo.pulsar.common.api.Commands;
import com.yahoo.pulsar.common.api.PulsarHandler;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandCloseConsumer;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandCloseProducer;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandConnected;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandError;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandMessage;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadataResponse;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandProducerSuccess;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSendError;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSendReceipt;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSuccess;
import com.yahoo.pulsar.common.api.proto.PulsarApi.ServerError;
import com.yahoo.pulsar.common.util.collections.ConcurrentLongHashMap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Promise;

public class ClientCnx extends PulsarHandler {

    private final Authentication authentication;
    private State state;

    private final ConcurrentLongHashMap<CompletableFuture<String>> pendingRequests = new ConcurrentLongHashMap<>(16, 1);
    private final ConcurrentLongHashMap<CompletableFuture<LookupDataResult>> pendingLookupRequests = new ConcurrentLongHashMap<>(16, 1);
    private final ConcurrentLongHashMap<ProducerImpl> producers = new ConcurrentLongHashMap<>(16, 1);
    private final ConcurrentLongHashMap<ConsumerImpl> consumers = new ConcurrentLongHashMap<>(16, 1);

    private final CompletableFuture<Void> connectionFuture = new CompletableFuture<Void>();
    private final Semaphore pendingLookupRequestSemaphore;
    private final EventLoopGroup eventLoopGroup;
    private static final AtomicIntegerFieldUpdater<ClientCnx> NUMBER_OF_REJECTED_REQUESTS_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(ClientCnx.class, "numberOfRejectRequests");
    private volatile int numberOfRejectRequests = 0;
    private final int maxNumberOfRejectedRequestPerConnection;
    private final int rejectedRequestResetTimeSec = 60;

    enum State {
        None, SentConnectFrame, Ready
    }

    public ClientCnx(PulsarClientImpl pulsarClient) {
        super(30, TimeUnit.SECONDS);
        this.pendingLookupRequestSemaphore = new Semaphore(pulsarClient.getConfiguration().getConcurrentLookupRequest(),
                true);
        this.authentication = pulsarClient.getConfiguration().getAuthentication();
        this.eventLoopGroup = pulsarClient.eventLoopGroup();
        this.maxNumberOfRejectedRequestPerConnection = pulsarClient.getConfiguration().getMaxNumberOfRejectedRequestPerConnection();
        this.state = State.None;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        log.info("{} Connected to broker", ctx.channel());
        String authData = "";
        if (authentication.getAuthData().hasDataFromCommand()) {
            authData = authentication.getAuthData().getCommandData();
        }
        // Send CONNECT command
        ctx.writeAndFlush(Commands.newConnect(authentication.getAuthMethodName(), authData)).addListener(future -> {
            if (future.isSuccess()) {
                if (log.isDebugEnabled()) {
                    log.debug("Complete: {}", future.isSuccess());
                }
                state = State.SentConnectFrame;
            } else {
                log.warn("Error during handshake", future.cause());
                ctx.close();
            }
        });
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("{} Disconnected", ctx.channel());
        if (!connectionFuture.isDone()) {
            connectionFuture.completeExceptionally(new PulsarClientException("Connection already closed"));
        }

        PulsarClientException e = new PulsarClientException(
                "Disconnected from server at " + ctx.channel().remoteAddress());

        // Fail out all the pending ops
        pendingRequests.forEach((key, future) -> future.completeExceptionally(e));
        pendingLookupRequests.forEach((key, future) -> future.completeExceptionally(e));

        // Notify all attached producers/consumers so they have a chance to reconnect
        producers.forEach((id, producer) -> producer.connectionClosed(this));
        consumers.forEach((id, consumer) -> consumer.connectionClosed(this));
    }

    // Command Handlers

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn("{} Exception caught: {}", ctx.channel(), cause.getMessage(), cause);
        ctx.close();
    }

    @Override
    protected void handleConnected(CommandConnected connected) {
        checkArgument(state == State.SentConnectFrame);

        log.info("{} Connection is ready", ctx.channel());
        connectionFuture.complete(null);
        remoteEndpointProtocolVersion = connected.getProtocolVersion();
        state = State.Ready;
    }

    @Override
    protected void handleSendReceipt(CommandSendReceipt sendReceipt) {
        checkArgument(state == State.Ready);

        long producerId = sendReceipt.getProducerId();
        long sequenceId = sendReceipt.getSequenceId();
        long ledgerId = -1;
        long entryId = -1;
        if (sendReceipt.hasMessageId()) {
            ledgerId = sendReceipt.getMessageId().getLedgerId();
            entryId = sendReceipt.getMessageId().getEntryId();
        }

        if (log.isDebugEnabled()) {
            log.debug("{} Got receipt for producer: {} -- msg: {} -- id: {}:{}", ctx.channel(), producerId, sequenceId,
                    ledgerId, entryId);
        }

        producers.get(producerId).ackReceived(this, sequenceId, ledgerId, entryId);
    }

    @Override
    protected void handleMessage(CommandMessage cmdMessage, ByteBuf headersAndPayload) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received a message from the server: {}", ctx.channel(), cmdMessage);
        }
        ConsumerImpl consumer = consumers.get(cmdMessage.getConsumerId());
        if (consumer != null) {
            consumer.messageReceived(cmdMessage.getMessageId(), headersAndPayload, this);
        }
    }

    @Override
    protected void handleSuccess(CommandSuccess success) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received success response from server: {}", ctx.channel(), success.getRequestId());
        }
        long requestId = success.getRequestId();
        CompletableFuture<String> requestFuture = pendingRequests.remove(requestId);
        if (requestFuture != null) {
            requestFuture.complete(null);
        } else {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), success.getRequestId());
        }
    }

    @Override
    protected void handleProducerSuccess(CommandProducerSuccess success) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received producer success response from server: {} - producer-name: {}", ctx.channel(),
                    success.getRequestId(), success.getProducerName());
        }
        long requestId = success.getRequestId();
        CompletableFuture<String> requestFuture = pendingRequests.remove(requestId);
        if (requestFuture != null) {
            requestFuture.complete(success.getProducerName());
        } else {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), success.getRequestId());
        }
    }

    @Override
    protected void handleLookupResponse(CommandLookupTopicResponse lookupResult) {
        log.info("Received Broker lookup response: {}", lookupResult.getResponse());

        long requestId = lookupResult.getRequestId();
        CompletableFuture<LookupDataResult> requestFuture = getAndRemovePendingLookupRequest(requestId);

        if (requestFuture != null) {
            // Complete future with exception if : Result.response=fail/null
            if (!lookupResult.hasResponse() || CommandLookupTopicResponse.LookupType.Failed.equals(lookupResult.getResponse())) {
                if (lookupResult.hasError()) {
                    checkServerError(lookupResult.getError(), lookupResult.getMessage());
                    requestFuture.completeExceptionally(
                            getPulsarClientException(lookupResult.getError(), lookupResult.getMessage()));
                } else {
                    requestFuture
                            .completeExceptionally(new PulsarClientException.LookupException("Empty lookup response"));
                }
            } else {
                // return LookupDataResult when Result.response = connect/redirect
                boolean redirect = CommandLookupTopicResponse.LookupType.Redirect.equals(lookupResult.getResponse());
                requestFuture.complete(new LookupDataResult(lookupResult.getBrokerServiceUrl(),
                        lookupResult.getBrokerServiceUrlTls(), redirect, lookupResult.getAuthoritative()));
            }
        } else {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), lookupResult.getRequestId());
        }
    }

    @Override
    protected void handlePartitionResponse(CommandPartitionedTopicMetadataResponse lookupResult) {
        log.info("Received Broker Partition response: {}", lookupResult.getPartitions());

        long requestId = lookupResult.getRequestId();
        CompletableFuture<LookupDataResult> requestFuture = getAndRemovePendingLookupRequest(requestId);

        if (requestFuture != null) {
            // Complete future with exception if : Result.response=fail/null
            if (!lookupResult.hasResponse() || CommandPartitionedTopicMetadataResponse.LookupType.Failed.equals(lookupResult.getResponse())) {
                if (lookupResult.hasError()) {
                    checkServerError(lookupResult.getError(), lookupResult.getMessage());
                    requestFuture.completeExceptionally(
                            getPulsarClientException(lookupResult.getError(), lookupResult.getMessage()));
                } else {
                    requestFuture
                            .completeExceptionally(new PulsarClientException.LookupException("Empty lookup response"));
                }
            } else {
                // return LookupDataResult when Result.response = success/redirect
                requestFuture.complete(new LookupDataResult(lookupResult.getPartitions()));
            }
        } else {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), lookupResult.getRequestId());
        }
    }

    private boolean addPendingLookupRequests(long requestId, CompletableFuture<LookupDataResult> future) {
        if (pendingLookupRequestSemaphore.tryAcquire()) {
            pendingLookupRequests.put(requestId, future);
            return true;
        }
        return false;
    }

    private CompletableFuture<LookupDataResult> getAndRemovePendingLookupRequest(long requestId) {
        CompletableFuture<LookupDataResult> result = pendingLookupRequests.remove(requestId);
        if (result != null) {
            pendingLookupRequestSemaphore.release();
        }
        return result;
    }

    @Override
    protected void handleSendError(CommandSendError sendError) {
        log.warn("{} Received send error from server: {}", ctx.channel(), sendError);
        if (ServerError.ChecksumError.equals(sendError.getError())) {
            long producerId = sendError.getProducerId();
            long sequenceId = sendError.getSequenceId();
            producers.get(producerId).recoverChecksumError(this, sequenceId);
        } else {
            ctx.close();
        }
    }

    @Override
    protected void handleError(CommandError error) {
        checkArgument(state == State.Ready);

        log.warn("{} Received error from server: {}", ctx.channel(), error.getMessage());
        long requestId = error.getRequestId();
        if (error.getError() == ServerError.ProducerBlockedQuotaExceededError) {
            log.warn("{} Producer creation has been blocked because backlog quota exceeded for producer topic",
                    ctx.channel());
        }
        CompletableFuture<String> requestFuture = pendingRequests.remove(requestId);
        if (requestFuture != null) {
            requestFuture.completeExceptionally(getPulsarClientException(error.getError(), error.getMessage()));
        } else {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), error.getRequestId());
        }
    }

    @Override
    protected void handleCloseProducer(CommandCloseProducer closeProducer) {
        log.info("[{}] Broker notification of Closed producer: {}", remoteAddress, closeProducer.getProducerId());
        final long producerId = closeProducer.getProducerId();
        ProducerImpl producer = producers.get(producerId);
        if (producer != null) {
            producer.connectionClosed(this);
        } else {
            log.warn("Producer with id {} not found while closing producer ", producerId);
        }
    }

    @Override
    protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
        log.info("[{}] Broker notification of Closed consumer: {}", remoteAddress, closeConsumer.getConsumerId());
        final long consumerId = closeConsumer.getConsumerId();
        ConsumerImpl consumer = consumers.get(consumerId);
        if (consumer != null) {
            consumer.connectionClosed(this);
        } else {
            log.warn("Consumer with id {} not found while closing consumer ", consumerId);
        }
    }

    @Override
    protected boolean isHandshakeCompleted() {
        return state == State.Ready;
    }

    CompletableFuture<LookupDataResult> newLookup(ByteBuf request, long requestId) {
        CompletableFuture<LookupDataResult> future = new CompletableFuture<>();

        if (addPendingLookupRequests(requestId, future)) {
            ctx.writeAndFlush(request).addListener(writeFuture -> {
                if (!writeFuture.isSuccess()) {
                    log.warn("{} Failed to send request {} to broker: {}", ctx.channel(), requestId,
                            writeFuture.cause().getMessage());
                    getAndRemovePendingLookupRequest(requestId);
                    future.completeExceptionally(writeFuture.cause());
                }
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("{} Failed to add lookup-request into pending queue", requestId);
            }
            future.completeExceptionally(new PulsarClientException.TooManyLookupRequestException(
                    "Failed due to too many pending lookup requests"));
        }
        return future;
    }

    Promise<Void> newPromise() {
        return ctx.newPromise();
    }

    ChannelHandlerContext ctx() {
        return ctx;
    }

    Channel channel() {
        return ctx.channel();
    }

    SocketAddress serverAddrees() {
        return remoteAddress;
    }

    CompletableFuture<Void> connectionFuture() {
        return connectionFuture;
    }

    CompletableFuture<String> sendRequestWithId(ByteBuf cmd, long requestId) {
        CompletableFuture<String> future = new CompletableFuture<>();
        pendingRequests.put(requestId, future);
        ctx.writeAndFlush(cmd).addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                log.warn("{} Failed to send request to broker: {}", ctx.channel(), writeFuture.cause().getMessage());
                pendingRequests.remove(requestId);
                future.completeExceptionally(writeFuture.cause());
            }
        });
        return future;
    }

    /**
     * check serverError and take appropriate action
     * <ul>
     * <li>InternalServerError: close connection immediately</li>
     * <li>TooManyRequest: received error count is more than maxNumberOfRejectedRequestPerConnection in
     * #rejectedRequestResetTimeSec</li>
     * </ul>
     * 
     * @param error
     * @param errMsg
     */
    private void checkServerError(ServerError error, String errMsg) {
        if (ServerError.ServiceNotReady.equals(error)) {
            log.error("{} Close connection becaues received internal-server error {}", ctx.channel(), errMsg);
            ctx.close();
        } else if (ServerError.TooManyRequests.equals(error)) {
            long rejectedRequests = NUMBER_OF_REJECTED_REQUESTS_UPDATER.getAndIncrement(this);
            if (rejectedRequests == 0) {
                // schedule timer
                eventLoopGroup.schedule(() -> NUMBER_OF_REJECTED_REQUESTS_UPDATER.set(ClientCnx.this, 0),
                        rejectedRequestResetTimeSec, TimeUnit.SECONDS);
            } else if (rejectedRequests >= maxNumberOfRejectedRequestPerConnection) {
                log.error("{} Close connection becaues received {} rejected request in {} seconds ", ctx.channel(),
                        NUMBER_OF_REJECTED_REQUESTS_UPDATER.get(ClientCnx.this), rejectedRequestResetTimeSec);
                ctx.close();
            }
        }
    }
    
    void registerConsumer(final long consumerId, final ConsumerImpl consumer) {
        consumers.put(consumerId, consumer);
    }

    void registerProducer(final long producerId, final ProducerImpl producer) {
        producers.put(producerId, producer);
    }

    void removeProducer(final long producerId) {
        producers.remove(producerId);
    }

    void removeConsumer(final long consumerId) {
        consumers.remove(consumerId);
    }

    private PulsarClientException getPulsarClientException(ServerError error, String errorMsg) {
        switch (error) {
        case AuthenticationError:
            return new PulsarClientException.AuthenticationException(errorMsg);
        case AuthorizationError:
            return new PulsarClientException.AuthorizationException(errorMsg);
        case ConsumerBusy:
            return new PulsarClientException.ConsumerBusyException(errorMsg);
        case MetadataError:
            return new PulsarClientException.BrokerMetadataException(errorMsg);
        case PersistenceError:
            return new PulsarClientException.BrokerPersistenceException(errorMsg);
        case ServiceNotReady:
            return new PulsarClientException.LookupException(errorMsg);
        case TooManyRequests:
            return new PulsarClientException.TooManyLookupRequestException(errorMsg);    
        case ProducerBlockedQuotaExceededError:
            return new PulsarClientException.ProducerBlockedQuotaExceededError(errorMsg);
        case ProducerBlockedQuotaExceededException:
            return new PulsarClientException.ProducerBlockedQuotaExceededException(errorMsg);
        case UnknownError:
        default:
            return new PulsarClientException(errorMsg);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClientCnx.class);
}
