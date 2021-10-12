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
package org.apache.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.pulsar.client.impl.TransactionMetaStoreHandler.getExceptionByServerError;

import com.google.common.collect.Queues;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.Errors.NativeIoException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.net.ssl.SSLSession;
import lombok.Getter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.ConnectException;
import org.apache.pulsar.client.api.PulsarClientException.TimeoutException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.BinaryProtoLookupService.LookupDataResult;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.CommandTcClientConnectResponse;
import org.apache.pulsar.common.tls.TlsHostnameVerifier;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;
import org.apache.pulsar.client.util.TimedCompletableFuture;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.CommandAckResponse;
import org.apache.pulsar.common.api.proto.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAddSubscriptionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.api.proto.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartitionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscriptionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnResponse;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.CommandGetLastMessageIdResponse;
import org.apache.pulsar.common.api.proto.CommandGetOrCreateSchemaResponse;
import org.apache.pulsar.common.api.proto.CommandGetSchemaResponse;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespaceResponse;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.apache.pulsar.common.api.proto.CommandNewTxnResponse;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.api.proto.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.CommandReachedEndOfTopic;
import org.apache.pulsar.common.api.proto.CommandSendError;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.client.impl.schema.SchemaInfoUtil;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class ClientCnx extends PulsarHandler {

    protected final Authentication authentication;
    private State state;

    private final ConcurrentLongHashMap<TimedCompletableFuture<? extends Object>> pendingRequests =
        new ConcurrentLongHashMap<>(16, 1);
    // LookupRequests that waiting in client side.
    private final Queue<Pair<Long, Pair<ByteBuf, TimedCompletableFuture<LookupDataResult>>>> waitingLookupRequests;

    private final ConcurrentLongHashMap<ProducerImpl<?>> producers = new ConcurrentLongHashMap<>(16, 1);
    private final ConcurrentLongHashMap<ConsumerImpl<?>> consumers = new ConcurrentLongHashMap<>(16, 1);
    private final ConcurrentLongHashMap<TransactionMetaStoreHandler> transactionMetaStoreHandlers = new ConcurrentLongHashMap<>(16, 1);

    private final CompletableFuture<Void> connectionFuture = new CompletableFuture<Void>();
    private final ConcurrentLinkedQueue<RequestTime> requestTimeoutQueue = new ConcurrentLinkedQueue<>();
    private final Semaphore pendingLookupRequestSemaphore;
    private final Semaphore maxLookupRequestSemaphore;
    private final EventLoopGroup eventLoopGroup;

    private static final AtomicIntegerFieldUpdater<ClientCnx> NUMBER_OF_REJECTED_REQUESTS_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(ClientCnx.class, "numberOfRejectRequests");
    @SuppressWarnings("unused")
    private volatile int numberOfRejectRequests = 0;

    @Getter
    private static int maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE;

    private final int maxNumberOfRejectedRequestPerConnection;
    private final int rejectedRequestResetTimeSec = 60;
    private final int protocolVersion;
    private final long operationTimeoutMs;

    protected String proxyToTargetBrokerAddress = null;
    // Remote hostName with which client is connected
    protected String remoteHostName = null;
    private boolean isTlsHostnameVerificationEnable;

    private static final TlsHostnameVerifier HOSTNAME_VERIFIER = new TlsHostnameVerifier();

    private ScheduledFuture<?> timeoutTask;
    private SocketAddress localAddress;
    private SocketAddress remoteAddress;

    // Added for mutual authentication.
    @Getter
    protected AuthenticationDataProvider authenticationDataProvider;
    private TransactionBufferHandler transactionBufferHandler;

    enum State {
        None, SentConnectFrame, Ready, Failed, Connecting
    }

    private static class RequestTime {
        private final long creationTimeNanos;
        final long requestId;
        final RequestType requestType;

        RequestTime(long requestId, RequestType requestType) {
            this.creationTimeNanos = System.nanoTime();
            this.requestId = requestId;
            this.requestType = requestType;
        }

        boolean isTimedOut(long timeoutMillis) {
            long requestAgeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - creationTimeNanos);
            return requestAgeMillis > timeoutMillis;
        }
    }

    private enum RequestType {
        Command,
        GetLastMessageId,
        GetTopics,
        GetSchema,
        GetOrCreateSchema,
        AckResponse,
        Lookup;

        String getDescription() {
            if (this == Command) {
                return "request";
            } else {
                return name() + " request";
            }
        }
    }


    public ClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) {
        this(conf, eventLoopGroup, Commands.getCurrentProtocolVersion());
    }

    public ClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, int protocolVersion) {
        super(conf.getKeepAliveIntervalSeconds(), TimeUnit.SECONDS);
        checkArgument(conf.getMaxLookupRequest() > conf.getConcurrentLookupRequest());
        this.pendingLookupRequestSemaphore = new Semaphore(conf.getConcurrentLookupRequest(), false);
        this.maxLookupRequestSemaphore = new Semaphore(conf.getMaxLookupRequest() - conf.getConcurrentLookupRequest(), false);
        this.waitingLookupRequests = Queues.newConcurrentLinkedQueue();
        this.authentication = conf.getAuthentication();
        this.eventLoopGroup = eventLoopGroup;
        this.maxNumberOfRejectedRequestPerConnection = conf.getMaxNumberOfRejectedRequestPerConnection();
        this.operationTimeoutMs = conf.getOperationTimeoutMs();
        this.state = State.None;
        this.isTlsHostnameVerificationEnable = conf.isTlsHostnameVerificationEnable();
        this.protocolVersion = protocolVersion;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.localAddress = ctx.channel().localAddress();
        this.remoteAddress = ctx.channel().remoteAddress();

        this.timeoutTask = this.eventLoopGroup.scheduleAtFixedRate(() -> checkRequestTimeout(), operationTimeoutMs,
                operationTimeoutMs, TimeUnit.MILLISECONDS);

        if (proxyToTargetBrokerAddress == null) {
            if (log.isDebugEnabled()) {
                log.debug("{} Connected to broker", ctx.channel());
            }
        } else {
            log.info("{} Connected through proxy to target broker at {}", ctx.channel(), proxyToTargetBrokerAddress);
        }
        // Send CONNECT command
        ctx.writeAndFlush(newConnectCommand())
                .addListener(future -> {
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

    protected ByteBuf newConnectCommand() throws Exception {
        // mutual authentication is to auth between `remoteHostName` and this client for this channel.
        // each channel will have a mutual client/server pair, mutual client evaluateChallenge with init data,
        // and return authData to server.
        authenticationDataProvider = authentication.getAuthData(remoteHostName);
        AuthData authData = authenticationDataProvider.authenticate(AuthData.INIT_AUTH_DATA);
        return Commands.newConnect(authentication.getAuthMethodName(), authData, this.protocolVersion,
                PulsarVersion.getVersion(), proxyToTargetBrokerAddress, null, null, null);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("{} Disconnected", ctx.channel());
        if (!connectionFuture.isDone()) {
            connectionFuture.completeExceptionally(new PulsarClientException("Connection already closed"));
        }

        ConnectException e = new ConnectException(
                "Disconnected from server at " + ctx.channel().remoteAddress());

        // Fail out all the pending ops
        pendingRequests.forEach((key, future) -> future.completeExceptionally(e));
        waitingLookupRequests.forEach(pair -> pair.getRight().getRight().completeExceptionally(e));

        // Notify all attached producers/consumers so they have a chance to reconnect
        producers.forEach((id, producer) -> producer.connectionClosed(this));
        consumers.forEach((id, consumer) -> consumer.connectionClosed(this));
        transactionMetaStoreHandlers.forEach((id, handler) -> handler.connectionClosed(this));

        pendingRequests.clear();
        waitingLookupRequests.clear();

        producers.clear();
        consumers.clear();

        timeoutTask.cancel(true);
    }

    // Command Handlers

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (state != State.Failed) {
            // No need to report stack trace for known exceptions that happen in disconnections
            log.warn("[{}] Got exception {}", remoteAddress,
                    ClientCnx.isKnownException(cause) ? cause : ExceptionUtils.getStackTrace(cause));
            state = State.Failed;
        } else {
            // At default info level, suppress all subsequent exceptions that are thrown when the connection has already
            // failed
            if (log.isDebugEnabled()) {
                log.debug("[{}] Got exception: {}", remoteAddress, cause.getMessage(), cause);
            }
        }

        ctx.close();
    }

    public static boolean isKnownException(Throwable t) {
        return t instanceof NativeIoException || t instanceof ClosedChannelException;
    }

    @Override
    protected void handleConnected(CommandConnected connected) {

        if (isTlsHostnameVerificationEnable && remoteHostName != null && !verifyTlsHostName(remoteHostName, ctx)) {
            // close the connection if host-verification failed with the broker
            log.warn("[{}] Failed to verify hostname of {}", ctx.channel(), remoteHostName);
            ctx.close();
            return;
        }

        checkArgument(state == State.SentConnectFrame || state == State.Connecting);
        if (connected.hasMaxMessageSize()) {
            if (log.isDebugEnabled()) {
                log.debug("{} Connection has max message size setting, replace old frameDecoder with "
                          + "server frame size {}", ctx.channel(), connected.getMaxMessageSize());
            }
            maxMessageSize = connected.getMaxMessageSize();
            ctx.pipeline().replace("frameDecoder", "newFrameDecoder", new LengthFieldBasedFrameDecoder(
                connected.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING, 0, 4, 0, 4));
        }
        if (log.isDebugEnabled()) {
            log.debug("{} Connection is ready", ctx.channel());
        }
        // set remote protocol version to the correct version before we complete the connection future
        setRemoteEndpointProtocolVersion(connected.getProtocolVersion());
        connectionFuture.complete(null);
        state = State.Ready;
    }

    @Override
    protected void handleAuthChallenge(CommandAuthChallenge authChallenge) {
        checkArgument(authChallenge.hasChallenge());
        checkArgument(authChallenge.getChallenge().hasAuthData());

        if (Arrays.equals(AuthData.REFRESH_AUTH_DATA_BYTES, authChallenge.getChallenge().getAuthData())) {
            try {
                authenticationDataProvider = authentication.getAuthData(remoteHostName);
            } catch (PulsarClientException e) {
                log.error("{} Error when refreshing authentication data provider: {}", ctx.channel(), e);
                connectionFuture.completeExceptionally(e);
                return;
            }
        }

        // mutual authn. If auth not complete, continue auth; if auth complete, complete connectionFuture.
        try {
            AuthData authData = authenticationDataProvider
                .authenticate(AuthData.of(authChallenge.getChallenge().getAuthData()));

            checkState(!authData.isComplete());

            ByteBuf request = Commands.newAuthResponse(authentication.getAuthMethodName(),
                authData,
                this.protocolVersion,
                PulsarVersion.getVersion());

            if (log.isDebugEnabled()) {
                log.debug("{} Mutual auth {}", ctx.channel(), authentication.getAuthMethodName());
            }

            ctx.writeAndFlush(request).addListener(writeFuture -> {
                if (!writeFuture.isSuccess()) {
                    log.warn("{} Failed to send request for mutual auth to broker: {}", ctx.channel(),
                        writeFuture.cause().getMessage());
                    connectionFuture.completeExceptionally(writeFuture.cause());
                }
            });

            if (state == State.SentConnectFrame) {
                state = State.Connecting;
            }
        } catch (Exception e) {
            log.error("{} Error mutual verify: {}", ctx.channel(), e);
            connectionFuture.completeExceptionally(e);
            return;
        }
    }

    @Override
    protected void handleSendReceipt(CommandSendReceipt sendReceipt) {
        checkArgument(state == State.Ready);

        long producerId = sendReceipt.getProducerId();
        long sequenceId = sendReceipt.getSequenceId();
        long highestSequenceId = sendReceipt.getHighestSequenceId();
        long ledgerId = -1;
        long entryId = -1;
        if (sendReceipt.hasMessageId()) {
            ledgerId = sendReceipt.getMessageId().getLedgerId();
            entryId = sendReceipt.getMessageId().getEntryId();
        }

        if (ledgerId == -1 && entryId == -1) {
            log.warn("{} Message with sequence-id {} published by producer {} has been dropped", ctx.channel(),
                    sequenceId, producerId);
        }

        if (log.isDebugEnabled()) {
            log.debug("{} Got receipt for producer: {} -- msg: {} -- id: {}:{}", ctx.channel(), producerId, sequenceId,
                    ledgerId, entryId);
        }

        ProducerImpl<?> producer = producers.get(producerId);
        if (producer != null) {
            producer.ackReceived(this, sequenceId, highestSequenceId, ledgerId, entryId);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Producer is {} already closed, ignore published message [{}-{}]", producerId, ledgerId,
                        entryId);
            }
        }
    }

    @Override
    protected void handleAckResponse(CommandAckResponse ackResponse) {
        checkArgument(state == State.Ready);
        checkArgument(ackResponse.getRequestId() >= 0);
        CompletableFuture<?> completableFuture = pendingRequests.remove(ackResponse.getRequestId());
        if (completableFuture != null && !completableFuture.isDone()) {
            if (!ackResponse.hasError()) {
                completableFuture.complete(null);
            } else {
                completableFuture.completeExceptionally(
                        getPulsarClientException(ackResponse.getError(),
                                                 buildError(ackResponse.getRequestId(), ackResponse.getMessage())));
            }
        } else {
            log.warn("AckResponse has complete when receive response! requestId : {}, consumerId : {}",
                    ackResponse.getRequestId(), ackResponse.hasConsumerId());
        }
    }


    @Override
    protected void handleMessage(CommandMessage cmdMessage, ByteBuf headersAndPayload) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received a message from the server: {}", ctx.channel(), cmdMessage);
        }
        ConsumerImpl<?> consumer = consumers.get(cmdMessage.getConsumerId());
        if (consumer != null) {
            List<Long> ackSets = Collections.emptyList();
            if (cmdMessage.getAckSetsCount() > 0) {
                ackSets = new ArrayList<>(cmdMessage.getAckSetsCount());
                for (int i = 0; i < cmdMessage.getAckSetsCount(); i++) {
                    ackSets.add(cmdMessage.getAckSetAt(i));
                }
            }
            consumer.messageReceived(cmdMessage.getMessageId(), cmdMessage.getRedeliveryCount(), ackSets, headersAndPayload, this);
        }
    }

    @Override
    protected void handleActiveConsumerChange(CommandActiveConsumerChange change) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received a consumer group change message from the server : {}", ctx.channel(), change);
        }
        ConsumerImpl<?> consumer = consumers.get(change.getConsumerId());
        if (consumer != null) {
            consumer.activeConsumerChanged(change.isIsActive());
        }
    }

    @Override
    protected void handleSuccess(CommandSuccess success) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received success response from server: {}", ctx.channel(), success.getRequestId());
        }
        long requestId = success.getRequestId();
        CompletableFuture<?> requestFuture = pendingRequests.remove(requestId);
        if (requestFuture != null) {
            requestFuture.complete(null);
        } else {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), success.getRequestId());
        }
    }

    @Override
    protected void handleGetLastMessageIdSuccess(CommandGetLastMessageIdResponse success) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received success GetLastMessageId response from server: {}", ctx.channel(), success.getRequestId());
        }
        long requestId = success.getRequestId();
        CompletableFuture<CommandGetLastMessageIdResponse> requestFuture =
                (CompletableFuture<CommandGetLastMessageIdResponse>) pendingRequests.remove(requestId);
        if (requestFuture != null) {
            requestFuture.complete(new CommandGetLastMessageIdResponse().copyFrom(success));
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
        if (!success.isProducerReady()) {
            // We got a success operation but the producer is not ready. This means that the producer has been queued up
            // in broker. We need to leave the future pending until we get the final confirmation. We just mark that
            // we have received a response, in order to avoid the timeout.
            TimedCompletableFuture<?> requestFuture = pendingRequests.get(requestId);
            if (requestFuture != null) {
                log.info("{} Producer {} has been queued up at broker. request: {}", ctx.channel(),
                        success.getProducerName(), requestId);
                requestFuture.markAsResponded();
            }
            return;
        }

        CompletableFuture<ProducerResponse> requestFuture = (CompletableFuture<ProducerResponse>) pendingRequests.remove(requestId);
        if (requestFuture != null) {
            ProducerResponse pr = new ProducerResponse(success.getProducerName(),
                    success.getLastSequenceId(),
                    success.getSchemaVersion(),
                    success.hasTopicEpoch() ? Optional.of(success.getTopicEpoch()) : Optional.empty());
            requestFuture.complete(pr);
        } else {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), success.getRequestId());
        }
    }

    @Override
    protected void handleLookupResponse(CommandLookupTopicResponse lookupResult) {
        if (log.isDebugEnabled()) {
            log.debug("Received Broker lookup response: {}", lookupResult.getResponse());
        }

        long requestId = lookupResult.getRequestId();
        CompletableFuture<LookupDataResult> requestFuture = getAndRemovePendingLookupRequest(requestId);

        if (requestFuture != null) {
            if (requestFuture.isCompletedExceptionally()) {
                if (log.isDebugEnabled()) {
                    log.debug("{} Request {} already timed-out", ctx.channel(), lookupResult.getRequestId());
                }
                return;
            }
            // Complete future with exception if : Result.response=fail/null
            if (!lookupResult.hasResponse()
                    || CommandLookupTopicResponse.LookupType.Failed.equals(lookupResult.getResponse())) {
                if (lookupResult.hasError()) {
                    checkServerError(lookupResult.getError(), lookupResult.getMessage());
                    requestFuture.completeExceptionally(
                            getPulsarClientException(lookupResult.getError(),
                                    buildError(lookupResult.getRequestId(), lookupResult.getMessage())));
                } else {
                    requestFuture
                            .completeExceptionally(new PulsarClientException.LookupException("Empty lookup response"));
                }
            } else {
                requestFuture.complete(new LookupDataResult(lookupResult));
            }
        } else {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), lookupResult.getRequestId());
        }
    }

    @Override
    protected void handlePartitionResponse(CommandPartitionedTopicMetadataResponse lookupResult) {
        if (log.isDebugEnabled()) {
            log.debug("Received Broker Partition response: {}", lookupResult.getPartitions());
        }

        long requestId = lookupResult.getRequestId();
        CompletableFuture<LookupDataResult> requestFuture = getAndRemovePendingLookupRequest(requestId);

        if (requestFuture != null) {
            if (requestFuture.isCompletedExceptionally()) {
                if (log.isDebugEnabled()) {
                    log.debug("{} Request {} already timed-out", ctx.channel(), lookupResult.getRequestId());
                }
                return;
            }
            // Complete future with exception if : Result.response=fail/null
            if (!lookupResult.hasResponse()
                    || CommandPartitionedTopicMetadataResponse.LookupType.Failed.equals(lookupResult.getResponse())) {
                if (lookupResult.hasError()) {
                    String message = buildError(lookupResult.getRequestId(),
                                                lookupResult.hasMessage() ? lookupResult.getMessage() : null);
                    checkServerError(lookupResult.getError(), message);
                    requestFuture.completeExceptionally(
                            getPulsarClientException(lookupResult.getError(), message));
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

    @Override
    protected void handleReachedEndOfTopic(CommandReachedEndOfTopic commandReachedEndOfTopic) {
        final long consumerId = commandReachedEndOfTopic.getConsumerId();

        log.info("[{}] Broker notification reached the end of topic: {}", remoteAddress, consumerId);

        ConsumerImpl<?> consumer = consumers.get(consumerId);
        if (consumer != null) {
            consumer.setTerminated();
        }
    }

    // caller of this method needs to be protected under pendingLookupRequestSemaphore
    private void addPendingLookupRequests(long requestId, TimedCompletableFuture<LookupDataResult> future) {
        pendingRequests.put(requestId, future);
        requestTimeoutQueue.add(new RequestTime(requestId, RequestType.Lookup));
    }

    private CompletableFuture<LookupDataResult> getAndRemovePendingLookupRequest(long requestId) {
        CompletableFuture<LookupDataResult> result = (CompletableFuture<LookupDataResult>) pendingRequests.remove(requestId);
        if (result != null) {
            Pair<Long, Pair<ByteBuf, TimedCompletableFuture<LookupDataResult>>> firstOneWaiting = waitingLookupRequests.poll();
            if (firstOneWaiting != null) {
                maxLookupRequestSemaphore.release();
                // schedule a new lookup in.
                eventLoopGroup.submit(() -> {
                    long newId = firstOneWaiting.getLeft();
                    TimedCompletableFuture<LookupDataResult> newFuture = firstOneWaiting.getRight().getRight();
                    addPendingLookupRequests(newId, newFuture);
                    ctx.writeAndFlush(firstOneWaiting.getRight().getLeft()).addListener(writeFuture -> {
                        if (!writeFuture.isSuccess()) {
                            log.warn("{} Failed to send request {} to broker: {}", ctx.channel(), newId,
                                writeFuture.cause().getMessage());
                            getAndRemovePendingLookupRequest(newId);
                            newFuture.completeExceptionally(writeFuture.cause());
                        }
                    });
                });
            } else {
                pendingLookupRequestSemaphore.release();
            }
        }
        return result;
    }

    @Override
    protected void handleSendError(CommandSendError sendError) {
        log.warn("{} Received send error from server: {} : {}", ctx.channel(), sendError.getError(),
                sendError.getMessage());

        long producerId = sendError.getProducerId();
        long sequenceId = sendError.getSequenceId();

        switch (sendError.getError()) {
        case ChecksumError:
            producers.get(producerId).recoverChecksumError(this, sequenceId);
            break;

        case TopicTerminatedError:
            producers.get(producerId).terminated(this);
            break;
        case NotAllowedError:
            producers.get(producerId).recoverNotAllowedError(sequenceId);
            break;

        default:
            // By default, for transient error, let the reconnection logic
            // to take place and re-establish the produce again
            ctx.close();
        }
    }

    @Override
    protected void handleError(CommandError error) {
        checkArgument(state == State.SentConnectFrame || state == State.Ready);

        log.warn("{} Received error from server: {}", ctx.channel(), error.getMessage());
        long requestId = error.getRequestId();
        if (error.getError() == ServerError.ProducerBlockedQuotaExceededError) {
            log.warn("{} Producer creation has been blocked because backlog quota exceeded for producer topic",
                    ctx.channel());
        }
        if (error.getError() == ServerError.AuthenticationError) {
            connectionFuture.completeExceptionally(new PulsarClientException.AuthenticationException(error.getMessage()));
            log.error("{} Failed to authenticate the client", ctx.channel());
        }
        if (error.getError() == ServerError.NotAllowedError) {
            log.error("Get not allowed error, {}", error.getMessage());
            connectionFuture.completeExceptionally(new PulsarClientException.NotAllowedException(error.getMessage()));
        }
        CompletableFuture<?> requestFuture = pendingRequests.remove(requestId);
        if (requestFuture != null) {
            requestFuture.completeExceptionally(
                    getPulsarClientException(error.getError(),
                                             buildError(error.getRequestId(), error.getMessage())));
        } else {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), error.getRequestId());
        }
    }

    @Override
    protected void handleCloseProducer(CommandCloseProducer closeProducer) {
        log.info("[{}] Broker notification of Closed producer: {}", remoteAddress, closeProducer.getProducerId());
        final long producerId = closeProducer.getProducerId();
        ProducerImpl<?> producer = producers.get(producerId);
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
        ConsumerImpl<?> consumer = consumers.get(consumerId);
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

    public CompletableFuture<LookupDataResult> newLookup(ByteBuf request, long requestId) {
        TimedCompletableFuture<LookupDataResult> future = new TimedCompletableFuture<>();

        if (pendingLookupRequestSemaphore.tryAcquire()) {
            addPendingLookupRequests(requestId, future);
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

            if (maxLookupRequestSemaphore.tryAcquire()) {
                waitingLookupRequests.add(Pair.of(requestId, Pair.of(request, future)));
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("{} Failed to add lookup-request into waiting queue", requestId);
                }
                future.completeExceptionally(new PulsarClientException.TooManyRequestsException(String.format(
                    "Requests number out of config: There are {%s} lookup requests outstanding and {%s} requests pending.",
                    pendingLookupRequestSemaphore.availablePermits(),
                    waitingLookupRequests.size())));
            }
        }
        return future;
    }

    public CompletableFuture<List<String>> newGetTopicsOfNamespace(ByteBuf request, long requestId) {
        return sendRequestAndHandleTimeout(request, requestId, RequestType.GetTopics, true);
    }

    public CompletableFuture<Void> newAckForReceipt(ByteBuf request, long requestId) {
        return sendRequestAndHandleTimeout(request, requestId, RequestType.AckResponse,true);
    }

    public void newAckForReceiptWithFuture(ByteBuf request, long requestId,
                                           TimedCompletableFuture<Void> future) {
        sendRequestAndHandleTimeout(request, requestId, RequestType.AckResponse, false, future);
    }

    @Override
    protected void handleGetTopicsOfNamespaceSuccess(CommandGetTopicsOfNamespaceResponse success) {
        checkArgument(state == State.Ready);

        long requestId = success.getRequestId();
        List<String> topics = new ArrayList<String>(success.getTopicsCount());
        for (int i = 0; i < success.getTopicsCount(); i++) {
            topics.add(success.getTopicAt(i));
        }

        if (log.isDebugEnabled()) {
            log.debug("{} Received get topics of namespace success response from server: {} - topics.size: {}",
                ctx.channel(), success.getRequestId(), topics.size());
        }

        CompletableFuture<List<String>> requestFuture = (CompletableFuture<List<String>>) pendingRequests.remove(requestId);
        if (requestFuture != null) {
            requestFuture.complete(topics);
        } else {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), success.getRequestId());
        }
    }

    @Override
    protected void handleGetSchemaResponse(CommandGetSchemaResponse commandGetSchemaResponse) {
        checkArgument(state == State.Ready);

        long requestId = commandGetSchemaResponse.getRequestId();

        CompletableFuture<CommandGetSchemaResponse> future = (CompletableFuture<CommandGetSchemaResponse>) pendingRequests
                .remove(requestId);
        if (future == null) {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), requestId);
            return;
        }
        future.complete(new CommandGetSchemaResponse().copyFrom(commandGetSchemaResponse));
    }

    @Override
    protected void handleGetOrCreateSchemaResponse(CommandGetOrCreateSchemaResponse commandGetOrCreateSchemaResponse) {
        checkArgument(state == State.Ready);
        long requestId = commandGetOrCreateSchemaResponse.getRequestId();
        CompletableFuture<CommandGetOrCreateSchemaResponse> future = (CompletableFuture<CommandGetOrCreateSchemaResponse>) pendingRequests
                .remove(requestId);
        if (future == null) {
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), requestId);
            return;
        }
        future.complete(new CommandGetOrCreateSchemaResponse().copyFrom(commandGetOrCreateSchemaResponse));
    }

    Promise<Void> newPromise() {
        return ctx.newPromise();
    }

    public ChannelHandlerContext ctx() {
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

    CompletableFuture<ProducerResponse> sendRequestWithId(ByteBuf cmd, long requestId) {
        return sendRequestAndHandleTimeout(cmd, requestId, RequestType.Command, true);
    }

    private <T> void sendRequestAndHandleTimeout(ByteBuf requestMessage, long requestId,
                                                                 RequestType requestType, boolean flush,
                                                                 TimedCompletableFuture<T> future) {
        pendingRequests.put(requestId, future);
        if (flush) {
            ctx.writeAndFlush(requestMessage).addListener(writeFuture -> {
                if (!writeFuture.isSuccess()) {
                    CompletableFuture<?> newFuture = pendingRequests.remove(requestId);
                    if (newFuture != null && !newFuture.isDone()) {
                        log.warn("{} Failed to send {} to broker: {}", ctx.channel(),
                                requestType.getDescription(), writeFuture.cause().getMessage());
                        future.completeExceptionally(writeFuture.cause());
                    }
                }
            });
        } else {
            ctx.write(requestMessage, ctx().voidPromise());
        }
        requestTimeoutQueue.add(new RequestTime(requestId, requestType));
    }

    private <T> CompletableFuture<T> sendRequestAndHandleTimeout(ByteBuf requestMessage, long requestId,
                                                 RequestType requestType, boolean flush) {
        TimedCompletableFuture<T> future = new TimedCompletableFuture<>();
        sendRequestAndHandleTimeout(requestMessage, requestId, requestType, flush, future);
        return future;
    }

    public CompletableFuture<CommandGetLastMessageIdResponse> sendGetLastMessageId(ByteBuf request, long requestId) {
        return sendRequestAndHandleTimeout(request, requestId, RequestType.GetLastMessageId, true);
    }

    public CompletableFuture<Optional<SchemaInfo>> sendGetSchema(ByteBuf request, long requestId) {
        return sendGetRawSchema(request, requestId).thenCompose(commandGetSchemaResponse -> {
            if (commandGetSchemaResponse.hasErrorCode()) {
                // Request has failed
                ServerError rc = commandGetSchemaResponse.getErrorCode();
                if (rc == ServerError.TopicNotFound) {
                    return CompletableFuture.completedFuture(Optional.empty());
                } else {
                    return FutureUtil.failedFuture(
                        getPulsarClientException(rc,
                                buildError(requestId, commandGetSchemaResponse.getErrorMessage())));
                }
            } else {
                return CompletableFuture.completedFuture(
                    Optional.of(SchemaInfoUtil.newSchemaInfo(commandGetSchemaResponse.getSchema())));
            }
        });
    }

    public CompletableFuture<CommandGetSchemaResponse> sendGetRawSchema(ByteBuf request, long requestId) {
        return sendRequestAndHandleTimeout(request, requestId, RequestType.GetSchema, true);
    }

    public CompletableFuture<byte[]> sendGetOrCreateSchema(ByteBuf request, long requestId) {
        CompletableFuture<CommandGetOrCreateSchemaResponse> future = sendRequestAndHandleTimeout(request, requestId,
                RequestType.GetOrCreateSchema, true);
        return future.thenCompose(response -> {
            if (response.hasErrorCode()) {
                // Request has failed
                ServerError rc = response.getErrorCode();
                if (rc == ServerError.TopicNotFound) {
                    return CompletableFuture.completedFuture(SchemaVersion.Empty.bytes());
                } else {
                    return FutureUtil.failedFuture(getPulsarClientException(
                                                           rc, buildError(requestId, response.getErrorMessage())));
                }
            } else {
                return CompletableFuture.completedFuture(response.getSchemaVersion());
            }
        });
    }

    @Override
    protected void handleNewTxnResponse(CommandNewTxnResponse command) {
        TransactionMetaStoreHandler handler = checkAndGetTransactionMetaStoreHandler(command.getTxnidMostBits());
        if (handler != null) {
            handler.handleNewTxnResponse(command);
        }
    }

    @Override
    protected void handleAddPartitionToTxnResponse(CommandAddPartitionToTxnResponse command) {
        TransactionMetaStoreHandler handler = checkAndGetTransactionMetaStoreHandler(command.getTxnidMostBits());
        if (handler != null) {
            handler.handleAddPublishPartitionToTxnResponse(command);
        }
    }

    @Override
    protected void handleAddSubscriptionToTxnResponse(CommandAddSubscriptionToTxnResponse command) {
        TransactionMetaStoreHandler handler = checkAndGetTransactionMetaStoreHandler(command.getTxnidMostBits());
        if (handler != null) {
            handler.handleAddSubscriptionToTxnResponse(command);
        }
    }

    @Override
    protected void handleEndTxnOnPartitionResponse(CommandEndTxnOnPartitionResponse command) {
        TransactionBufferHandler handler = checkAndGetTransactionBufferHandler();
        if (handler != null) {
            handler.handleEndTxnOnTopicResponse(command.getRequestId(), command);
        }
    }

    @Override
    protected void handleEndTxnOnSubscriptionResponse(CommandEndTxnOnSubscriptionResponse command) {
        TransactionBufferHandler handler = checkAndGetTransactionBufferHandler();
        if (handler != null) {
            handler.handleEndTxnOnSubscriptionResponse(command.getRequestId(), command);
        }
    }

    @Override
    protected void handleEndTxnResponse(CommandEndTxnResponse command) {
        TransactionMetaStoreHandler handler = checkAndGetTransactionMetaStoreHandler(command.getTxnidMostBits());
        if (handler != null) {
            handler.handleEndTxnResponse(command);
        }
    }

    @Override
    protected void handleTcClientConnectResponse(CommandTcClientConnectResponse response) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received tc client connect response "
                    + "from server: {}", ctx.channel(), response.getRequestId());
        }
        long requestId = response.getRequestId();
        CompletableFuture<?> requestFuture = pendingRequests.remove(requestId);

        if (requestFuture != null && !requestFuture.isDone()) {
            if (!response.hasError()) {
                requestFuture.complete(null);
            } else {
                ServerError error = response.getError();
                log.error("Got tc client connect response for request: {}, error: {}, errorMessage: {}",
                        response.getRequestId(), response.getError(), response.getMessage());
                requestFuture.completeExceptionally(getExceptionByServerError(error, response.getMessage()));
            }
        } else {
            log.warn("Tc client connect command has been completed and get response for request: {}",
                    response.getRequestId());
        }
    }

    private TransactionMetaStoreHandler checkAndGetTransactionMetaStoreHandler(long tcId) {
        TransactionMetaStoreHandler handler = transactionMetaStoreHandlers.get(tcId);
        if (handler == null) {
            channel().close();
            log.warn("Close the channel since can't get the transaction meta store handler, will reconnect later.");
        }
        return handler;
    }

    private TransactionBufferHandler checkAndGetTransactionBufferHandler() {
        if (transactionBufferHandler == null) {
            channel().close();
            log.warn("Close the channel since can't get the transaction buffer handler.");
        }
        return transactionBufferHandler;
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
            log.error("{} Close connection because received internal-server error {}", ctx.channel(), errMsg);
            ctx.close();
        } else if (ServerError.TooManyRequests.equals(error)) {
            incrementRejectsAndMaybeClose();
        }
    }

    private void incrementRejectsAndMaybeClose() {
        long rejectedRequests = NUMBER_OF_REJECTED_REQUESTS_UPDATER.getAndIncrement(this);
        if (rejectedRequests == 0) {
            // schedule timer
            eventLoopGroup.schedule(() -> NUMBER_OF_REJECTED_REQUESTS_UPDATER.set(ClientCnx.this, 0),
                                    rejectedRequestResetTimeSec, TimeUnit.SECONDS);
        } else if (rejectedRequests >= maxNumberOfRejectedRequestPerConnection) {
            log.error("{} Close connection because received {} rejected request in {} seconds ", ctx.channel(),
                      NUMBER_OF_REJECTED_REQUESTS_UPDATER.get(ClientCnx.this), rejectedRequestResetTimeSec);
            ctx.close();
        }
    }

    /**
     * verifies host name provided in x509 Certificate in tls session
     *
     * it matches hostname with below scenarios
     *
     * <pre>
     *  1. Supports IPV4 and IPV6 host matching
     *  2. Supports wild card matching for DNS-name
     *  eg:
     *     HostName                     CN           Result
     * 1.  localhost                    localhost    PASS
     * 2.  localhost                    local*       PASS
     * 3.  pulsar1-broker.com           pulsar*.com  PASS
     * </pre>
     *
     * @param ctx
     * @return true if hostname is verified else return false
     */
    private boolean verifyTlsHostName(String hostname, ChannelHandlerContext ctx) {
        ChannelHandler sslHandler = ctx.channel().pipeline().get("tls");

        SSLSession sslSession = null;
        if (sslHandler != null) {
            sslSession = ((SslHandler) sslHandler).engine().getSession();
            if (log.isDebugEnabled()) {
                log.debug("Verifying HostName for {}, Cipher {}, Protocols {}", hostname, sslSession.getCipherSuite(),
                        sslSession.getProtocol());
            }
            return HOSTNAME_VERIFIER.verify(hostname, sslSession);
        }
        return false;
    }

    void registerConsumer(final long consumerId, final ConsumerImpl<?> consumer) {
        consumers.put(consumerId, consumer);
    }

    void registerProducer(final long producerId, final ProducerImpl<?> producer) {
        producers.put(producerId, producer);
    }

    void registerTransactionMetaStoreHandler(final long transactionMetaStoreId, final TransactionMetaStoreHandler handler) {
        transactionMetaStoreHandlers.put(transactionMetaStoreId, handler);
    }

    public void registerTransactionBufferHandler(final TransactionBufferHandler handler) {
        transactionBufferHandler = handler;
    }

    void removeProducer(final long producerId) {
        producers.remove(producerId);
    }

    void removeConsumer(final long consumerId) {
        consumers.remove(consumerId);
    }

    void setTargetBroker(InetSocketAddress targetBrokerAddress) {
        this.proxyToTargetBrokerAddress = String.format("%s:%d", targetBrokerAddress.getHostString(),
                targetBrokerAddress.getPort());
    }

     void setRemoteHostName(String remoteHostName) {
        this.remoteHostName = remoteHostName;
    }

    private String buildError(long requestId, String errorMsg) {
        return new StringBuilder().append("{\"errorMsg\":\"").append(errorMsg)
            .append("\",\"reqId\":").append(requestId)
            .append(", \"remote\":\"").append(remoteAddress)
            .append("\", \"local\":\"").append(localAddress)
            .append("\"}").toString();
    }

    public static PulsarClientException getPulsarClientException(ServerError error, String errorMsg) {
        switch (error) {
        case AuthenticationError:
            return new PulsarClientException.AuthenticationException(errorMsg);
        case AuthorizationError:
            return new PulsarClientException.AuthorizationException(errorMsg);
        case ProducerBusy:
            return new PulsarClientException.ProducerBusyException(errorMsg);
        case ConsumerBusy:
            return new PulsarClientException.ConsumerBusyException(errorMsg);
        case MetadataError:
            return new PulsarClientException.BrokerMetadataException(errorMsg);
        case PersistenceError:
            return new PulsarClientException.BrokerPersistenceException(errorMsg);
        case ServiceNotReady:
            return new PulsarClientException.LookupException(errorMsg);
        case TooManyRequests:
            return new PulsarClientException.TooManyRequestsException(errorMsg);
        case ProducerBlockedQuotaExceededError:
            return new PulsarClientException.ProducerBlockedQuotaExceededError(errorMsg);
        case ProducerBlockedQuotaExceededException:
            return new PulsarClientException.ProducerBlockedQuotaExceededException(errorMsg);
        case TopicTerminatedError:
            return new PulsarClientException.TopicTerminatedException(errorMsg);
        case IncompatibleSchema:
            return new PulsarClientException.IncompatibleSchemaException(errorMsg);
        case TopicNotFound:
            return new PulsarClientException.TopicDoesNotExistException(errorMsg);
        case ConsumerAssignError:
            return new PulsarClientException.ConsumerAssignException(errorMsg);
        case NotAllowedError:
            return new PulsarClientException.NotAllowedException(errorMsg);
        case TransactionConflict:
            return new PulsarClientException.TransactionConflictException(errorMsg);
        case ProducerFenced:
            return new PulsarClientException.ProducerFencedException(errorMsg);
        case UnknownError:
        default:
            return new PulsarClientException(errorMsg);
        }
    }

    public void close() {
       if (ctx != null) {
           ctx.close();
       }
    }

    private void checkRequestTimeout() {
        while (!requestTimeoutQueue.isEmpty()) {
            RequestTime request = requestTimeoutQueue.peek();
            if (request == null || !request.isTimedOut(operationTimeoutMs)) {
                // if there is no request that is timed out then exit the loop
                break;
            }
            request = requestTimeoutQueue.poll();
            TimedCompletableFuture<?> requestFuture = pendingRequests.get(request.requestId);
            if (requestFuture != null
                    && !requestFuture.hasGotResponse()) {
                pendingRequests.remove(request.requestId, requestFuture);
                if (!requestFuture.isDone()) {
                    String timeoutMessage = String.format(
                            "%s timeout {'durationMs': '%d', 'reqId':'%d', 'remote':'%s', 'local':'%s'}",
                            request.requestType.getDescription(), operationTimeoutMs,
                            request.requestId, remoteAddress, localAddress);
                    if (requestFuture.completeExceptionally(new TimeoutException(timeoutMessage))) {
                        if (request.requestType == RequestType.Lookup) {
                            incrementRejectsAndMaybeClose();
                        }
                        log.warn("{} {}", ctx.channel(), timeoutMessage);
                    }
                }
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClientCnx.class);
}
