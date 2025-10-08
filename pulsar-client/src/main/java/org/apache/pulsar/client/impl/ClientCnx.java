/*
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
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.Errors.NativeIoException;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.concurrent.Promise;
import io.opentelemetry.api.common.Attributes;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.ConnectException;
import org.apache.pulsar.client.api.PulsarClientException.TimeoutException;
import org.apache.pulsar.client.impl.BinaryProtoLookupService.LookupDataResult;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.Counter;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.impl.metrics.Unit;
import org.apache.pulsar.client.impl.schema.SchemaInfoUtil;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;
import org.apache.pulsar.client.util.TimedCompletableFuture;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.BaseCommand;
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
import org.apache.pulsar.common.api.proto.CommandTcClientConnectResponse;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated.ResourceType;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicUpdate;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel handler for the Pulsar client.
 * <p>
 * Please see {@link org.apache.pulsar.common.protocol.PulsarDecoder} javadoc for important details about handle* method
 * parameter instance lifecycle.
 */
@SuppressWarnings("unchecked")
public class ClientCnx extends PulsarHandler {

    protected final Authentication authentication;
    protected State state;

    @VisibleForTesting
    protected AtomicLong duplicatedResponseCounter = new AtomicLong(0);

    @VisibleForTesting
    @Getter
    protected final ConcurrentLongHashMap<TimedCompletableFuture<? extends Object>> pendingRequests =
            ConcurrentLongHashMap.<TimedCompletableFuture<? extends Object>>newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(1)
                    .build();
    // LookupRequests that waiting in client side.
    private final Queue<Pair<Long, Pair<ByteBuf, TimedCompletableFuture<LookupDataResult>>>> waitingLookupRequests;

    @VisibleForTesting
    final ConcurrentLongHashMap<ProducerImpl<?>> producers =
            ConcurrentLongHashMap.<ProducerImpl<?>>newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(1)
                    .build();
    @VisibleForTesting
    final ConcurrentLongHashMap<ConsumerImpl<?>> consumers =
            ConcurrentLongHashMap.<ConsumerImpl<?>>newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(1)
                    .build();
    private final ConcurrentLongHashMap<TransactionMetaStoreHandler> transactionMetaStoreHandlers =
            ConcurrentLongHashMap.<TransactionMetaStoreHandler>newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(1)
                    .build();
    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentLongHashMap<TopicListWatcher> topicListWatchers =
            ConcurrentLongHashMap.<TopicListWatcher>newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(1)
                    .build();

    private final CompletableFuture<Void> connectionFuture = new CompletableFuture<Void>();
    private final ConcurrentLinkedQueue<RequestTime> requestTimeoutQueue = new ConcurrentLinkedQueue<>();

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final Semaphore pendingLookupRequestSemaphore;
    private final Semaphore maxLookupRequestSemaphore;
    private final EventLoopGroup eventLoopGroup;

    private static final AtomicIntegerFieldUpdater<ClientCnx> NUMBER_OF_REJECTED_REQUESTS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientCnx.class, "numberOfRejectRequests");
    @SuppressWarnings("unused")
    private volatile int numberOfRejectRequests = 0;

    @Getter
    private int maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE;
    private final int maxNumberOfRejectedRequestPerConnection;
    private final int rejectedRequestResetTimeSec = 60;
    protected final int protocolVersion;
    private final long operationTimeoutMs;

    protected String proxyToTargetBrokerAddress = null;
    // Remote hostName with which client is connected
    protected String remoteHostName = null;

    private ScheduledFuture<?> timeoutTask;
    private SocketAddress localAddress;
    private SocketAddress remoteAddress;

    // Added for mutual authentication.
    @Getter
    protected AuthenticationDataProvider authenticationDataProvider;
    private TransactionBufferHandler transactionBufferHandler;
    private boolean supportsTopicWatchers;
    @Getter
    private boolean supportsGetPartitionedMetadataWithoutAutoCreation;
    @Getter
    private boolean brokerSupportsReplDedupByLidAndEid;

    /** Idle stat. **/
    @Getter
    private final ClientCnxIdleState idleState;

    @Getter
    private long lastDisconnectedTimestamp;

    protected final String clientVersion;
    protected final String originalPrincipal;

    protected enum State {
        None, SentConnectFrame, Ready, Failed, Connecting
    }

    private final Counter connectionsOpenedCounter;
    private final Counter connectionsClosedCounter;

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

    public ClientCnx(InstrumentProvider instrumentProvider,
                     ClientConfigurationData conf, EventLoopGroup eventLoopGroup) {
        this(instrumentProvider, conf, eventLoopGroup, Commands.getCurrentProtocolVersion());
    }

    public ClientCnx(InstrumentProvider instrumentProvider, ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                     int protocolVersion) {
        super(conf.getKeepAliveIntervalSeconds(), TimeUnit.SECONDS);
        checkArgument(conf.getMaxLookupRequest() > conf.getConcurrentLookupRequest());
        this.pendingLookupRequestSemaphore = new Semaphore(conf.getConcurrentLookupRequest(), false);
        this.maxLookupRequestSemaphore =
                new Semaphore(conf.getMaxLookupRequest() - conf.getConcurrentLookupRequest(), false);
        this.waitingLookupRequests = Queues.newConcurrentLinkedQueue();
        this.authentication = conf.getAuthentication();
        this.eventLoopGroup = eventLoopGroup;
        this.maxNumberOfRejectedRequestPerConnection = conf.getMaxNumberOfRejectedRequestPerConnection();
        this.operationTimeoutMs = conf.getOperationTimeoutMs();
        this.state = State.None;
        this.protocolVersion = protocolVersion;
        this.idleState = new ClientCnxIdleState(this);
        this.clientVersion = "Pulsar-Java-v" + PulsarVersion.getVersion()
                + (conf.getDescription() == null ? "" : ("-" + conf.getDescription()));
        this.originalPrincipal = conf.getOriginalPrincipal();
        this.connectionsOpenedCounter =
                instrumentProvider.newCounter("pulsar.client.connection.opened", Unit.Connections,
                        "The number of connections opened", null, Attributes.empty());
        this.connectionsClosedCounter =
                instrumentProvider.newCounter("pulsar.client.connection.closed", Unit.Connections,
                        "The number of connections closed", null, Attributes.empty());

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        connectionsOpenedCounter.increment();
        this.localAddress = ctx.channel().localAddress();
        this.remoteAddress = ctx.channel().remoteAddress();

        this.timeoutTask = this.eventLoopGroup
                .scheduleAtFixedRate(catchingAndLoggingThrowables(this::checkRequestTimeout), operationTimeoutMs,
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
                clientVersion, proxyToTargetBrokerAddress, originalPrincipal, null, null);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        connectionsClosedCounter.increment();
        lastDisconnectedTimestamp = System.currentTimeMillis();
        log.info("{} Disconnected", ctx.channel());
        if (!connectionFuture.isDone()) {
            connectionFuture.completeExceptionally(new PulsarClientException("Connection already closed"));
        }

        ConnectException e = new ConnectException(
                "Disconnected from server at " + ctx.channel().remoteAddress());

        // Fail out all the pending ops
        pendingRequests.forEach((key, future) -> {
            if (pendingRequests.remove(key, future) && !future.isDone()) {
                future.completeExceptionally(e);
            }
        });
        waitingLookupRequests.forEach(pair -> pair.getRight().getRight().completeExceptionally(e));

        // Notify all attached producers/consumers so they have a chance to reconnect
        producers.forEach((id, producer) -> producer.connectionClosed(this, Optional.empty(), Optional.empty()));
        consumers.forEach((id, consumer) -> consumer.connectionClosed(this, Optional.empty(), Optional.empty()));
        transactionMetaStoreHandlers.forEach((id, handler) -> handler.connectionClosed(this));
        topicListWatchers.forEach((__, watcher) -> watcher.connectionClosed(this));

        waitingLookupRequests.clear();

        producers.clear();
        consumers.clear();
        topicListWatchers.clear();

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

    @VisibleForTesting
    public long getDuplicatedResponseCount() {
        return duplicatedResponseCounter.get();
    }

    @Override
    protected void handleConnected(CommandConnected connected) {
        checkArgument(state == State.SentConnectFrame || state == State.Connecting);
        if (connected.hasMaxMessageSize()) {
            if (log.isDebugEnabled()) {
                log.debug("{} Connection has max message size setting, replace old frameDecoder with "
                          + "server frame size {}", ctx.channel(), connected.getMaxMessageSize());
            }
            maxMessageSize = connected.getMaxMessageSize();
            FrameDecoderUtil.replaceFrameDecoder(ctx.pipeline(), connected.getMaxMessageSize());
        }
        if (log.isDebugEnabled()) {
            log.debug("{} Connection is ready", ctx.channel());
        }

        supportsTopicWatchers =
            connected.hasFeatureFlags() && connected.getFeatureFlags().isSupportsTopicWatchers();
        supportsGetPartitionedMetadataWithoutAutoCreation =
            connected.hasFeatureFlags()
                    && connected.getFeatureFlags().isSupportsGetPartitionedMetadataWithoutAutoCreation();
        brokerSupportsReplDedupByLidAndEid =
            connected.hasFeatureFlags() && connected.getFeatureFlags().isSupportsReplDedupByLidAndEid();

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
                    clientVersion);

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
        ProducerImpl<?> producer = producers.get(producerId);
        if (ledgerId == -1 && entryId == -1) {
            if (producer == null) {
                log.warn("{} Message with sequence-id {}-{} published by producer [id:{}, name:{}] has been dropped",
                        ctx.channel(), sequenceId, highestSequenceId, producerId, "null");
            } else {
                producer.printWarnLogWhenCanNotDetermineDeduplication(ctx.channel(), sequenceId, highestSequenceId);
            }

        } else {
            if (log.isDebugEnabled()) {
                log.debug("{} Got receipt for producer: [id:{}, name:{}] -- sequence-id: {}-{} -- entry-id: {}:{}",
                        ctx.channel(), producerId, producer.getProducerName(), sequenceId, highestSequenceId,
                        ledgerId, entryId);
            }
        }

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
            duplicatedResponseCounter.incrementAndGet();
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
            consumer.messageReceived(cmdMessage, headersAndPayload, this);
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
            duplicatedResponseCounter.incrementAndGet();
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), success.getRequestId());
        }
    }

    @Override
    protected void handleGetLastMessageIdSuccess(CommandGetLastMessageIdResponse success) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received success GetLastMessageId response from server: {}",
                    ctx.channel(), success.getRequestId());
        }
        long requestId = success.getRequestId();
        CompletableFuture<CommandGetLastMessageIdResponse> requestFuture =
                (CompletableFuture<CommandGetLastMessageIdResponse>) pendingRequests.remove(requestId);
        if (requestFuture != null) {
            requestFuture.complete(new CommandGetLastMessageIdResponse().copyFrom(success));
        } else {
            duplicatedResponseCounter.incrementAndGet();
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

        CompletableFuture<ProducerResponse> requestFuture =
                (CompletableFuture<ProducerResponse>) pendingRequests.remove(requestId);
        if (requestFuture != null) {
            ProducerResponse pr = new ProducerResponse(success.getProducerName(),
                    success.getLastSequenceId(),
                    success.getSchemaVersion(),
                    success.hasTopicEpoch() ? Optional.of(success.getTopicEpoch()) : Optional.empty());
            requestFuture.complete(pr);
        } else {
            duplicatedResponseCounter.incrementAndGet();
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), success.getRequestId());
        }
    }

    @Override
    protected void handleLookupResponse(CommandLookupTopicResponse lookupResult) {
        if (log.isDebugEnabled()) {
            CommandLookupTopicResponse.LookupType response =
                    lookupResult.hasResponse() ? lookupResult.getResponse() : null;
            log.debug("Received Broker lookup response: {} {}", lookupResult.getRequestId(), response);
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
                    checkServerError(lookupResult.getError(),
                            lookupResult.hasMessage() ? lookupResult.getMessage() : lookupResult.getError().name());
                    requestFuture.completeExceptionally(
                            getPulsarClientException(lookupResult.getError(),
                                    buildError(lookupResult.getRequestId(),
                                            lookupResult.hasMessage() ? lookupResult.getMessage() : null)));
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
            CommandPartitionedTopicMetadataResponse.LookupType response =
                    lookupResult.hasResponse() ? lookupResult.getResponse() : null;
            int partitions = lookupResult.hasPartitions() ? lookupResult.getPartitions() : -1;
            log.debug("Received Broker Partition response: {} {} {}", lookupResult.getRequestId(), response,
                    partitions);
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

    @Override
    protected void handleTopicMigrated(CommandTopicMigrated commandTopicMigrated) {
        final long resourceId = commandTopicMigrated.getResourceId();
        final String serviceUrl = commandTopicMigrated.hasBrokerServiceUrl()
                ? commandTopicMigrated.getBrokerServiceUrl()
                : null;
        final String serviceUrlTls = commandTopicMigrated.hasBrokerServiceUrlTls()
                ? commandTopicMigrated.getBrokerServiceUrlTls()
                : null;
        HandlerState resource = commandTopicMigrated.getResourceType() == ResourceType.Producer
                ? producers.get(resourceId)
                : consumers.get(resourceId);
        log.info("{} is migrated to {}/{}", commandTopicMigrated.getResourceType().name(), serviceUrl, serviceUrlTls);
        if (resource != null) {
            try {
                resource.setRedirectedClusterURI(serviceUrl, serviceUrlTls);
            } catch (URISyntaxException e) {
                log.info("[{}] Invalid redirect url {}/{} for {}", remoteAddress, serviceUrl, serviceUrlTls,
                        resourceId);
            }
        }
    }

    // caller of this method needs to be protected under pendingLookupRequestSemaphore
    private void addPendingLookupRequests(long requestId, TimedCompletableFuture<LookupDataResult> future) {
        pendingRequests.put(requestId, future);
        requestTimeoutQueue.add(new RequestTime(requestId, RequestType.Lookup));
    }

    private CompletableFuture<LookupDataResult> getAndRemovePendingLookupRequest(long requestId) {
        CompletableFuture<LookupDataResult> result =
                (CompletableFuture<LookupDataResult>) pendingRequests.remove(requestId);
        if (result != null) {
            Pair<Long, Pair<ByteBuf, TimedCompletableFuture<LookupDataResult>>> firstOneWaiting =
                    waitingLookupRequests.poll();
            if (firstOneWaiting != null) {
                maxLookupRequestSemaphore.release();
                // schedule a new lookup in.
                eventLoopGroup.execute(() -> {
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
        } else {
            duplicatedResponseCounter.incrementAndGet();
        }
        return result;
    }

    @Override
    protected void handleSendError(CommandSendError sendError) {
        log.warn("{} Received send error from server: {} : {}", ctx.channel(), sendError.getError(),
                sendError.getMessage());

        long producerId = sendError.getProducerId();
        long sequenceId = sendError.getSequenceId();

        ProducerImpl<?> producer = producers.get(producerId);
        if (producer == null) {
            log.warn("{} Producer with id {} not found while handling send error", ctx.channel(), producerId);
            return;
        }

        switch (sendError.getError()) {
        case ChecksumError:
            producer.recoverChecksumError(this, sequenceId);
            break;
        case TopicTerminatedError:
            producer.terminated(this);
            break;
        case NotAllowedError:
            producer.recoverNotAllowedError(sequenceId, sendError.getMessage());
            break;
        default:
            // don't close this ctx, otherwise it will close all consumers and producers which use this ctx
            producer.connectionClosed(this, Optional.empty(), Optional.empty());
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
            connectionFuture.completeExceptionally(
                    new PulsarClientException.AuthenticationException(error.getMessage()));
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
            duplicatedResponseCounter.incrementAndGet();
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), error.getRequestId());
        }
    }

    @Override
    protected void handleCloseProducer(CommandCloseProducer closeProducer) {
        final long producerId = closeProducer.getProducerId();
        log.info("[{}] Broker notification of closed producer: {}, assignedBrokerUrl: {}, assignedBrokerUrlTls: {}",
                remoteAddress, producerId,
                closeProducer.hasAssignedBrokerServiceUrl() ? closeProducer.getAssignedBrokerServiceUrl() : null,
                closeProducer.hasAssignedBrokerServiceUrlTls() ? closeProducer.getAssignedBrokerServiceUrlTls() : null);
        ProducerImpl<?> producer = producers.remove(producerId);
        if (producer != null) {
            String brokerServiceUrl = getBrokerServiceUrl(closeProducer, producer);
            Optional<URI> hostUri = parseUri(brokerServiceUrl,
                                             closeProducer.hasRequestId() ? closeProducer.getRequestId() : null);
            Optional<Long> initialConnectionDelayMs = hostUri.map(__ -> 0L);
            producer.connectionClosed(this, initialConnectionDelayMs, hostUri);
        } else {
            log.warn("[{}] Producer with id {} not found while closing producer", remoteAddress, producerId);
        }
    }

    private static String getBrokerServiceUrl(CommandCloseProducer closeProducer, ProducerImpl<?> producer) {
        if (producer.getClient().getConfiguration().isUseTls()) {
            if (closeProducer.hasAssignedBrokerServiceUrlTls()) {
                return closeProducer.getAssignedBrokerServiceUrlTls();
            }
        } else if (closeProducer.hasAssignedBrokerServiceUrl()) {
            return closeProducer.getAssignedBrokerServiceUrl();
        }
        return null;
    }

    @Override
    protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
        final long consumerId = closeConsumer.getConsumerId();
        log.info("[{}] Broker notification of closed consumer: {}, assignedBrokerUrl: {}, assignedBrokerUrlTls: {}",
                remoteAddress, consumerId,
                closeConsumer.hasAssignedBrokerServiceUrl() ? closeConsumer.getAssignedBrokerServiceUrl() : null,
                closeConsumer.hasAssignedBrokerServiceUrlTls() ? closeConsumer.getAssignedBrokerServiceUrlTls() : null);
        ConsumerImpl<?> consumer = consumers.remove(consumerId);
        if (consumer != null) {
            String brokerServiceUrl = getBrokerServiceUrl(closeConsumer, consumer);
            Optional<URI> hostUri = parseUri(brokerServiceUrl,
                                             closeConsumer.hasRequestId() ? closeConsumer.getRequestId() : null);
            Optional<Long> initialConnectionDelayMs = hostUri.map(__ -> 0L);
            consumer.connectionClosed(this, initialConnectionDelayMs, hostUri);
        } else {
            log.warn("[{}] Consumer with id {} not found while closing consumer", remoteAddress, consumerId);
        }
    }

    private static String getBrokerServiceUrl(CommandCloseConsumer closeConsumer, ConsumerImpl<?> consumer) {
        if (consumer.getClient().getConfiguration().isUseTls()) {
            if (closeConsumer.hasAssignedBrokerServiceUrlTls()) {
                return closeConsumer.getAssignedBrokerServiceUrlTls();
            }
        } else if (closeConsumer.hasAssignedBrokerServiceUrl()) {
            return closeConsumer.getAssignedBrokerServiceUrl();
        }
        return null;
    }

    private Optional<URI> parseUri(String url, Long requestId) {
        try {
            if (url != null) {
                return Optional.of(new URI(url));
            }
        } catch (URISyntaxException e) {
            log.warn("[{}] Invalid redirect URL {}, requestId {}: ", remoteAddress, url, requestId, e);
        }
        return Optional.empty();
    }

    @Override
    protected boolean isHandshakeCompleted() {
        return state == State.Ready;
    }

    public CompletableFuture<LookupDataResult> newLookup(ByteBuf request, long requestId) {
        TimedCompletableFuture<LookupDataResult> future = new TimedCompletableFuture<>();

        if (pendingLookupRequestSemaphore.tryAcquire()) {
            future.whenComplete((lookupDataResult, throwable) -> {
                if (throwable instanceof ConnectException
                        || throwable instanceof PulsarClientException.LookupException) {
                    pendingLookupRequestSemaphore.release();
                }
            });
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
                request.release();
                if (log.isDebugEnabled()) {
                    log.debug("{} Failed to add lookup-request into waiting queue", requestId);
                }
                future.completeExceptionally(new PulsarClientException.TooManyRequestsException(String.format(
                    "Requests number out of config: There are {%s} lookup requests outstanding and {%s} requests"
                            + " pending.",
                    pendingLookupRequestSemaphore.getQueueLength(),
                    waitingLookupRequests.size())));
            }
        }
        return future;
    }

    public CompletableFuture<GetTopicsResult> newGetTopicsOfNamespace(ByteBuf request, long requestId) {
        return sendRequestAndHandleTimeout(request, requestId, RequestType.GetTopics, true);
    }

    public CompletableFuture<Void> newAckForReceipt(ByteBuf request, long requestId) {
        return sendRequestAndHandleTimeout(request, requestId, RequestType.AckResponse, true);
    }

    public void newAckForReceiptWithFuture(ByteBuf request, long requestId,
                                           TimedCompletableFuture<Void> future) {
        sendRequestAndHandleTimeout(request, requestId, RequestType.AckResponse, false, future);
    }

    @Override
    protected void handleGetTopicsOfNamespaceSuccess(CommandGetTopicsOfNamespaceResponse success) {
        checkArgument(state == State.Ready);

        long requestId = success.getRequestId();
        List<String> topics = success.getTopicsList();


        if (log.isDebugEnabled()) {
            log.debug("{} Received get topics of namespace success response from server: {} - topics.size: {}",
                ctx.channel(), success.getRequestId(), topics.size());
        }

        CompletableFuture<GetTopicsResult> requestFuture =
                (CompletableFuture<GetTopicsResult>) pendingRequests.remove(requestId);
        if (requestFuture != null) {
            requestFuture.complete(new GetTopicsResult(topics,
                    success.hasTopicsHash() ? success.getTopicsHash() : null,
                    success.isFiltered(),
                    success.isChanged()));
        } else {
            duplicatedResponseCounter.incrementAndGet();
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), success.getRequestId());
        }
    }

    @Override
    protected void handleGetSchemaResponse(CommandGetSchemaResponse commandGetSchemaResponse) {
        checkArgument(state == State.Ready);

        long requestId = commandGetSchemaResponse.getRequestId();

        CompletableFuture<CommandGetSchemaResponse> future =
                (CompletableFuture<CommandGetSchemaResponse>) pendingRequests.remove(requestId);
        if (future == null) {
            duplicatedResponseCounter.incrementAndGet();
            log.warn("{} Received unknown request id from server: {}", ctx.channel(), requestId);
            return;
        }
        future.complete(new CommandGetSchemaResponse().copyFrom(commandGetSchemaResponse));
    }

    @Override
    protected void handleGetOrCreateSchemaResponse(CommandGetOrCreateSchemaResponse commandGetOrCreateSchemaResponse) {
        checkArgument(state == State.Ready);
        long requestId = commandGetOrCreateSchemaResponse.getRequestId();
        CompletableFuture<CommandGetOrCreateSchemaResponse> future =
                (CompletableFuture<CommandGetOrCreateSchemaResponse>) pendingRequests.remove(requestId);
        if (future == null) {
            duplicatedResponseCounter.incrementAndGet();
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

    @VisibleForTesting
    protected Channel channel() {
        return ctx.channel();
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
                    if (pendingRequests.remove(requestId, future) && !future.isDone()) {
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
            duplicatedResponseCounter.incrementAndGet();
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

    public CompletableFuture<CommandWatchTopicListSuccess> newWatchTopicList(
            BaseCommand commandWatchTopicList, long requestId) {
        if (!supportsTopicWatchers) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.NotAllowedException(
                            "Broker does not allow broker side pattern evaluation."));
        }
        return sendRequestAndHandleTimeout(Commands.serializeWithSize(commandWatchTopicList), requestId,
                RequestType.Command, true);
    }

    public CompletableFuture<CommandSuccess> newWatchTopicListClose(
            BaseCommand commandWatchTopicListClose, long requestId) {
        return sendRequestAndHandleTimeout(
                Commands.serializeWithSize(commandWatchTopicListClose), requestId, RequestType.Command, true);
    }

    @Override
    protected void handleCommandWatchTopicListSuccess(CommandWatchTopicListSuccess commandWatchTopicListSuccess) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received watchTopicListSuccess response from server: {}",
                    ctx.channel(), commandWatchTopicListSuccess.getRequestId());
        }
        long requestId = commandWatchTopicListSuccess.getRequestId();
        CompletableFuture<CommandWatchTopicListSuccess> requestFuture =
                (CompletableFuture<CommandWatchTopicListSuccess>) pendingRequests.remove(requestId);
        if (requestFuture != null) {
            requestFuture.complete(commandWatchTopicListSuccess);
        } else {
            duplicatedResponseCounter.incrementAndGet();
            log.warn("{} Received unknown request id from server: {}",
                    ctx.channel(), commandWatchTopicListSuccess.getRequestId());
        }
    }

    @Override
    protected void handleCommandWatchTopicUpdate(CommandWatchTopicUpdate commandWatchTopicUpdate) {
        checkArgument(state == State.Ready);

        if (log.isDebugEnabled()) {
            log.debug("{} Received watchTopicUpdate command from server: {}",
                    ctx.channel(), commandWatchTopicUpdate.getWatcherId());
        }
        long watcherId = commandWatchTopicUpdate.getWatcherId();
        TopicListWatcher watcher = topicListWatchers.get(watcherId);
        if (watcher != null) {
            watcher.handleCommandWatchTopicUpdate(commandWatchTopicUpdate);
        } else {
            log.warn("{} Received topic list update for unknown watcher from server: {}", ctx.channel(), watcherId);
        }
    }

    /**
     * check serverError and take appropriate action.
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

    void registerConsumer(final long consumerId, final ConsumerImpl<?> consumer) {
        consumers.put(consumerId, consumer);
    }

    void registerProducer(final long producerId, final ProducerImpl<?> producer) {
        producers.put(producerId, producer);
    }

    void registerTransactionMetaStoreHandler(final long transactionMetaStoreId,
                                             final TransactionMetaStoreHandler handler) {
        transactionMetaStoreHandlers.put(transactionMetaStoreId, handler);
    }

    void registerTopicListWatcher(final long watcherId, final TopicListWatcher watcher) {
        topicListWatchers.put(watcherId, watcher);

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

    void removeTopicListWatcher(final long watcherId) {
        topicListWatchers.remove(watcherId);
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
            return new PulsarClientException.ServiceNotReadyException(errorMsg);
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
        case SubscriptionNotFound:
            return new PulsarClientException.SubscriptionNotFoundException(errorMsg);
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

    public static ServerError revertClientExToErrorCode(PulsarClientException ex) {
        if (ex instanceof PulsarClientException.AuthenticationException) {
            return ServerError.AuthenticationError;
        } else if (ex instanceof PulsarClientException.AuthorizationException) {
            return ServerError.AuthorizationError;
        } else if (ex instanceof PulsarClientException.ProducerBusyException) {
            return ServerError.ProducerBusy;
        } else if (ex instanceof PulsarClientException.ConsumerBusyException) {
            return ServerError.ConsumerBusy;
        } else if (ex instanceof PulsarClientException.BrokerMetadataException) {
            return ServerError.MetadataError;
        } else if (ex instanceof PulsarClientException.BrokerPersistenceException) {
            return ServerError.PersistenceError;
        } else if (ex instanceof PulsarClientException.TooManyRequestsException) {
            return ServerError.TooManyRequests;
        } else if (ex instanceof PulsarClientException.LookupException) {
            return ServerError.ServiceNotReady;
        } else if (ex instanceof PulsarClientException.ProducerBlockedQuotaExceededError) {
            return ServerError.ProducerBlockedQuotaExceededError;
        } else if (ex instanceof PulsarClientException.ProducerBlockedQuotaExceededException) {
            return ServerError.ProducerBlockedQuotaExceededException;
        } else if (ex instanceof PulsarClientException.TopicTerminatedException) {
            return ServerError.TopicTerminatedError;
        } else if (ex instanceof PulsarClientException.IncompatibleSchemaException) {
            return ServerError.IncompatibleSchema;
        } else if (ex instanceof PulsarClientException.TopicDoesNotExistException) {
            return ServerError.TopicNotFound;
        } else if (ex instanceof PulsarClientException.SubscriptionNotFoundException) {
            return ServerError.SubscriptionNotFound;
        } else if (ex instanceof PulsarClientException.ConsumerAssignException) {
            return ServerError.ConsumerAssignError;
        } else if (ex instanceof PulsarClientException.NotAllowedException) {
            return ServerError.NotAllowedError;
        } else if (ex instanceof PulsarClientException.TransactionConflictException) {
            return ServerError.TransactionConflict;
        } else if (ex instanceof PulsarClientException.ProducerFencedException) {
            return ServerError.ProducerFenced;
        }
        return ServerError.UnknownError;
    }

    public void close() {
       if (ctx != null) {
           ctx.close();
       }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslHandshakeCompletionEvent) {
            SslHandshakeCompletionEvent sslHandshakeCompletionEvent = (SslHandshakeCompletionEvent) evt;
            if (sslHandshakeCompletionEvent.cause() != null) {
                log.warn("{} Got ssl handshake exception {}", ctx.channel(),
                        sslHandshakeCompletionEvent);
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    protected void closeWithException(Throwable e) {
       if (ctx != null) {
           connectionFuture.completeExceptionally(e);
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
            if (!requestTimeoutQueue.remove(request)) {
                // the request has been removed by another thread
                continue;
            }
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

    /**
     * Check client connection is now free. This method will not change the state to idle.
     * @return true if the connection is eligible.
     */
    public boolean idleCheck() {
        if (pendingRequests != null && !pendingRequests.isEmpty()) {
            return false;
        }
        if (waitingLookupRequests != null  && !waitingLookupRequests.isEmpty()) {
            return false;
        }
        if (!consumers.isEmpty()) {
            return false;
        }
        if (!producers.isEmpty()) {
            return false;
        }
        if (!transactionMetaStoreHandlers.isEmpty()) {
            return false;
        }
        if (!topicListWatchers.isEmpty()) {
            return false;
        }
        return true;
    }
}
