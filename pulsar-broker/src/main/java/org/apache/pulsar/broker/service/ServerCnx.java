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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.admin.PersistentTopics.getPartitionedTopicMetadata;
import static org.apache.pulsar.broker.lookup.DestinationLookup.lookupDestinationAsync;
import static org.apache.pulsar.common.api.Commands.newLookupErrorResponse;
import static org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion.v5;

import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.PulsarHandler;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnect;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConsumerStats;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConsumerStatsResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandFlow;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandRedeliverUnacknowledgedMessages;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSend;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandUnsubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslHandler;

public class ServerCnx extends PulsarHandler {
    private final BrokerService service;
    private final ConcurrentLongHashMap<CompletableFuture<Producer>> producers;
    private final ConcurrentLongHashMap<CompletableFuture<Consumer>> consumers;
    private State state;
    private volatile boolean isActive = true;
    private String authRole = null;

    // Max number of pending requests per connections. If multiple producers are sharing the same connection the flow
    // control done by a single producer might not be enough to prevent write spikes on the broker.
    private static final int MaxPendingSendRequests = 1000;
    private static final int ResumeReadsThreshold = MaxPendingSendRequests / 2;
    private int pendingSendRequest = 0;
    private final String replicatorPrefix;
    private String clientVersion = null;
    private final Semaphore nonPersistentMessageSemaphore;

    enum State {
        Start, Connected
    }

    public ServerCnx(BrokerService service) {
        super(service.getKeepAliveIntervalSeconds(), TimeUnit.SECONDS);
        this.service = service;
        this.state = State.Start;

        // This maps are not heavily contended since most accesses are within the cnx thread
        this.producers = new ConcurrentLongHashMap<>(8, 1);
        this.consumers = new ConcurrentLongHashMap<>(8, 1);
        this.replicatorPrefix = service.pulsar().getConfiguration().getReplicatorPrefix();
        this.nonPersistentMessageSemaphore = new Semaphore(
                service.pulsar().getConfiguration().getMaxConcurrentNonPersistentMessagePerConnection(), false);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        log.info("New connection from {}", remoteAddress);
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        isActive = false;
        log.info("Closed connection from {}", remoteAddress);

        // Connection is gone, close the producers immediately
        producers.values().forEach((producerFuture) -> {
            if (producerFuture.isDone() && !producerFuture.isCompletedExceptionally()) {
                Producer producer = producerFuture.getNow(null);
                producer.closeNow();
            }
        });

        consumers.values().forEach((consumerFuture) -> {
            Consumer consumer;
            if (consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
                consumer = consumerFuture.getNow(null);
            } else {
                return;
            }

            try {
                consumer.close();
            } catch (BrokerServiceException e) {
                log.warn("Consumer {} was already closed: {}", consumer, e.getMessage(), e);
            }
        });
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Channel writability has changed to: {}", ctx.channel().isWritable());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn("[{}] Got exception: {}", remoteAddress, cause.getMessage(), cause);
        ctx.close();
    }

    // ////
    // // Incoming commands handling
    // ////

    @Override
    protected void handleLookup(CommandLookupTopic lookup) {
        final long requestId = lookup.getRequestId();
        final String topic = lookup.getTopic();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received Lookup from {} for {}", topic, remoteAddress, requestId);
        }
        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            lookupDestinationAsync(getBrokerService().pulsar(), DestinationName.get(topic), lookup.getAuthoritative(),
                    getRole(), lookup.getRequestId()).handle((lookupResponse, ex) -> {
                        if (ex == null) {
                            ctx.writeAndFlush(lookupResponse);
                        } else {
                            // it should never happen
                            log.warn("[{}] lookup failed with error {}, {}", remoteAddress, topic, ex.getMessage(), ex);
                            ctx.writeAndFlush(
                                    newLookupErrorResponse(ServerError.ServiceNotReady, ex.getMessage(), requestId));
                        }
                        lookupSemaphore.release();
                        return null;
                    });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed lookup due to too many lookup-requets {}", remoteAddress, topic);
            }
            ctx.writeAndFlush(newLookupErrorResponse(ServerError.TooManyRequests,
                    "Failed due to too many pending lookup requests", requestId));
        }

    }

    @Override
    protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata partitionMetadata) {
        final long requestId = partitionMetadata.getRequestId();
        final String topic = partitionMetadata.getTopic();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received PartitionMetadataLookup from {} for {}", topic, remoteAddress, requestId);
        }
        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            getPartitionedTopicMetadata(getBrokerService().pulsar(), getRole(), DestinationName.get(topic))
                    .handle((metadata, ex) -> {
                        if (ex == null) {
                            int partitions = metadata.partitions;
                            ctx.writeAndFlush(Commands.newPartitionMetadataResponse(partitions, requestId));
                        } else {
                            if (ex instanceof PulsarClientException) {
                                log.warn("Failed to authorize {} at [{}] on topic {} : {}", getRole(), remoteAddress,
                                        topic, ex.getMessage());
                                ctx.writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.AuthorizationError,
                                        ex.getMessage(), requestId));
                            } else {
                                log.warn("Failed to get Partitioned Metadata [{}] {}: {}", remoteAddress, topic,
                                        ex.getMessage(), ex);
                                ctx.writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.ServiceNotReady,
                                        ex.getMessage(), requestId));
                            }
                        }
                        lookupSemaphore.release();
                        return null;
                    });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed Partition-Metadata lookup due to too many lookup-requets {}", remoteAddress,
                        topic);
            }
            ctx.writeAndFlush(newLookupErrorResponse(ServerError.TooManyRequests,
                    "Failed due to too many pending lookup requests", requestId));
        }
    }

    @Override
    protected void handleConsumerStats(CommandConsumerStats commandConsumerStats) {
        if (log.isDebugEnabled()) {
            log.debug("Received CommandConsumerStats call from {}", remoteAddress);
        }

        final long requestId = commandConsumerStats.getRequestId();
        final long consumerId = commandConsumerStats.getConsumerId();
        CompletableFuture<Consumer> consumerFuture = consumers.get(consumerId);
        Consumer consumer = consumerFuture.getNow(null);
        ByteBuf msg = null;

        if (consumer == null) {
            log.error(
                    "Failed to get consumer-stats response - Consumer not found for CommandConsumerStats[remoteAddress = {}, requestId = {}, consumerId = {}]",
                    remoteAddress, requestId, consumerId);
            msg = Commands.newConsumerStatsResponse(ServerError.ConsumerNotFound,
                    "Consumer " + consumerId + " not found", requestId);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("CommandConsumerStats[requestId = {}, consumer = {}]", requestId, consumer);
            }
            msg = Commands.newConsumerStatsResponse(createConsumerStatsResponse(consumer, requestId));
        }

        ctx.writeAndFlush(msg);
    }

    CommandConsumerStatsResponse.Builder createConsumerStatsResponse(Consumer consumer, long requestId) {
        CommandConsumerStatsResponse.Builder commandConsumerStatsResponseBuilder = CommandConsumerStatsResponse
                .newBuilder();
        ConsumerStats consumerStats = consumer.getStats();
        commandConsumerStatsResponseBuilder.setRequestId(requestId);
        commandConsumerStatsResponseBuilder.setMsgRateOut(consumerStats.msgRateOut);
        commandConsumerStatsResponseBuilder.setMsgThroughputOut(consumerStats.msgThroughputOut);
        commandConsumerStatsResponseBuilder.setMsgRateRedeliver(consumerStats.msgRateRedeliver);
        commandConsumerStatsResponseBuilder.setConsumerName(consumerStats.consumerName);
        commandConsumerStatsResponseBuilder.setAvailablePermits(consumerStats.availablePermits);
        commandConsumerStatsResponseBuilder.setUnackedMessages(consumerStats.unackedMessages);
        commandConsumerStatsResponseBuilder.setBlockedConsumerOnUnackedMsgs(consumerStats.blockedConsumerOnUnackedMsgs);
        commandConsumerStatsResponseBuilder.setAddress(consumerStats.address);
        commandConsumerStatsResponseBuilder.setConnectedSince(consumerStats.connectedSince);

        Subscription subscription = consumer.getSubscription();
        commandConsumerStatsResponseBuilder.setMsgBacklog(subscription.getNumberOfEntriesInBacklog());
        commandConsumerStatsResponseBuilder.setMsgRateExpired(subscription.getExpiredMessageRate());
        commandConsumerStatsResponseBuilder.setType(subscription.getTypeString());

        return commandConsumerStatsResponseBuilder;
    }

    @Override
    protected void handleConnect(CommandConnect connect) {
        checkArgument(state == State.Start);
        if (service.isAuthenticationEnabled()) {
            try {
                String authMethod = "none";
                if (connect.hasAuthMethodName()) {
                    authMethod = connect.getAuthMethodName();
                } else if (connect.hasAuthMethod()) {
                    // Legacy client is passing enum
                    authMethod = connect.getAuthMethod().name().substring(10).toLowerCase();
                }

                String authData = connect.getAuthData().toStringUtf8();
                ChannelHandler sslHandler = ctx.channel().pipeline().get(PulsarChannelInitializer.TLS_HANDLER);
                SSLSession sslSession = null;
                if (sslHandler != null) {
                    sslSession = ((SslHandler) sslHandler).engine().getSession();
                }
                authRole = getBrokerService().getAuthenticationService()
                        .authenticate(new AuthenticationDataCommand(authData, remoteAddress, sslSession), authMethod);

                log.info("[{}] Client successfully authenticated with {} role {}", remoteAddress, authMethod, authRole);
            } catch (AuthenticationException e) {
                String msg = "Unable to authenticate";
                log.warn("[{}] {}: {}", remoteAddress, msg, e.getMessage());
                ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, msg));
                close();
                return;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Received CONNECT from {}", remoteAddress);
        }
        ctx.writeAndFlush(Commands.newConnected(connect.getProtocolVersion()));
        state = State.Connected;
        remoteEndpointProtocolVersion = connect.getProtocolVersion();
        String version = connect.hasClientVersion() ? connect.getClientVersion() : null;
        if (isNotBlank(version) && !version.contains(" ") /* ignore default version: pulsar client */) {
            this.clientVersion = version;
        }
    }

    @Override
    protected void handleSubscribe(final CommandSubscribe subscribe) {
        checkArgument(state == State.Connected);
        CompletableFuture<Boolean> authorizationFuture;
        if (service.isAuthorizationEnabled()) {
            authorizationFuture = service.getAuthorizationManager()
                    .canConsumeAsync(DestinationName.get(subscribe.getTopic()), authRole);
        } else {
            authorizationFuture = CompletableFuture.completedFuture(true);
        }
        final String topicName = subscribe.getTopic();
        final String subscriptionName = subscribe.getSubscription();
        final long requestId = subscribe.getRequestId();
        final long consumerId = subscribe.getConsumerId();
        final SubType subType = subscribe.getSubType();
        final String consumerName = subscribe.getConsumerName();
        final boolean isDurable = subscribe.getDurable();
        final MessageIdImpl startMessageId = subscribe.hasStartMessageId()
                ? new MessageIdImpl(subscribe.getStartMessageId().getLedgerId(),
                        subscribe.getStartMessageId().getEntryId(), subscribe.getStartMessageId().getPartition())
                : null;

        final int priorityLevel = subscribe.hasPriorityLevel() ? subscribe.getPriorityLevel() : 0;

        authorizationFuture.thenApply(isAuthorized -> {
            if (isAuthorized) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Client is authorized to subscribe with role {}", remoteAddress, authRole);
                }

                log.info("[{}] Subscribing on topic {} / {}", remoteAddress, topicName, subscriptionName);

                CompletableFuture<Consumer> consumerFuture = new CompletableFuture<>();
                CompletableFuture<Consumer> existingConsumerFuture = consumers.putIfAbsent(consumerId, consumerFuture);

                if (existingConsumerFuture != null) {
                    if (existingConsumerFuture.isDone() && !existingConsumerFuture.isCompletedExceptionally()) {
                        Consumer consumer = existingConsumerFuture.getNow(null);
                        log.info("[{}] Consumer with the same id is already created: {}", remoteAddress, consumer);
                        ctx.writeAndFlush(Commands.newSuccess(requestId));
                        return null;
                    } else {
                        // There was an early request to create a consumer with same consumerId. This can happen when
                        // client timeout is lower the broker timeouts. We need to wait until the previous consumer
                        // creation request either complete or fails.
                        log.warn("[{}][{}][{}] Consumer is already present on the connection", remoteAddress, topicName,
                                subscriptionName);
                        ServerError error = !existingConsumerFuture.isDone() ? ServerError.ServiceNotReady
                                : getErrorCode(existingConsumerFuture);
                        ctx.writeAndFlush(
                                Commands.newError(requestId, error, "Consumer is already present on the connection"));
                        return null;
                    }
                }

                service.getTopic(topicName).thenCompose(topic -> topic.subscribe(ServerCnx.this, subscriptionName,
                        consumerId, subType, priorityLevel, consumerName, isDurable, startMessageId))
                        .thenAccept(consumer -> {
                            if (consumerFuture.complete(consumer)) {
                                log.info("[{}] Created subscription on topic {} / {}", remoteAddress, topicName,
                                        subscriptionName);
                                ctx.writeAndFlush(Commands.newSuccess(requestId), ctx.voidPromise());
                            } else {
                                // The consumer future was completed before by a close command
                                try {
                                    consumer.close();
                                    log.info("[{}] Cleared consumer created after timeout on client side {}",
                                            remoteAddress, consumer);
                                } catch (BrokerServiceException e) {
                                    log.warn("[{}] Error closing consumer created after timeout on client side {}: {}",
                                            remoteAddress, consumer, e.getMessage());
                                }
                                consumers.remove(consumerId, consumerFuture);
                            }

                        }) //
                        .exceptionally(exception -> {
                            log.warn("[{}][{}][{}] Failed to create consumer: {}", remoteAddress, topicName,
                                    subscriptionName, exception.getCause().getMessage(), exception);

                            // If client timed out, the future would have been completed by subsequent close. Send error
                            // back to client, only if not completed already.
                            if (consumerFuture.completeExceptionally(exception)) {
                                ctx.writeAndFlush(Commands.newError(requestId,
                                        BrokerServiceException.getClientErrorCode(exception.getCause()),
                                        exception.getCause().getMessage()));
                            }
                            consumers.remove(consumerId, consumerFuture);

                            return null;

                        });
            } else {
                String msg = "Client is not authorized to subscribe";
                log.warn("[{}] {} with role {}", remoteAddress, msg, authRole);
                ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
            }
            return null;
        });
    }

    @Override
    protected void handleProducer(final CommandProducer cmdProducer) {
        checkArgument(state == State.Connected);
        CompletableFuture<Boolean> authorizationFuture;
        if (service.isAuthorizationEnabled()) {
            authorizationFuture = service.getAuthorizationManager()
                    .canProduceAsync(DestinationName.get(cmdProducer.getTopic().toString()), authRole);
        } else {
            authorizationFuture = CompletableFuture.completedFuture(true);
        }

        // Use producer name provided by client if present
        final String producerName = cmdProducer.hasProducerName() ? cmdProducer.getProducerName()
                : service.generateUniqueProducerName();
        final String topicName = cmdProducer.getTopic();
        final long producerId = cmdProducer.getProducerId();
        final long requestId = cmdProducer.getRequestId();
        authorizationFuture.thenApply(isAuthorized -> {
            if (isAuthorized) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Client is authorized to Produce with role {}", remoteAddress, authRole);
                }
                CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
                CompletableFuture<Producer> existingProducerFuture = producers.putIfAbsent(producerId, producerFuture);

                if (existingProducerFuture != null) {
                    if (existingProducerFuture.isDone() && !existingProducerFuture.isCompletedExceptionally()) {
                        Producer producer = existingProducerFuture.getNow(null);
                        log.info("[{}] Producer with the same id is already created: {}", remoteAddress, producer);
                        ctx.writeAndFlush(Commands.newProducerSuccess(requestId, producer.getProducerName()));
                        return null;
                    } else {
                        // There was an early request to create a producer with
                        // same producerId. This can happen when
                        // client
                        // timeout is lower the broker timeouts. We need to wait
                        // until the previous producer creation
                        // request
                        // either complete or fails.
                        ServerError error = !existingProducerFuture.isDone() ? ServerError.ServiceNotReady
                                : getErrorCode(existingProducerFuture);
                        log.warn("[{}][{}] Producer is already present on the connection", remoteAddress, topicName);
                        ctx.writeAndFlush(
                                Commands.newError(requestId, error, "Producer is already present on the connection"));
                        return null;
                    }
                }

                log.info("[{}][{}] Creating producer. producerId={}", remoteAddress, topicName, producerId);

                service.getTopic(topicName).thenAccept((Topic topic) -> {
                    // Before creating producer, check if backlog quota exceeded
                    // on topic
                    if (topic.isBacklogQuotaExceeded(producerName)) {
                        IllegalStateException illegalStateException = new IllegalStateException(
                                "Cannot create producer on topic with backlog quota exceeded");
                        BacklogQuota.RetentionPolicy retentionPolicy = topic.getBacklogQuota().getPolicy();
                        if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold) {
                            ctx.writeAndFlush(Commands.newError(requestId,
                                    ServerError.ProducerBlockedQuotaExceededError, illegalStateException.getMessage()));
                        } else if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception) {
                            ctx.writeAndFlush(
                                    Commands.newError(requestId, ServerError.ProducerBlockedQuotaExceededException,
                                            illegalStateException.getMessage()));
                        }
                        producerFuture.completeExceptionally(illegalStateException);
                        producers.remove(producerId, producerFuture);
                        return;
                    }

                    disableTcpNoDelayIfNeeded(topicName, producerName);

                    Producer producer = new Producer(topic, ServerCnx.this, producerId, producerName, authRole);

                    try {
                        topic.addProducer(producer);

                        if (isActive()) {
                            if (producerFuture.complete(producer)) {
                                log.info("[{}] Created new producer: {}", remoteAddress, producer);
                                ctx.writeAndFlush(Commands.newProducerSuccess(requestId, producerName));
                                return;
                            } else {
                                // The producer's future was completed before by
                                // a close command
                                producer.closeNow();
                                log.info("[{}] Cleared producer created after timeout on client side {}", remoteAddress,
                                        producer);
                            }
                        } else {
                            producer.closeNow();
                            log.info("[{}] Cleared producer created after connection was closed: {}", remoteAddress,
                                    producer);
                            producerFuture.completeExceptionally(
                                    new IllegalStateException("Producer created after connection was closed"));
                        }
                    } catch (BrokerServiceException ise) {
                        log.error("[{}] Failed to add producer to topic {}: {}", remoteAddress, topicName,
                                ise.getMessage());
                        ctx.writeAndFlush(Commands.newError(requestId, BrokerServiceException.getClientErrorCode(ise),
                                ise.getMessage()));
                        producerFuture.completeExceptionally(ise);
                    }

                    producers.remove(producerId, producerFuture);
                }).exceptionally(exception -> {
                    Throwable cause = exception.getCause();
                    if (!(cause instanceof ServiceUnitNotReadyException)) {
                        // Do not print stack traces for expected exceptions
                        log.error("[{}] Failed to create topic {}", remoteAddress, topicName, exception);
                    }

                    // If client timed out, the future would have been completed
                    // by subsequent close. Send error back to
                    // client, only if not completed already.
                    if (producerFuture.completeExceptionally(exception)) {
                        ctx.writeAndFlush(Commands.newError(requestId, BrokerServiceException.getClientErrorCode(cause),
                                cause.getMessage()));
                    }
                    producers.remove(producerId, producerFuture);

                    return null;
                });
            } else {
                String msg = "Client is not authorized to Produce";
                log.warn("[{}] {} with role {}", remoteAddress, msg, authRole);
                ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
            }
            return null;
        });
    }

    @Override
    protected void handleSend(CommandSend send, ByteBuf headersAndPayload) {
        checkArgument(state == State.Connected);

        CompletableFuture<Producer> producerFuture = producers.get(send.getProducerId());

        if (producerFuture == null || !producerFuture.isDone() || producerFuture.isCompletedExceptionally()) {
            log.warn("[{}] Producer had already been closed: {}", remoteAddress, send.getProducerId());
            return;
        }

        Producer producer = producerFuture.getNow(null);
        if (log.isDebugEnabled()) {
            printSendCommandDebug(send, headersAndPayload);
        }

        // avoid processing non-persist message if reached max concurrent-message limit
        if (producer.isNonPersistentTopic() && !nonPersistentMessageSemaphore.tryAcquire()) {
            final long producerId = send.getProducerId();
            final long sequenceId = send.getSequenceId();
            service.getTopicOrderedExecutor().submitOrdered(producer.getTopic(), SafeRun.safeRun(() -> {
                ctx.writeAndFlush(Commands.newSendReceipt(producerId, sequenceId, -1, -1), ctx.voidPromise());
            }));
            producer.recordMessageDrop();
            return;
        }

        startSendOperation();

        // Persist the message
        producer.publishMessage(send.getProducerId(), send.getSequenceId(), headersAndPayload, send.getNumMessages());
    }

    private void printSendCommandDebug(CommandSend send, ByteBuf headersAndPayload) {
        headersAndPayload.markReaderIndex();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        headersAndPayload.resetReaderIndex();

        log.debug("[{}] Received send message request. producer: {}:{} {}:{} size: {}", remoteAddress,
                send.getProducerId(), send.getSequenceId(), msgMetadata.getProducerName(), msgMetadata.getSequenceId(),
                headersAndPayload.readableBytes());
        msgMetadata.recycle();
    }

    @Override
    protected void handleAck(CommandAck ack) {
        checkArgument(state == State.Connected);
        CompletableFuture<Consumer> consumerFuture = consumers.get(ack.getConsumerId());

        if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
            consumerFuture.getNow(null).messageAcked(ack);
        }
    }

    @Override
    protected void handleFlow(CommandFlow flow) {
        checkArgument(state == State.Connected);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received flow from consumer {} permits: {}", remoteAddress, flow.getConsumerId(),
                    flow.getMessagePermits());
        }

        CompletableFuture<Consumer> consumerFuture = consumers.get(flow.getConsumerId());

        if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
            consumerFuture.getNow(null).flowPermits(flow.getMessagePermits());
        }
    }

    @Override
    protected void handleRedeliverUnacknowledged(CommandRedeliverUnacknowledgedMessages redeliver) {
        checkArgument(state == State.Connected);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received Resend Command from consumer {} ", remoteAddress, redeliver.getConsumerId());
        }

        CompletableFuture<Consumer> consumerFuture = consumers.get(redeliver.getConsumerId());

        if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
            Consumer consumer = consumerFuture.getNow(null);
            if (redeliver.getMessageIdsCount() > 0 && consumer.subType() == SubType.Shared) {
                consumer.redeliverUnacknowledgedMessages(redeliver.getMessageIdsList());
            } else {
                consumer.redeliverUnacknowledgedMessages();
            }
        }
    }

    @Override
    protected void handleUnsubscribe(CommandUnsubscribe unsubscribe) {
        checkArgument(state == State.Connected);

        CompletableFuture<Consumer> consumerFuture = consumers.get(unsubscribe.getConsumerId());

        if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
            consumerFuture.getNow(null).doUnsubscribe(unsubscribe.getRequestId());
        } else {
            ctx.writeAndFlush(
                    Commands.newError(unsubscribe.getRequestId(), ServerError.MetadataError, "Consumer not found"));
        }
    }

    @Override
    protected void handleCloseProducer(CommandCloseProducer closeProducer) {
        checkArgument(state == State.Connected);

        final long producerId = closeProducer.getProducerId();
        final long requestId = closeProducer.getRequestId();

        CompletableFuture<Producer> producerFuture = producers.get(producerId);
        if (producerFuture == null) {
            log.warn("[{}] Producer {} was not registered on the connection", remoteAddress, producerId);
            ctx.writeAndFlush(Commands.newError(requestId, ServerError.UnknownError,
                    "Producer was not registered on the connection"));
            return;
        }

        if (!producerFuture.isDone() && producerFuture
                .completeExceptionally(new IllegalStateException("Closed producer before creation was complete"))) {
            // We have received a request to close the producer before it was actually completed, we have marked the
            // producer future as failed and we can tell the client the close operation was successful. When the actual
            // create operation will complete, the new producer will be discarded.
            log.info("[{}] Closed producer {} before its creation was completed", remoteAddress, producerId);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
            return;
        } else if (producerFuture.isCompletedExceptionally()) {
            log.info("[{}] Closed producer {} that already failed to be created", remoteAddress, producerId);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
            return;
        }

        // Proceed with normal close, the producer
        Producer producer = producerFuture.getNow(null);
        log.info("[{}][{}] Closing producer on cnx {}", producer.getTopic(), producer.getProducerName(), remoteAddress);

        producer.close().thenAccept(v -> {
            log.info("[{}][{}] Closed producer on cnx {}", producer.getTopic(), producer.getProducerName(),
                    remoteAddress);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
            producers.remove(producerId, producerFuture);
        });
    }

    @Override
    protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
        checkArgument(state == State.Connected);
        log.info("[{}] Closing consumer: {}", remoteAddress, closeConsumer.getConsumerId());

        long requestId = closeConsumer.getRequestId();
        long consumerId = closeConsumer.getConsumerId();

        CompletableFuture<Consumer> consumerFuture = consumers.get(consumerId);
        if (consumerFuture == null) {
            log.warn("[{}] Consumer was not registered on the connection: {}", consumerId, remoteAddress);
            ctx.writeAndFlush(Commands.newError(requestId, ServerError.MetadataError, "Consumer not found"));
            return;
        }

        if (!consumerFuture.isDone() && consumerFuture
                .completeExceptionally(new IllegalStateException("Closed consumer before creation was complete"))) {
            // We have received a request to close the consumer before it was actually completed, we have marked the
            // consumer future as failed and we can tell the client the close operation was successful. When the actual
            // create operation will complete, the new consumer will be discarded.
            log.info("[{}] Closed consumer {} before its creation was completed", remoteAddress, consumerId);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
            return;
        }

        if (consumerFuture.isCompletedExceptionally()) {
            log.info("[{}] Closed consumer {} that already failed to be created", remoteAddress, consumerId);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
            return;
        }

        // Proceed with normal consumer close
        Consumer consumer = consumerFuture.getNow(null);
        try {
            consumer.close();
            consumers.remove(consumerId, consumerFuture);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
            log.info("[{}] Closed consumer {}", remoteAddress, consumer);
        } catch (BrokerServiceException e) {
            log.warn("[{]] Error closing consumer: ", remoteAddress, consumer, e);
            ctx.writeAndFlush(
                    Commands.newError(requestId, BrokerServiceException.getClientErrorCode(e), e.getMessage()));
        }
    }

    @Override
    protected boolean isHandshakeCompleted() {
        return state == State.Connected;
    }

    ChannelHandlerContext ctx() {
        return ctx;
    }

    public void closeProducer(Producer producer) {
        // removes producer-connection from map and send close command to producer
        if (log.isDebugEnabled()) {
            log.debug("[{}] Removed producer: {}", remoteAddress, producer);
        }
        long producerId = producer.getProducerId();
        producers.remove(producerId);
        if (remoteEndpointProtocolVersion >= v5.getNumber()) {
            ctx.writeAndFlush(Commands.newCloseProducer(producerId, -1L));
        } else {
            close();
        }

    }

    public void closeConsumer(Consumer consumer) {
        // removes consumer-connection from map and send close command to consumer
        if (log.isDebugEnabled()) {
            log.debug("[{}] Removed consumer: {}", remoteAddress, consumer);
        }
        long consumerId = consumer.consumerId();
        consumers.remove(consumerId);
        if (remoteEndpointProtocolVersion >= v5.getNumber()) {
            ctx.writeAndFlush(Commands.newCloseConsumer(consumerId, -1L));
        } else {
            close();
        }
    }

    /**
     * It closes the connection with client which triggers {@code channelInactive()} which clears all producers and
     * consumers from connection-map
     */
    protected void close() {
        ctx.close();
    }

    public SocketAddress clientAddress() {
        return remoteAddress;
    }

    public void removedConsumer(Consumer consumer) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Removed consumer: {}", remoteAddress, consumer);
        }

        consumers.remove(consumer.consumerId());
    }

    public void removedProducer(Producer producer) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Removed producer: {}", remoteAddress, producer);
        }
        producers.remove(producer.getProducerId());
    }

    public boolean isActive() {
        return isActive;
    }

    public boolean isWritable() {
        return ctx.channel().isWritable();
    }

    public void startSendOperation() {
        if (++pendingSendRequest == MaxPendingSendRequests) {
            // When the quota of pending send requests is reached, stop reading from socket to cause backpressure on
            // client connection, possibly shared between multiple producers
            ctx.channel().config().setAutoRead(false);
        }
    }

    public void completedSendOperation(boolean isNonPersistentTopic) {
        if (--pendingSendRequest == ResumeReadsThreshold) {
            // Resume reading from socket
            ctx.channel().config().setAutoRead(true);
        }
        if (isNonPersistentTopic) {
            nonPersistentMessageSemaphore.release();
        }
    }

    private <T> ServerError getErrorCode(CompletableFuture<T> future) {
        ServerError error = ServerError.UnknownError;
        try {
            future.getNow(null);
        } catch (Exception e) {
            if (e.getCause() instanceof BrokerServiceException) {
                error = BrokerServiceException.getClientErrorCode((BrokerServiceException) e.getCause());
            }
        }
        return error;
    }

    private final void disableTcpNoDelayIfNeeded(String topic, String producerName) {
        if (producerName != null && producerName.startsWith(replicatorPrefix)) {
            // Re-enable nagle algorithm on connections used for replication purposes
            try {
                if (ctx.channel().config().getOption(ChannelOption.TCP_NODELAY).booleanValue() == true) {
                    ctx.channel().config().setOption(ChannelOption.TCP_NODELAY, false);
                }
            } catch (Throwable t) {
                log.warn("[{}] [{}] Failed to remove TCP no-delay property on client cnx {}", topic, producerName,
                        ctx.channel());
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ServerCnx.class);

    /**
     * Helper method for testability
     *
     * @return
     */
    public State getState() {
        return state;
    }

    public BrokerService getBrokerService() {
        return service;
    }

    public String getRole() {
        return authRole;
    }

    boolean hasConsumer(long consumerId) {
        return consumers.containsKey(consumerId);
    }

    public boolean isBatchMessageCompatibleVersion() {
        return remoteEndpointProtocolVersion >= ProtocolVersion.v4.getNumber();
    }

    public String getClientVersion() {
        return clientVersion;
    }
}
