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
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.admin.impl.PersistentTopicsBase.getPartitionedTopicMetadata;
import static org.apache.pulsar.broker.admin.impl.PersistentTopicsBase.unsafeGetPartitionedTopicMetadataAsync;
import static org.apache.pulsar.broker.lookup.TopicLookupBase.lookupTopicAsync;
import static org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion.v5;
import static org.apache.pulsar.common.protocol.Commands.newLookupErrorResponse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslHandler;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Collectors;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionNotFoundException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicNotFoundException;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAuthResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnect;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConsumerStats;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConsumerStatsResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandFlow;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetLastMessageId;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetOrCreateSchema;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetSchema;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandNewTxn;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandRedeliverUnacknowledgedMessages;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSeek;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSend;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandUnsubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.FeatureFlags;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.naming.Metadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.protocol.CommandUtils;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaInfoUtil;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.shaded.com.google.protobuf.v241.GeneratedMessageLite;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerCnx extends PulsarHandler {
    private final BrokerService service;
    private final SchemaRegistryService schemaService;
    private final ConcurrentLongHashMap<CompletableFuture<Producer>> producers;
    private final ConcurrentLongHashMap<CompletableFuture<Consumer>> consumers;
    private State state;
    private volatile boolean isActive = true;
    String authRole = null;
    AuthenticationDataSource authenticationData;
    AuthenticationProvider authenticationProvider;
    AuthenticationState authState;
    // In case of proxy, if the authentication credentials are forwardable,
    // it will hold the credentials of the original client
    AuthenticationState originalAuthState;
    AuthenticationDataSource originalAuthData;
    private boolean pendingAuthChallengeResponse = false;

    // Max number of pending requests per connections. If multiple producers are sharing the same connection the flow
    // control done by a single producer might not be enough to prevent write spikes on the broker.
    private final int maxPendingSendRequests;
    private final int resumeReadsThreshold;
    private int pendingSendRequest = 0;
    private final String replicatorPrefix;
    private String clientVersion = null;
    private int nonPersistentPendingMessages = 0;
    private final int MaxNonPersistentPendingMessages;
    private String originalPrincipal = null;
    private Set<String> proxyRoles;
    private boolean authenticateOriginalAuthData;
    private final boolean schemaValidationEnforced;
    private String authMethod = "none";
    private final int maxMessageSize;
    private boolean preciseDispatcherFlowControl;

    private boolean preciseTopicPublishRateLimitingEnable;

    // Flag to manage throttling-rate by atomically enable/disable read-channel.
    private volatile boolean autoReadDisabledRateLimiting = false;
    private FeatureFlags features;
    // Flag to manage throttling-publish-buffer by atomically enable/disable read-channel.
    private volatile boolean autoReadDisabledPublishBufferLimiting = false;
    private static final AtomicLongFieldUpdater<ServerCnx> MSG_PUBLISH_BUFFER_SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ServerCnx.class, "messagePublishBufferSize");
    private volatile long messagePublishBufferSize = 0;

    enum State {
        Start, Connected, Failed, Connecting
    }

    public ServerCnx(PulsarService pulsar) {
        super(pulsar.getBrokerService().getKeepAliveIntervalSeconds(), TimeUnit.SECONDS);
        this.service = pulsar.getBrokerService();
        this.schemaService = pulsar.getSchemaRegistryService();
        this.state = State.Start;

        // This maps are not heavily contended since most accesses are within the cnx thread
        this.producers = new ConcurrentLongHashMap<>(8, 1);
        this.consumers = new ConcurrentLongHashMap<>(8, 1);
        this.replicatorPrefix = service.pulsar().getConfiguration().getReplicatorPrefix();
        this.MaxNonPersistentPendingMessages = service.pulsar().getConfiguration()
                .getMaxConcurrentNonPersistentMessagePerConnection();
        this.proxyRoles = service.pulsar().getConfiguration().getProxyRoles();
        this.authenticateOriginalAuthData = service.pulsar().getConfiguration().isAuthenticateOriginalAuthData();
        this.schemaValidationEnforced = pulsar.getConfiguration().isSchemaValidationEnforced();
        this.maxMessageSize = pulsar.getConfiguration().getMaxMessageSize();
        this.maxPendingSendRequests = pulsar.getConfiguration().getMaxPendingPublishRequestsPerConnection();
        this.resumeReadsThreshold = maxPendingSendRequests / 2;
        this.preciseDispatcherFlowControl = pulsar.getConfiguration().isPreciseDispatcherFlowControl();
        this.preciseTopicPublishRateLimitingEnable = pulsar.getConfiguration().isPreciseTopicPublishRateLimiterEnable();
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
                producer.closeNow(true);
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
                log.warn("Consumer {} was already closed: {}", consumer, e);
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
        if (state != State.Failed) {
            // No need to report stack trace for known exceptions that happen in disconnections
            log.warn("[{}] Got exception {}", remoteAddress,
                    ClientCnx.isKnownException(cause) ? cause : ExceptionUtils.getStackTrace(cause));
            state = State.Failed;
        } else {
            // At default info level, suppress all subsequent exceptions that are thrown when the connection has already
            // failed
            if (log.isDebugEnabled()) {
                log.debug("[{}] Got exception: {}", remoteAddress, cause);
            }
        }
        ctx.close();
    }

    /*
     * If authentication and authorization is enabled(and not sasl) and if the authRole is one of proxyRoles we want to enforce
     * - the originalPrincipal is given while connecting
     * - originalPrincipal is not blank
     * - originalPrincipal is not a proxy principal
     */
    private boolean invalidOriginalPrincipal(String originalPrincipal) {
        return (service.isAuthenticationEnabled() && service.isAuthorizationEnabled()
            && proxyRoles.contains(authRole) && (StringUtils.isBlank(originalPrincipal) || proxyRoles.contains(originalPrincipal)));
    }

    // ////
    // // Incoming commands handling
    // ////

    private CompletableFuture<Boolean> isTopicOperationAllowed(TopicName topicName, TopicOperation operation) {
        CompletableFuture<Boolean> isProxyAuthorizedFuture;
        CompletableFuture<Boolean> isAuthorizedFuture;
        if (service.isAuthorizationEnabled()) {
            if (originalPrincipal != null) {
                isProxyAuthorizedFuture = service.getAuthorizationService().allowTopicOperationAsync(
                    topicName, operation, originalPrincipal, getAuthenticationData());
            } else {
                isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
            }
            isAuthorizedFuture = service.getAuthorizationService().allowTopicOperationAsync(
                topicName, operation, authRole, authenticationData);
        } else {
            isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
            isAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        return isProxyAuthorizedFuture.thenCombine(isAuthorizedFuture, (isProxyAuthorized, isAuthorized) -> {
            if (!isProxyAuthorized) {
                log.error("OriginalRole {} is not authorized to perform operation {} on topic {}",
                    originalPrincipal, operation, topicName);
            }
            if (!isAuthorized) {
                log.error("Role {} is not authorized to perform operation {} on topic {}",
                    authRole, operation, topicName);
            }
            return isProxyAuthorized && isAuthorized;
        });
    }

    private CompletableFuture<Boolean> isTopicOperationAllowed(TopicName topicName, String subscriptionName, TopicOperation operation) {
        CompletableFuture<Boolean> isProxyAuthorizedFuture;
        CompletableFuture<Boolean> isAuthorizedFuture;
        if (service.isAuthorizationEnabled()) {
            if (authenticationData == null) {
                authenticationData = new AuthenticationDataCommand("", subscriptionName);
            } else {
                authenticationData.setSubscription(subscriptionName);
            }
            if (originalAuthData != null) {
                originalAuthData.setSubscription(subscriptionName);
            }
            return isTopicOperationAllowed(topicName, operation);
        } else {
            isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
            isAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        return isProxyAuthorizedFuture.thenCombine(isAuthorizedFuture, (isProxyAuthorized, isAuthorized) -> {
            if (!isProxyAuthorized) {
                log.warn("OriginalRole {} is not authorized to perform operation {} on topic {}, subscription {}",
                    originalPrincipal, operation, topicName, subscriptionName);
            }
            if (!isAuthorized) {
                log.warn("Role {} is not authorized to perform operation {} on topic {}, subscription {}",
                    authRole, operation, topicName, subscriptionName);
            }
            return isProxyAuthorized && isAuthorized;
        });
    }

    @Override
    protected void handleLookup(CommandLookupTopic lookup) {
        final long requestId = lookup.getRequestId();
        final boolean authoritative = lookup.getAuthoritative();
        final String advertisedListenerName = lookup.getAdvertisedListenerName();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received Lookup from {} for {}", lookup.getTopic(), remoteAddress, requestId);
        }

        TopicName topicName = validateTopicName(lookup.getTopic(), requestId, lookup);
        if (topicName == null) {
            return;
        }

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            if (invalidOriginalPrincipal(originalPrincipal)) {
                final String msg = "Valid Proxy Client role should be provided for lookup ";
                log.warn("[{}] {} with role {} and proxyClientAuthRole {} on topic {}", remoteAddress, msg, authRole,
                        originalPrincipal, topicName);
                ctx.writeAndFlush(newLookupErrorResponse(ServerError.AuthorizationError, msg, requestId));
                lookupSemaphore.release();
                return;
            }
            isTopicOperationAllowed(topicName, TopicOperation.LOOKUP).thenApply(isAuthorized -> {
                if (isAuthorized) {
                    lookupTopicAsync(getBrokerService().pulsar(), topicName, authoritative,
                            getPrincipal(), getAuthenticationData(),
                            requestId, advertisedListenerName).handle((lookupResponse, ex) -> {
                                if (ex == null) {
                                    ctx.writeAndFlush(lookupResponse);
                                } else {
                                    // it should never happen
                                    log.warn("[{}] lookup failed with error {}, {}", remoteAddress, topicName,
                                            ex.getMessage(), ex);
                                    ctx.writeAndFlush(newLookupErrorResponse(ServerError.ServiceNotReady,
                                            ex.getMessage(), requestId));
                                }
                                lookupSemaphore.release();
                                return null;
                            });
                } else {
                    final String msg = "Proxy Client is not authorized to Lookup";
                    log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, getPrincipal(), topicName);
                    ctx.writeAndFlush(newLookupErrorResponse(ServerError.AuthorizationError, msg, requestId));
                    lookupSemaphore.release();
                }
                return null;
            }).exceptionally(ex -> {
                final String msg = "Exception occurred while trying to authorize lookup";
                log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, getPrincipal(), topicName, ex);
                ctx.writeAndFlush(newLookupErrorResponse(ServerError.AuthorizationError, msg, requestId));
                lookupSemaphore.release();
                return null;
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed lookup due to too many lookup-requests {}", remoteAddress, topicName);
            }
            ctx.writeAndFlush(newLookupErrorResponse(ServerError.TooManyRequests,
                    "Failed due to too many pending lookup requests", requestId));
        }
    }

    @Override
    protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata partitionMetadata) {
        final long requestId = partitionMetadata.getRequestId();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received PartitionMetadataLookup from {} for {}", partitionMetadata.getTopic(),
                    remoteAddress, requestId);
        }

        TopicName topicName = validateTopicName(partitionMetadata.getTopic(), requestId, partitionMetadata);
        if (topicName == null) {
            return;
        }

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            if (invalidOriginalPrincipal(originalPrincipal)) {
                final String msg = "Valid Proxy Client role should be provided for getPartitionMetadataRequest ";
                log.warn("[{}] {} with role {} and proxyClientAuthRole {} on topic {}", remoteAddress, msg, authRole,
                        originalPrincipal, topicName);
                ctx.writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.AuthorizationError,
                        msg, requestId));
                lookupSemaphore.release();
                return;
            }
            isTopicOperationAllowed(topicName, TopicOperation.LOOKUP).thenApply(isAuthorized -> {
                if (isAuthorized) {
                    unsafeGetPartitionedTopicMetadataAsync(getBrokerService().pulsar(), topicName)
                        .handle((metadata, ex) -> {
                                if (ex == null) {
                                    int partitions = metadata.partitions;
                                    ctx.writeAndFlush(Commands.newPartitionMetadataResponse(partitions, requestId));
                                } else {
                                    if (ex instanceof PulsarClientException) {
                                        log.warn("Failed to authorize {} at [{}] on topic {} : {}", getRole(),
                                                remoteAddress, topicName, ex.getMessage());
                                        ctx.writeAndFlush(Commands.newPartitionMetadataResponse(
                                                ServerError.AuthorizationError, ex.getMessage(), requestId));
                                    } else {
                                        log.warn("Failed to get Partitioned Metadata [{}] {}: {}", remoteAddress,
                                                topicName, ex.getMessage(), ex);
                                        ServerError error = (ex instanceof RestException)
                                                && ((RestException) ex).getResponse().getStatus() < 500
                                                        ? ServerError.MetadataError
                                                        : ServerError.ServiceNotReady;
                                        ctx.writeAndFlush(Commands.newPartitionMetadataResponse(error,
                                                ex.getMessage(), requestId));
                                    }
                                }
                                lookupSemaphore.release();
                                return null;
                            });
                } else {
                    final String msg = "Proxy Client is not authorized to Get Partition Metadata";
                    log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, getPrincipal(), topicName);
                    ctx.writeAndFlush(
                            Commands.newPartitionMetadataResponse(ServerError.AuthorizationError, msg, requestId));
                    lookupSemaphore.release();
                }
                return null;
            }).exceptionally(ex -> {
                final String msg = "Exception occurred while trying to authorize get Partition Metadata";
                log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, getPrincipal(), topicName);
                ctx.writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.AuthorizationError, msg, requestId));
                lookupSemaphore.release();
                return null;
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed Partition-Metadata lookup due to too many lookup-requests {}", remoteAddress,
                        topicName);
            }
            ctx.writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.TooManyRequests,
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
        commandConsumerStatsResponseBuilder.setAddress(consumerStats.getAddress());
        commandConsumerStatsResponseBuilder.setConnectedSince(consumerStats.getConnectedSince());

        Subscription subscription = consumer.getSubscription();
        commandConsumerStatsResponseBuilder.setMsgBacklog(subscription.getNumberOfEntriesInBacklog(false));
        commandConsumerStatsResponseBuilder.setMsgRateExpired(subscription.getExpiredMessageRate());
        commandConsumerStatsResponseBuilder.setType(subscription.getTypeString());

        return commandConsumerStatsResponseBuilder;
    }

    // complete the connect and sent newConnected command
    private void completeConnect(int clientProtoVersion, String clientVersion) {
        ctx.writeAndFlush(Commands.newConnected(clientProtoVersion, maxMessageSize));
        state = State.Connected;
        remoteEndpointProtocolVersion = clientProtoVersion;
        if (isNotBlank(clientVersion) && !clientVersion.contains(" ") /* ignore default version: pulsar client */) {
            this.clientVersion = clientVersion.intern();
        }
    }

    // According to auth result, send newConnected or newAuthChallenge command.
    private State doAuthentication(AuthData clientData,
                                   int clientProtocolVersion,
                                   String clientVersion) throws Exception {

        // The original auth state can only be set on subsequent auth attempts (and only
        // in presence of a proxy and if the proxy is forwarding the credentials).
        // In this case, the re-validation needs to be done against the original client
        // credentials.
        boolean useOriginalAuthState = (originalAuthState != null);
        AuthenticationState authState =  useOriginalAuthState ? originalAuthState : this.authState;
        String authRole = useOriginalAuthState ? originalPrincipal : this.authRole;
        AuthData brokerData = authState.authenticate(clientData);

        if (log.isDebugEnabled()) {
            log.debug("Authenticate using original auth state : {}, role = {}", useOriginalAuthState, authRole);
        }

        if (authState.isComplete()) {
            // Authentication has completed. It was either:
            // 1. the 1st time the authentication process was done, in which case we'll
            //    a `CommandConnected` response
            // 2. an authentication refresh, in which case we don't need to do anything else

            String newAuthRole = authState.getAuthRole();

            if (!useOriginalAuthState) {
                this.authRole = newAuthRole;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Client successfully authenticated with {} role {} and originalPrincipal {}",
                        remoteAddress, authMethod, this.authRole, originalPrincipal);
            }

            if (state != State.Connected) {
                // First time authentication is done
                completeConnect(clientProtocolVersion, clientVersion);
            } else {
                // If the connection was already ready, it means we're doing a refresh
                if (!StringUtils.isEmpty(authRole)) {
                    if (!authRole.equals(newAuthRole)) {
                        log.warn("[{}] Principal cannot be changed during an authentication refresh", remoteAddress);
                        ctx.close();
                    } else {
                        log.info("[{}] Refreshed authentication credentials for role {}", remoteAddress, authRole);
                    }
                }
            }

            return State.Connected;
        }

        // auth not complete, continue auth with client side.
        ctx.writeAndFlush(Commands.newAuthChallenge(authMethod, brokerData, clientProtocolVersion));
        if (log.isDebugEnabled()) {
            log.debug("[{}] Authentication in progress client by method {}.",
                remoteAddress, authMethod);
        }
        return State.Connecting;
    }

    public void refreshAuthenticationCredentials() {
        AuthenticationState authState = this.originalAuthState != null ? originalAuthState : this.authState;

        if (authState == null) {
            // Authentication is disabled or there's no local state to refresh
            return;
        } else if (getState() != State.Connected || !isActive) {
            // Connection is either still being established or already closed.
            return;
        } else if (authState != null && !authState.isExpired()) {
            // Credentials are still valid. Nothing to do at this point
            return;
        } else if (originalPrincipal != null && originalAuthState == null) {
            log.info(
                    "[{}] Cannot revalidate user credential when using proxy and not forwarding the credentials. Closing connection",
                    remoteAddress);
            return;
        }

        ctx.executor().execute(SafeRun.safeRun(() -> {
            log.info("[{}] Refreshing authentication credentials for originalPrincipal {} and authRole {}", remoteAddress, originalPrincipal, this.authRole);

            if (!supportsAuthenticationRefresh()) {
                log.warn("[{}] Closing connection because client doesn't support auth credentials refresh", remoteAddress);
                ctx.close();
                return;
            }

            if (pendingAuthChallengeResponse) {
                log.warn("[{}] Closing connection after timeout on refreshing auth credentials", remoteAddress);
                ctx.close();
                return;
            }

            try {
                AuthData brokerData = authState.refreshAuthentication();

                ctx.writeAndFlush(Commands.newAuthChallenge(authMethod, brokerData, remoteEndpointProtocolVersion));
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Sent auth challenge to client to refresh credentials with method: {}.",
                        remoteAddress, authMethod);
                }

                pendingAuthChallengeResponse = true;

            } catch (AuthenticationException e) {
                log.warn("[{}] Failed to refresh authentication: {}", remoteAddress, e);
                ctx.close();
            }
        }));
    }

    @Override
    protected void handleConnect(CommandConnect connect) {
        checkArgument(state == State.Start);

        if (log.isDebugEnabled()) {
            log.debug("Received CONNECT from {}, auth enabled: {}:"
                    + " has original principal = {}, original principal = {}",
                remoteAddress,
                service.isAuthenticationEnabled(),
                connect.hasOriginalPrincipal(),
                connect.getOriginalPrincipal());
        }

        String clientVersion = connect.getClientVersion();
        int clientProtocolVersion = connect.getProtocolVersion();
        features = connect.getFeatureFlags();

        if (!service.isAuthenticationEnabled()) {
            completeConnect(clientProtocolVersion, clientVersion);
            return;
        }

        try {
            AuthData clientData = AuthData.of(connect.getAuthData().toByteArray());

            // init authentication
            if (connect.hasAuthMethodName()) {
                authMethod = connect.getAuthMethodName();
            } else if (connect.hasAuthMethod()) {
                // Legacy client is passing enum
                authMethod = connect.getAuthMethod().name().substring(10).toLowerCase();
            } else {
                authMethod = "none";
            }

            authenticationProvider = getBrokerService()
                .getAuthenticationService()
                .getAuthenticationProvider(authMethod);

            // Not find provider named authMethod. Most used for tests.
            // In AuthenticationDisabled, it will set authMethod "none".
            if (authenticationProvider == null) {
                authRole = getBrokerService().getAuthenticationService().getAnonymousUserRole()
                    .orElseThrow(() ->
                        new AuthenticationException("No anonymous role, and no authentication provider configured"));
                completeConnect(clientProtocolVersion, clientVersion);
                return;
            }

            // init authState and other var
            ChannelHandler sslHandler = ctx.channel().pipeline().get(PulsarChannelInitializer.TLS_HANDLER);
            SSLSession sslSession = null;
            if (sslHandler != null) {
                sslSession = ((SslHandler) sslHandler).engine().getSession();
            }

            authState = authenticationProvider.newAuthState(clientData, remoteAddress, sslSession);
            authenticationData = authState.getAuthDataSource();

            if (log.isDebugEnabled()) {
                log.debug("[{}] Authenticate role : {}", remoteAddress,
                    authState != null ? authState.getAuthRole() : null);
            }

            state = doAuthentication(clientData, clientProtocolVersion, clientVersion);

            // This will fail the check if:
            //  1. client is coming through a proxy
            //  2. we require to validate the original credentials
            //  3. no credentials were passed
            if (connect.hasOriginalPrincipal() && service.getPulsar().getConfig().isAuthenticateOriginalAuthData()) {
                // init authentication
                String originalAuthMethod;
                if (connect.hasOriginalAuthMethod()) {
                    originalAuthMethod = connect.getOriginalAuthMethod();
                } else {
                    originalAuthMethod = "none";
                }

                AuthenticationProvider originalAuthenticationProvider = getBrokerService()
                        .getAuthenticationService()
                        .getAuthenticationProvider(originalAuthMethod);

                if (originalAuthenticationProvider == null) {
                    throw new AuthenticationException(String.format("Can't find AuthenticationProvider for original role" +
                            " using auth method [%s] is not available", originalAuthMethod));
                }

                originalAuthState = originalAuthenticationProvider.newAuthState(
                        AuthData.of(connect.getOriginalAuthData().getBytes()),
                        remoteAddress,
                        sslSession);
                originalAuthData = originalAuthState.getAuthDataSource();
                originalPrincipal = originalAuthState.getAuthRole();

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Authenticate original role : {}", remoteAddress, originalPrincipal);
                }
            } else {
                originalPrincipal = connect.hasOriginalPrincipal() ? connect.getOriginalPrincipal() : null;
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Authenticate original role (forwarded from proxy): {}",
                        remoteAddress, originalPrincipal);
                }
            }
        } catch (Exception e) {
            String msg = "Unable to authenticate";
            if (e instanceof AuthenticationException) {
                log.warn("[{}] {}: {}", remoteAddress, msg, e.getMessage());
            } else {
                log.warn("[{}] {}", remoteAddress, msg, e);
            }
            ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, msg));
            close();
        }
    }

    @Override
    protected void handleAuthResponse(CommandAuthResponse authResponse) {
        checkArgument(authResponse.hasResponse());
        checkArgument(authResponse.getResponse().hasAuthData() && authResponse.getResponse().hasAuthMethodName());

        pendingAuthChallengeResponse = false;

        if (log.isDebugEnabled()) {
            log.debug("Received AuthResponse from {}, auth method: {}",
                remoteAddress, authResponse.getResponse().getAuthMethodName());
        }

        try {
            AuthData clientData = AuthData.of(authResponse.getResponse().getAuthData().toByteArray());
            doAuthentication(clientData, authResponse.getProtocolVersion(), authResponse.getClientVersion());
        } catch (AuthenticationException e) {
            log.warn("[{}] Authentication failed: {} ", remoteAddress, e.getMessage());
            ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, e.getMessage()));
            close();
        } catch (Exception e) {
            String msg = "Unable to handleAuthResponse";
            log.warn("[{}] {} ", remoteAddress, msg, e);
            ctx.writeAndFlush(Commands.newError(-1, ServerError.UnknownError, msg));
            close();
        }
    }

    @Override
    protected void handleSubscribe(final CommandSubscribe subscribe) {
        checkArgument(state == State.Connected);
        final long requestId = subscribe.getRequestId();
        final long consumerId = subscribe.getConsumerId();
        TopicName topicName = validateTopicName(subscribe.getTopic(), requestId, subscribe);
        if (topicName == null) {
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Handle subscribe command: auth role = {}, original auth role = {}",
                remoteAddress, authRole, originalPrincipal);
        }

        if (invalidOriginalPrincipal(originalPrincipal)) {
            final String msg = "Valid Proxy Client role should be provided while subscribing ";
            log.warn("[{}] {} with role {} and proxyClientAuthRole {} on topic {}", remoteAddress, msg, authRole,
                    originalPrincipal, topicName);
            ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
            return;
        }

        final String subscriptionName = subscribe.getSubscription();
        final SubType subType = subscribe.getSubType();
        final String consumerName = subscribe.getConsumerName();
        final boolean isDurable = subscribe.getDurable();
        final MessageIdImpl startMessageId = subscribe.hasStartMessageId() ? new BatchMessageIdImpl(
                subscribe.getStartMessageId().getLedgerId(), subscribe.getStartMessageId().getEntryId(),
                subscribe.getStartMessageId().getPartition(), subscribe.getStartMessageId().getBatchIndex())
                : null;
        final int priorityLevel = subscribe.hasPriorityLevel() ? subscribe.getPriorityLevel() : 0;
        final boolean readCompacted = subscribe.getReadCompacted();
        final Map<String, String> metadata = CommandUtils.metadataFromCommand(subscribe);
        final InitialPosition initialPosition = subscribe.getInitialPosition();
        final long startMessageRollbackDurationSec = subscribe.hasStartMessageRollbackDurationSec()
                ? subscribe.getStartMessageRollbackDurationSec()
                : -1;
        final SchemaData schema = subscribe.hasSchema() ? getSchema(subscribe.getSchema()) : null;
        final boolean isReplicated = subscribe.hasReplicateSubscriptionState() && subscribe.getReplicateSubscriptionState();
        final boolean forceTopicCreation = subscribe.getForceTopicCreation();
        final PulsarApi.KeySharedMeta keySharedMeta = subscribe.hasKeySharedMeta() ? subscribe.getKeySharedMeta() : null;

        CompletableFuture<Boolean> isAuthorizedFuture = isTopicOperationAllowed(
            topicName,
            subscriptionName,
            TopicOperation.CONSUME
        );
        isAuthorizedFuture.thenApply(isAuthorized -> {
                    if (isAuthorized) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Client is authorized to subscribe with role {}", remoteAddress, getPrincipal());
                        }

                        log.info("[{}] Subscribing on topic {} / {}", remoteAddress, topicName, subscriptionName);
                        try {
                            Metadata.validateMetadata(metadata);
                        } catch (IllegalArgumentException iae) {
                            final String msg = iae.getMessage();
                            ctx.writeAndFlush(Commands.newError(requestId, ServerError.MetadataError, msg));
                            return null;
                        }
                        CompletableFuture<Consumer> consumerFuture = new CompletableFuture<>();
                        CompletableFuture<Consumer> existingConsumerFuture = consumers.putIfAbsent(consumerId,
                                consumerFuture);

                        if (existingConsumerFuture != null) {
                            if (existingConsumerFuture.isDone() && !existingConsumerFuture.isCompletedExceptionally()) {
                                Consumer consumer = existingConsumerFuture.getNow(null);
                                log.info("[{}] Consumer with the same id {} is already created: {}", remoteAddress,
                                        consumerId, consumer);
                                ctx.writeAndFlush(Commands.newSuccess(requestId));
                                return null;
                            } else {
                                // There was an early request to create a consumer with same consumerId. This can happen
                                // when
                                // client timeout is lower the broker timeouts. We need to wait until the previous
                                // consumer
                                // creation request either complete or fails.
                                log.warn("[{}][{}][{}] Consumer with id {} is already present on the connection", remoteAddress,
                                        topicName, subscriptionName, consumerId);
                                ServerError error = null;
                                if(!existingConsumerFuture.isDone()) {
                                    error = ServerError.ServiceNotReady;
                                }else {
                                    error = getErrorCode(existingConsumerFuture);
                                    consumers.remove(consumerId);
                                }
                                ctx.writeAndFlush(Commands.newError(requestId, error,
                                        "Consumer is already present on the connection"));
                                return null;
                            }
                        }

                        boolean createTopicIfDoesNotExist = forceTopicCreation
                                && service.isAllowAutoTopicCreation(topicName.toString());

                        service.getTopic(topicName.toString(), createTopicIfDoesNotExist)
                                .thenCompose(optTopic -> {
                                    if (!optTopic.isPresent()) {
                                        return FutureUtil
                                                .failedFuture(new TopicNotFoundException("Topic does not exist"));
                                    }

                                    Topic topic = optTopic.get();

                                    boolean rejectSubscriptionIfDoesNotExist = isDurable
                                        && !service.isAllowAutoSubscriptionCreation(topicName.toString())
                                        && !topic.getSubscriptions().containsKey(subscriptionName);

                                    if (rejectSubscriptionIfDoesNotExist) {
                                        return FutureUtil
                                            .failedFuture(new SubscriptionNotFoundException("Subscription does not exist"));
                                    }

                                    if (schema != null) {
                                        return topic.addSchemaIfIdleOrCheckCompatible(schema)
                                            .thenCompose(v -> topic.subscribe(ServerCnx.this, subscriptionName, consumerId,
                                                    subType, priorityLevel, consumerName, isDurable,
                                                    startMessageId, metadata,
                                                    readCompacted, initialPosition, startMessageRollbackDurationSec, isReplicated, keySharedMeta));
                                    } else {
                                        return topic.subscribe(ServerCnx.this, subscriptionName, consumerId,
                                            subType, priorityLevel, consumerName, isDurable,
                                            startMessageId, metadata, readCompacted, initialPosition,
                                            startMessageRollbackDurationSec, isReplicated, keySharedMeta);
                                    }
                                })
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
                                            log.warn(
                                                    "[{}] Error closing consumer created after timeout on client side {}: {}",
                                                    remoteAddress, consumer, e.getMessage());
                                        }
                                        consumers.remove(consumerId, consumerFuture);
                                    }

                                }) //
                                .exceptionally(exception -> {
                                    if (exception.getCause() instanceof ConsumerBusyException) {
                                        if (log.isDebugEnabled()) {
                                            log.debug(
                                                    "[{}][{}][{}] Failed to create consumer because exclusive consumer is already connected: {}",
                                                    remoteAddress, topicName, subscriptionName,
                                                    exception.getCause().getMessage());
                                        }
                                    } else if (exception.getCause() instanceof BrokerServiceException) {
                                        log.warn("[{}][{}][{}] Failed to create consumer: {}", remoteAddress, topicName,
                                                subscriptionName, exception.getCause().getMessage());
                                    } else {
                                        log.warn("[{}][{}][{}] Failed to create consumer: {}", remoteAddress, topicName,
                                                subscriptionName, exception.getCause().getMessage(), exception);
                                    }

                                    // If client timed out, the future would have been completed by subsequent close.
                                    // Send error
                                    // back to client, only if not completed already.
                                    if (consumerFuture.completeExceptionally(exception)) {
                                        ctx.writeAndFlush(Commands.newError(requestId,
                                                BrokerServiceException.getClientErrorCode(exception),
                                                exception.getCause().getMessage()));
                                    }
                                    consumers.remove(consumerId, consumerFuture);

                                    return null;

                                });
                    } else {
                        String msg = "Client is not authorized to subscribe";
                        log.warn("[{}] {} with role {}", remoteAddress, msg, getPrincipal());
                        ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
                    }
                    return null;
        }).exceptionally(ex -> {
            String msg = String.format("[%s] %s with role %s", remoteAddress, ex.getMessage(), getPrincipal());
            if (ex.getCause() instanceof PulsarServerException) {
                log.info(msg);
            } else {
                log.warn(msg);
            }
            ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, ex.getMessage()));
            return null;
        });
    }

    private SchemaData getSchema(PulsarApi.Schema protocolSchema) {
        return SchemaData.builder()
            .data(protocolSchema.getSchemaData().toByteArray())
            .isDeleted(false)
            .timestamp(System.currentTimeMillis())
            .user(Strings.nullToEmpty(originalPrincipal))
            .type(Commands.getSchemaType(protocolSchema.getType()))
            .props(protocolSchema.getPropertiesList().stream().collect(
                Collectors.toMap(
                    PulsarApi.KeyValue::getKey,
                    PulsarApi.KeyValue::getValue
                )
            )).build();
    }

    @Override
    protected void handleProducer(final CommandProducer cmdProducer) {
        checkArgument(state == State.Connected);
        final long producerId = cmdProducer.getProducerId();
        final long requestId = cmdProducer.getRequestId();
        // Use producer name provided by client if present
        final String producerName = cmdProducer.hasProducerName() ? cmdProducer.getProducerName()
                : service.generateUniqueProducerName();
        final long epoch = cmdProducer.getEpoch();
        final boolean userProvidedProducerName = cmdProducer.getUserProvidedProducerName();
        final boolean isEncrypted = cmdProducer.getEncrypted();
        final Map<String, String> metadata = CommandUtils.metadataFromCommand(cmdProducer);
        final SchemaData schema = cmdProducer.hasSchema() ? getSchema(cmdProducer.getSchema()) : null;

        TopicName topicName = validateTopicName(cmdProducer.getTopic(), requestId, cmdProducer);
        if (topicName == null) {
            return;
        }

        if (invalidOriginalPrincipal(originalPrincipal)) {
            final String msg = "Valid Proxy Client role should be provided while creating producer ";
            log.warn("[{}] {} with role {} and proxyClientAuthRole {} on topic {}", remoteAddress, msg, authRole,
                    originalPrincipal, topicName);
            ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
            return;
        }

        CompletableFuture<Boolean> isAuthorizedFuture = isTopicOperationAllowed(
            topicName, TopicOperation.PRODUCE
        );
        isAuthorizedFuture.thenApply(isAuthorized -> {
                    if (isAuthorized) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Client is authorized to Produce with role {}", remoteAddress, getPrincipal());
                        }
                        CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
                        CompletableFuture<Producer> existingProducerFuture = producers.putIfAbsent(producerId,
                                producerFuture);

                        if (existingProducerFuture != null) {
                            if (existingProducerFuture.isDone() && !existingProducerFuture.isCompletedExceptionally()) {
                                Producer producer = existingProducerFuture.getNow(null);
                                log.info("[{}] Producer with the same id {} is already created: {}", remoteAddress,
                                        producerId, producer);
                                ctx.writeAndFlush(Commands.newProducerSuccess(requestId, producer.getProducerName(),
                                    producer.getSchemaVersion()));
                                return null;
                            } else {
                                // There was an early request to create a producer with
                                // same producerId. This can happen when
                                // client
                                // timeout is lower the broker timeouts. We need to wait
                                // until the previous producer creation
                                // request
                                // either complete or fails.
                                ServerError error = null;
                                if(!existingProducerFuture.isDone()) {
                                    error = ServerError.ServiceNotReady;
                                } else {
                                    error = getErrorCode(existingProducerFuture);
                                    // remove producer with producerId as it's already completed with exception
                                    producers.remove(producerId);
                                }
                                log.warn("[{}][{}] Producer with id {} is already present on the connection", remoteAddress,
                                        producerId, topicName);
                                ctx.writeAndFlush(Commands.newError(requestId, error,
                                        "Producer is already present on the connection"));
                                return null;
                            }
                        }

                        log.info("[{}][{}] Creating producer. producerId={}", remoteAddress, topicName, producerId);

                        service.getOrCreateTopic(topicName.toString()).thenAccept((Topic topic) -> {
                            // Before creating producer, check if backlog quota exceeded
                            // on topic
                            if (topic.isBacklogQuotaExceeded(producerName)) {
                                IllegalStateException illegalStateException = new IllegalStateException(
                                        "Cannot create producer on topic with backlog quota exceeded");
                                BacklogQuota.RetentionPolicy retentionPolicy = topic.getBacklogQuota().getPolicy();
                                if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold) {
                                    ctx.writeAndFlush(
                                            Commands.newError(requestId, ServerError.ProducerBlockedQuotaExceededError,
                                                    illegalStateException.getMessage()));
                                } else if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception) {
                                    ctx.writeAndFlush(Commands.newError(requestId,
                                            ServerError.ProducerBlockedQuotaExceededException,
                                            illegalStateException.getMessage()));
                                }
                                producerFuture.completeExceptionally(illegalStateException);
                                producers.remove(producerId, producerFuture);
                                return;
                            }

                            // Check whether the producer will publish encrypted messages or not
                            if (topic.isEncryptionRequired() && !isEncrypted) {
                                String msg = String.format("Encryption is required in %s", topicName);
                                log.warn("[{}] {}", remoteAddress, msg);
                                ctx.writeAndFlush(Commands.newError(requestId, ServerError.MetadataError, msg));
                                producers.remove(producerId, producerFuture);
                                return;
                            }

                            disableTcpNoDelayIfNeeded(topicName.toString(), producerName);

                            CompletableFuture<SchemaVersion> schemaVersionFuture = tryAddSchema(topic, schema);

                            schemaVersionFuture.exceptionally(exception -> {
                                ctx.writeAndFlush(Commands.newError(requestId,
                                        BrokerServiceException.getClientErrorCode(exception),
                                        exception.getMessage()));
                                producers.remove(producerId, producerFuture);
                                return null;
                            });

                            schemaVersionFuture.thenAccept(schemaVersion -> {
                                Producer producer = new Producer(topic, ServerCnx.this, producerId, producerName, getPrincipal(),
                                    isEncrypted, metadata, schemaVersion, epoch, userProvidedProducerName);

                                try {
                                    topic.addProducer(producer);

                                    if (isActive()) {
                                        if (producerFuture.complete(producer)) {
                                            log.info("[{}] Created new producer: {}", remoteAddress, producer);
                                            ctx.writeAndFlush(Commands.newProducerSuccess(requestId, producerName,
                                                producer.getLastSequenceId(), producer.getSchemaVersion()));
                                            return;
                                        } else {
                                            // The producer's future was completed before by
                                            // a close command
                                            producer.closeNow(true);
                                            log.info("[{}] Cleared producer created after timeout on client side {}",
                                                remoteAddress, producer);
                                        }
                                    } else {
                                        producer.closeNow(true);
                                        log.info("[{}] Cleared producer created after connection was closed: {}",
                                            remoteAddress, producer);
                                        producerFuture.completeExceptionally(
                                            new IllegalStateException("Producer created after connection was closed"));
                                    }
                                } catch (Exception ise) {
                                    log.error("[{}] Failed to add producer to topic {}: {}", remoteAddress, topicName,
                                        ise.getMessage());
                                    ctx.writeAndFlush(Commands.newError(requestId,
                                        BrokerServiceException.getClientErrorCode(ise), ise.getMessage()));
                                    producerFuture.completeExceptionally(ise);
                                }

                                producers.remove(producerId, producerFuture);
                            });
                        }).exceptionally(exception -> {
                            Throwable cause = exception.getCause();

                            if (cause instanceof NoSuchElementException) {
                                cause = new TopicNotFoundException("Topic Not Found.");
                            }

                            if (!(cause instanceof ServiceUnitNotReadyException)) {
                                // Do not print stack traces for expected exceptions
                                log.error("[{}] Failed to create topic {}", remoteAddress, topicName, exception);
                            }

                            // If client timed out, the future would have been completed
                            // by subsequent close. Send error back to
                            // client, only if not completed already.
                            if (producerFuture.completeExceptionally(exception)) {
                                ctx.writeAndFlush(Commands.newError(requestId,
                                        BrokerServiceException.getClientErrorCode(cause), cause.getMessage()));
                            }
                            producers.remove(producerId, producerFuture);

                            return null;
                        });
                    } else {
                        String msg = "Client is not authorized to Produce";
                        log.warn("[{}] {} with role {}", remoteAddress, msg, getPrincipal());
                        ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
                    }
                    return null;
        }).exceptionally(ex -> {
            String msg = String.format("[%s] %s with role %s", remoteAddress, ex.getMessage(), getPrincipal());
            log.warn(msg);
            ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, ex.getMessage()));
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

        if (producer.isNonPersistentTopic()) {
            // avoid processing non-persist message if reached max concurrent-message limit
            if (nonPersistentPendingMessages > MaxNonPersistentPendingMessages) {
                final long producerId = send.getProducerId();
                final long sequenceId = send.getSequenceId();
                final long highestSequenceId = send.getHighestSequenceId();
                service.getTopicOrderedExecutor().executeOrdered(producer.getTopic().getName(), SafeRun.safeRun(() -> {
                    ctx.writeAndFlush(Commands.newSendReceipt(producerId, sequenceId, highestSequenceId, -1, -1), ctx.voidPromise());
                }));
                producer.recordMessageDrop(send.getNumMessages());
                return;
            } else {
                nonPersistentPendingMessages++;
            }
        }

        startSendOperation(producer, headersAndPayload.readableBytes(), send.getNumMessages());

        if (send.hasTxnidMostBits() && send.hasTxnidLeastBits()) {
            TxnID txnID = new TxnID(send.getTxnidMostBits(), send.getTxnidLeastBits());
            producer.publishTxnMessage(txnID, producer.getProducerId(), send.getSequenceId(),
                    send.getHighestSequenceId(), headersAndPayload, send.getNumMessages(), send.getIsChunk());
            return;
        }

        // Persist the message
        if (send.hasHighestSequenceId() && send.getSequenceId() <= send.getHighestSequenceId()) {
            producer.publishMessage(send.getProducerId(), send.getSequenceId(), send.getHighestSequenceId(),
                    headersAndPayload, send.getNumMessages(), send.getIsChunk());
        } else {
            producer.publishMessage(send.getProducerId(), send.getSequenceId(), headersAndPayload,
                    send.getNumMessages(), send.getIsChunk());
        }
    }

    private void printSendCommandDebug(CommandSend send, ByteBuf headersAndPayload) {
        headersAndPayload.markReaderIndex();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        headersAndPayload.resetReaderIndex();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received send message request. producer: {}:{} {}:{} size: {}, partition key is: {}, ordering key is {}",
                    remoteAddress, send.getProducerId(), send.getSequenceId(), msgMetadata.getProducerName(), msgMetadata.getSequenceId(),
                    headersAndPayload.readableBytes(), msgMetadata.getPartitionKey(), msgMetadata.getOrderingKey());
        }
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
            Consumer consumer = consumerFuture.getNow(null);
            if (consumer != null) {
                consumer.flowPermits(flow.getMessagePermits());
            } else {
                log.info("[{}] Couldn't find consumer {}", remoteAddress, flow.getConsumerId());
            }
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
            if (redeliver.getMessageIdsCount() > 0 && Subscription.isIndividualAckMode(consumer.subType())) {
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
    protected void handleSeek(CommandSeek seek) {
        checkArgument(state == State.Connected);
        final long requestId = seek.getRequestId();
        CompletableFuture<Consumer> consumerFuture = consumers.get(seek.getConsumerId());

        if (!seek.hasMessageId() && !seek.hasMessagePublishTime()) {
            ctx.writeAndFlush(
                    Commands.newError(requestId, ServerError.MetadataError, "Message id and message publish time were not present"));
            return;
        }

        boolean consumerCreated = consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally();

        if (consumerCreated && seek.hasMessageId()) {
            Consumer consumer = consumerFuture.getNow(null);
            Subscription subscription = consumer.getSubscription();
            MessageIdData msgIdData = seek.getMessageId();

            Position position = new PositionImpl(msgIdData.getLedgerId(), msgIdData.getEntryId());


            subscription.resetCursor(position).thenRun(() -> {
                log.info("[{}] [{}][{}] Reset subscription to message id {}", remoteAddress,
                        subscription.getTopic().getName(), subscription.getName(), position);
                ctx.writeAndFlush(Commands.newSuccess(requestId));
            }).exceptionally(ex -> {
                log.warn("[{}][{}] Failed to reset subscription: {}", remoteAddress, subscription, ex.getMessage(), ex);
                ctx.writeAndFlush(Commands.newError(requestId, ServerError.UnknownError,
                        "Error when resetting subscription: " + ex.getCause().getMessage()));
                return null;
            });
        } else if (consumerCreated && seek.hasMessagePublishTime()){
            Consumer consumer = consumerFuture.getNow(null);
            Subscription subscription = consumer.getSubscription();
            long timestamp = seek.getMessagePublishTime();

            subscription.resetCursor(timestamp).thenRun(() -> {
                log.info("[{}] [{}][{}] Reset subscription to publish time {}", remoteAddress,
                        subscription.getTopic().getName(), subscription.getName(), timestamp);
                ctx.writeAndFlush(Commands.newSuccess(requestId));
            }).exceptionally(ex -> {
                log.warn("[{}][{}] Failed to reset subscription: {}", remoteAddress, subscription, ex.getMessage(), ex);
                ctx.writeAndFlush(Commands.newError(requestId, ServerError.UnknownError,
                        "Reset subscription to publish time error: " + ex.getCause().getMessage()));
                return null;
            });
        } else {
            ctx.writeAndFlush(Commands.newError(requestId, ServerError.MetadataError, "Consumer not found"));
        }
    }

    @Override
    protected void handleCloseProducer(CommandCloseProducer closeProducer) {
        checkArgument(state == State.Connected);

        final long producerId = closeProducer.getProducerId();
        final long requestId = closeProducer.getRequestId();

        CompletableFuture<Producer> producerFuture = producers.get(producerId);
        if (producerFuture == null) {
            log.info("[{}] Producer {} was not registered on the connection", remoteAddress, producerId);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
            return;
        }

        if (!producerFuture.isDone() && producerFuture
                .completeExceptionally(new IllegalStateException("Closed producer before creation was complete"))) {
            // We have received a request to close the producer before it was actually completed, we have marked the
            // producer future as failed and we can tell the client the close operation was successful.
            log.info("[{}] Closed producer {} before its creation was completed", remoteAddress, producerId);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
            producers.remove(producerId, producerFuture);
            return;
        } else if (producerFuture.isCompletedExceptionally()) {
            log.info("[{}] Closed producer {} that already failed to be created", remoteAddress, producerId);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
            producers.remove(producerId, producerFuture);
            return;
        }

        // Proceed with normal close, the producer
        Producer producer = producerFuture.getNow(null);
        log.info("[{}][{}] Closing producer on cnx {}", producer.getTopic(), producer.getProducerName(), remoteAddress);

        producer.close(true).thenAccept(v -> {
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
            log.info("[{}] Consumer was not registered on the connection: {}", consumerId, remoteAddress);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
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
            log.warn("[{]] Error closing consumer {} : {}", remoteAddress, consumer, e);
            ctx.writeAndFlush(
                    Commands.newError(requestId, BrokerServiceException.getClientErrorCode(e), e.getMessage()));
        }
    }

    @Override
    protected void handleGetLastMessageId(CommandGetLastMessageId getLastMessageId) {
        checkArgument(state == State.Connected);

        CompletableFuture<Consumer> consumerFuture = consumers.get(getLastMessageId.getConsumerId());

        if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
            Consumer consumer = consumerFuture.getNow(null);
            long requestId = getLastMessageId.getRequestId();

            Topic topic = consumer.getSubscription().getTopic();
            Position position = topic.getLastPosition();
            int partitionIndex = TopicName.getPartitionIndex(topic.getName());

            getLargestBatchIndexWhenPossible(
                    topic,
                    (PositionImpl) position,
                    partitionIndex,
                    requestId,
                    consumer.getSubscription().getName());

        } else {
            ctx.writeAndFlush(Commands.newError(getLastMessageId.getRequestId(), ServerError.MetadataError, "Consumer not found"));
        }
    }

    private void getLargestBatchIndexWhenPossible(
            Topic topic,
            PositionImpl position,
            int partitionIndex,
            long requestId,
            String subscriptionName) {

        PersistentTopic persistentTopic = (PersistentTopic) topic;
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();

        // If it's not pointing to a valid entry, respond messageId of the current position.
        if (position.getEntryId() == -1) {
            MessageIdData messageId = MessageIdData.newBuilder()
                    .setLedgerId(position.getLedgerId())
                    .setEntryId(position.getEntryId())
                    .setPartition(partitionIndex).build();

            ctx.writeAndFlush(Commands.newGetLastMessageIdResponse(requestId, messageId));
            return;
        }

        // For a valid position, we read the entry out and parse the batch size from its metadata.
        CompletableFuture<Entry> entryFuture = new CompletableFuture<>();
        ml.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                entryFuture.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                entryFuture.completeExceptionally(exception);
            }
        }, null);

        CompletableFuture<Integer> batchSizeFuture = entryFuture.thenApply(entry -> {
            MessageMetadata metadata = Commands.parseMessageMetadata(entry.getDataBuffer());
            int batchSize = metadata.getNumMessagesInBatch();
            entry.release();
            return batchSize;
        });

        batchSizeFuture.whenComplete((batchSize, e) -> {
            if (e != null) {
                ctx.writeAndFlush(Commands.newError(
                        requestId, ServerError.MetadataError, "Failed to get batch size for entry " + e.getMessage()));
            } else {
                int largestBatchIndex = batchSize > 1 ? batchSize - 1 : -1;

                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}][{}] Get LastMessageId {} partitionIndex {}", remoteAddress,
                            topic.getName(), subscriptionName, position, partitionIndex);
                }

                MessageIdData messageId = MessageIdData.newBuilder()
                        .setLedgerId(position.getLedgerId())
                        .setEntryId(position.getEntryId())
                        .setPartition(partitionIndex)
                        .setBatchIndex(largestBatchIndex).build();

                ctx.writeAndFlush(Commands.newGetLastMessageIdResponse(requestId, messageId));
            }
        });
    }

    @Override
    protected void handleGetTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace) {
        final long requestId = commandGetTopicsOfNamespace.getRequestId();
        final String namespace = commandGetTopicsOfNamespace.getNamespace();
        final CommandGetTopicsOfNamespace.Mode mode = commandGetTopicsOfNamespace.getMode();
        final NamespaceName namespaceName = NamespaceName.get(namespace);

        getBrokerService().pulsar().getNamespaceService().getListOfTopics(namespaceName, mode)
                .thenAccept(topics -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Received CommandGetTopicsOfNamespace for namespace [//{}] by {}, size:{}",
                                remoteAddress, namespace, requestId, topics.size());
                    }

                    ctx.writeAndFlush(Commands.newGetTopicsOfNamespaceResponse(topics, requestId));
                })
                .exceptionally(ex -> {
                    log.warn("[{}] Error GetTopicsOfNamespace for namespace [//{}] by {}",
                            remoteAddress, namespace, requestId);
                    ctx.writeAndFlush(
                            Commands.newError(requestId,
                                    BrokerServiceException.getClientErrorCode(new ServerMetadataException(ex)),
                                    ex.getMessage()));

                    return null;
                });
    }

    @Override
    protected void handleGetSchema(CommandGetSchema commandGetSchema) {
        if (log.isDebugEnabled()) {
            log.debug("Received CommandGetSchema call from {}, schemaVersion: {}, topic: {}, requestId: {}",
                    remoteAddress, new String(commandGetSchema.getSchemaVersion().toByteArray()),
                    commandGetSchema.getTopic(), commandGetSchema.getRequestId());
        }

        long requestId = commandGetSchema.getRequestId();
        SchemaVersion schemaVersion = SchemaVersion.Latest;
        if (commandGetSchema.hasSchemaVersion()) {
            schemaVersion = schemaService.versionFromBytes(commandGetSchema.getSchemaVersion().toByteArray());
        }

        String schemaName;
        try {
            schemaName = TopicName.get(commandGetSchema.getTopic()).getSchemaName();
        } catch (Throwable t) {
            ctx.writeAndFlush(
                    Commands.newGetSchemaResponseError(requestId, ServerError.InvalidTopicName, t.getMessage()));
            return;
        }

        schemaService.getSchema(schemaName, schemaVersion).thenAccept(schemaAndMetadata -> {
            if (schemaAndMetadata == null) {
                ctx.writeAndFlush(Commands.newGetSchemaResponseError(requestId, ServerError.TopicNotFound,
                        "Topic not found or no-schema"));
            } else {
                ctx.writeAndFlush(Commands.newGetSchemaResponse(requestId,
                        SchemaInfoUtil.newSchemaInfo(schemaName, schemaAndMetadata.schema), schemaAndMetadata.version));
            }
        }).exceptionally(ex -> {
            ctx.writeAndFlush(
                    Commands.newGetSchemaResponseError(requestId, ServerError.UnknownError, ex.getMessage()));
            return null;
        });
    }

    @Override
    protected void handleGetOrCreateSchema(CommandGetOrCreateSchema commandGetOrCreateSchema) {
        if (log.isDebugEnabled()) {
            log.debug("Received CommandGetOrCreateSchema call from {}", remoteAddress);
        }
        long requestId = commandGetOrCreateSchema.getRequestId();
        String topicName = commandGetOrCreateSchema.getTopic();
        SchemaData schemaData = getSchema(commandGetOrCreateSchema.getSchema());
        SchemaData schema = schemaData.getType() == SchemaType.NONE ? null : schemaData;
        service.getTopicIfExists(topicName).thenAccept(topicOpt -> {
            if (topicOpt.isPresent()) {
                Topic topic = topicOpt.get();
                CompletableFuture<SchemaVersion> schemaVersionFuture = tryAddSchema(topic, schema);
                schemaVersionFuture.exceptionally(ex -> {
                    ServerError errorCode = BrokerServiceException.getClientErrorCode(ex);
                    ctx.writeAndFlush(Commands.newGetOrCreateSchemaResponseError(
                            requestId, errorCode, ex.getMessage()));
                    return null;
                }).thenAccept(schemaVersion -> {
                        ctx.writeAndFlush(Commands.newGetOrCreateSchemaResponse(
                                requestId, schemaVersion));
                });
            } else {
                ctx.writeAndFlush(Commands.newGetOrCreateSchemaResponseError(
                        requestId, ServerError.TopicNotFound, "Topic not found"));
            }
        }).exceptionally(ex -> {
            ServerError errorCode = BrokerServiceException.getClientErrorCode(ex);
            ctx.writeAndFlush(Commands.newGetOrCreateSchemaResponseError(
                    requestId, errorCode, ex.getMessage()));
            return null;
        });
    }

    @Override
    protected void handleNewTxn(CommandNewTxn command) {
        if (log.isDebugEnabled()) {
            log.debug("Receive new txn request {} to transaction meta store {} from {}.", command.getRequestId(), command.getTcId(), remoteAddress);
        }
        TransactionCoordinatorID tcId = TransactionCoordinatorID.get(command.getTcId());
        service.pulsar().getTransactionMetadataStoreService().newTransaction(tcId)
            .whenComplete(((txnID, ex) -> {
                if (ex == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Send response {} for new txn request {}", tcId.getId(),  command.getRequestId());
                    }
                    ctx.writeAndFlush(Commands.newTxnResponse(command.getRequestId(), txnID.getLeastSigBits(), txnID.getMostSigBits()));
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Send response error for new txn request {}", command.getRequestId(), ex);
                    }
                    ctx.writeAndFlush(Commands.newTxnResponse(command.getRequestId(), tcId.getId(), BrokerServiceException.getClientErrorCode(ex), ex.getMessage()));
                }
            }));
    }

    @Override
    protected void handleAddPartitionToTxn(PulsarApi.CommandAddPartitionToTxn command) {
            TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        if (log.isDebugEnabled()) {
            log.debug("Receive add published partition to txn request {} from {} with txnId {}", command.getRequestId(), remoteAddress, txnID);
        }
        service.pulsar().getTransactionMetadataStoreService().addProducedPartitionToTxn(txnID, command.getPartitionsList())
            .whenComplete(((v, ex) -> {
                if (ex == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Send response success for add published partition to txn request {}",  command.getRequestId());
                    }
                    ctx.writeAndFlush(Commands.newAddPartitionToTxnResponse(command.getRequestId(),
                            txnID.getLeastSigBits(), txnID.getMostSigBits()));
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Send response error for add published partition to txn request {}",  command.getRequestId(), ex);
                    }
                    ctx.writeAndFlush(Commands.newAddPartitionToTxnResponse(command.getRequestId(), txnID.getMostSigBits(),
                            BrokerServiceException.getClientErrorCode(ex), ex.getMessage()));
                }
            }));
    }

    @Override
    protected void handleEndTxn(PulsarApi.CommandEndTxn command) {
        final long requestId = command.getRequestId();
        final int txnAction = command.getTxnAction().getNumber();
        TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());

        service.pulsar().getTransactionMetadataStoreService().endTransaction(txnID, txnAction)
            .thenRun(() -> {
                ctx.writeAndFlush(Commands.newEndTxnResponse(requestId,
                        txnID.getLeastSigBits(), txnID.getMostSigBits()));
            }).exceptionally(throwable -> {
                log.error("Send response error for end txn request.", throwable);
                ctx.writeAndFlush(Commands.newEndTxnResponse(command.getRequestId(), txnID.getMostSigBits(),
                        BrokerServiceException.getClientErrorCode(throwable), throwable.getMessage()));
                return null;
        });
    }

    @Override
    protected void handleEndTxnOnPartition(PulsarApi.CommandEndTxnOnPartition command) {
        final long requestId = command.getRequestId();
        final int txnAction = command.getTxnAction().getNumber();
        TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());

        service.getTopics().get(TopicName.get(command.getTopic()).toString()).whenComplete((topic, t) -> {
            if (!topic.isPresent()) {
                ctx.writeAndFlush(Commands.newEndTxnOnPartitionResponse(
                        command.getRequestId(), ServerError.TopicNotFound,
                        "Topic " + command.getTopic() + " is not found."));
                return;
            }
            topic.get().endTxn(txnID, txnAction)
                .whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        log.error("Handle endTxnOnPartition {} failed.", command.getTopic(), throwable);
                        ctx.writeAndFlush(Commands.newEndTxnOnPartitionResponse(
                                requestId, ServerError.UnknownError, throwable.getMessage()));
                        return;
                    }
                    ctx.writeAndFlush(Commands.newEndTxnOnPartitionResponse(requestId,
                            txnID.getLeastSigBits(), txnID.getMostSigBits()));
                });
        });
    }

    @Override
    protected void handleEndTxnOnSubscription(PulsarApi.CommandEndTxnOnSubscription command) {
        final long requestId = command.getRequestId();
        final long txnidMostBits = command.getTxnidMostBits();
        final long txnidLeastBits = command.getTxnidLeastBits();
        final String topic = command.getSubscription().getTopic();
        final String subName = command.getSubscription().getSubscription();
        final int txnAction = command.getTxnAction().getNumber();

        service.getTopics().get(TopicName.get(command.getSubscription().getTopic()).toString())
            .thenAccept(optionalTopic -> {
                if (!optionalTopic.isPresent()) {
                    log.error("The topic {} is not exist in broker.", command.getSubscription().getTopic());
                    ctx.writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                            requestId, txnidLeastBits, txnidMostBits,
                            ServerError.UnknownError,
                            "The topic " + topic + " is not exist in broker."));
                    return;
                }

                Subscription subscription = optionalTopic.get().getSubscription(subName);
                if (subscription == null) {
                    log.error("Topic {} subscription {} is not exist.", optionalTopic.get().getName(), subName);
                    ctx.writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                            requestId, txnidLeastBits, txnidMostBits,
                            ServerError.UnknownError,
                            "Topic " + optionalTopic.get().getName() + " subscription " + subName + " is not exist."));
                    return;
                }

                CompletableFuture<Void> completableFuture =
                        subscription.endTxn(txnidMostBits, txnidLeastBits, txnAction);
                completableFuture.whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        log.error("Handle end txn on subscription failed for request {}", requestId);
                        ctx.writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                                requestId, txnidLeastBits, txnidMostBits,
                                ServerError.UnknownError,
                                "Handle end txn on subscription failed."));
                        return;
                    }
                    ctx.writeAndFlush(
                            Commands.newEndTxnOnSubscriptionResponse(requestId, txnidLeastBits, txnidMostBits));
                });
            });
    }

    private CompletableFuture<SchemaVersion> tryAddSchema(Topic topic, SchemaData schema) {
        if (schema != null) {
            return topic.addSchema(schema);
        } else {
            return topic.hasSchema().thenCompose((hasSchema) -> {
                log.info("[{}] {} configured with schema {}",
                         remoteAddress, topic.getName(), hasSchema);
                CompletableFuture<SchemaVersion> result = new CompletableFuture<>();
                if (hasSchema && (schemaValidationEnforced || topic.getSchemaValidationEnforced())) {
                    result.completeExceptionally(new IncompatibleSchemaException(
                            "Producers cannot connect or send message without a schema to topics with a schema"));
                } else {
                    result.complete(SchemaVersion.Empty);
                }
                return result;
            });
        }
    }

    @Override
    protected void handleAddSubscriptionToTxn(PulsarApi.CommandAddSubscriptionToTxn command) {
        TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        if (log.isDebugEnabled()) {
            log.debug("Receive add published partition to txn request {} from {} with txnId {}",
                    command.getRequestId(), remoteAddress, txnID);
        }
        List<TransactionSubscription> subscriptionList = command.getSubscriptionList().stream()
                .map(subscription -> TransactionSubscription.builder()
                        .topic(subscription.getTopic())
                        .subscription(subscription.getSubscription())
                        .build())
                .collect(Collectors.toList());
        service.pulsar().getTransactionMetadataStoreService().addAckedPartitionToTxn(txnID, subscriptionList)
                .whenComplete(((v, ex) -> {
                    if (ex == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response success for add published partition to txn request {}",
                                    command.getRequestId());
                        }
                        ctx.writeAndFlush(Commands.newAddSubscriptionToTxnResponse(command.getRequestId(),
                                txnID.getLeastSigBits(), txnID.getMostSigBits()));
                        log.info("handle add partition to txn finish.");
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response error for add published partition to txn request {}",
                                    command.getRequestId(), ex);
                        }
                        ctx.writeAndFlush(Commands.newAddSubscriptionToTxnResponse(command.getRequestId(),
                                txnID.getMostSigBits(), BrokerServiceException.getClientErrorCode(ex),
                                ex.getMessage()));
                    }
                }));
    }

    @Override
    protected boolean isHandshakeCompleted() {
        return state == State.Connected;
    }

    ChannelHandlerContext ctx() {
        return ctx;
    }

    @Override
    protected void onCommand(PulsarApi.BaseCommand command) throws Exception {
        if (getBrokerService().getInterceptor() != null) {
            getBrokerService().getInterceptor().onPulsarCommand(command, this);
        }
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


    public void startSendOperation(Producer producer, int msgSize, int numMessages) {
        MSG_PUBLISH_BUFFER_SIZE_UPDATER.getAndAdd(this, msgSize);
        boolean isPublishRateExceeded = false;
        if (preciseTopicPublishRateLimitingEnable) {
            boolean isPreciseTopicPublishRateExceeded = producer.getTopic().isTopicPublishRateExceeded(numMessages, msgSize);
            if (isPreciseTopicPublishRateExceeded) {
                producer.getTopic().disableCnxAutoRead();
                return;
            }
            isPublishRateExceeded = producer.getTopic().isBrokerPublishRateExceeded();
        } else {
            isPublishRateExceeded = producer.getTopic().isPublishRateExceeded();
        }

        if (++pendingSendRequest == maxPendingSendRequests || isPublishRateExceeded) {
            // When the quota of pending send requests is reached, stop reading from socket to cause backpressure on
            // client connection, possibly shared between multiple producers
            ctx.channel().config().setAutoRead(false);
            autoReadDisabledRateLimiting = isPublishRateExceeded;

        }
        if (getBrokerService().isReachMessagePublishBufferThreshold()) {
            ctx.channel().config().setAutoRead(false);
            autoReadDisabledPublishBufferLimiting = true;
        }
    }

    public void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {
        MSG_PUBLISH_BUFFER_SIZE_UPDATER.getAndAdd(this, -msgSize);
        if (--pendingSendRequest == resumeReadsThreshold) {
            // Resume reading from socket
            ctx.channel().config().setAutoRead(true);
            // triggers channel read if autoRead couldn't trigger it
            ctx.read();
        }
        if (isNonPersistentTopic) {
            nonPersistentPendingMessages--;
        }
    }

    public void enableCnxAutoRead() {
        // we can add check (&& pendingSendRequest < MaxPendingSendRequests) here but then it requires
        // pendingSendRequest to be volatile and it can be expensive while writing. also this will be called on if
        // throttling is enable on the topic. so, avoid pendingSendRequest check will be fine.
        if (!ctx.channel().config().isAutoRead() && !autoReadDisabledRateLimiting && !autoReadDisabledPublishBufferLimiting) {
            // Resume reading from socket if pending-request is not reached to threshold
            ctx.channel().config().setAutoRead(true);
            // triggers channel read
            ctx.read();
        }
    }

    public void disableCnxAutoRead() {
        if (ctx.channel().config().isAutoRead() ) {
            ctx.channel().config().setAutoRead(false);
        }
    }

    @VisibleForTesting
    void cancelPublishRateLimiting() {
        if (autoReadDisabledRateLimiting) {
            autoReadDisabledRateLimiting = false;
        }
    }

    @VisibleForTesting
    void cancelPublishBufferLimiting() {
        if (autoReadDisabledPublishBufferLimiting) {
            autoReadDisabledPublishBufferLimiting = false;
        }
    }

    private <T> ServerError getErrorCode(CompletableFuture<T> future) {
        ServerError error = ServerError.UnknownError;
        try {
            future.getNow(null);
        } catch (Exception e) {
            if (e.getCause() instanceof BrokerServiceException) {
                error = BrokerServiceException.getClientErrorCode(e.getCause());
            }
        }
        return error;
    }

    private void disableTcpNoDelayIfNeeded(String topic, String producerName) {
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

    private TopicName validateTopicName(String topic, long requestId, GeneratedMessageLite requestCommand) {
        try {
            return TopicName.get(topic);
        } catch (Throwable t) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to parse topic name '{}'", remoteAddress, topic, t);
            }

            if (requestCommand instanceof CommandLookupTopic) {
                ctx.writeAndFlush(Commands.newLookupErrorResponse(ServerError.InvalidTopicName,
                        "Invalid topic name: " + t.getMessage(), requestId));
            } else if (requestCommand instanceof CommandPartitionedTopicMetadata) {
                ctx.writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.InvalidTopicName,
                        "Invalid topic name: " + t.getMessage(), requestId));
            } else {
                ctx.writeAndFlush(Commands.newError(requestId, ServerError.InvalidTopicName,
                        "Invalid topic name: " + t.getMessage()));
            }

            return null;
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

    public SocketAddress getRemoteAddress() {
        return remoteAddress;
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

    boolean supportsAuthenticationRefresh() {
        return features != null && features.getSupportsAuthRefresh();
    }

    public String getClientVersion() {
        return clientVersion;
    }

    public long getMessagePublishBufferSize() {
        return this.messagePublishBufferSize;
    }

    @VisibleForTesting
    void setMessagePublishBufferSize(long bufferSize) {
        this.messagePublishBufferSize = bufferSize;
    }

    @VisibleForTesting
    void setAutoReadDisabledRateLimiting(boolean isLimiting) {
        this.autoReadDisabledRateLimiting = isLimiting;
    }

    public boolean isPreciseDispatcherFlowControl() {
        return preciseDispatcherFlowControl;
    }

    public AuthenticationState getAuthState() {
        return authState;
    }

    public AuthenticationDataSource getAuthenticationData() {
        return originalAuthData != null ? originalAuthData : authenticationData;
    }

    public String getPrincipal() {
        return originalPrincipal != null ? originalPrincipal : authRole;
    }

    public AuthenticationProvider getAuthenticationProvider() {
        return authenticationProvider;
    }

    public String getAuthRole() {
        return authRole;
    }

    public String getAuthMethod() {
        return authMethod;
    }
}
