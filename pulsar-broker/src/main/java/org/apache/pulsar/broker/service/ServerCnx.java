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
import static org.apache.pulsar.broker.admin.impl.PersistentTopicsBase.unsafeGetPartitionedTopicMetadataAsync;
import static org.apache.pulsar.broker.lookup.TopicLookupBase.lookupTopicAsync;
import static org.apache.pulsar.common.api.proto.ProtocolVersion.v5;
import static org.apache.pulsar.common.protocol.Commands.newLookupErrorResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.Promise;
import io.prometheus.client.Gauge;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import lombok.val;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
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
import org.apache.pulsar.client.impl.schema.SchemaInfoUtil;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxn;
import org.apache.pulsar.common.api.proto.CommandAddSubscriptionToTxn;
import org.apache.pulsar.common.api.proto.CommandAuthResponse;
import org.apache.pulsar.common.api.proto.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnect;
import org.apache.pulsar.common.api.proto.CommandConsumerStats;
import org.apache.pulsar.common.api.proto.CommandEndTxn;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartition;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscription;
import org.apache.pulsar.common.api.proto.CommandFlow;
import org.apache.pulsar.common.api.proto.CommandGetLastMessageId;
import org.apache.pulsar.common.api.proto.CommandGetOrCreateSchema;
import org.apache.pulsar.common.api.proto.CommandGetSchema;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandNewTxn;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.CommandProducer;
import org.apache.pulsar.common.api.proto.CommandRedeliverUnacknowledgedMessages;
import org.apache.pulsar.common.api.proto.CommandSeek;
import org.apache.pulsar.common.api.proto.CommandSend;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.CommandTcClientConnectRequest;
import org.apache.pulsar.common.api.proto.CommandUnsubscribe;
import org.apache.pulsar.common.api.proto.FeatureFlags;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.Schema;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.naming.Metadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.CommandUtils;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.functions.utils.Exceptions;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerCnx extends PulsarHandler implements TransportCnx {
    private final BrokerService service;
    private final SchemaRegistryService schemaService;
    private final String listenerName;
    private final ConcurrentLongHashMap<CompletableFuture<Producer>> producers;
    private final ConcurrentLongHashMap<CompletableFuture<Consumer>> consumers;
    private State state;
    private volatile boolean isActive = true;
    String authRole = null;
    private volatile AuthenticationDataSource authenticationData;
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
    private final int maxNonPersistentPendingMessages;
    private String originalPrincipal = null;
    private Set<String> proxyRoles;
    private boolean authenticateOriginalAuthData;
    private final boolean schemaValidationEnforced;
    private String authMethod = "none";
    private final int maxMessageSize;
    private boolean preciseDispatcherFlowControl;

    private boolean preciseTopicPublishRateLimitingEnable;
    private boolean encryptionRequireOnProducer;

    // Flag to manage throttling-rate by atomically enable/disable read-channel.
    private volatile boolean autoReadDisabledRateLimiting = false;
    private FeatureFlags features;

    private PulsarCommandSender commandSender;
    private final ConnectionController connectionController;

    private static final KeySharedMeta emptyKeySharedMeta = new KeySharedMeta()
            .setKeySharedMode(KeySharedMode.AUTO_SPLIT);

    // Flag to manage throttling-publish-buffer by atomically enable/disable read-channel.
    private boolean autoReadDisabledPublishBufferLimiting = false;
    private final long maxPendingBytesPerThread;
    private final long resumeThresholdPendingBytesPerThread;

    // Number of bytes pending to be published from a single specific IO thread.
    private static final FastThreadLocal<MutableLong> pendingBytesPerThread = new FastThreadLocal<MutableLong>() {
        @Override
        protected MutableLong initialValue() throws Exception {
            return new MutableLong();
        }
    };

    // A set of connections tied to the current thread
    private static final FastThreadLocal<Set<ServerCnx>> cnxsPerThread = new FastThreadLocal<Set<ServerCnx>>() {
        @Override
        protected Set<ServerCnx> initialValue() throws Exception {
            return Collections.newSetFromMap(new IdentityHashMap<>());
        }
    };


    enum State {
        Start, Connected, Failed, Connecting
    }

    public ServerCnx(PulsarService pulsar) {
        this(pulsar, null);
    }

    public ServerCnx(PulsarService pulsar, String listenerName) {
        super(pulsar.getBrokerService().getKeepAliveIntervalSeconds(), TimeUnit.SECONDS);
        this.service = pulsar.getBrokerService();
        this.schemaService = pulsar.getSchemaRegistryService();
        this.listenerName = listenerName;
        this.state = State.Start;
        ServiceConfiguration conf = pulsar.getConfiguration();

        // This maps are not heavily contended since most accesses are within the cnx thread
        this.producers = new ConcurrentLongHashMap<>(8, 1);
        this.consumers = new ConcurrentLongHashMap<>(8, 1);
        this.replicatorPrefix = conf.getReplicatorPrefix();
        this.maxNonPersistentPendingMessages = conf.getMaxConcurrentNonPersistentMessagePerConnection();
        this.proxyRoles = conf.getProxyRoles();
        this.authenticateOriginalAuthData = conf.isAuthenticateOriginalAuthData();
        this.schemaValidationEnforced = conf.isSchemaValidationEnforced();
        this.maxMessageSize = conf.getMaxMessageSize();
        this.maxPendingSendRequests = conf.getMaxPendingPublishRequestsPerConnection();
        this.resumeReadsThreshold = maxPendingSendRequests / 2;
        this.preciseDispatcherFlowControl = conf.isPreciseDispatcherFlowControl();
        this.preciseTopicPublishRateLimitingEnable = conf.isPreciseTopicPublishRateLimiterEnable();
        this.encryptionRequireOnProducer = conf.isEncryptionRequireOnProducer();
        // Assign a portion of max-pending bytes to each IO thread
        this.maxPendingBytesPerThread = conf.getMaxMessagePublishBufferSizeInMB() * 1024L * 1024L
                / conf.getNumIOThreads();
        this.resumeThresholdPendingBytesPerThread = this.maxPendingBytesPerThread / 2;
        this.connectionController = new ConnectionController.DefaultConnectionController(conf);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        ConnectionController.Sate sate = connectionController.increaseConnection(remoteAddress);
        if (!sate.equals(ConnectionController.Sate.OK)) {
            ctx.channel().writeAndFlush(Commands.newError(-1, ServerError.NotAllowedError,
                    sate.equals(ConnectionController.Sate.REACH_MAX_CONNECTION)
                            ? "Reached the maximum number of connections"
                            : "Reached the maximum number of connections on address" + remoteAddress));
            ctx.channel().close();
            return;
        }
        log.info("New connection from {}", remoteAddress);
        this.ctx = ctx;
        this.commandSender = new PulsarCommandSenderImpl(getBrokerService().getInterceptor(), this);
        this.service.getPulsarStats().recordConnectionCreate();
        cnxsPerThread.get().add(this);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        connectionController.decreaseConnection(ctx.channel().remoteAddress());
        isActive = false;
        log.info("Closed connection from {}", remoteAddress);
        BrokerInterceptor brokerInterceptor = getBrokerService().getInterceptor();
        if (brokerInterceptor != null) {
            brokerInterceptor.onConnectionClosed(this);
        }

        cnxsPerThread.get().remove(this);

        // Connection is gone, close the producers immediately
        producers.forEach((__, producerFuture) -> {
            if (producerFuture.isDone() && !producerFuture.isCompletedExceptionally()) {
                Producer producer = producerFuture.getNow(null);
                producer.closeNow(true);
            }
        });

        consumers.forEach((__, consumerFuture) -> {
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
        this.service.getPulsarStats().recordConnectionClose();
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
            if (log.isDebugEnabled()) {
                log.debug("[{}] connect state change to : [{}]", remoteAddress, State.Failed.name());
            }
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
     * If authentication and authorization is enabled(and not sasl) and
     *  if the authRole is one of proxyRoles we want to enforce
     * - the originalPrincipal is given while connecting
     * - originalPrincipal is not blank
     * - originalPrincipal is not a proxy principal
     */
    private boolean invalidOriginalPrincipal(String originalPrincipal) {
        return (service.isAuthenticationEnabled() && service.isAuthorizationEnabled()
                && proxyRoles.contains(authRole) && (StringUtils.isBlank(originalPrincipal)
                || proxyRoles.contains(originalPrincipal)));
    }

    // ////
    // // Incoming commands handling
    // ////

    private CompletableFuture<Boolean> isTopicOperationAllowed(TopicName topicName, TopicOperation operation) {
        if (!service.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> isProxyAuthorizedFuture;
        if (originalPrincipal != null) {
            isProxyAuthorizedFuture = service.getAuthorizationService().allowTopicOperationAsync(
                topicName, operation, originalPrincipal, getAuthenticationData());
        } else {
            isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> isAuthorizedFuture = service.getAuthorizationService().allowTopicOperationAsync(
            topicName, operation, authRole, authenticationData);
        return isProxyAuthorizedFuture.thenCombine(isAuthorizedFuture, (isProxyAuthorized, isAuthorized) -> {
            if (!isProxyAuthorized) {
                log.warn("OriginalRole {} is not authorized to perform operation {} on topic {}",
                        originalPrincipal, operation, topicName);
            }
            if (!isAuthorized) {
                log.warn("Role {} is not authorized to perform operation {} on topic {}",
                        authRole, operation, topicName);
            }
            return isProxyAuthorized && isAuthorized;
        });
    }

    private CompletableFuture<Boolean> isTopicOperationAllowed(TopicName topicName, String subscriptionName,
                                                               TopicOperation operation) {
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
            return CompletableFuture.completedFuture(true);
        }
    }

    @Override
    protected void handleLookup(CommandLookupTopic lookup) {
        final long requestId = lookup.getRequestId();
        final boolean authoritative = lookup.isAuthoritative();

        // use the connection-specific listener name by default.
        final String advertisedListenerName = lookup.hasAdvertisedListenerName() ? lookup.getAdvertisedListenerName()
                : this.listenerName;
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
                logAuthException(remoteAddress, "lookup", getPrincipal(), Optional.of(topicName), ex);
                final String msg = "Exception occurred while trying to authorize lookup";
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
                commandSender.sendPartitionMetadataResponse(ServerError.AuthorizationError, msg, requestId);
                lookupSemaphore.release();
                return;
            }
            isTopicOperationAllowed(topicName, TopicOperation.LOOKUP).thenApply(isAuthorized -> {
                if (isAuthorized) {
                    unsafeGetPartitionedTopicMetadataAsync(getBrokerService().pulsar(), topicName)
                        .handle((metadata, ex) -> {
                                if (ex == null) {
                                    int partitions = metadata.partitions;
                                    commandSender.sendPartitionMetadataResponse(partitions, requestId);
                                } else {
                                    if (ex instanceof PulsarClientException) {
                                        log.warn("Failed to authorize {} at [{}] on topic {} : {}", getRole(),
                                                remoteAddress, topicName, ex.getMessage());
                                        commandSender.sendPartitionMetadataResponse(ServerError.AuthorizationError,
                                                ex.getMessage(), requestId);
                                    } else {
                                        log.warn("Failed to get Partitioned Metadata [{}] {}: {}", remoteAddress,
                                                topicName, ex.getMessage(), ex);
                                        ServerError error = (ex instanceof RestException)
                                                && ((RestException) ex).getResponse().getStatus() < 500
                                                        ? ServerError.MetadataError
                                                        : ServerError.ServiceNotReady;
                                        commandSender.sendPartitionMetadataResponse(error, ex.getMessage(), requestId);
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
                logAuthException(remoteAddress, "partition-metadata", getPrincipal(), Optional.of(topicName), ex);
                final String msg = "Exception occurred while trying to authorize get Partition Metadata";
                ctx.writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.AuthorizationError, msg,
                        requestId));
                lookupSemaphore.release();
                return null;
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed Partition-Metadata lookup due to too many lookup-requests {}", remoteAddress,
                        topicName);
            }
            commandSender.sendPartitionMetadataResponse(ServerError.TooManyRequests,
                    "Failed due to too many pending lookup requests", requestId);
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
                    "Failed to get consumer-stats response - Consumer not found for"
                            + " CommandConsumerStats[remoteAddress = {}, requestId = {}, consumerId = {}]",
                    remoteAddress, requestId, consumerId);
            msg = Commands.newConsumerStatsResponse(ServerError.ConsumerNotFound,
                    "Consumer " + consumerId + " not found", requestId);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("CommandConsumerStats[requestId = {}, consumer = {}]", requestId, consumer);
            }
            msg = createConsumerStatsResponse(consumer, requestId);
        }

        ctx.writeAndFlush(msg);
    }

    ByteBuf createConsumerStatsResponse(Consumer consumer, long requestId) {
        ConsumerStatsImpl consumerStats = consumer.getStats();
        Subscription subscription = consumer.getSubscription();

        BaseCommand cmd = Commands.newConsumerStatsResponseCommand(ServerError.UnknownError, null, requestId);
        cmd.getConsumerStatsResponse()
                .clearErrorCode()
                .setRequestId(requestId)
                .setMsgRateOut(consumerStats.msgRateOut)
                .setMsgThroughputOut(consumerStats.msgThroughputOut)
                .setMsgRateRedeliver(consumerStats.msgRateRedeliver)
                .setConsumerName(consumerStats.consumerName)
                .setAvailablePermits(consumerStats.availablePermits)
                .setUnackedMessages(consumerStats.unackedMessages)
                .setBlockedConsumerOnUnackedMsgs(consumerStats.blockedConsumerOnUnackedMsgs)
                .setAddress(consumerStats.getAddress())
                .setConnectedSince(consumerStats.getConnectedSince())
                .setMsgBacklog(subscription.getNumberOfEntriesInBacklog(false))
                .setMsgRateExpired(subscription.getExpiredMessageRate())
                .setType(subscription.getTypeString());

        return Commands.serializeWithSize(cmd);
    }

    // complete the connect and sent newConnected command
    private void completeConnect(int clientProtoVersion, String clientVersion) {
        ctx.writeAndFlush(Commands.newConnected(clientProtoVersion, maxMessageSize));
        state = State.Connected;
        service.getPulsarStats().recordConnectionCreateSuccess();
        if (log.isDebugEnabled()) {
            log.debug("[{}] connect state change to : [{}]", remoteAddress, State.Connected.name());
        }
        setRemoteEndpointProtocolVersion(clientProtoVersion);
        if (isNotBlank(clientVersion) && !clientVersion.contains(" ") /* ignore default version: pulsar client */) {
            this.clientVersion = clientVersion.intern();
        }
        BrokerInterceptor brokerInterceptor = getBrokerService().getInterceptor();
        if (brokerInterceptor != null) {
            brokerInterceptor.onConnectionCreated(this);
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
            // 1. the 1st time the authentication process was done, in which case we'll send
            //    a `CommandConnected` response
            // 2. an authentication refresh, in which case we need to refresh authenticationData

            String newAuthRole = authState.getAuthRole();

            // Refresh the auth data.
            this.authenticationData = authState.getAuthDataSource();
            if (log.isDebugEnabled()) {
                log.debug("[{}] Auth data refreshed for role={}", remoteAddress, this.authRole);
            }

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
                        log.warn("[{}] Principal cannot change during an authentication refresh expected={} got={}",
                                remoteAddress, authRole, newAuthRole);
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
            log.debug("[{}] connect state change to : [{}]", remoteAddress, State.Connecting.name());
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
        } else if (!authState.isExpired()) {
            // Credentials are still valid. Nothing to do at this point
            return;
        } else if (originalPrincipal != null && originalAuthState == null) {
            log.info(
                    "[{}] Cannot revalidate user credential when using proxy and"
                            + " not forwarding the credentials. Closing connection",
                    remoteAddress);
            return;
        }

        ctx.executor().execute(SafeRun.safeRun(() -> {
            log.info("[{}] Refreshing authentication credentials for originalPrincipal {} and authRole {}",
                    remoteAddress, originalPrincipal, this.authRole);

            if (!supportsAuthenticationRefresh()) {
                log.warn("[{}] Closing connection because client doesn't support auth credentials refresh",
                        remoteAddress);
                ctx.close();
                return;
            }

            if (pendingAuthChallengeResponse) {
                log.warn("[{}] Closing connection after timeout on refreshing auth credentials",
                        remoteAddress);
                ctx.close();
                return;
            }

            try {
                AuthData brokerData = authState.refreshAuthentication();

                ctx.writeAndFlush(Commands.newAuthChallenge(authMethod, brokerData,
                        getRemoteEndpointProtocolVersion()));
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

    private static final byte[] emptyArray = new byte[0];

    @Override
    protected void handleConnect(CommandConnect connect) {
        checkArgument(state == State.Start);

        if (log.isDebugEnabled()) {
            log.debug("Received CONNECT from {}, auth enabled: {}:"
                    + " has original principal = {}, original principal = {}",
                remoteAddress,
                service.isAuthenticationEnabled(),
                connect.hasOriginalPrincipal(),
                connect.hasOriginalPrincipal() ? connect.getOriginalPrincipal() : null);
        }

        String clientVersion = connect.getClientVersion();
        int clientProtocolVersion = connect.getProtocolVersion();
        features = new FeatureFlags();
        if (connect.hasFeatureFlags()) {
            features.copyFrom(connect.getFeatureFlags());
        }

        if (!service.isAuthenticationEnabled()) {
            completeConnect(clientProtocolVersion, clientVersion);
            return;
        }

        try {
            byte[] authData = connect.hasAuthData() ? connect.getAuthData() : emptyArray;
            AuthData clientData = AuthData.of(authData);

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
                    throw new AuthenticationException(
                            String.format("Can't find AuthenticationProvider for original role"
                                    + " using auth method [%s] is not available", originalAuthMethod));
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
            service.getPulsarStats().recordConnectionCreateFail();
            logAuthException(remoteAddress, "connect", getPrincipal(), Optional.empty(), e);
            String msg = "Unable to authenticate";
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
            AuthData clientData = AuthData.of(authResponse.getResponse().getAuthData());
            doAuthentication(clientData, authResponse.getProtocolVersion(), authResponse.getClientVersion());
        } catch (AuthenticationException e) {
            service.getPulsarStats().recordConnectionCreateFail();
            log.warn("[{}] Authentication failed: {} ", remoteAddress, e.getMessage());
            ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, e.getMessage()));
            close();
        } catch (Exception e) {
            service.getPulsarStats().recordConnectionCreateFail();
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
            commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, msg);
            return;
        }

        final String subscriptionName = subscribe.getSubscription();
        final SubType subType = subscribe.getSubType();
        final String consumerName = subscribe.hasConsumerName() ? subscribe.getConsumerName() : "";
        final boolean isDurable = subscribe.isDurable();
        final MessageIdImpl startMessageId = subscribe.hasStartMessageId() ? new BatchMessageIdImpl(
                subscribe.getStartMessageId().getLedgerId(), subscribe.getStartMessageId().getEntryId(),
                subscribe.getStartMessageId().getPartition(), subscribe.getStartMessageId().getBatchIndex())
                : null;
        final int priorityLevel = subscribe.hasPriorityLevel() ? subscribe.getPriorityLevel() : 0;
        final boolean readCompacted = subscribe.hasReadCompacted() && subscribe.isReadCompacted();
        final Map<String, String> metadata = CommandUtils.metadataFromCommand(subscribe);
        final InitialPosition initialPosition = subscribe.getInitialPosition();
        final long startMessageRollbackDurationSec = subscribe.hasStartMessageRollbackDurationSec()
                ? subscribe.getStartMessageRollbackDurationSec()
                : -1;
        final SchemaData schema = subscribe.hasSchema() ? getSchema(subscribe.getSchema()) : null;
        final boolean isReplicated = subscribe.hasReplicateSubscriptionState()
                && subscribe.isReplicateSubscriptionState();
        final boolean forceTopicCreation = subscribe.isForceTopicCreation();
        final KeySharedMeta keySharedMeta = subscribe.hasKeySharedMeta()
              ? new KeySharedMeta().copyFrom(subscribe.getKeySharedMeta())
              : emptyKeySharedMeta;

        CompletableFuture<Boolean> isAuthorizedFuture = isTopicOperationAllowed(
                topicName,
                subscriptionName,
                TopicOperation.CONSUME
        );
        isAuthorizedFuture.thenApply(isAuthorized -> {
            if (isAuthorized) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Client is authorized to subscribe with role {}",
                            remoteAddress, getPrincipal());
                }

                log.info("[{}] Subscribing on topic {} / {}", remoteAddress, topicName, subscriptionName);
                try {
                    Metadata.validateMetadata(metadata);
                } catch (IllegalArgumentException iae) {
                    final String msg = iae.getMessage();
                    commandSender.sendErrorResponse(requestId, ServerError.MetadataError, msg);
                    return null;
                }
                CompletableFuture<Consumer> consumerFuture = new CompletableFuture<>();
                CompletableFuture<Consumer> existingConsumerFuture = consumers.putIfAbsent(consumerId,
                        consumerFuture);

                if (existingConsumerFuture != null) {
                    if (existingConsumerFuture.isDone() && !existingConsumerFuture.isCompletedExceptionally()) {
                        Consumer consumer = existingConsumerFuture.getNow(null);
                        log.info("[{}] Consumer with the same id is already created:"
                                 + " consumerId={}, consumer={}",
                                 remoteAddress, consumerId, consumer);
                        commandSender.sendSuccessResponse(requestId);
                        return null;
                    } else {
                        // There was an early request to create a consumer with same consumerId. This can happen
                        // when
                        // client timeout is lower the broker timeouts. We need to wait until the previous
                        // consumer
                        // creation request either complete or fails.
                        log.warn("[{}][{}][{}] Consumer with id is already present on the connection,"
                                 + " consumerId={}", remoteAddress, topicName, subscriptionName, consumerId);
                        ServerError error = null;
                        if (!existingConsumerFuture.isDone()) {
                            error = ServerError.ServiceNotReady;
                        } else {
                            error = getErrorCode(existingConsumerFuture);
                            consumers.remove(consumerId, existingConsumerFuture);
                        }
                        commandSender.sendErrorResponse(requestId, error,
                                "Consumer is already present on the connection");
                        return null;
                    }
                }

                boolean createTopicIfDoesNotExist = forceTopicCreation
                        && service.isAllowAutoTopicCreation(topicName.toString());

                service.getTopic(topicName.toString(), createTopicIfDoesNotExist)
                        .thenCompose(optTopic -> {
                            if (!optTopic.isPresent()) {
                                return FutureUtil
                                        .failedFuture(new TopicNotFoundException(
                                                "Topic " + topicName + " does not exist"));
                            }

                            Topic topic = optTopic.get();

                            boolean rejectSubscriptionIfDoesNotExist = isDurable
                                && !service.isAllowAutoSubscriptionCreation(topicName.toString())
                                && !topic.getSubscriptions().containsKey(subscriptionName);

                            if (rejectSubscriptionIfDoesNotExist) {
                                return FutureUtil
                                        .failedFuture(
                                                new SubscriptionNotFoundException(
                                                        "Subscription does not exist"));
                            }
                            SubscriptionOption option = SubscriptionOption.builder().cnx(ServerCnx.this)
                                    .subscriptionName(subscriptionName)
                                    .consumerId(consumerId).subType(subType).priorityLevel(priorityLevel)
                                    .consumerName(consumerName).isDurable(isDurable).subType(subType)
                                    .startMessageId(startMessageId).metadata(metadata).readCompacted(readCompacted)
                                    .initialPosition(initialPosition)
                                    .startMessageRollbackDurationSec(startMessageRollbackDurationSec)
                                    .replicatedSubscriptionStateArg(isReplicated).keySharedMeta(keySharedMeta)
                                    .subscriptionProperties(SubscriptionOption.getPropertiesMap(
                                            subscribe.getSubscriptionPropertiesList()))
                                    .build();
                            if (schema != null) {
                                return topic.addSchemaIfIdleOrCheckCompatible(schema)
                                        .thenCompose(v -> topic.subscribe(option));
                            } else {
                                return topic.subscribe(option);
                            }
                        })
                        .thenAccept(consumer -> {
                            if (consumerFuture.complete(consumer)) {
                                log.info("[{}] Created subscription on topic {} / {}",
                                        remoteAddress, topicName, subscriptionName);
                                commandSender.sendSuccessResponse(requestId);
                                if (getBrokerService().getInterceptor() != null){
                                    getBrokerService().getInterceptor().consumerCreated(this, consumer, metadata);
                                }
                            } else {
                                // The consumer future was completed before by a close command
                                try {
                                    consumer.close();
                                    log.info("[{}] Cleared consumer created after timeout on client side {}",
                                            remoteAddress, consumer);
                                } catch (BrokerServiceException e) {
                                    log.warn(
                                            "[{}] Error closing consumer created"
                                                    + " after timeout on client side {}: {}",
                                            remoteAddress, consumer, e.getMessage());
                                }
                                consumers.remove(consumerId, consumerFuture);
                            }

                        })
                        .exceptionally(exception -> {
                            if (exception.getCause() instanceof ConsumerBusyException) {
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "[{}][{}][{}] Failed to create consumer because exclusive consumer"
                                                    + " is already connected: {}",
                                            remoteAddress, topicName, subscriptionName,
                                            exception.getCause().getMessage());
                                }
                            } else if (exception.getCause() instanceof BrokerServiceException) {
                                log.warn("[{}][{}][{}] Failed to create consumer: consumerId={}, {}",
                                         remoteAddress, topicName, subscriptionName,
                                         consumerId,  exception.getCause().getMessage());
                            } else {
                                log.warn("[{}][{}][{}] Failed to create consumer: consumerId={}, {}",
                                         remoteAddress, topicName, subscriptionName,
                                         consumerId, exception.getCause().getMessage(), exception);
                            }

                            // If client timed out, the future would have been completed by subsequent close.
                            // Send error
                            // back to client, only if not completed already.
                            if (consumerFuture.completeExceptionally(exception)) {
                                commandSender.sendErrorResponse(requestId,
                                        BrokerServiceException.getClientErrorCode(exception),
                                        exception.getCause().getMessage());
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
            logAuthException(remoteAddress, "subscribe", getPrincipal(), Optional.of(topicName), ex);
            commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, ex.getMessage());
            return null;
        });
    }

    private SchemaData getSchema(Schema protocolSchema) {
        return SchemaData.builder()
            .data(protocolSchema.getSchemaData())
            .isDeleted(false)
            .timestamp(System.currentTimeMillis())
            .user(Strings.nullToEmpty(originalPrincipal))
            .type(Commands.getSchemaType(protocolSchema.getType()))
            .props(protocolSchema.getPropertiesList().stream().collect(
                Collectors.toMap(
                    KeyValue::getKey,
                    KeyValue::getValue
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
        final boolean userProvidedProducerName = cmdProducer.isUserProvidedProducerName();
        final boolean isEncrypted = cmdProducer.isEncrypted();
        final Map<String, String> metadata = CommandUtils.metadataFromCommand(cmdProducer);
        final SchemaData schema = cmdProducer.hasSchema() ? getSchema(cmdProducer.getSchema()) : null;

        final ProducerAccessMode producerAccessMode = cmdProducer.getProducerAccessMode();
        final Optional<Long> topicEpoch = cmdProducer.hasTopicEpoch()
                ? Optional.of(cmdProducer.getTopicEpoch()) : Optional.empty();
        final boolean isTxnEnabled = cmdProducer.isTxnEnabled();

        TopicName topicName = validateTopicName(cmdProducer.getTopic(), requestId, cmdProducer);
        if (topicName == null) {
            return;
        }

        if (invalidOriginalPrincipal(originalPrincipal)) {
            final String msg = "Valid Proxy Client role should be provided while creating producer ";
            log.warn("[{}] {} with role {} and proxyClientAuthRole {} on topic {}", remoteAddress, msg, authRole,
                    originalPrincipal, topicName);
            commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, msg);
            return;
        }

        CompletableFuture<Boolean> isAuthorizedFuture = isTopicOperationAllowed(
            topicName, TopicOperation.PRODUCE
        );
        isAuthorizedFuture.thenApply(isAuthorized -> {
            if (!isAuthorized) {
                String msg = "Client is not authorized to Produce";
                log.warn("[{}] {} with role {}", remoteAddress, msg, getPrincipal());
                ctx.writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
                return null;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Client is authorized to Produce with role {}", remoteAddress, getPrincipal());
            }
            CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
            CompletableFuture<Producer> existingProducerFuture = producers.putIfAbsent(producerId, producerFuture);

            if (existingProducerFuture != null) {
                if (existingProducerFuture.isDone() && !existingProducerFuture.isCompletedExceptionally()) {
                    Producer producer = existingProducerFuture.getNow(null);
                    log.info("[{}] Producer with the same id is already created:"
                            + " producerId={}, producer={}", remoteAddress, producerId, producer);
                    commandSender.sendProducerSuccessResponse(requestId, producer.getProducerName(),
                            producer.getSchemaVersion());
                    return null;
                } else {
                    // There was an early request to create a producer with same producerId.
                    // This can happen when client timeout is lower than the broker timeouts.
                    // We need to wait until the previous producer creation request
                    // either complete or fails.
                    ServerError error = null;
                    if (!existingProducerFuture.isDone()) {
                        error = ServerError.ServiceNotReady;
                    } else {
                        error = getErrorCode(existingProducerFuture);
                        // remove producer with producerId as it's already completed with exception
                        producers.remove(producerId, existingProducerFuture);
                    }
                    log.warn("[{}][{}] Producer with id is already present on the connection, producerId={}",
                            remoteAddress, topicName, producerId);
                    commandSender.sendErrorResponse(requestId, error, "Producer is already present on the connection");
                    return null;
                }
            }

            log.info("[{}][{}] Creating producer. producerId={}", remoteAddress, topicName, producerId);

            service.getOrCreateTopic(topicName.toString()).thenAccept((Topic topic) -> {
                // Before creating producer, check if backlog quota exceeded
                // on topic for size based limit and time based limit
                for (BacklogQuota.BacklogQuotaType backlogQuotaType : BacklogQuota.BacklogQuotaType.values()) {
                    if (topic.isBacklogQuotaExceeded(producerName, backlogQuotaType)) {
                        IllegalStateException illegalStateException = new IllegalStateException(
                                "Cannot create producer on topic with backlog quota exceeded");
                        BacklogQuota.RetentionPolicy retentionPolicy = topic
                                .getBacklogQuota(backlogQuotaType).getPolicy();
                        if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold) {
                            commandSender.sendErrorResponse(requestId,
                                    ServerError.ProducerBlockedQuotaExceededError,
                                    illegalStateException.getMessage());
                        } else if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception) {
                            commandSender.sendErrorResponse(requestId,
                                    ServerError.ProducerBlockedQuotaExceededException,
                                    illegalStateException.getMessage());
                        }
                        producerFuture.completeExceptionally(illegalStateException);
                        producers.remove(producerId, producerFuture);
                        return;
                    }
                }
                // Check whether the producer will publish encrypted messages or not
                if ((topic.isEncryptionRequired() || encryptionRequireOnProducer) && !isEncrypted) {
                    String msg = String.format("Encryption is required in %s", topicName);
                    log.warn("[{}] {}", remoteAddress, msg);
                    commandSender.sendErrorResponse(requestId, ServerError.MetadataError, msg);
                    producers.remove(producerId, producerFuture);
                    return;
                }

                disableTcpNoDelayIfNeeded(topicName.toString(), producerName);

                CompletableFuture<SchemaVersion> schemaVersionFuture = tryAddSchema(topic, schema);

                schemaVersionFuture.exceptionally(exception -> {
                    String message = exception.getMessage();
                    if (exception.getCause() != null) {
                        message += (" caused by " + exception.getCause());
                    }
                    commandSender.sendErrorResponse(requestId,
                            BrokerServiceException.getClientErrorCode(exception),
                            message);
                    producers.remove(producerId, producerFuture);
                    return null;
                });

                schemaVersionFuture.thenAccept(schemaVersion -> {
                    topic.checkIfTransactionBufferRecoverCompletely(isTxnEnabled).thenAccept(future -> {
                        buildProducerAndAddTopic(topic, producerId, producerName, requestId, isEncrypted,
                                metadata, schemaVersion, epoch, userProvidedProducerName, topicName,
                                producerAccessMode, topicEpoch, producerFuture);
                    }).exceptionally(exception -> {
                        Throwable cause = exception.getCause();
                        log.error("producerId {}, requestId {} : TransactionBuffer recover failed",
                                producerId, requestId, exception);
                        commandSender.sendErrorResponse(requestId,
                                ServiceUnitNotReadyException.getClientErrorCode(cause),
                                cause.getMessage());
                        return null;
                    });
                });
            }).exceptionally(exception -> {
                Throwable cause = exception.getCause();
                if (cause instanceof NoSuchElementException) {
                    cause = new TopicNotFoundException("Topic Not Found.");
                }
                if (!Exceptions.areExceptionsPresentInChain(cause,
                        ServiceUnitNotReadyException.class, ManagedLedgerException.class)) {
                    // Do not print stack traces for expected exceptions
                    log.error("[{}] Failed to create topic {}, producerId={}",
                            remoteAddress, topicName, producerId, exception);
                }

                // If client timed out, the future would have been completed
                // by subsequent close. Send error back to
                // client, only if not completed already.
                if (producerFuture.completeExceptionally(exception)) {
                    commandSender.sendErrorResponse(requestId,
                            BrokerServiceException.getClientErrorCode(cause), cause.getMessage());
                }
                producers.remove(producerId, producerFuture);
                return null;
            });
            return null;
        }).exceptionally(ex -> {
            logAuthException(remoteAddress, "producer", getPrincipal(), Optional.of(topicName), ex);
            commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, ex.getMessage());
            return null;
        });
    }

    private void buildProducerAndAddTopic(Topic topic, long producerId, String producerName, long requestId,
                             boolean isEncrypted, Map<String, String> metadata, SchemaVersion schemaVersion, long epoch,
                             boolean userProvidedProducerName, TopicName topicName,
                             ProducerAccessMode producerAccessMode,
                             Optional<Long> topicEpoch, CompletableFuture<Producer> producerFuture){
        CompletableFuture<Void> producerQueuedFuture = new CompletableFuture<>();
        Producer producer = new Producer(topic, ServerCnx.this, producerId, producerName,
                getPrincipal(), isEncrypted, metadata, schemaVersion, epoch,
                userProvidedProducerName, producerAccessMode, topicEpoch);

        topic.addProducer(producer, producerQueuedFuture).thenAccept(newTopicEpoch -> {
            if (isActive()) {
                if (producerFuture.complete(producer)) {
                    log.info("[{}] Created new producer: {}", remoteAddress, producer);
                    commandSender.sendProducerSuccessResponse(requestId, producerName,
                            producer.getLastSequenceId(), producer.getSchemaVersion(),
                            newTopicEpoch, true /* producer is ready now */);
                    if (getBrokerService().getInterceptor() != null) {
                        getBrokerService().getInterceptor().
                            producerCreated(this, producer, metadata);
                    }
                    return;
                } else {
                    // The producer's future was completed before by
                    // a close command
                    producer.closeNow(true);
                    log.info("[{}] Cleared producer created after"
                                    + " timeout on client side {}",
                            remoteAddress, producer);
                }
            } else {
                producer.closeNow(true);
                log.info("[{}] Cleared producer created after connection was closed: {}",
                        remoteAddress, producer);
                producerFuture.completeExceptionally(
                        new IllegalStateException(
                                "Producer created after connection was closed"));
            }

            producers.remove(producerId, producerFuture);
        }).exceptionally(ex -> {
            log.error("[{}] Failed to add producer to topic {}: producerId={}, {}",
                    remoteAddress, topicName, producerId, ex.getMessage());

            producer.closeNow(true);
            if (producerFuture.completeExceptionally(ex)) {
                commandSender.sendErrorResponse(requestId,
                        BrokerServiceException.getClientErrorCode(ex), ex.getMessage());
            }
            return null;
        });

        producerQueuedFuture.thenRun(() -> {
            // If the producer is queued waiting, we will get an immediate notification
            // that we need to pass to client
            if (isActive()) {
                log.info("[{}] Producer is waiting in queue: {}", remoteAddress, producer);
                commandSender.sendProducerSuccessResponse(requestId, producerName,
                        producer.getLastSequenceId(), producer.getSchemaVersion(),
                        Optional.empty(), false/* producer is not ready now */);
                if (getBrokerService().getInterceptor() != null) {
                    getBrokerService().getInterceptor().
                       producerCreated(this, producer, metadata);
                }
            }
        });
    }
    @Override
    protected void handleSend(CommandSend send, ByteBuf headersAndPayload) {
        checkArgument(state == State.Connected);

        CompletableFuture<Producer> producerFuture = producers.get(send.getProducerId());

        if (producerFuture == null || !producerFuture.isDone() || producerFuture.isCompletedExceptionally()) {
            log.warn("[{}] Received message, but the producer is not ready : {}. Closing the connection.",
                    remoteAddress, send.getProducerId());
            close();
            return;
        }

        Producer producer = producerFuture.getNow(null);
        if (log.isDebugEnabled()) {
            printSendCommandDebug(send, headersAndPayload);
        }

        if (producer.isNonPersistentTopic()) {
            // avoid processing non-persist message if reached max concurrent-message limit
            if (nonPersistentPendingMessages > maxNonPersistentPendingMessages) {
                final long producerId = send.getProducerId();
                final long sequenceId = send.getSequenceId();
                final long highestSequenceId = send.getHighestSequenceId();
                service.getTopicOrderedExecutor().executeOrdered(producer.getTopic().getName(), SafeRun.safeRun(() -> {
                    commandSender.sendSendReceiptResponse(producerId, sequenceId, highestSequenceId, -1, -1);
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
                    send.getHighestSequenceId(), headersAndPayload, send.getNumMessages(), send.isIsChunk(),
                    send.isMarker());
            return;
        }

        // Persist the message
        if (send.hasHighestSequenceId() && send.getSequenceId() <= send.getHighestSequenceId()) {
            producer.publishMessage(send.getProducerId(), send.getSequenceId(), send.getHighestSequenceId(),
                    headersAndPayload, send.getNumMessages(), send.isIsChunk(), send.isMarker());
        } else {
            producer.publishMessage(send.getProducerId(), send.getSequenceId(), headersAndPayload,
                    send.getNumMessages(), send.isIsChunk(), send.isMarker());
        }
    }

    private void printSendCommandDebug(CommandSend send, ByteBuf headersAndPayload) {
        headersAndPayload.markReaderIndex();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        headersAndPayload.resetReaderIndex();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received send message request. producer: {}:{} {}:{} size: {},"
                            + " partition key is: {}, ordering key is {}",
                    remoteAddress, send.getProducerId(), send.getSequenceId(), msgMetadata.getProducerName(),
                    msgMetadata.getSequenceId(), headersAndPayload.readableBytes(),
                    msgMetadata.hasPartitionKey() ? msgMetadata.getPartitionKey() : null,
                    msgMetadata.hasOrderingKey() ? msgMetadata.getOrderingKey() : null);
        }
    }

    @Override
    protected void handleAck(CommandAck ack) {
        checkArgument(state == State.Connected);
        CompletableFuture<Consumer> consumerFuture = consumers.get(ack.getConsumerId());
        final boolean hasRequestId = ack.hasRequestId();
        final long requestId = hasRequestId ? ack.getRequestId() : 0;
        final long consumerId = ack.getConsumerId();

        if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
            Consumer consumer = consumerFuture.getNow(null);
            consumer.messageAcked(ack).thenRun(() -> {
                        if (hasRequestId) {
                            ctx.writeAndFlush(Commands.newAckResponse(
                                    requestId, null, null, consumerId));
                        }
                        if (getBrokerService().getInterceptor() != null) {
                            getBrokerService().getInterceptor().messageAcked(this, consumer, ack);
                        }
                    }).exceptionally(e -> {
                        if (hasRequestId) {
                            ctx.writeAndFlush(Commands.newAckResponse(requestId,
                                    BrokerServiceException.getClientErrorCode(e),
                                    e.getMessage(), consumerId));
                        }
                        return null;
                    });
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
            commandSender.sendErrorResponse(unsubscribe.getRequestId(), ServerError.MetadataError,
                    "Consumer not found");
        }
    }

    @Override
    protected void handleSeek(CommandSeek seek) {
        checkArgument(state == State.Connected);
        final long requestId = seek.getRequestId();
        CompletableFuture<Consumer> consumerFuture = consumers.get(seek.getConsumerId());

        if (!seek.hasMessageId() && !seek.hasMessagePublishTime()) {
            commandSender.sendErrorResponse(requestId, ServerError.MetadataError,
                    "Message id and message publish time were not present");
            return;
        }

        boolean consumerCreated = consumerFuture != null
                && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally();

        if (consumerCreated && seek.hasMessageId()) {
            Consumer consumer = consumerFuture.getNow(null);
            Subscription subscription = consumer.getSubscription();
            MessageIdData msgIdData = seek.getMessageId();

            long[] ackSet = null;
            if (msgIdData.getAckSetsCount() > 0) {
                ackSet = new long[msgIdData.getAckSetsCount()];
                for (int i = 0; i < ackSet.length; i++) {
                    ackSet[i] = msgIdData.getAckSetAt(i);
                }
            }

            Position position = new PositionImpl(msgIdData.getLedgerId(),
                    msgIdData.getEntryId(), ackSet);


            subscription.resetCursor(position).thenRun(() -> {
                log.info("[{}] [{}][{}] Reset subscription to message id {}", remoteAddress,
                        subscription.getTopic().getName(), subscription.getName(), position);
                commandSender.sendSuccessResponse(requestId);
            }).exceptionally(ex -> {
                log.warn("[{}][{}] Failed to reset subscription: {}",
                        remoteAddress, subscription, ex.getMessage(), ex);
                commandSender.sendErrorResponse(requestId, ServerError.UnknownError,
                        "Error when resetting subscription: " + ex.getCause().getMessage());
                return null;
            });
        } else if (consumerCreated && seek.hasMessagePublishTime()){
            Consumer consumer = consumerFuture.getNow(null);
            Subscription subscription = consumer.getSubscription();
            long timestamp = seek.getMessagePublishTime();

            subscription.resetCursor(timestamp).thenRun(() -> {
                log.info("[{}] [{}][{}] Reset subscription to publish time {}", remoteAddress,
                        subscription.getTopic().getName(), subscription.getName(), timestamp);
                commandSender.sendSuccessResponse(requestId);
            }).exceptionally(ex -> {
                log.warn("[{}][{}] Failed to reset subscription: {}", remoteAddress,
                        subscription, ex.getMessage(), ex);
                commandSender.sendErrorResponse(requestId, ServerError.UnknownError,
                        "Reset subscription to publish time error: " + ex.getCause().getMessage());
                return null;
            });
        } else {
            commandSender.sendErrorResponse(requestId, ServerError.MetadataError, "Consumer not found");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServerCnx other = (ServerCnx) o;
        return Objects.equals(ctx().channel().id(), other.ctx().channel().id());
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctx().channel().id());
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
            log.info("[{}] Closed producer before its creation was completed. producerId={}",
                     remoteAddress, producerId);
            commandSender.sendSuccessResponse(requestId);
            producers.remove(producerId, producerFuture);
            return;
        } else if (producerFuture.isCompletedExceptionally()) {
            log.info("[{}] Closed producer that already failed to be created. producerId={}",
                     remoteAddress, producerId);
            commandSender.sendSuccessResponse(requestId);
            producers.remove(producerId, producerFuture);
            return;
        }

        // Proceed with normal close, the producer
        Producer producer = producerFuture.getNow(null);
        log.info("[{}][{}] Closing producer on cnx {}. producerId={}",
                 producer.getTopic(), producer.getProducerName(), remoteAddress, producerId);

        producer.close(true).thenAccept(v -> {
            log.info("[{}][{}] Closed producer on cnx {}. producerId={}",
                     producer.getTopic(), producer.getProducerName(),
                     remoteAddress, producerId);
            commandSender.sendSuccessResponse(requestId);
            producers.remove(producerId, producerFuture);
        });
    }

    @Override
    protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
        checkArgument(state == State.Connected);
        log.info("[{}] Closing consumer: consumerId={}", remoteAddress, closeConsumer.getConsumerId());

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
            log.info("[{}] Closed consumer before its creation was completed. consumerId={}",
                     remoteAddress, consumerId);
            commandSender.sendSuccessResponse(requestId);
            return;
        }

        if (consumerFuture.isCompletedExceptionally()) {
            log.info("[{}] Closed consumer that already failed to be created. consumerId={}",
                     remoteAddress, consumerId);
            commandSender.sendSuccessResponse(requestId);
            return;
        }

        // Proceed with normal consumer close
        Consumer consumer = consumerFuture.getNow(null);
        try {
            consumer.close();
            consumers.remove(consumerId, consumerFuture);
            commandSender.sendSuccessResponse(requestId);
            log.info("[{}] Closed consumer, consumerId={}", remoteAddress, consumerId);
        } catch (BrokerServiceException e) {
            log.warn("[{]] Error closing consumer {} : {}", remoteAddress, consumer, e);
            commandSender.sendErrorResponse(requestId, BrokerServiceException.getClientErrorCode(e), e.getMessage());
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
            Position lastPosition = topic.getLastPosition();
            int partitionIndex = TopicName.getPartitionIndex(topic.getName());

            Position markDeletePosition = null;
            if (consumer.getSubscription() instanceof PersistentSubscription) {
                markDeletePosition = ((PersistentSubscription) consumer.getSubscription()).getCursor()
                        .getMarkDeletedPosition();
            }

            getLargestBatchIndexWhenPossible(
                    topic,
                    (PositionImpl) lastPosition,
                    (PositionImpl) markDeletePosition,
                    partitionIndex,
                    requestId,
                    consumer.getSubscription().getName());

        } else {
            ctx.writeAndFlush(Commands.newError(getLastMessageId.getRequestId(),
                    ServerError.MetadataError, "Consumer not found"));
        }
    }

    private void getLargestBatchIndexWhenPossible(
            Topic topic,
            PositionImpl lastPosition,
            PositionImpl markDeletePosition,
            int partitionIndex,
            long requestId,
            String subscriptionName) {

        PersistentTopic persistentTopic = (PersistentTopic) topic;
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();

        // If it's not pointing to a valid entry, respond messageId of the current position.
        if (lastPosition.getEntryId() == -1) {
            handleLastMessageIdFromCompactedLedger(persistentTopic, requestId, partitionIndex,
                    markDeletePosition);
            return;
        }

        // For a valid position, we read the entry out and parse the batch size from its metadata.
        CompletableFuture<Entry> entryFuture = new CompletableFuture<>();
        ml.asyncReadEntry(lastPosition, new AsyncCallbacks.ReadEntryCallback() {
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
            return metadata.hasNumMessagesInBatch() ? batchSize : -1;
        });

        batchSizeFuture.whenComplete((batchSize, e) -> {
            if (e != null) {
                if (e.getCause() instanceof ManagedLedgerException.NonRecoverableLedgerException) {
                    handleLastMessageIdFromCompactedLedger(persistentTopic, requestId, partitionIndex,
                            markDeletePosition);
                } else {
                    ctx.writeAndFlush(Commands.newError(
                            requestId, ServerError.MetadataError,
                            "Failed to get batch size for entry " + e.getMessage()));
                }
            } else {
                int largestBatchIndex = batchSize > 0 ? batchSize - 1 : -1;

                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}][{}] Get LastMessageId {} partitionIndex {}", remoteAddress,
                            topic.getName(), subscriptionName, lastPosition, partitionIndex);
                }

                ctx.writeAndFlush(Commands.newGetLastMessageIdResponse(requestId, lastPosition.getLedgerId(),
                        lastPosition.getEntryId(), partitionIndex, largestBatchIndex,
                        markDeletePosition != null ? markDeletePosition.getLedgerId() : -1,
                        markDeletePosition != null ? markDeletePosition.getEntryId() : -1));
            }
        });
    }

    private void handleLastMessageIdFromCompactedLedger(PersistentTopic persistentTopic, long requestId,
            int partitionIndex, PositionImpl markDeletePosition) {
        persistentTopic.getCompactedTopic().readLastEntryOfCompactedLedger().thenAccept(entry -> {
            if (entry != null) {
                // in this case, all the data has been compacted, so return the last position
                // in the compacted ledger to the client
                MessageMetadata metadata = Commands.parseMessageMetadata(entry.getDataBuffer());
                int bs = metadata.getNumMessagesInBatch();
                int largestBatchIndex = bs > 0 ? bs - 1 : -1;
                ctx.writeAndFlush(Commands.newGetLastMessageIdResponse(requestId,
                        entry.getLedgerId(), entry.getEntryId(), partitionIndex, largestBatchIndex,
                        markDeletePosition != null ? markDeletePosition.getLedgerId() : -1,
                        markDeletePosition != null ? markDeletePosition.getEntryId() : -1));
                entry.release();
            } else {
                // in this case, the ledgers been removed except the current ledger
                // and current ledger without any data
                ctx.writeAndFlush(Commands.newGetLastMessageIdResponse(requestId,
                        -1, -1, partitionIndex, -1,
                        markDeletePosition != null ? markDeletePosition.getLedgerId() : -1,
                        markDeletePosition != null ? markDeletePosition.getEntryId() : -1));
            }
        }).exceptionally(ex -> {
            ctx.writeAndFlush(Commands.newError(
                    requestId, ServerError.MetadataError,
                    "Failed to read last entry of the compacted Ledger "
                            + ex.getCause().getMessage()));
            return null;
        });
    }

    private CompletableFuture<Boolean> isNamespaceOperationAllowed(NamespaceName namespaceName,
                                                                   NamespaceOperation operation) {
        if (!service.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> isProxyAuthorizedFuture;
        if (originalPrincipal != null) {
            isProxyAuthorizedFuture = service.getAuthorizationService().allowNamespaceOperationAsync(
                    namespaceName, operation, originalPrincipal, getAuthenticationData());
        } else {
            isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> isAuthorizedFuture = service.getAuthorizationService().allowNamespaceOperationAsync(
                namespaceName, operation, authRole, authenticationData);
        return isProxyAuthorizedFuture.thenCombine(isAuthorizedFuture, (isProxyAuthorized, isAuthorized) -> {
            if (!isProxyAuthorized) {
                log.warn("OriginalRole {} is not authorized to perform operation {} on namespace {}",
                        originalPrincipal, operation, namespaceName);
            }
            if (!isAuthorized) {
                log.warn("Role {} is not authorized to perform operation {} on namespace {}",
                        authRole, operation, namespaceName);
            }
            return isProxyAuthorized && isAuthorized;
        });
    }

    @Override
    protected void handleGetTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace) {
        final long requestId = commandGetTopicsOfNamespace.getRequestId();
        final String namespace = commandGetTopicsOfNamespace.getNamespace();
        final CommandGetTopicsOfNamespace.Mode mode = commandGetTopicsOfNamespace.getMode();
        final NamespaceName namespaceName = NamespaceName.get(namespace);

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            if (invalidOriginalPrincipal(originalPrincipal)) {
                final String msg = "Valid Proxy Client role should be provided for getTopicsOfNamespaceRequest ";
                log.warn("[{}] {} with role {} and proxyClientAuthRole {} on namespace {}", remoteAddress, msg,
                        authRole, originalPrincipal, namespaceName);
                commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, msg);
                lookupSemaphore.release();
                return;
            }
            isNamespaceOperationAllowed(namespaceName, NamespaceOperation.GET_TOPICS).thenApply(isAuthorized -> {
                if (isAuthorized) {
                    getBrokerService().pulsar().getNamespaceService().getListOfTopics(namespaceName, mode)
                        .thenAccept(topics -> {
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "[{}] Received CommandGetTopicsOfNamespace for namespace [//{}] by {}, size:{}",
                                        remoteAddress, namespace, requestId, topics.size());
                            }
                            commandSender.sendGetTopicsOfNamespaceResponse(topics, requestId);
                            lookupSemaphore.release();
                        })
                        .exceptionally(ex -> {
                            log.warn("[{}] Error GetTopicsOfNamespace for namespace [//{}] by {}",
                                    remoteAddress, namespace, requestId);
                            commandSender.sendErrorResponse(requestId,
                                    BrokerServiceException.getClientErrorCode(new ServerMetadataException(ex)),
                                    ex.getMessage());
                            lookupSemaphore.release();
                            return null;
                        });
                } else {
                    final String msg = "Proxy Client is not authorized to GetTopicsOfNamespace";
                    log.warn("[{}] {} with role {} on namespace {}", remoteAddress, msg, getPrincipal(), namespaceName);
                    commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, msg);
                    lookupSemaphore.release();
                }
                return null;
            }).exceptionally(ex -> {
                logNamespaceNameAuthException(remoteAddress, "GetTopicsOfNamespace", getPrincipal(),
                        Optional.of(namespaceName), ex);
                final String msg = "Exception occurred while trying to authorize GetTopicsOfNamespace";
                commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, msg);
                lookupSemaphore.release();
                return null;
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed GetTopicsOfNamespace lookup due to too many lookup-requests {}", remoteAddress,
                        namespaceName);
            }
            commandSender.sendErrorResponse(requestId, ServerError.TooManyRequests,
                    "Failed due to too many pending lookup requests");
        }
    }

    @Override
    protected void handleGetSchema(CommandGetSchema commandGetSchema) {
        if (log.isDebugEnabled()) {
            if (commandGetSchema.hasSchemaVersion()) {
                log.debug("Received CommandGetSchema call from {}, schemaVersion: {}, topic: {}, requestId: {}",
                        remoteAddress, new String(commandGetSchema.getSchemaVersion()),
                        commandGetSchema.getTopic(), commandGetSchema.getRequestId());
            } else {
                log.debug("Received CommandGetSchema call from {}, schemaVersion: {}, topic: {}, requestId: {}",
                        remoteAddress, null,
                        commandGetSchema.getTopic(), commandGetSchema.getRequestId());
            }
        }

        long requestId = commandGetSchema.getRequestId();
        SchemaVersion schemaVersion = SchemaVersion.Latest;
        if (commandGetSchema.hasSchemaVersion()) {
            schemaVersion = schemaService.versionFromBytes(commandGetSchema.getSchemaVersion());
        }

        String schemaName;
        try {
            schemaName = TopicName.get(commandGetSchema.getTopic()).getSchemaName();
        } catch (Throwable t) {
            commandSender.sendGetSchemaErrorResponse(requestId, ServerError.InvalidTopicName, t.getMessage());
            return;
        }

        schemaService.getSchema(schemaName, schemaVersion).thenAccept(schemaAndMetadata -> {
            if (schemaAndMetadata == null) {
                commandSender.sendGetSchemaErrorResponse(requestId, ServerError.TopicNotFound,
                        "Topic not found or no-schema");
            } else {
                commandSender.sendGetSchemaResponse(requestId,
                        SchemaInfoUtil.newSchemaInfo(schemaName, schemaAndMetadata.schema), schemaAndMetadata.version);
            }
        }).exceptionally(ex -> {
            commandSender.sendGetSchemaErrorResponse(requestId, ServerError.UnknownError, ex.getMessage());
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
                    String message = ex.getMessage();
                    if (ex.getCause() != null) {
                        message += (" caused by " + ex.getCause());
                    }
                    commandSender.sendGetOrCreateSchemaErrorResponse(requestId, errorCode, message);
                    return null;
                }).thenAccept(schemaVersion -> {
                    commandSender.sendGetOrCreateSchemaResponse(requestId, schemaVersion);
                });
            } else {
                commandSender.sendGetOrCreateSchemaErrorResponse(requestId, ServerError.TopicNotFound,
                        "Topic not found");
            }
        }).exceptionally(ex -> {
            ServerError errorCode = BrokerServiceException.getClientErrorCode(ex);
            commandSender.sendGetOrCreateSchemaErrorResponse(requestId, errorCode, ex.getMessage());
            return null;
        });
    }

    @Override
    protected void handleTcClientConnectRequest(CommandTcClientConnectRequest command) {
        final long requestId = command.getRequestId();
        final TransactionCoordinatorID tcId = TransactionCoordinatorID.get(command.getTcId());
        if (log.isDebugEnabled()) {
            log.debug("Receive tc client connect request {} to transaction meta store {} from {}.",
                    requestId, tcId, remoteAddress);
        }

        if (!checkTransactionEnableAndSendError(requestId)) {
            return;
        }

        TransactionMetadataStoreService transactionMetadataStoreService =
                service.pulsar().getTransactionMetadataStoreService();

        transactionMetadataStoreService.handleTcClientConnect(tcId).thenAccept(connection -> {
            if (log.isDebugEnabled()) {
                log.debug("Handle tc client connect request {} to transaction meta store {} from {} success.",
                        requestId, tcId, remoteAddress);
            }
            commandSender.sendTcClientConnectResponse(requestId);
        }).exceptionally(e -> {
            log.error("Handle tc client connect request {} to transaction meta store {} from {} fail.",
                    requestId, tcId, remoteAddress, e.getCause());
            commandSender.sendTcClientConnectResponse(requestId,
                    BrokerServiceException.getClientErrorCode(e), e.getMessage());
            return null;
        });
    }

    private boolean checkTransactionEnableAndSendError(long requestId) {
        if (!service.getPulsar().getConfig().isTransactionCoordinatorEnabled()) {
            BrokerServiceException.NotAllowedException ex =
                    new BrokerServiceException.NotAllowedException(
                            "Transaction manager is not not enabled");
            commandSender.sendErrorResponse(requestId, BrokerServiceException.getClientErrorCode(ex), ex.getMessage());
            return false;
        } else {
            return true;
        }
    }

    @Override
    protected void handleNewTxn(CommandNewTxn command) {
        final long requestId = command.getRequestId();
        final TransactionCoordinatorID tcId = TransactionCoordinatorID.get(command.getTcId());
        if (log.isDebugEnabled()) {
            log.debug("Receive new txn request {} to transaction meta store {} from {}.",
                    requestId, tcId, remoteAddress);
        }

        if (!checkTransactionEnableAndSendError(requestId)) {
            return;
        }

        TransactionMetadataStoreService transactionMetadataStoreService =
                service.pulsar().getTransactionMetadataStoreService();
        transactionMetadataStoreService.newTransaction(tcId, command.getTxnTtlSeconds())
            .whenComplete(((txnID, ex) -> {
                if (ex == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Send response {} for new txn request {}", tcId.getId(), requestId);
                    }
                    ctx.writeAndFlush(Commands.newTxnResponse(requestId, txnID.getLeastSigBits(),
                            txnID.getMostSigBits()));
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Send response error for new txn request {}", requestId, ex);
                    }

                    ctx.writeAndFlush(Commands.newTxnResponse(requestId, tcId.getId(),
                            BrokerServiceException.getClientErrorCode(ex), ex.getMessage()));
                    transactionMetadataStoreService.handleOpFail(ex, tcId);
                }
            }));
    }

    @Override
    protected void handleAddPartitionToTxn(CommandAddPartitionToTxn command) {
        final TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        final TransactionCoordinatorID tcId = TransactionCoordinatorID.get(command.getTxnidMostBits());
        final long requestId = command.getRequestId();
        if (log.isDebugEnabled()) {
            command.getPartitionsList().forEach(partion ->
                    log.debug("Receive add published partition to txn request {} "
                            + "from {} with txnId {}, topic: [{}]", requestId, remoteAddress, txnID, partion));
        }

        if (!checkTransactionEnableAndSendError(requestId)) {
            return;
        }

        TransactionMetadataStoreService transactionMetadataStoreService =
                service.pulsar().getTransactionMetadataStoreService();
        service.pulsar().getTransactionMetadataStoreService().addProducedPartitionToTxn(txnID,
                command.getPartitionsList())
                .whenComplete(((v, ex) -> {
                    if (ex == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response success for add published partition to txn request {}", requestId);
                        }
                        ctx.writeAndFlush(Commands.newAddPartitionToTxnResponse(requestId,
                                txnID.getLeastSigBits(), txnID.getMostSigBits()));
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response error for add published partition to txn request {}", requestId,
                                    ex);
                        }

                        if (ex instanceof CoordinatorException.CoordinatorNotFoundException) {
                            ctx.writeAndFlush(Commands.newAddPartitionToTxnResponse(requestId, txnID.getMostSigBits(),
                                    BrokerServiceException.getClientErrorCode(ex), ex.getMessage()));
                        } else {
                            ctx.writeAndFlush(Commands.newAddPartitionToTxnResponse(requestId, txnID.getMostSigBits(),
                                    BrokerServiceException.getClientErrorCode(ex.getCause()),
                                    ex.getCause().getMessage()));
                        }
                        transactionMetadataStoreService.handleOpFail(ex, tcId);
                    }
            }));
    }

    @Override
    protected void handleEndTxn(CommandEndTxn command) {
        final long requestId = command.getRequestId();
        final int txnAction = command.getTxnAction().getValue();
        TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        final TransactionCoordinatorID tcId = TransactionCoordinatorID.get(command.getTxnidMostBits());

        if (!checkTransactionEnableAndSendError(requestId)) {
            return;
        }

        TransactionMetadataStoreService transactionMetadataStoreService =
                service.pulsar().getTransactionMetadataStoreService();

        transactionMetadataStoreService
                .endTransaction(txnID, txnAction, false)
                .whenComplete((v, ex) -> {
                    if (ex == null) {
                        ctx.writeAndFlush(Commands.newEndTxnResponse(requestId,
                                txnID.getLeastSigBits(), txnID.getMostSigBits()));
                    } else {
                        log.error("Send response error for end txn request.", ex);

                        if (ex instanceof CoordinatorException.CoordinatorNotFoundException) {
                            ctx.writeAndFlush(Commands.newEndTxnResponse(requestId, txnID.getMostSigBits(),
                                    BrokerServiceException.getClientErrorCode(ex), ex.getMessage()));
                        } else {
                            ctx.writeAndFlush(Commands.newEndTxnResponse(requestId, txnID.getMostSigBits(),
                                    BrokerServiceException.getClientErrorCode(ex.getCause()),
                                    ex.getCause().getMessage()));
                        }
                        transactionMetadataStoreService.handleOpFail(ex, tcId);
                    }
                });
    }

    @Override
    protected void handleEndTxnOnPartition(CommandEndTxnOnPartition command) {
        final long requestId = command.getRequestId();
        final String topic = command.getTopic();
        final int txnAction = command.getTxnAction().getValue();
        TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        final long lowWaterMark = command.getTxnidLeastBitsOfLowWatermark();

        if (log.isDebugEnabled()) {
            log.debug("[{}] handleEndTxnOnPartition txnId: [{}], txnAction: [{}]", topic,
                    txnID, txnAction);
        }
        CompletableFuture<Optional<Topic>> topicFuture = service.getTopicIfExists(TopicName.get(topic).toString());
        topicFuture.thenAccept(optionalTopic -> {
            if (optionalTopic.isPresent()) {
                optionalTopic.get().endTxn(txnID, txnAction, lowWaterMark)
                        .whenComplete((ignored, throwable) -> {
                            if (throwable != null) {
                                log.error("handleEndTxnOnPartition fail!, topic {}, txnId: [{}], "
                                        + "txnAction: [{}]", topic, txnID, TxnAction.valueOf(txnAction), throwable);
                                ctx.writeAndFlush(Commands.newEndTxnOnPartitionResponse(
                                        requestId, BrokerServiceException.getClientErrorCode(throwable),
                                        throwable.getMessage(),
                                        txnID.getLeastSigBits(), txnID.getMostSigBits()));
                                return;
                            }
                            ctx.writeAndFlush(Commands.newEndTxnOnPartitionResponse(requestId,
                                    txnID.getLeastSigBits(), txnID.getMostSigBits()));
                        });

            } else {
                getBrokerService().getManagedLedgerFactory()
                        .asyncExists(TopicName.get(topic).getPersistenceNamingEncoding())
                        .thenAccept((b) -> {
                            if (b) {
                                log.error("handleEndTxnOnPartition fail ! The topic {} does not exist in broker, "
                                                + "txnId: [{}], txnAction: [{}]", topic,
                                        txnID, TxnAction.valueOf(txnAction));
                                ctx.writeAndFlush(Commands.newEndTxnOnPartitionResponse(requestId,
                                        ServerError.ServiceNotReady,
                                        "The topic " + topic + " does not exist in broker.",
                                        txnID.getMostSigBits(), txnID.getLeastSigBits()));
                            } else {
                                log.warn("handleEndTxnOnPartition fail ! The topic {} has not been created, "
                                                + "txnId: [{}], txnAction: [{}]",
                                        topic, txnID, TxnAction.valueOf(txnAction));
                                ctx.writeAndFlush(Commands.newEndTxnOnPartitionResponse(requestId,
                                        txnID.getLeastSigBits(), txnID.getMostSigBits()));
                            }
                        }).exceptionally(e -> {
                    log.error("handleEndTxnOnPartition fail ! topic {} , "
                                    + "txnId: [{}], txnAction: [{}]", topic, txnID,
                            TxnAction.valueOf(txnAction), e.getCause());
                    ctx.writeAndFlush(Commands.newEndTxnOnPartitionResponse(
                            requestId, ServerError.ServiceNotReady,
                            e.getMessage(), txnID.getLeastSigBits(), txnID.getMostSigBits()));
                    return null;
                });
            }
        }).exceptionally(e -> {
            log.error("handleEndTxnOnPartition fail ! topic {} , "
                            + "txnId: [{}], txnAction: [{}]", topic, txnID,
                    TxnAction.valueOf(txnAction), e.getCause());
            ctx.writeAndFlush(Commands.newEndTxnOnPartitionResponse(
                    requestId, ServerError.ServiceNotReady,
                    e.getMessage(), txnID.getLeastSigBits(), txnID.getMostSigBits()));
            return null;
        });
    }

    @Override
    protected void handleEndTxnOnSubscription(CommandEndTxnOnSubscription command) {
        final long requestId = command.getRequestId();
        final long txnidMostBits = command.getTxnidMostBits();
        final long txnidLeastBits = command.getTxnidLeastBits();
        final String topic = command.getSubscription().getTopic();
        final String subName = command.getSubscription().getSubscription();
        final int txnAction = command.getTxnAction().getValue();
        final TxnID txnID = new TxnID(txnidMostBits, txnidLeastBits);
        final long lowWaterMark = command.getTxnidLeastBitsOfLowWatermark();

        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] handleEndTxnOnSubscription txnId: [{}], txnAction: [{}]", topic, subName,
                    new TxnID(txnidMostBits, txnidLeastBits), txnAction);
        }

        CompletableFuture<Optional<Topic>> topicFuture = service.getTopicIfExists(TopicName.get(topic).toString());
        topicFuture.thenAccept(optionalTopic -> {
            if (optionalTopic.isPresent()) {
                Subscription subscription = optionalTopic.get().getSubscription(subName);
                if (subscription == null) {
                    log.warn("handleEndTxnOnSubscription fail! "
                                    + "topic {} subscription {} does not exist. txnId: [{}], txnAction: [{}]",
                            optionalTopic.get().getName(), subName, txnID, TxnAction.valueOf(txnAction));
                    ctx.writeAndFlush(
                            Commands.newEndTxnOnSubscriptionResponse(requestId, txnidLeastBits, txnidMostBits));
                    return;
                }

                CompletableFuture<Void> completableFuture =
                        subscription.endTxn(txnidMostBits, txnidLeastBits, txnAction, lowWaterMark);
                completableFuture.whenComplete((ignored, e) -> {
                    if (e != null) {
                        log.error("handleEndTxnOnSubscription fail ! topic: {} , subscription: {}"
                                        + "txnId: [{}], txnAction: [{}]", topic, subName,
                                txnID, TxnAction.valueOf(txnAction), e.getCause());
                        ctx.writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                                requestId, txnidLeastBits, txnidMostBits,
                                BrokerServiceException.getClientErrorCode(e),
                                "Handle end txn on subscription failed."));
                        return;
                    }
                    ctx.writeAndFlush(
                            Commands.newEndTxnOnSubscriptionResponse(requestId, txnidLeastBits, txnidMostBits));
                });
            } else {
                getBrokerService().getManagedLedgerFactory()
                        .asyncExists(TopicName.get(topic).getPersistenceNamingEncoding())
                        .thenAccept((b) -> {
                            if (b) {
                                log.error("handleEndTxnOnSubscription fail! The topic {} does not exist in broker, "
                                                + "subscription: {} ,txnId: [{}], txnAction: [{}]", topic, subName,
                                        new TxnID(txnidMostBits, txnidLeastBits), TxnAction.valueOf(txnAction));
                                ctx.writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                                        requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(),
                                        ServerError.ServiceNotReady,
                                        "The topic " + topic + " does not exist in broker."));
                            } else {
                                log.warn("handleEndTxnOnSubscription fail ! The topic {} has not been created, "
                                                + "subscription: {} txnId: [{}], txnAction: [{}]",
                                        topic, subName, txnID, TxnAction.valueOf(txnAction));
                                ctx.writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(requestId,
                                        txnID.getLeastSigBits(), txnID.getMostSigBits()));
                            }
                        }).exceptionally(e -> {
                    log.error("handleEndTxnOnSubscription fail ! topic {} , subscription: {}"
                                    + "txnId: [{}], txnAction: [{}]", topic, subName,
                            txnID, TxnAction.valueOf(txnAction), e.getCause());
                    ctx.writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                            requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(),
                            ServerError.ServiceNotReady, e.getMessage()));
                    return null;
                });
            }
        }).exceptionally(e -> {
            log.error("handleEndTxnOnSubscription fail ! topic: {} , subscription: {}"
                            + "txnId: [{}], txnAction: [{}]", topic, subName,
                    txnID, TxnAction.valueOf(txnAction), e.getCause());
            ctx.writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                    requestId, txnidLeastBits, txnidMostBits,
                    ServerError.ServiceNotReady,
                    "Handle end txn on subscription failed."));
            return null;
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
    protected void handleAddSubscriptionToTxn(CommandAddSubscriptionToTxn command) {
        final TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        final long requestId = command.getRequestId();
        if (log.isDebugEnabled()) {
            log.debug("Receive add published partition to txn request {} from {} with txnId {}",
                    requestId, remoteAddress, txnID);
        }

        final TransactionCoordinatorID tcId = TransactionCoordinatorID.get(command.getTxnidMostBits());

        if (!checkTransactionEnableAndSendError(requestId)) {
            return;
        }

        TransactionMetadataStoreService transactionMetadataStoreService =
                service.pulsar().getTransactionMetadataStoreService();

        transactionMetadataStoreService.addAckedPartitionToTxn(txnID,
                MLTransactionMetadataStore.subscriptionToTxnSubscription(command.getSubscriptionsList()))
                .whenComplete(((v, ex) -> {
                    if (ex == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response success for add published partition to txn request {}",
                                    requestId);
                        }
                        ctx.writeAndFlush(Commands.newAddSubscriptionToTxnResponse(requestId,
                                txnID.getLeastSigBits(), txnID.getMostSigBits()));
                        log.info("handle add partition to txn finish.");
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response error for add published partition to txn request {}",
                                    requestId, ex);
                        }

                        if (ex instanceof CoordinatorException.CoordinatorNotFoundException) {
                            ctx.writeAndFlush(Commands.newAddSubscriptionToTxnResponse(requestId,
                                    txnID.getMostSigBits(), BrokerServiceException.getClientErrorCode(ex),
                                    ex.getMessage()));
                        } else {
                            ctx.writeAndFlush(Commands.newAddSubscriptionToTxnResponse(requestId,
                                    txnID.getMostSigBits(), BrokerServiceException.getClientErrorCode(ex.getCause()),
                                    ex.getCause().getMessage()));
                        }
                        transactionMetadataStoreService.handleOpFail(ex, tcId);
                    }
                }));
    }

    @Override
    protected boolean isHandshakeCompleted() {
        return state == State.Connected;
    }

    public ChannelHandlerContext ctx() {
        return ctx;
    }

    @Override
    protected void interceptCommand(BaseCommand command) throws InterceptException {
        if (getBrokerService().getInterceptor() != null) {
            getBrokerService().getInterceptor().onPulsarCommand(command, this);
        }
    }

    public void closeProducer(Producer producer) {
        // removes producer-connection from map and send close command to producer
        safelyRemoveProducer(producer);
        if (getRemoteEndpointProtocolVersion() >= v5.getValue()) {
            ctx.writeAndFlush(Commands.newCloseProducer(producer.getProducerId(), -1L));
        } else {
            close();
        }

    }

    @Override
    public void closeConsumer(Consumer consumer) {
        // removes consumer-connection from map and send close command to consumer
        safelyRemoveConsumer(consumer);
        if (getRemoteEndpointProtocolVersion() >= v5.getValue()) {
            ctx.writeAndFlush(Commands.newCloseConsumer(consumer.consumerId(), -1L));
        } else {
            close();
        }
    }

    /**
     * It closes the connection with client which triggers {@code channelInactive()} which clears all producers and
     * consumers from connection-map.
     */
    protected void close() {
        ctx.close();
    }

    @Override
    public SocketAddress clientAddress() {
        return remoteAddress;
    }

    @Override
    public void removedConsumer(Consumer consumer) {
        safelyRemoveConsumer(consumer);
    }

    @Override
    public void removedProducer(Producer producer) {
        safelyRemoveProducer(producer);
    }

    private void safelyRemoveProducer(Producer producer) {
        long producerId = producer.getProducerId();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Removed producer: producerId={}, producer={}", remoteAddress, producerId, producer);
        }
        CompletableFuture<Producer> future = producers.get(producerId);
        if (future != null) {
            future.whenComplete((producer2, exception) -> {
                    if (exception != null || producer2 == producer) {
                        producers.remove(producerId, future);
                    }
                });
        }
    }

    private void safelyRemoveConsumer(Consumer consumer) {
        long consumerId = consumer.consumerId();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Removed consumer: consumerId={}, consumer={}", remoteAddress, consumerId, consumer);
        }
        CompletableFuture<Consumer> future = consumers.get(consumerId);
        if (future != null) {
            future.whenComplete((consumer2, exception) -> {
                    if (exception != null || consumer2 == consumer) {
                        consumers.remove(consumerId, future);
                    }
                });
        }
    }

    @Override
    public boolean isActive() {
        return isActive;
    }

    @Override
    public boolean isWritable() {
        return ctx.channel().isWritable();
    }

    private static final Gauge throttledConnections = Gauge.build()
            .name("pulsar_broker_throttled_connections")
            .help("Counter of connections throttled because of per-connection limit")
            .register();

    private static final Gauge throttledConnectionsGlobal = Gauge.build()
            .name("pulsar_broker_throttled_connections_global_limit")
            .help("Counter of connections throttled because of per-connection limit")
            .register();

    public void startSendOperation(Producer producer, int msgSize, int numMessages) {
        boolean isPublishRateExceeded = false;
        if (preciseTopicPublishRateLimitingEnable) {
            boolean isPreciseTopicPublishRateExceeded =
                    producer.getTopic().isTopicPublishRateExceeded(numMessages, msgSize);
            if (isPreciseTopicPublishRateExceeded) {
                producer.getTopic().disableCnxAutoRead();
                return;
            }
            isPublishRateExceeded = producer.getTopic().isBrokerPublishRateExceeded();
        } else {
            if (producer.getTopic().isResourceGroupRateLimitingEnabled()) {
                final boolean resourceGroupPublishRateExceeded =
                  producer.getTopic().isResourceGroupPublishRateExceeded(numMessages, msgSize);
                if (resourceGroupPublishRateExceeded) {
                    producer.getTopic().disableCnxAutoRead();
                    return;
                }
            }
            isPublishRateExceeded = producer.getTopic().isPublishRateExceeded();
        }

        if (++pendingSendRequest == maxPendingSendRequests || isPublishRateExceeded) {
            // When the quota of pending send requests is reached, stop reading from socket to cause backpressure on
            // client connection, possibly shared between multiple producers
            ctx.channel().config().setAutoRead(false);
            autoReadDisabledRateLimiting = isPublishRateExceeded;
            throttledConnections.inc();
        }

        if (pendingBytesPerThread.get().addAndGet(msgSize) >= maxPendingBytesPerThread
                && !autoReadDisabledPublishBufferLimiting
                && maxPendingBytesPerThread > 0) {
            // Disable reading from all the connections associated with this thread
            MutableInt pausedConnections = new MutableInt();
            cnxsPerThread.get().forEach(cnx -> {
                if (cnx.hasProducers() && !cnx.autoReadDisabledPublishBufferLimiting) {
                    cnx.disableCnxAutoRead();
                    cnx.autoReadDisabledPublishBufferLimiting = true;
                    pausedConnections.increment();
                }
            });

            getBrokerService().pausedConnections(pausedConnections.intValue());
        }
    }

    @Override
    public void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {
        if (pendingBytesPerThread.get().addAndGet(-msgSize) < resumeThresholdPendingBytesPerThread
                && autoReadDisabledPublishBufferLimiting) {
            // Re-enable reading on all the blocked connections
            MutableInt resumedConnections = new MutableInt();
            cnxsPerThread.get().forEach(cnx -> {
                if (cnx.autoReadDisabledPublishBufferLimiting) {
                    cnx.autoReadDisabledPublishBufferLimiting = false;
                    cnx.enableCnxAutoRead();
                    resumedConnections.increment();
                }
            });

            getBrokerService().resumedConnections(resumedConnections.intValue());
        }

        if (--pendingSendRequest == resumeReadsThreshold) {
            enableCnxAutoRead();
        }
        if (isNonPersistentTopic) {
            nonPersistentPendingMessages--;
        }
    }

    @Override
    public void enableCnxAutoRead() {
        // we can add check (&& pendingSendRequest < MaxPendingSendRequests) here but then it requires
        // pendingSendRequest to be volatile and it can be expensive while writing. also this will be called on if
        // throttling is enable on the topic. so, avoid pendingSendRequest check will be fine.
        if (ctx != null && !ctx.channel().config().isAutoRead()
                && !autoReadDisabledRateLimiting && !autoReadDisabledPublishBufferLimiting) {
            // Resume reading from socket if pending-request is not reached to threshold
            ctx.channel().config().setAutoRead(true);
            // triggers channel read
            ctx.read();
            throttledConnections.dec();
        }
    }

    @Override
    public void disableCnxAutoRead() {
        if (ctx != null && ctx.channel().config().isAutoRead()) {
            ctx.channel().config().setAutoRead(false);
        }
    }

    @Override
    public void cancelPublishRateLimiting() {
        if (autoReadDisabledRateLimiting) {
            autoReadDisabledRateLimiting = false;
        }
    }

    @Override
    public void cancelPublishBufferLimiting() {
        if (autoReadDisabledPublishBufferLimiting) {
            autoReadDisabledPublishBufferLimiting = false;
            throttledConnectionsGlobal.dec();
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
                if (ctx.channel().config().getOption(ChannelOption.TCP_NODELAY)) {
                    ctx.channel().config().setOption(ChannelOption.TCP_NODELAY, false);
                }
            } catch (Throwable t) {
                log.warn("[{}] [{}] Failed to remove TCP no-delay property on client cnx {}", topic, producerName,
                        ctx.channel());
            }
        }
    }

    private TopicName validateTopicName(String topic, long requestId, Object requestCommand) {
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

    public ByteBufPair newMessageAndIntercept(long consumerId, long ledgerId, long entryId, int partition,
            int redeliveryCount, ByteBuf metadataAndPayload, long[] ackSet, String topic) {
        BaseCommand command = Commands.newMessageCommand(consumerId, ledgerId, entryId, partition, redeliveryCount,
                ackSet);
        ByteBufPair res = Commands.serializeCommandMessageWithSize(command, metadataAndPayload);
        try {
            val brokerInterceptor = getBrokerService().getInterceptor();
            if (brokerInterceptor != null) {
                brokerInterceptor.onPulsarCommand(command, this);

                CompletableFuture<Consumer> consumerFuture = consumers.get(consumerId);
                if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
                    Consumer consumer = consumerFuture.getNow(null);
                    brokerInterceptor.messageDispatched(this, consumer, ledgerId, entryId, metadataAndPayload);
                }
            } else {
                log.debug("BrokerInterceptor is not set in newMessageAndIntercept");
            }
        } catch (Exception e) {
            log.error("Exception occur when intercept messages.", e);
        }
        return res;
    }

    private static final Logger log = LoggerFactory.getLogger(ServerCnx.class);

    /**
     * Helper method for testability.
     *
     * @return the connection state
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

    @Override
    public Promise<Void> newPromise() {
        return ctx.newPromise();
    }

    @Override
    public HAProxyMessage getHAProxyMessage() {
        return proxyMessage;
    }

    @Override
    public boolean hasHAProxyMessage() {
        return proxyMessage != null;
    }

    boolean hasConsumer(long consumerId) {
        return consumers.containsKey(consumerId);
    }

    @Override
    public boolean isBatchMessageCompatibleVersion() {
        return getRemoteEndpointProtocolVersion() >= ProtocolVersion.v4.getValue();
    }

    boolean supportsAuthenticationRefresh() {
        return features != null && features.isSupportsAuthRefresh();
    }


    boolean supportBrokerMetadata() {
        return features != null && features.isSupportsBrokerEntryMetadata();
    }

    @Override
    public String getClientVersion() {
        return clientVersion;
    }

    @VisibleForTesting
    void setAutoReadDisabledRateLimiting(boolean isLimiting) {
        this.autoReadDisabledRateLimiting = isLimiting;
    }

    @Override
    public boolean isPreciseDispatcherFlowControl() {
        return preciseDispatcherFlowControl;
    }

    public AuthenticationState getAuthState() {
        return authState;
    }

    @Override
    public AuthenticationDataSource getAuthenticationData() {
        return originalAuthData != null ? originalAuthData : authenticationData;
    }

    public String getPrincipal() {
        return originalPrincipal != null ? originalPrincipal : authRole;
    }

    public AuthenticationProvider getAuthenticationProvider() {
        return authenticationProvider;
    }

    @Override
    public String getAuthRole() {
        return authRole;
    }

    public String getAuthMethod() {
        return authMethod;
    }

    public ConcurrentLongHashMap<CompletableFuture<Consumer>> getConsumers() {
        return consumers;
    }

    public ConcurrentLongHashMap<CompletableFuture<Producer>> getProducers() {
        return producers;
    }

    @Override
    public PulsarCommandSender getCommandSender() {
        return commandSender;
    }

    @Override
    public void execute(Runnable runnable) {
        ctx().channel().eventLoop().execute(runnable);
    }

    @Override
    public String clientSourceAddress() {
        if (proxyMessage != null) {
            return proxyMessage.sourceAddress();
        } else if (remoteAddress instanceof InetSocketAddress) {
            InetSocketAddress inetAddress = (InetSocketAddress) remoteAddress;
            return inetAddress.getAddress().getHostAddress();
        } else {
            return null;
        }
    }

    private static void logAuthException(SocketAddress remoteAddress, String operation,
                                         String principal, Optional<TopicName> topic, Throwable ex) {
        String topicString = topic.map(t -> ", topic=" + t.toString()).orElse("");
        if (ex instanceof AuthenticationException) {
            log.info("[{}] Failed to authenticate: operation={}, principal={}{}, reason={}",
                    remoteAddress, operation, principal, topicString, ex.getMessage());
        } else {
            log.error("[{}] Error trying to authenticate: operation={}, principal={}{}",
                    remoteAddress, operation, principal, topicString, ex);
        }
    }

    private static void logNamespaceNameAuthException(SocketAddress remoteAddress, String operation,
                                         String principal, Optional<NamespaceName> namespaceName, Throwable ex) {
        String namespaceNameString = namespaceName.map(t -> ", namespace=" + t.toString()).orElse("");
        if (ex instanceof AuthenticationException) {
            log.info("[{}] Failed to authenticate: operation={}, principal={}{}, reason={}",
                    remoteAddress, operation, principal, namespaceNameString, ex.getMessage());
        } else {
            log.error("[{}] Error trying to authenticate: operation={}, principal={}{}",
                    remoteAddress, operation, principal, namespaceNameString, ex);
        }
    }

    public boolean hasProducers() {
        return !producers.isEmpty();
    }
}
