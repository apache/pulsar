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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.admin.impl.PersistentTopicsBase.unsafeGetPartitionedTopicMetadataAsync;
import static org.apache.pulsar.broker.lookup.TopicLookupBase.lookupTopicAsync;
import static org.apache.pulsar.broker.service.persistent.PersistentTopic.getMigratedClusterUrl;
import static org.apache.pulsar.common.api.proto.ProtocolVersion.v5;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
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
import io.netty.util.concurrent.ScheduledFuture;
import io.prometheus.client.Gauge;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.limiter.ConnectionController;
import org.apache.pulsar.broker.namespace.NamespaceService;
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
import org.apache.pulsar.common.api.proto.CommandTopicMigrated.ResourceType;
import org.apache.pulsar.common.api.proto.CommandUnsubscribe;
import org.apache.pulsar.common.api.proto.CommandWatchTopicList;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListClose;
import org.apache.pulsar.common.api.proto.CompressionType;
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
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.naming.Metadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.ClusterData.ClusterUrl;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.CommandUtils;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.common.util.netty.NettyChannelUtil;
import org.apache.pulsar.common.util.netty.NettyFutureUtil;
import org.apache.pulsar.functions.utils.Exceptions;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel handler for the Pulsar broker.
 * <p>
 * Please see {@link org.apache.pulsar.common.protocol.PulsarDecoder} javadoc for important details about handle* method
 * parameter instance lifecycle.
 */
public class ServerCnx extends PulsarHandler implements TransportCnx {
    private final BrokerService service;
    private final SchemaRegistryService schemaService;
    private final String listenerName;
    private final HashMap<Long, Long> recentlyClosedProducers;
    private final ConcurrentLongHashMap<CompletableFuture<Producer>> producers;
    private final ConcurrentLongHashMap<CompletableFuture<Consumer>> consumers;
    private final boolean enableSubscriptionPatternEvaluation;
    private final int maxSubscriptionPatternLength;
    private final TopicListService topicListService;
    private final BrokerInterceptor brokerInterceptor;
    private State state;
    private volatile boolean isActive = true;
    private String authRole = null;
    private volatile AuthenticationDataSource authenticationData;
    private AuthenticationProvider authenticationProvider;
    private AuthenticationState authState;
    // In case of proxy, if the authentication credentials are forwardable,
    // it will hold the credentials of the original client
    private AuthenticationState originalAuthState;
    private volatile AuthenticationDataSource originalAuthData;
    // Keep temporarily in order to verify after verifying proxy's authData
    private AuthData originalAuthDataCopy;
    private boolean pendingAuthChallengeResponse = false;
    private ScheduledFuture<?> authRefreshTask;

    // Max number of pending requests per connections. If multiple producers are sharing the same connection the flow
    // control done by a single producer might not be enough to prevent write spikes on the broker.
    private final int maxPendingSendRequests;
    private final int resumeReadsThreshold;
    private int pendingSendRequest = 0;
    private final String replicatorPrefix;
    private String clientVersion = null;
    private String proxyVersion = null;
    private int nonPersistentPendingMessages = 0;
    private final int maxNonPersistentPendingMessages;
    private String originalPrincipal = null;
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

    private final long connectionLivenessCheckTimeoutMillis;

    private final boolean ignoreConsumerReplicateSubscriptionState;

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
        // pulsar.getBrokerService() can sometimes be null in unit tests when using mocks
        // the null check is a workaround for #13620
        super(pulsar.getBrokerService() != null ? pulsar.getBrokerService().getKeepAliveIntervalSeconds() : 0,
                TimeUnit.SECONDS);
        this.service = pulsar.getBrokerService();
        this.schemaService = pulsar.getSchemaRegistryService();
        this.listenerName = listenerName;
        this.state = State.Start;
        ServiceConfiguration conf = pulsar.getConfiguration();

        this.connectionLivenessCheckTimeoutMillis = conf.getConnectionLivenessCheckTimeoutMillis();

        // This maps are not heavily contended since most accesses are within the cnx thread
        this.producers = ConcurrentLongHashMap.<CompletableFuture<Producer>>newBuilder()
                .expectedItems(8)
                .concurrencyLevel(1)
                .build();
        this.consumers = ConcurrentLongHashMap.<CompletableFuture<Consumer>>newBuilder()
                .expectedItems(8)
                .concurrencyLevel(1)
                .build();
        this.recentlyClosedProducers = new HashMap<>();
        this.replicatorPrefix = conf.getReplicatorPrefix();
        this.maxNonPersistentPendingMessages = conf.getMaxConcurrentNonPersistentMessagePerConnection();
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
        this.connectionController = new ConnectionController.DefaultConnectionController(
                conf.getBrokerMaxConnections(),
                conf.getBrokerMaxConnectionsPerIp());
        this.enableSubscriptionPatternEvaluation = conf.isEnableBrokerSideSubscriptionPatternEvaluation();
        this.maxSubscriptionPatternLength = conf.getSubscriptionPatternMaxLength();
        this.topicListService = new TopicListService(pulsar, this,
                enableSubscriptionPatternEvaluation, maxSubscriptionPatternLength);
        this.brokerInterceptor = this.service != null ? this.service.getInterceptor() : null;
        this.ignoreConsumerReplicateSubscriptionState =
                Boolean.parseBoolean(
                        conf.getProperties().getProperty("ignoreConsumerReplicateSubscriptionState", "false"));
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        ConnectionController.State state = connectionController.increaseConnection(remoteAddress);
        if (!state.equals(ConnectionController.State.OK)) {
            final ByteBuf msg = Commands.newError(-1, ServerError.NotAllowedError,
                    state.equals(ConnectionController.State.REACH_MAX_CONNECTION)
                            ? "Reached the maximum number of connections"
                            : "Reached the maximum number of connections on address" + remoteAddress);
            NettyChannelUtil.writeAndFlushWithClosePromise(ctx, msg);
            return;
        }
        if (log.isDebugEnabled()) {
            // Connection information is logged after a successful Connect command is processed.
            log.debug("New connection from {}", remoteAddress);
        }
        this.ctx = ctx;
        this.commandSender = new PulsarCommandSenderImpl(brokerInterceptor, this);
        this.service.getPulsarStats().recordConnectionCreate();
        cnxsPerThread.get().add(this);
        service.getPulsar().runWhenReadyForIncomingRequests(() -> {
            // enable auto read after PulsarService is ready to accept incoming requests
            ctx.channel().config().setAutoRead(true);
        });
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        connectionController.decreaseConnection(ctx.channel().remoteAddress());
        isActive = false;
        log.info("Closed connection from {}", remoteAddress);
        if (brokerInterceptor != null) {
            brokerInterceptor.onConnectionClosed(this);
        }

        cnxsPerThread.get().remove(this);
        if (authRefreshTask != null) {
            authRefreshTask.cancel(false);
        }

        // Connection is gone, close the producers immediately
        producers.forEach((__, producerFuture) -> {
            // prevent race conditions in completing producers
            if (!producerFuture.isDone()
                    && producerFuture.completeExceptionally(new IllegalStateException("Connection closed."))) {
                return;
            }
            if (producerFuture.isDone() && !producerFuture.isCompletedExceptionally()) {
                Producer producer = producerFuture.getNow(null);
                producer.closeNow(true);
                if (brokerInterceptor != null) {
                    brokerInterceptor.producerClosed(this, producer, producer.getMetadata());
                }
            }
        });

        consumers.forEach((__, consumerFuture) -> {
            // prevent race conditions in completing consumers
            if (!consumerFuture.isDone()
                    && consumerFuture.completeExceptionally(new IllegalStateException("Connection closed."))) {
                return;
            }
            if (consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
                Consumer consumer = consumerFuture.getNow(null);
                try {
                    consumer.close();
                    if (brokerInterceptor != null) {
                        brokerInterceptor.consumerClosed(this, consumer, consumer.getMetadata());
                    }
                } catch (BrokerServiceException e) {
                    log.warn("Consumer {} was already closed: {}", consumer, e);
                }
            }
        });
        this.topicListService.inactivate();
        this.service.getPulsarStats().recordConnectionClose();

        // complete possible pending connection check future
        if (connectionCheckInProgress != null && !connectionCheckInProgress.isDone()) {
            connectionCheckInProgress.complete(false);
        }
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
                    ClientCnx.isKnownException(cause) ? cause.toString() : ExceptionUtils.getStackTrace(cause));
            state = State.Failed;
            if (log.isDebugEnabled()) {
                log.debug("[{}] connect state change to : [{}]", remoteAddress, State.Failed.name());
            }
        } else {
            // At default info level, suppress all subsequent exceptions that are thrown when the connection has already
            // failed
            if (log.isDebugEnabled()) {
                log.debug("[{}] Got exception {}", remoteAddress,
                        ClientCnx.isKnownException(cause) ? cause.toString() : ExceptionUtils.getStackTrace(cause));
            }
        }
        ctx.close();
    }

    // ////
    // // Incoming commands handling
    // ////

    private CompletableFuture<Boolean> isTopicOperationAllowed(TopicName topicName, TopicOperation operation,
                    AuthenticationDataSource authDataSource, AuthenticationDataSource originalAuthDataSource) {
        if (!service.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> isProxyAuthorizedFuture;
        if (originalPrincipal != null) {
            isProxyAuthorizedFuture = service.getAuthorizationService().allowTopicOperationAsync(
                    topicName, operation, originalPrincipal,
                    originalAuthDataSource != null ? originalAuthDataSource : authDataSource);
        } else {
            isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> isAuthorizedFuture = service.getAuthorizationService().allowTopicOperationAsync(
            topicName, operation, authRole, authDataSource);
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
            AuthenticationDataSource authDataSource =
                    new AuthenticationDataSubscription(authenticationData, subscriptionName);
            AuthenticationDataSource originalAuthDataSource = null;
            if (originalAuthData != null) {
                originalAuthDataSource = new AuthenticationDataSubscription(originalAuthData, subscriptionName);
            }
            return isTopicOperationAllowed(topicName, operation, authDataSource, originalAuthDataSource);
        } else {
            return CompletableFuture.completedFuture(true);
        }
    }

    @Override
    protected void handleLookup(CommandLookupTopic lookup) {
        checkArgument(state == State.Connected);
        final long requestId = lookup.getRequestId();
        final boolean authoritative = lookup.isAuthoritative();

        // use the connection-specific listener name by default.
        final String advertisedListenerName =
                lookup.hasAdvertisedListenerName() && StringUtils.isNotBlank(lookup.getAdvertisedListenerName())
                        ? lookup.getAdvertisedListenerName() : this.listenerName;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received Lookup from {} for {} requesting listener {}", lookup.getTopic(), remoteAddress,
                    requestId, StringUtils.isNotBlank(advertisedListenerName) ? advertisedListenerName : "(none)");
        }

        TopicName topicName = validateTopicName(lookup.getTopic(), requestId, lookup);
        if (topicName == null) {
            return;
        }

        if (!this.service.getPulsar().isRunning()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed lookup topic {} due to pulsar service is not ready: {} state", remoteAddress,
                        topicName, this.service.getPulsar().getState().toString());
            }
            writeAndFlush(newLookupErrorResponse(ServerError.ServiceNotReady,
                    "Failed due to pulsar service is not ready", requestId));
            return;
        }

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            isTopicOperationAllowed(topicName, TopicOperation.LOOKUP, authenticationData, originalAuthData).thenApply(
                    isAuthorized -> {
                if (isAuthorized) {
                    lookupTopicAsync(getBrokerService().pulsar(), topicName, authoritative,
                            getPrincipal(), getAuthenticationData(),
                            requestId, advertisedListenerName).handle((lookupResponse, ex) -> {
                                if (ex == null) {
                                    writeAndFlush(lookupResponse);
                                } else {
                                    // it should never happen
                                    log.warn("[{}] lookup failed with error {}, {}", remoteAddress, topicName,
                                            ex.getMessage(), ex);
                                    writeAndFlush(newLookupErrorResponse(ServerError.ServiceNotReady,
                                            ex.getMessage(), requestId));
                                }
                                lookupSemaphore.release();
                                return null;
                            });
                } else {
                    final String msg = "Client is not authorized to Lookup";
                    log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, getPrincipal(), topicName);
                    writeAndFlush(newLookupErrorResponse(ServerError.AuthorizationError, msg, requestId));
                    lookupSemaphore.release();
                }
                return null;
            }).exceptionally(ex -> {
                logAuthException(remoteAddress, "lookup", getPrincipal(), Optional.of(topicName), ex);
                final String msg = "Exception occurred while trying to authorize lookup";
                writeAndFlush(newLookupErrorResponse(ServerError.AuthorizationError, msg, requestId));
                lookupSemaphore.release();
                return null;
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed lookup due to too many lookup-requests {}", remoteAddress, topicName);
            }
            writeAndFlush(newLookupErrorResponse(ServerError.TooManyRequests,
                    "Failed due to too many pending lookup requests", requestId));
        }
    }

    private void writeAndFlush(ByteBuf cmd) {
        NettyChannelUtil.writeAndFlushWithVoidPromise(ctx, cmd);
    }

    @Override
    protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata partitionMetadata) {
        checkArgument(state == State.Connected);
        final long requestId = partitionMetadata.getRequestId();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received PartitionMetadataLookup from {} for {}", partitionMetadata.getTopic(),
                    remoteAddress, requestId);
        }

        TopicName topicName = validateTopicName(partitionMetadata.getTopic(), requestId, partitionMetadata);
        if (topicName == null) {
            return;
        }

        if (!this.service.getPulsar().isRunning()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed PartitionMetadataLookup from {} for {} "
                                + "due to pulsar service is not ready: {} state",
                        partitionMetadata.getTopic(), remoteAddress, requestId,
                        this.service.getPulsar().getState().toString());
            }
            writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.ServiceNotReady,
                    "Failed due to pulsar service is not ready", requestId));
            return;
        }

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            isTopicOperationAllowed(topicName, TopicOperation.LOOKUP, authenticationData, originalAuthData).thenApply(
                    isAuthorized -> {
                if (isAuthorized) {
                    // Get if exists, respond not found error if not exists.
                    getBrokerService().isAllowAutoTopicCreationAsync(topicName).thenAccept(brokerAllowAutoCreate -> {
                        boolean autoCreateIfNotExist = partitionMetadata.isMetadataAutoCreationEnabled()
                                && brokerAllowAutoCreate;
                        if (!autoCreateIfNotExist) {
                            NamespaceService namespaceService = getBrokerService().getPulsar().getNamespaceService();
                            namespaceService.checkTopicExists(topicName).thenAccept(topicExistsInfo -> {
                                lookupSemaphore.release();
                                if (!topicExistsInfo.isExists()) {
                                    writeAndFlush(Commands.newPartitionMetadataResponse(
                                            ServerError.TopicNotFound, "", requestId));
                                } else if (topicExistsInfo.getTopicType().equals(TopicType.PARTITIONED)) {
                                    commandSender.sendPartitionMetadataResponse(topicExistsInfo.getPartitions(),
                                            requestId);
                                } else {
                                    commandSender.sendPartitionMetadataResponse(0, requestId);
                                }
                                // release resources.
                                topicExistsInfo.recycle();
                            }).exceptionally(ex -> {
                                lookupSemaphore.release();
                                log.error("{} {} Failed to get partition metadata", topicName,
                                        ServerCnx.this.toString(), ex);
                                writeAndFlush(
                                        Commands.newPartitionMetadataResponse(ServerError.MetadataError,
                                                "Failed to get partition metadata",
                                                requestId));
                                return null;
                            });
                        } else {
                            // Get if exists, create a new one if not exists.
                            unsafeGetPartitionedTopicMetadataAsync(getBrokerService().pulsar(), topicName)
                                .whenComplete((metadata, ex) -> {
                                    lookupSemaphore.release();
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
                                            ServerError error = ServerError.ServiceNotReady;
                                            if (ex instanceof MetadataStoreException) {
                                                error = ServerError.MetadataError;
                                            } else if (ex instanceof RestException restException){
                                                int responseCode = restException.getResponse().getStatus();
                                                if (responseCode == NOT_FOUND.getStatusCode()){
                                                    error = ServerError.TopicNotFound;
                                                } else if (responseCode < INTERNAL_SERVER_ERROR.getStatusCode()){
                                                    error = ServerError.MetadataError;
                                                }
                                            }
                                            if (error == ServerError.TopicNotFound) {
                                                log.info("Trying to get Partitioned Metadata for a resource not exist"
                                                                + "[{}] {}: {}", remoteAddress,
                                                        topicName, ex.getMessage());
                                            } else {
                                                log.warn("Failed to get Partitioned Metadata [{}] {}: {}",
                                                        remoteAddress, topicName, ex.getMessage(), ex);
                                            }
                                            commandSender.sendPartitionMetadataResponse(error, ex.getMessage(),
                                                    requestId);
                                        }
                                    }
                                });
                        }
                    });
                } else {
                    final String msg = "Client is not authorized to Get Partition Metadata";
                    log.warn("[{}] {} with role {} on topic {}", remoteAddress, msg, getPrincipal(), topicName);
                    writeAndFlush(
                            Commands.newPartitionMetadataResponse(ServerError.AuthorizationError, msg, requestId));
                    lookupSemaphore.release();
                }
                return null;
            }).exceptionally(ex -> {
                logAuthException(remoteAddress, "partition-metadata", getPrincipal(), Optional.of(topicName), ex);
                Throwable actEx = FutureUtil.unwrapCompletionException(ex);
                if (actEx instanceof WebApplicationException restException) {
                    if (restException.getResponse().getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                        writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.TopicNotFound,
                        "Tenant or namespace or topic does not exist: " + topicName.getNamespace() ,
                                requestId));
                        lookupSemaphore.release();
                        return null;
                    }
                }
                final String msg = "Exception occurred while trying to authorize get Partition Metadata";
                writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.AuthorizationError, msg,
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
        checkArgument(state == State.Connected);
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

        writeAndFlush(msg);
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
                .setMessageAckRate(consumerStats.messageAckRate)
                .setType(subscription.getTypeString());

        return Commands.serializeWithSize(cmd);
    }

    // complete the connect and sent newConnected command
    private void completeConnect(int clientProtoVersion, String clientVersion) {
        if (service.isAuthenticationEnabled()) {
            if (service.isAuthorizationEnabled()) {
                if (!service.getAuthorizationService()
                    .isValidOriginalPrincipal(authRole, originalPrincipal, remoteAddress, false)) {
                    state = State.Failed;
                    service.getPulsarStats().recordConnectionCreateFail();
                    final ByteBuf msg = Commands.newError(-1, ServerError.AuthorizationError, "Invalid roles.");
                    NettyChannelUtil.writeAndFlushWithClosePromise(ctx, msg);
                    return;
                }
                if (proxyVersion != null && !service.getAuthorizationService().isProxyRole(authRole)) {
                    // Only allow proxyVersion to be set when connecting with a proxy
                    state = State.Failed;
                    service.getPulsarStats().recordConnectionCreateFail();
                    final ByteBuf msg = Commands.newError(-1, ServerError.AuthorizationError,
                            "Must not set proxyVersion without connecting as a ProxyRole.");
                    NettyChannelUtil.writeAndFlushWithClosePromise(ctx, msg);
                    return;
                }
            }
            maybeScheduleAuthenticationCredentialsRefresh();
        }
        writeAndFlush(Commands.newConnected(clientProtoVersion, maxMessageSize, enableSubscriptionPatternEvaluation));
        state = State.Connected;
        service.getPulsarStats().recordConnectionCreateSuccess();
        if (log.isDebugEnabled()) {
            log.debug("[{}] connect state change to : [{}]", remoteAddress, State.Connected.name());
        }
        setRemoteEndpointProtocolVersion(clientProtoVersion);
        if (isNotBlank(clientVersion)) {
            this.clientVersion = clientVersion.intern();
        }
        if (!service.isAuthenticationEnabled()) {
            log.info("[{}] connected with clientVersion={}, clientProtocolVersion={}, proxyVersion={}", remoteAddress,
                    clientVersion, clientProtoVersion, proxyVersion);
        } else if (originalPrincipal != null) {
            log.info("[{}] connected role={} and originalAuthRole={} using authMethod={}, clientVersion={}, "
                            + "clientProtocolVersion={}, proxyVersion={}", remoteAddress, authRole, originalPrincipal,
                    authMethod, clientVersion, clientProtoVersion, proxyVersion);
        } else {
            log.info("[{}] connected with role={} using authMethod={}, clientVersion={}, clientProtocolVersion={}, "
                            + "proxyVersion={}", remoteAddress, authRole, authMethod, clientVersion, clientProtoVersion,
                    proxyVersion);
        }
        if (brokerInterceptor != null) {
            brokerInterceptor.onConnectionCreated(this);
        }
    }

    // According to auth result, send Connected, AuthChallenge, or Error command.
    private void doAuthentication(AuthData clientData,
                                  boolean useOriginalAuthState,
                                  int clientProtocolVersion,
                                  final String clientVersion) {
        // The original auth state can only be set on subsequent auth attempts (and only
        // in presence of a proxy and if the proxy is forwarding the credentials).
        // In this case, the re-validation needs to be done against the original client
        // credentials.
        AuthenticationState authState = useOriginalAuthState ? originalAuthState : this.authState;
        String authRole = useOriginalAuthState ? originalPrincipal : this.authRole;
        if (log.isDebugEnabled()) {
            log.debug("Authenticate using original auth state : {}, role = {}", useOriginalAuthState, authRole);
        }
        authState
                .authenticateAsync(clientData)
                .whenCompleteAsync((authChallenge, throwable) -> {
                    if (throwable == null) {
                        authChallengeSuccessCallback(authChallenge, useOriginalAuthState, authRole,
                                clientProtocolVersion, clientVersion);
                    } else {
                        authenticationFailed(throwable);
                    }
                }, ctx.executor());
    }

    public void authChallengeSuccessCallback(AuthData authChallenge,
                                             boolean useOriginalAuthState,
                                             String authRole,
                                             int clientProtocolVersion,
                                             String clientVersion) {
        try {
            if (authChallenge == null) {
                // Authentication has completed. It was either:
                // 1. the 1st time the authentication process was done, in which case we'll send
                //    a `CommandConnected` response
                // 2. an authentication refresh, in which case we need to refresh authenticationData
                AuthenticationState authState = useOriginalAuthState ? originalAuthState : this.authState;
                String newAuthRole = authState.getAuthRole();
                AuthenticationDataSource newAuthDataSource = authState.getAuthDataSource();

                if (state != State.Connected) {
                    // Set the auth data and auth role
                    if (!useOriginalAuthState) {
                        this.authRole = newAuthRole;
                        this.authenticationData = newAuthDataSource;
                    }
                    // First time authentication is done
                    if (originalAuthState != null) {
                        // We only set originalAuthState when we are going to use it.
                        authenticateOriginalData(clientProtocolVersion, clientVersion);
                    } else {
                        completeConnect(clientProtocolVersion, clientVersion);
                    }
                } else {
                    // Refresh the auth data
                    if (!useOriginalAuthState) {
                        this.authenticationData = newAuthDataSource;
                    } else {
                        this.originalAuthData = newAuthDataSource;
                    }
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
            } else {
                // auth not complete, continue auth with client side.
                ctx.writeAndFlush(Commands.newAuthChallenge(authMethod, authChallenge, clientProtocolVersion));
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Authentication in progress client by method {}.", remoteAddress, authMethod);
                }
            }
        } catch (Exception | AssertionError e) {
            authenticationFailed(e);
        }
    }

    private void authenticateOriginalData(int clientProtoVersion, String clientVersion) {
        originalAuthState
                .authenticateAsync(originalAuthDataCopy)
                .whenCompleteAsync((authChallenge, throwable) -> {
                    if (throwable != null) {
                        authenticationFailed(throwable);
                    } else if (authChallenge != null) {
                        // The protocol does not yet handle an auth challenge here.
                        // See https://github.com/apache/pulsar/issues/19291.
                        authenticationFailed(new AuthenticationException("Failed to authenticate original auth data "
                                + "due to unsupported authChallenge."));
                    } else {
                        try {
                            // No need to retain these bytes anymore
                            originalAuthDataCopy = null;
                            originalAuthData = originalAuthState.getAuthDataSource();
                            originalPrincipal = originalAuthState.getAuthRole();
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Authenticated original role (forwarded from proxy): {}",
                                        remoteAddress, originalPrincipal);
                            }
                            completeConnect(clientProtoVersion, clientVersion);
                        } catch (Exception | AssertionError e) {
                            authenticationFailed(e);
                        }
                    }
                }, ctx.executor());
    }

    // Handle authentication and authentication refresh failures. Must be called from event loop.
    private void authenticationFailed(Throwable t) {
        String operation;
        if (state == State.Connecting) {
            service.getPulsarStats().recordConnectionCreateFail();
            operation = "connect";
        } else {
            operation = "authentication-refresh";
        }
        state = State.Failed;
        logAuthException(remoteAddress, operation, getPrincipal(), Optional.empty(), t);
        final ByteBuf msg = Commands.newError(-1, ServerError.AuthenticationError, "Failed to authenticate");
        NettyChannelUtil.writeAndFlushWithClosePromise(ctx, msg);
    }

    /**
     * Method to initialize the {@link #authRefreshTask} task.
     */
    private void maybeScheduleAuthenticationCredentialsRefresh() {
        assert ctx.executor().inEventLoop();
        assert authRefreshTask == null;
        if (authState == null) {
            // Authentication is disabled or there's no local state to refresh
            return;
        }
        authRefreshTask = ctx.executor().scheduleAtFixedRate(this::refreshAuthenticationCredentials,
                service.getPulsar().getConfig().getAuthenticationRefreshCheckSeconds(),
                service.getPulsar().getConfig().getAuthenticationRefreshCheckSeconds(),
                TimeUnit.SECONDS);
    }

    private void refreshAuthenticationCredentials() {
        assert ctx.executor().inEventLoop();
        AuthenticationState authState = this.originalAuthState != null ? originalAuthState : this.authState;
        if (getState() == State.Failed) {
            // Happens when an exception is thrown that causes this connection to close.
            return;
        } else if (!authState.isExpired()) {
            // Credentials are still valid. Nothing to do at this point
            return;
        } else if (originalPrincipal != null && originalAuthState == null) {
            // This case is only checked when the authState is expired because we've reached a point where
            // authentication needs to be refreshed, but the protocol does not support it unless the proxy forwards
            // the originalAuthData.
            log.info(
                    "[{}] Cannot revalidate user credential when using proxy and"
                            + " not forwarding the credentials. Closing connection",
                    remoteAddress);
            ctx.close();
            return;
        }

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

        log.info("[{}] Refreshing authentication credentials for originalPrincipal {} and authRole {}",
                remoteAddress, originalPrincipal, this.authRole);
        try {
            AuthData brokerData = authState.refreshAuthentication();

            writeAndFlush(Commands.newAuthChallenge(authMethod, brokerData,
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

        if (!this.service.getPulsar().isRunning()) {
            if (log.isDebugEnabled()) {
                log.debug("Failed CONNECT from {} due to pulsar service is not ready: {} state", remoteAddress,
                        this.service.getPulsar().getState().toString());
            }
            writeAndFlush(
                    Commands.newError(
                            -1,
                            ServerError.ServiceNotReady,
                            "Failed due to pulsar service is not ready")
            );
            close();
            return;
        }

        String clientVersion = connect.getClientVersion();
        int clientProtocolVersion = connect.getProtocolVersion();
        features = new FeatureFlags();
        if (connect.hasFeatureFlags()) {
            features.copyFrom(connect.getFeatureFlags());
        }

        if (connect.hasProxyVersion()) {
            proxyVersion = connect.getProxyVersion();
        }

        if (!service.isAuthenticationEnabled()) {
            completeConnect(clientProtocolVersion, clientVersion);
            return;
        }

        // Go to Connecting state now because auth can be async.
        state = State.Connecting;

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

            if (log.isDebugEnabled()) {
                String role = "";
                if (authState != null && authState.isComplete()) {
                    role = authState.getAuthRole();
                } else {
                    role = "authentication incomplete or null";
                }
                log.debug("[{}] Authenticate role : {}", remoteAddress, role);
            }

            if (connect.hasOriginalPrincipal() && service.getPulsar().getConfig().isAuthenticateOriginalAuthData()) {
                // Flow:
                // 1. Initialize original authentication.
                // 2. Authenticate the proxy's authentication data.
                // 3. Authenticate the original authentication data.
                String originalAuthMethod;
                if (connect.hasOriginalAuthMethod()) {
                    originalAuthMethod = connect.getOriginalAuthMethod();
                } else {
                    originalAuthMethod = "none";
                }

                AuthenticationProvider originalAuthenticationProvider = getBrokerService()
                        .getAuthenticationService()
                        .getAuthenticationProvider(originalAuthMethod);

                /**
                 * When both the broker and the proxy are configured with anonymousUserRole
                 * if the client does not configure an authentication method
                 * the proxy side will set the value of anonymousUserRole to clientAuthRole when it creates a connection
                 * and the value of clientAuthMethod will be none.
                 * Similarly, should also set the value of authRole to anonymousUserRole on the broker side.
                 */
                if (originalAuthenticationProvider == null) {
                    authRole = getBrokerService().getAuthenticationService().getAnonymousUserRole()
                            .orElseThrow(() ->
                                    new AuthenticationException("No anonymous role, and can't find "
                                            + "AuthenticationProvider for original role using auth method "
                                            + "[" + originalAuthMethod + "] is not available"));
                    originalPrincipal = authRole;
                    completeConnect(clientProtocolVersion, clientVersion);
                    return;
                }

                originalAuthDataCopy = AuthData.of(connect.getOriginalAuthData().getBytes());
                originalAuthState = originalAuthenticationProvider.newAuthState(
                        originalAuthDataCopy,
                        remoteAddress,
                        sslSession);
            } else if (connect.hasOriginalPrincipal()) {
                originalPrincipal = connect.getOriginalPrincipal();

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Setting original role (forwarded from proxy): {}",
                        remoteAddress, originalPrincipal);
                }
            }

            doAuthentication(clientData, false, clientProtocolVersion, clientVersion);
        } catch (Exception e) {
            authenticationFailed(e);
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
            doAuthentication(clientData, originalAuthState != null, authResponse.getProtocolVersion(),
                    authResponse.hasClientVersion() ? authResponse.getClientVersion() : EMPTY);
        } catch (Exception e) {
            authenticationFailed(e);
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
        final Boolean isReplicated = ignoreConsumerReplicateSubscriptionState ? null :
                (subscribe.hasReplicateSubscriptionState() ? subscribe.isReplicateSubscriptionState() : null);
        final boolean forceTopicCreation = subscribe.isForceTopicCreation();
        final KeySharedMeta keySharedMeta = subscribe.hasKeySharedMeta()
              ? new KeySharedMeta().copyFrom(subscribe.getKeySharedMeta())
              : emptyKeySharedMeta;
        final long consumerEpoch = subscribe.hasConsumerEpoch() ? subscribe.getConsumerEpoch() : DEFAULT_CONSUMER_EPOCH;
        final Optional<Map<String, String>> subscriptionProperties = SubscriptionOption.getPropertiesMap(
                subscribe.getSubscriptionPropertiesList());

        if (log.isDebugEnabled()) {
            log.debug("Topic name = {}, subscription name = {}, schema is {}", topicName, subscriptionName,
                    schema == null ? "absent" : "present");
        }

        CompletableFuture<Boolean> isAuthorizedFuture = isTopicOperationAllowed(
                topicName,
                subscriptionName,
                TopicOperation.CONSUME
        );

        // Make sure the consumer future is put into the consumers map first to avoid the same consumer
        // epoch using different consumer futures, and only remove the consumer future from the map
        // if subscribe failed .
        CompletableFuture<Consumer> consumerFuture = new CompletableFuture<>();
        CompletableFuture<Consumer> existingConsumerFuture =
                consumers.putIfAbsent(consumerId, consumerFuture);
        isAuthorizedFuture.thenApply(isAuthorized -> {
            if (isAuthorized) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Client is authorized to subscribe with role {}",
                            remoteAddress, getPrincipal());
                }

                log.info("[{}] Subscribing on topic {} / {}. consumerId: {}", this.toString(),
                        topicName, subscriptionName, consumerId);
                try {
                    Metadata.validateMetadata(metadata,
                            service.getPulsar().getConfiguration().getMaxConsumerMetadataSize());
                } catch (IllegalArgumentException iae) {
                    final String msg = iae.getMessage();
                    consumers.remove(consumerId, consumerFuture);
                    commandSender.sendErrorResponse(requestId, ServerError.MetadataError, msg);
                    return null;
                }

                if (existingConsumerFuture != null) {
                    if (!existingConsumerFuture.isDone()){
                        // There was an early request to create a consumer with same consumerId. This can happen
                        // when
                        // client timeout is lower the broker timeouts. We need to wait until the previous
                        // consumer
                        // creation request either complete or fails.
                        log.warn("[{}][{}][{}] Consumer with id is already present on the connection,"
                                + " consumerId={}", remoteAddress, topicName, subscriptionName, consumerId);
                        commandSender.sendErrorResponse(requestId, ServerError.ServiceNotReady,
                                "Consumer is already present on the connection");
                    } else if (existingConsumerFuture.isCompletedExceptionally()){
                        log.warn("[{}][{}][{}] A failed consumer with id is already present on the connection,"
                                + " consumerId={}", remoteAddress, topicName, subscriptionName, consumerId);
                        ServerError error = getErrorCodeWithErrorLog(existingConsumerFuture, true,
                                String.format("A failed consumer with id is already present on the connection."
                                                + " consumerId: %s, remoteAddress: %s, subscription: %s",
                                        consumerId, remoteAddress, subscriptionName));
                        /**
                         * This future may was failed due to the client closed a in-progress subscribing.
                         * See {@link #handleCloseConsumer(CommandCloseConsumer)}
                         * Do not remove the failed future at current line, it will be removed after the progress of
                         * the previous subscribing is done.
                         * Before the previous subscribing is done, the new subscribe request will always fail.
                         * This mechanism is in order to prevent more complex logic to handle the race conditions.
                         */
                        commandSender.sendErrorResponse(requestId, error,
                                "Consumer that failed is already present on the connection");
                    } else {
                        Consumer consumer = existingConsumerFuture.getNow(null);
                        log.warn("[{}] Consumer with the same id is already created:"
                                        + " consumerId={}, consumer={}",
                                remoteAddress, consumerId, consumer);
                        commandSender.sendSuccessResponse(requestId);
                    }
                    return null;
                }

                service.isAllowAutoTopicCreationAsync(topicName.toString())
                        .thenApply(isAllowed -> forceTopicCreation && isAllowed)
                        .thenCompose(createTopicIfDoesNotExist ->
                                service.getTopic(topicName.toString(), createTopicIfDoesNotExist))
                        .thenCompose(optTopic -> {
                            if (!optTopic.isPresent()) {
                                return FutureUtil
                                        .failedFuture(new TopicNotFoundException(
                                                "Topic " + topicName + " does not exist"));
                            }
                            final Topic topic = optTopic.get();
                            // Check max consumer limitation to avoid unnecessary ops wasting resources. For example:
                            // the new consumer reached max producer limitation, but pulsar did schema check first,
                            // it would waste CPU.
                            if (((AbstractTopic) topic).isConsumersExceededOnTopic()) {
                                log.warn("[{}] Attempting to add consumer to topic which reached max"
                                        + " consumers limit", topic);
                                Throwable t =
                                        new ConsumerBusyException("Topic reached max consumers limit");
                                return FutureUtil.failedFuture(t);
                            }
                            return service.isAllowAutoSubscriptionCreationAsync(topicName)
                                    .thenCompose(isAllowedAutoSubscriptionCreation -> {
                                        boolean rejectSubscriptionIfDoesNotExist = isDurable
                                                && !isAllowedAutoSubscriptionCreation
                                                && !topic.getSubscriptions().containsKey(subscriptionName)
                                                && topic.isPersistent();

                                        if (rejectSubscriptionIfDoesNotExist) {
                                            return FutureUtil
                                                    .failedFuture(
                                                            new SubscriptionNotFoundException(
                                                                    "Subscription does not exist"));
                                        }

                                        SubscriptionOption option = SubscriptionOption.builder().cnx(ServerCnx.this)
                                                .subscriptionName(subscriptionName)
                                                .consumerId(consumerId).subType(subType)
                                                .priorityLevel(priorityLevel)
                                                .consumerName(consumerName).isDurable(isDurable)
                                                .startMessageId(startMessageId).metadata(metadata)
                                                .readCompacted(readCompacted)
                                                .initialPosition(initialPosition)
                                                .startMessageRollbackDurationSec(startMessageRollbackDurationSec)
                                                .replicatedSubscriptionStateArg(isReplicated)
                                                .keySharedMeta(keySharedMeta)
                                                .subscriptionProperties(subscriptionProperties)
                                                .consumerEpoch(consumerEpoch)
                                                .schemaType(schema == null ? null : schema.getType())
                                                .build();
                                        if (schema != null && schema.getType() != SchemaType.AUTO_CONSUME) {
                                            return topic.addSchemaIfIdleOrCheckCompatible(schema)
                                                    .thenCompose(v -> topic.subscribe(option));
                                        } else {
                                            return topic.subscribe(option);
                                        }
                                    });
                        })
                        .thenAccept(consumer -> {
                            if (consumerFuture.complete(consumer)) {
                                log.info("[{}] Created subscription on topic {} / {}",
                                        remoteAddress, topicName, subscriptionName);
                                commandSender.sendSuccessResponse(requestId);
                                if (brokerInterceptor != null) {
                                    try {
                                        brokerInterceptor.consumerCreated(this, consumer, metadata);
                                    } catch (Throwable t) {
                                        log.error("Exception occur when intercept consumer created.", t);
                                    }
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
                            // Send error back to client, only if not completed already.
                            if (consumerFuture.completeExceptionally(exception)) {
                                commandSender.sendErrorResponse(requestId,
                                        BrokerServiceException.getClientErrorCode(exception.getCause()),
                                        exception.getCause().getMessage());
                            }
                            consumers.remove(consumerId, consumerFuture);

                            return null;

                        });
            } else {
                String msg = "Client is not authorized to subscribe";
                log.warn("[{}] {} with role {}", remoteAddress, msg, getPrincipal());
                consumers.remove(consumerId, consumerFuture);
                writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
            }
            return null;
        }).exceptionally(ex -> {
            logAuthException(remoteAddress, "subscribe", getPrincipal(), Optional.of(topicName), ex);
            consumers.remove(consumerId, consumerFuture);
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
        final String initialSubscriptionName =
                cmdProducer.hasInitialSubscriptionName() ? cmdProducer.getInitialSubscriptionName() : null;
        final boolean supportsPartialProducer = supportsPartialProducer();

        final TopicName topicName = validateTopicName(cmdProducer.getTopic(), requestId, cmdProducer);
        if (topicName == null) {
            return;
        }

        CompletableFuture<Boolean> isAuthorizedFuture = isTopicOperationAllowed(
                topicName, TopicOperation.PRODUCE, authenticationData, originalAuthData
        );

        if (!Strings.isNullOrEmpty(initialSubscriptionName)) {
            isAuthorizedFuture =
                    isAuthorizedFuture.thenCombine(
                            isTopicOperationAllowed(topicName, initialSubscriptionName, TopicOperation.SUBSCRIBE),
                            (canProduce, canSubscribe) -> canProduce && canSubscribe);
        }

        isAuthorizedFuture.thenApply(isAuthorized -> {
            if (!isAuthorized) {
                String msg = "Client is not authorized to Produce";
                log.warn("[{}] {} with role {}", remoteAddress, msg, getPrincipal());
                writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
                return null;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Client is authorized to Produce with role {}", remoteAddress, getPrincipal());
            }
            CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
            CompletableFuture<Producer> existingProducerFuture = producers.putIfAbsent(producerId, producerFuture);

            if (existingProducerFuture != null) {
                if (!existingProducerFuture.isDone()) {
                    // There was an early request to create a producer with same producerId.
                    // This can happen when client timeout is lower than the broker timeouts.
                    // We need to wait until the previous producer creation request
                    // either complete or fails.
                    log.warn("[{}][{}] Producer with id is already present on the connection, producerId={}",
                            remoteAddress, topicName, producerId);
                    commandSender.sendErrorResponse(requestId, ServerError.ServiceNotReady,
                            "Producer is already present on the connection");
                } else if (existingProducerFuture.isCompletedExceptionally()) {
                    // remove producer with producerId as it's already completed with exception
                    log.warn("[{}][{}] Producer with id is failed to register present on the connection, producerId={}",
                            remoteAddress, topicName, producerId);
                    ServerError error = getErrorCode(existingProducerFuture);
                    producers.remove(producerId, existingProducerFuture);
                    commandSender.sendErrorResponse(requestId, error,
                            "Producer is already failed to register present on the connection");
                } else {
                    Producer producer = existingProducerFuture.getNow(null);
                    log.info("[{}] [{}] Producer with the same id is already created:"
                            + " producerId={}, producer={}", remoteAddress, topicName, producerId, producer);
                    commandSender.sendProducerSuccessResponse(requestId, producer.getProducerName(),
                            producer.getSchemaVersion());
                }
                return null;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Creating producer. producerId={}, producerName={}, schema is {}", remoteAddress,
                        topicName, producerId, producerName, schema == null ? "absent" : "present");
            }

            service.getOrCreateTopic(topicName.toString()).thenCompose((Topic topic) -> {
                // Check max producer limitation to avoid unnecessary ops wasting resources. For example: the new
                // producer reached max producer limitation, but pulsar did schema check first, it would waste CPU
                if (((AbstractTopic) topic).isProducersExceeded(producerName)) {
                    log.warn("[{}] Attempting to add producer to topic which reached max producers limit", topic);
                    String errorMsg = "Topic '" + topicName.toString() + "' reached max producers limit";
                    Throwable t = new BrokerServiceException.ProducerBusyException(errorMsg);
                    return CompletableFuture.failedFuture(t);
                }

                // Before creating producer, check if backlog quota exceeded
                // on topic for size based limit and time based limit
                CompletableFuture<Void> backlogQuotaCheckFuture = CompletableFuture.allOf(
                        topic.checkBacklogQuotaExceeded(producerName, BacklogQuotaType.destination_storage),
                        topic.checkBacklogQuotaExceeded(producerName, BacklogQuotaType.message_age));

                backlogQuotaCheckFuture.thenRun(() -> {
                    // Check whether the producer will publish encrypted messages or not
                    if ((topic.isEncryptionRequired() || encryptionRequireOnProducer)
                            && !isEncrypted
                            && !SystemTopicNames.isSystemTopic(topicName)) {
                        String msg = String.format("Encryption is required in %s", topicName);
                        log.warn("[{}] {}", remoteAddress, msg);
                        if (producerFuture.completeExceptionally(new ServerMetadataException(msg))) {
                            commandSender.sendErrorResponse(requestId, ServerError.MetadataError, msg);
                        }
                        producers.remove(producerId, producerFuture);
                        return;
                    }

                    disableTcpNoDelayIfNeeded(topicName.toString(), producerName);

                    CompletableFuture<SchemaVersion> schemaVersionFuture = tryAddSchema(topic, schema);

                    schemaVersionFuture.exceptionally(exception -> {
                        if (producerFuture.completeExceptionally(exception)) {
                            String message = exception.getMessage();
                            if (exception.getCause() != null) {
                                message += (" caused by " + exception.getCause());
                            }
                            commandSender.sendErrorResponse(requestId,
                                    BrokerServiceException.getClientErrorCode(exception),
                                    message);
                        }
                        var cause = FutureUtil.unwrapCompletionException(exception);
                        if (!(cause instanceof IncompatibleSchemaException)) {
                            log.error("Try add schema failed, remote address {}, topic {}, producerId {}",
                                    remoteAddress,
                                    topicName, producerId, exception);
                        }
                        producers.remove(producerId, producerFuture);
                        return null;
                    });

                    schemaVersionFuture.thenAccept(schemaVersion -> {
                        topic.checkIfTransactionBufferRecoverCompletely(isTxnEnabled).thenAccept(future -> {
                            CompletionStage<Subscription> createInitSubFuture;
                            if (!Strings.isNullOrEmpty(initialSubscriptionName)
                                    && topic.isPersistent()
                                    && !topic.getSubscriptions().containsKey(initialSubscriptionName)) {
                                createInitSubFuture = service.isAllowAutoSubscriptionCreationAsync(topicName)
                                        .thenCompose(isAllowAutoSubscriptionCreation -> {
                                            if (!isAllowAutoSubscriptionCreation) {
                                                return CompletableFuture.failedFuture(
                                                        new BrokerServiceException.NotAllowedException(
                                                        "Could not create the initial subscription due to"
                                                            + " the auto subscription creation is not allowed."));
                                            }
                                            return topic.createSubscription(initialSubscriptionName,
                                                    InitialPosition.Earliest, false, null);
                                        });
                            } else {
                                createInitSubFuture = CompletableFuture.completedFuture(null);
                            }

                            createInitSubFuture.whenComplete((sub, ex) -> {
                                if (ex != null) {
                                    final Throwable rc = FutureUtil.unwrapCompletionException(ex);
                                    if (rc instanceof BrokerServiceException.NotAllowedException) {
                                        log.warn("[{}] {} initialSubscriptionName: {}, topic: {}",
                                                remoteAddress, rc.getMessage(), initialSubscriptionName, topicName);
                                        if (producerFuture.completeExceptionally(rc)) {
                                            commandSender.sendErrorResponse(requestId,
                                                    ServerError.NotAllowedError, rc.getMessage());
                                        }
                                        producers.remove(producerId, producerFuture);
                                        return;
                                    }
                                    String msg =
                                            "Failed to create the initial subscription: " + ex.getCause().getMessage();
                                    log.warn("[{}] {} initialSubscriptionName: {}, topic: {}",
                                            remoteAddress, msg, initialSubscriptionName, topicName);
                                    if (producerFuture.completeExceptionally(ex)) {
                                        commandSender.sendErrorResponse(requestId,
                                                BrokerServiceException.getClientErrorCode(ex), msg);
                                    }
                                    producers.remove(producerId, producerFuture);
                                    return;
                                }

                                buildProducerAndAddTopic(topic, producerId, producerName, requestId, isEncrypted,
                                    metadata, schemaVersion, epoch, userProvidedProducerName, topicName,
                                    producerAccessMode, topicEpoch, supportsPartialProducer, producerFuture);
                            });
                        }).exceptionally(exception -> {
                            Throwable cause = exception.getCause();
                            log.error("producerId {}, requestId {} : TransactionBuffer recover failed",
                                    producerId, requestId, exception);
                            if (producerFuture.completeExceptionally(exception)) {
                                commandSender.sendErrorResponse(requestId,
                                        ServiceUnitNotReadyException.getClientErrorCode(cause),
                                        cause.getMessage());
                            }
                            producers.remove(producerId, producerFuture);
                            return null;
                        });
                    });
                });
                return backlogQuotaCheckFuture;
            }).exceptionally(exception -> {
                Throwable cause = exception.getCause();
                if (cause instanceof BrokerServiceException.TopicBacklogQuotaExceededException) {
                    BrokerServiceException.TopicBacklogQuotaExceededException tbqe =
                            (BrokerServiceException.TopicBacklogQuotaExceededException) cause;
                    IllegalStateException illegalStateException = new IllegalStateException(tbqe);
                    BacklogQuota.RetentionPolicy retentionPolicy = tbqe.getRetentionPolicy();
                    if (producerFuture.completeExceptionally(illegalStateException)) {
                        if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_request_hold) {
                            commandSender.sendErrorResponse(requestId,
                                    ServerError.ProducerBlockedQuotaExceededError,
                                    illegalStateException.getMessage());
                        } else if (retentionPolicy == BacklogQuota.RetentionPolicy.producer_exception) {
                            commandSender.sendErrorResponse(requestId,
                                    ServerError.ProducerBlockedQuotaExceededException,
                                    illegalStateException.getMessage());
                        }
                    }
                    producers.remove(producerId, producerFuture);
                    return null;
                }

                // Do not print stack traces for expected exceptions
                if (cause instanceof NoSuchElementException) {
                    cause = new TopicNotFoundException(String.format("Topic not found %s", topicName.toString()));
                    log.warn("[{}] Failed to load topic {}, producerId={}: Topic not found", remoteAddress, topicName,
                            producerId);
                } else if (!Exceptions.areExceptionsPresentInChain(cause,
                        ServiceUnitNotReadyException.class, ManagedLedgerException.class,
                        BrokerServiceException.ProducerBusyException.class)) {
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
                             Optional<Long> topicEpoch, boolean supportsPartialProducer,
                             CompletableFuture<Producer> producerFuture){
        CompletableFuture<Void> producerQueuedFuture = new CompletableFuture<>();
        Producer producer = new Producer(topic, ServerCnx.this, producerId, producerName,
                getPrincipal(), isEncrypted, metadata, schemaVersion, epoch,
                userProvidedProducerName, producerAccessMode, topicEpoch, supportsPartialProducer);

        topic.addProducer(producer, producerQueuedFuture).thenAccept(newTopicEpoch -> {
            if (isActive()) {
                if (producerFuture.complete(producer)) {
                    log.info("[{}] Created new producer: {}", remoteAddress, producer);
                    commandSender.sendProducerSuccessResponse(requestId, producerName,
                            producer.getLastSequenceId(), producer.getSchemaVersion(),
                            newTopicEpoch, true /* producer is ready now */);
                    if (brokerInterceptor != null) {
                        try {
                            brokerInterceptor.producerCreated(this, producer, metadata);
                        } catch (Throwable t) {
                            log.error("Exception occur when intercept producer created.", t);
                        }
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
        }).exceptionallyAsync(ex -> {
            if (ex.getCause() instanceof BrokerServiceException.TopicMigratedException) {
                Optional<ClusterUrl> clusterURL = getMigratedClusterUrl(service.getPulsar());
                if (clusterURL.isPresent()) {
                    if (topic.isReplicationBacklogExist()) {
                        log.info("Topic {} is migrated but replication backlog exist: "
                                        + "producerId = {}, producerName = {}, {}", topicName,
                                producerId, producerName, ex.getCause().getMessage());
                    } else {
                        log.info("[{}] redirect migrated producer to topic {}: "
                                        + "producerId={}, producerName = {}, {}", remoteAddress,
                                topicName, producerId, producerName, ex.getCause().getMessage());
                        boolean msgSent = commandSender.sendTopicMigrated(ResourceType.Producer, producerId,
                                clusterURL.get().getBrokerServiceUrl(), clusterURL.get().getBrokerServiceUrlTls());
                        if (!msgSent) {
                            log.info("client doesn't support topic migration handling {}-{}-{}", topic,
                                    remoteAddress, producerId);
                        }
                        closeProducer(producer);
                        return null;
                    }
                } else {
                    log.warn("[{}] failed producer because migration url not configured topic {}: producerId={}, {}",
                            remoteAddress, topicName, producerId, ex.getCause().getMessage());
                }
            } else if (ex.getCause() instanceof BrokerServiceException.ProducerFencedException) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Failed to add producer to topic {}: producerId={}, {}",
                            remoteAddress, topicName, producerId, ex.getCause().getMessage());
                }
            } else {
                log.warn("[{}] Failed to add producer to topic {}: producerId={}, {}",
                        remoteAddress, topicName, producerId, ex.getCause().getMessage());
            }

            producer.closeNow(true);
            if (producerFuture.completeExceptionally(ex)) {
                commandSender.sendErrorResponse(requestId,
                        BrokerServiceException.getClientErrorCode(ex), ex.getMessage());
            }
            return null;
        }, ctx.executor());

        producerQueuedFuture.thenRun(() -> {
            // If the producer is queued waiting, we will get an immediate notification
            // that we need to pass to client
            if (isActive()) {
                log.info("[{}] Producer is waiting in queue: {}", remoteAddress, producer);
                commandSender.sendProducerSuccessResponse(requestId, producerName,
                        producer.getLastSequenceId(), producer.getSchemaVersion(),
                        Optional.empty(), false/* producer is not ready now */);
                if (brokerInterceptor != null) {
                    brokerInterceptor.
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
            if (recentlyClosedProducers.containsKey(send.getProducerId())) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Received message, but the producer was recently closed : {}. Ignoring message.",
                            remoteAddress, send.getProducerId());
                }
                // We expect these messages because we recently closed the producer. Do not close the connection.
                return;
            }
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
                service.getTopicOrderedExecutor().executeOrdered(producer.getTopic().getName(), () -> {
                    commandSender.sendSendReceiptResponse(producerId, sequenceId, highestSequenceId, -1, -1);
                });
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

        // This position is only used for shadow replicator
        Position position = send.hasMessageId()
                ? PositionImpl.get(send.getMessageId().getLedgerId(), send.getMessageId().getEntryId()) : null;

        // Persist the message
        if (send.hasHighestSequenceId() && send.getSequenceId() <= send.getHighestSequenceId()) {
            producer.publishMessage(send.getProducerId(), send.getSequenceId(), send.getHighestSequenceId(),
                    headersAndPayload, send.getNumMessages(), send.isIsChunk(), send.isMarker(), position);
        } else {
            producer.publishMessage(send.getProducerId(), send.getSequenceId(), headersAndPayload,
                    send.getNumMessages(), send.isIsChunk(), send.isMarker(), position);
        }
    }

    private void printSendCommandDebug(CommandSend send, ByteBuf headersAndPayload) {
        headersAndPayload.markReaderIndex();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        headersAndPayload.resetReaderIndex();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received send message request. producer: {}:{} {}:{} size: {},"
                            + " partition key is: {}, ordering key is {}, uncompressedSize is {}",
                    remoteAddress, send.getProducerId(), send.getSequenceId(), msgMetadata.getProducerName(),
                    msgMetadata.getSequenceId(), headersAndPayload.readableBytes(),
                    msgMetadata.hasPartitionKey() ? msgMetadata.getPartitionKey() : null,
                    msgMetadata.hasOrderingKey() ? msgMetadata.getOrderingKey() : null,
                    msgMetadata.getUncompressedSize());
        }
    }

    @Override
    protected void handleAck(CommandAck ack) {
        checkArgument(state == State.Connected);
        CompletableFuture<Consumer> consumerFuture = consumers.get(ack.getConsumerId());
        final boolean hasRequestId = ack.hasRequestId();
        final long requestId = hasRequestId ? ack.getRequestId() : 0;
        final long consumerId = ack.getConsumerId();
        // It is necessary to make a copy of the CommandAck instance for the interceptor.
        final CommandAck copyOfAckForInterceptor = brokerInterceptor != null ? new CommandAck().copyFrom(ack) : null;

        if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
            Consumer consumer = consumerFuture.getNow(null);
            consumer.messageAcked(ack).thenRun(() -> {
                if (hasRequestId) {
                    writeAndFlush(Commands.newAckResponse(
                            requestId, null, null, consumerId));
                }
                if (brokerInterceptor != null) {
                    try {
                        brokerInterceptor.messageAcked(this, consumer, copyOfAckForInterceptor);
                    } catch (Throwable t) {
                        log.error("Exception occur when intercept message acked.", t);
                    }
                }
            }).exceptionally(e -> {
                if (hasRequestId) {
                    writeAndFlush(Commands.newAckResponse(requestId,
                            BrokerServiceException.getClientErrorCode(e),
                            e.getMessage(), consumerId));
                }
                return null;
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Consumer future is not complete(not complete or error), but received command ack. so discard"
                                + " this command. consumerId: {}, cnx: {}, messageIdCount: {}", ack.getConsumerId(),
                        this.toString(), ack.getMessageIdsCount());
            }
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
            log.debug("[{}] redeliverUnacknowledged from consumer {}, consumerEpoch {}",
                    remoteAddress, redeliver.getConsumerId(),
                    redeliver.hasConsumerEpoch() ? redeliver.getConsumerEpoch() : null);
        }

        CompletableFuture<Consumer> consumerFuture = consumers.get(redeliver.getConsumerId());

        if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
            Consumer consumer = consumerFuture.getNow(null);
            if (redeliver.getMessageIdsCount() > 0 && Subscription.isIndividualAckMode(consumer.subType())) {
                consumer.redeliverUnacknowledgedMessages(redeliver.getMessageIdsList());
            } else {
                if (redeliver.hasConsumerEpoch()) {
                    consumer.redeliverUnacknowledgedMessages(redeliver.getConsumerEpoch());
                } else {
                    consumer.redeliverUnacknowledgedMessages(DEFAULT_CONSUMER_EPOCH);
                }
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
            writeAndFlush(Commands.newSuccess(requestId));
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
            if (brokerInterceptor != null) {
                brokerInterceptor.producerClosed(this, producer, producer.getMetadata());
            }
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
            writeAndFlush(Commands.newSuccess(requestId));
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
            if (brokerInterceptor != null) {
                brokerInterceptor.consumerClosed(this, consumer, consumer.getMetadata());
            }
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
            topic.checkIfTransactionBufferRecoverCompletely(true)
                 .thenCompose(__ -> topic.getLastDispatchablePosition())
                 .thenApply(lastPosition -> {
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
                             consumer.getSubscription().getName(),
                             consumer.readCompacted());
                    return null;
                 }).exceptionally(e -> {
                     writeAndFlush(Commands.newError(getLastMessageId.getRequestId(),
                             ServerError.UnknownError, "Failed to recover Transaction Buffer."));
                     return null;
                 });
        } else {
            writeAndFlush(Commands.newError(getLastMessageId.getRequestId(),
                    ServerError.MetadataError, "Consumer not found"));
        }
    }

    private void getLargestBatchIndexWhenPossible(
            Topic topic,
            PositionImpl lastPosition,
            PositionImpl markDeletePosition,
            int partitionIndex,
            long requestId,
            String subscriptionName,
            boolean readCompacted) {
        PersistentTopic persistentTopic = (PersistentTopic) topic;
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();

        // If it's not pointing to a valid entry, respond messageId of the current position.
        // If the compaction cursor reach the end of the topic, respond messageId from compacted ledger
        Optional<Position> compactionHorizon = readCompacted
                ? persistentTopic.getCompactedTopic().getCompactionHorizon() : Optional.empty();
        if (lastPosition.getEntryId() == -1 || !ml.ledgerExists(lastPosition.getLedgerId())) {
            // there is no entry in the original topic
            if (compactionHorizon != null && compactionHorizon.isPresent()) {
                // if readCompacted is true, we need to read the last entry from compacted topic
                handleLastMessageIdFromCompactedLedger(persistentTopic, requestId, partitionIndex,
                        markDeletePosition);
                return;
            } else {
                // if readCompacted is false, we need to return MessageId.earliest
                writeAndFlush(Commands.newGetLastMessageIdResponse(requestId, -1, -1, partitionIndex, -1,
                        markDeletePosition != null ? markDeletePosition.getLedgerId() : -1,
                        markDeletePosition != null ? markDeletePosition.getEntryId() : -1));
            }
            return;
        }

        if (compactionHorizon != null && compactionHorizon.isPresent()
                        && lastPosition.compareTo((PositionImpl) compactionHorizon.get()) <= 0) {
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
                if (e.getCause() instanceof ManagedLedgerException.NonRecoverableLedgerException
                        && readCompacted) {
                    handleLastMessageIdFromCompactedLedger(persistentTopic, requestId, partitionIndex,
                            markDeletePosition);
                } else {
                    writeAndFlush(Commands.newError(
                            requestId, ServerError.MetadataError,
                            "Failed to get batch size for entry " + e.getMessage()));
                }
            } else {
                int largestBatchIndex = batchSize > 0 ? batchSize - 1 : -1;

                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}][{}] Get LastMessageId {} partitionIndex {}", ServerCnx.this.toString(),
                            topic.getName(), subscriptionName, lastPosition, partitionIndex);
                }

                writeAndFlush(Commands.newGetLastMessageIdResponse(requestId, lastPosition.getLedgerId(),
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
                try {
                    // in this case, all the data has been compacted, so return the last position
                    // in the compacted ledger to the client
                    ByteBuf payload = entry.getDataBuffer();
                    MessageMetadata metadata = Commands.parseMessageMetadata(payload);
                    int largestBatchIndex;
                    try {
                        largestBatchIndex = calculateTheLastBatchIndexInBatch(metadata, payload);
                    } catch (IOException ioEx) {
                        writeAndFlush(Commands.newError(requestId, ServerError.MetadataError,
                                "Failed to deserialize batched message from the last entry of the compacted Ledger: "
                                        + ioEx.getMessage()));
                        return;
                    }
                    writeAndFlush(Commands.newGetLastMessageIdResponse(requestId,
                            entry.getLedgerId(), entry.getEntryId(), partitionIndex, largestBatchIndex,
                            markDeletePosition != null ? markDeletePosition.getLedgerId() : -1,
                            markDeletePosition != null ? markDeletePosition.getEntryId() : -1));
                } finally {
                    entry.release();
                }
            } else {
                // in this case, the ledgers been removed except the current ledger
                // and current ledger without any data
                writeAndFlush(Commands.newGetLastMessageIdResponse(requestId,
                        -1, -1, partitionIndex, -1,
                        markDeletePosition != null ? markDeletePosition.getLedgerId() : -1,
                        markDeletePosition != null ? markDeletePosition.getEntryId() : -1));
            }
        }).exceptionally(ex -> {
            writeAndFlush(Commands.newError(
                    requestId, ServerError.MetadataError,
                    "Failed to read last entry of the compacted Ledger "
                            + ex.getCause().getMessage()));
            return null;
        });
    }

    private int calculateTheLastBatchIndexInBatch(MessageMetadata metadata, ByteBuf payload) throws IOException {
        int batchSize = metadata.getNumMessagesInBatch();
        if (batchSize <= 1){
            return -1;
        }
        if (metadata.hasCompression()) {
            var tmp = payload;
            CompressionType compressionType = metadata.getCompression();
            CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
            int uncompressedSize = metadata.getUncompressedSize();
            payload = codec.decode(payload, uncompressedSize);
            tmp.release();
        }
        SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
        int lastBatchIndexInBatch = -1;
        for (int i = 0; i < batchSize; i++){
            ByteBuf singleMessagePayload =
                    Commands.deSerializeSingleMessageInBatch(payload, singleMessageMetadata, i, batchSize);
            singleMessagePayload.release();
            if (singleMessageMetadata.isCompactedOut()){
                continue;
            }
            lastBatchIndexInBatch = i;
        }
        return lastBatchIndexInBatch;
    }

    private CompletableFuture<Boolean> isNamespaceOperationAllowed(NamespaceName namespaceName,
                                                                   NamespaceOperation operation) {
        if (!service.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> isProxyAuthorizedFuture;
        if (originalPrincipal != null) {
            isProxyAuthorizedFuture = service.getAuthorizationService().allowNamespaceOperationAsync(
                    namespaceName, operation, originalPrincipal, originalAuthData);
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
        checkArgument(state == State.Connected);
        final long requestId = commandGetTopicsOfNamespace.getRequestId();
        final String namespace = commandGetTopicsOfNamespace.getNamespace();
        final CommandGetTopicsOfNamespace.Mode mode = commandGetTopicsOfNamespace.getMode();
        final Optional<String> topicsPattern = Optional.ofNullable(commandGetTopicsOfNamespace.hasTopicsPattern()
                ? commandGetTopicsOfNamespace.getTopicsPattern() : null);
        final Optional<String> topicsHash = Optional.ofNullable(commandGetTopicsOfNamespace.hasTopicsHash()
                ? commandGetTopicsOfNamespace.getTopicsHash() : null);
        final NamespaceName namespaceName = NamespaceName.get(namespace);

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            isNamespaceOperationAllowed(namespaceName, NamespaceOperation.GET_TOPICS).thenApply(isAuthorized -> {
                if (isAuthorized) {
                    getBrokerService().pulsar().getNamespaceService().getListOfUserTopics(namespaceName, mode)
                        .thenAccept(topics -> {
                            boolean filterTopics = false;
                            // filter system topic
                            List<String> filteredTopics = topics;

                            if (enableSubscriptionPatternEvaluation && topicsPattern.isPresent()) {
                                if (topicsPattern.get().length() <= maxSubscriptionPatternLength) {
                                    filterTopics = true;
                                    filteredTopics = TopicList.filterTopics(filteredTopics, topicsPattern.get());
                                } else {
                                    log.info("[{}] Subscription pattern provided [{}] was longer than maximum {}.",
                                            remoteAddress, topicsPattern.get(), maxSubscriptionPatternLength);
                                }
                            }
                            String hash = TopicList.calculateHash(filteredTopics);
                            boolean hashUnchanged = topicsHash.isPresent() && topicsHash.get().equals(hash);
                            if (hashUnchanged) {
                                filteredTopics = Collections.emptyList();
                            }
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "[{}] Received CommandGetTopicsOfNamespace for namespace [//{}] by {}, size:{}",
                                        remoteAddress, namespace, requestId, topics.size());
                            }
                            commandSender.sendGetTopicsOfNamespaceResponse(filteredTopics, hash, filterTopics,
                                    !hashUnchanged, requestId);
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
                    final String msg = "Client is not authorized to GetTopicsOfNamespace";
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
        checkArgument(state == State.Connected);
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
            if (commandGetSchema.getSchemaVersion().length == 0) {
                commandSender.sendGetSchemaErrorResponse(requestId, ServerError.IncompatibleSchema,
                        "Empty schema version");
                return;
            }
            schemaVersion = schemaService.versionFromBytes(commandGetSchema.getSchemaVersion());
        }

        final String topic = commandGetSchema.getTopic();
        String schemaName;
        try {
            schemaName = TopicName.get(topic).getSchemaName();
        } catch (Throwable t) {
            commandSender.sendGetSchemaErrorResponse(requestId, ServerError.InvalidTopicName, t.getMessage());
            return;
        }

        schemaService.getSchema(schemaName, schemaVersion).thenAccept(schemaAndMetadata -> {
            if (schemaAndMetadata == null) {
                commandSender.sendGetSchemaErrorResponse(requestId, ServerError.TopicNotFound,
                        String.format("Topic not found or no-schema %s", topic));
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
        checkArgument(state == State.Connected);
        if (log.isDebugEnabled()) {
            log.debug("Received CommandGetOrCreateSchema call from {}", remoteAddress);
        }
        long requestId = commandGetOrCreateSchema.getRequestId();
        final String topicName = commandGetOrCreateSchema.getTopic();
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
                        String.format("Topic not found %s", topicName));
            }
        }).exceptionally(ex -> {
            ServerError errorCode = BrokerServiceException.getClientErrorCode(ex);
            commandSender.sendGetOrCreateSchemaErrorResponse(requestId, errorCode, ex.getMessage());
            return null;
        });
    }

    @Override
    protected void handleTcClientConnectRequest(CommandTcClientConnectRequest command) {
        checkArgument(state == State.Connected);
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
                            "Transactions are not enabled.");
            commandSender.sendErrorResponse(requestId, BrokerServiceException.getClientErrorCode(ex), ex.getMessage());
            return false;
        } else {
            return true;
        }
    }
    private Throwable handleTxnException(Throwable ex, String op, long requestId) {
        Throwable cause = FutureUtil.unwrapCompletionException(ex);
        if (cause instanceof CoordinatorException.CoordinatorNotFoundException) {
            if (log.isDebugEnabled()) {
                log.debug("The Coordinator was not found for the request {}", op);
            }
            return cause;
        }
        if (cause instanceof ManagedLedgerException.ManagedLedgerFencedException) {
            if (log.isDebugEnabled()) {
                log.debug("Throw a CoordinatorNotFoundException to client "
                        + "with the message got from a ManagedLedgerFencedException for the request {}", op);
            }
            return new CoordinatorException.CoordinatorNotFoundException(cause.getMessage());

        }
        log.error("Send response error for {} request {}.", op, requestId, cause);
        return cause;
    }
    @Override
    protected void handleNewTxn(CommandNewTxn command) {
        checkArgument(state == State.Connected);
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
        final String owner = getPrincipal();
        transactionMetadataStoreService.newTransaction(tcId, command.getTxnTtlSeconds(), owner)
            .whenComplete(((txnID, ex) -> {
                if (ex == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Send response {} for new txn request {}", tcId.getId(), requestId);
                    }
                    commandSender.sendNewTxnResponse(requestId, txnID, tcId.getId());
                } else {
                    if (ex instanceof CoordinatorException.ReachMaxActiveTxnException) {
                        // if new txn throw ReachMaxActiveTxnException, don't return any response to client,
                        // otherwise client will retry, it will wast o lot of resources
                        // link https://github.com/apache/pulsar/issues/15133
                        log.warn("New txn op reach max active transactions! tcId : {}, requestId : {}",
                                tcId.getId(), requestId, ex);
                        // do-nothing
                    } else {
                        ex = handleTxnException(ex, BaseCommand.Type.NEW_TXN.name(), requestId);

                        commandSender.sendNewTxnErrorResponse(requestId, tcId.getId(),
                                BrokerServiceException.getClientErrorCode(ex), ex.getMessage());
                        transactionMetadataStoreService.handleOpFail(ex, tcId);
                    }
                }
            }));
    }

    @Override
    protected void handleAddPartitionToTxn(CommandAddPartitionToTxn command) {
        checkArgument(state == State.Connected);
        final TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        final TransactionCoordinatorID tcId = TransactionCoordinatorID.get(command.getTxnidMostBits());
        final long requestId = command.getRequestId();
        final List<String> partitionsList = command.getPartitionsList();
        if (log.isDebugEnabled()) {
            partitionsList.forEach(partition ->
                    log.debug("Receive add published partition to txn request {} "
                            + "from {} with txnId {}, topic: [{}]", requestId, remoteAddress, txnID, partition));
        }

        if (!checkTransactionEnableAndSendError(requestId)) {
            return;
        }

        TransactionMetadataStoreService transactionMetadataStoreService =
                service.pulsar().getTransactionMetadataStoreService();
        verifyTxnOwnership(txnID)
                .thenCompose(isOwner -> {
                    if (!isOwner) {
                        return failedFutureTxnNotOwned(txnID);
                    }
                    return transactionMetadataStoreService
                            .addProducedPartitionToTxn(txnID, partitionsList);
                })
                .whenComplete((v, ex) -> {
                    if (ex == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response success for add published partition to txn request {}", requestId);
                        }
                        writeAndFlush(Commands.newAddPartitionToTxnResponse(requestId,
                                txnID.getLeastSigBits(), txnID.getMostSigBits()));
                    } else {
                        ex = handleTxnException(ex, BaseCommand.Type.ADD_PARTITION_TO_TXN.name(), requestId);

                        writeAndFlush(Commands.newAddPartitionToTxnResponse(requestId,
                                txnID.getLeastSigBits(),
                                txnID.getMostSigBits(),
                                BrokerServiceException.getClientErrorCode(ex),
                                ex.getMessage()));
                        transactionMetadataStoreService.handleOpFail(ex, tcId);
                    }
                });
    }

    private CompletableFuture<Void> failedFutureTxnNotOwned(TxnID txnID) {
        String msg = String.format(
                "Client (%s) is neither the owner of the transaction %s nor a super user",
                getPrincipal(), txnID
        );
        log.warn("[{}] {}", remoteAddress, msg);
        return CompletableFuture.failedFuture(new CoordinatorException.TransactionNotFoundException(msg));
    }

    private CompletableFuture<Void> failedFutureTxnTcNotAllowed(TxnID txnID) {
        String msg = String.format(
                "TC client (%s) is not a super user, and is not allowed to operate on transaction %s",
                getPrincipal(), txnID
        );
        log.warn("[{}] {}", remoteAddress, msg);
        return CompletableFuture.failedFuture(new CoordinatorException.TransactionNotFoundException(msg));
    }

    @Override
    protected void handleEndTxn(CommandEndTxn command) {
        checkArgument(state == State.Connected);
        final long requestId = command.getRequestId();
        final int txnAction = command.getTxnAction().getValue();
        TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        final TransactionCoordinatorID tcId = TransactionCoordinatorID.get(command.getTxnidMostBits());

        if (!checkTransactionEnableAndSendError(requestId)) {
            return;
        }

        TransactionMetadataStoreService transactionMetadataStoreService =
                service.pulsar().getTransactionMetadataStoreService();

        verifyTxnOwnership(txnID)
                .thenCompose(isOwner -> {
                    if (!isOwner) {
                        return failedFutureTxnNotOwned(txnID);
                    }
                    return transactionMetadataStoreService.endTransaction(txnID, txnAction, false);
                })
                .whenComplete((v, ex) -> {
                    if (ex == null) {
                        commandSender.sendEndTxnResponse(requestId, txnID, txnAction);
                    } else {
                        ex = handleTxnException(ex, BaseCommand.Type.END_TXN.name(), requestId);
                        commandSender.sendEndTxnErrorResponse(requestId, txnID,
                                BrokerServiceException.getClientErrorCode(ex), ex.getMessage());

                        transactionMetadataStoreService.handleOpFail(ex, tcId);
                    }
                });
    }

    private CompletableFuture<Boolean> isSuperUser() {
        assert ctx.executor().inEventLoop();
        if (service.isAuthenticationEnabled() && service.isAuthorizationEnabled()) {
            CompletableFuture<Boolean> isAuthRoleAuthorized = service.getAuthorizationService().isSuperUser(
                    authRole, authenticationData);
            if (originalPrincipal != null) {
                CompletableFuture<Boolean> isOriginalPrincipalAuthorized = service.getAuthorizationService()
                        .isSuperUser(originalPrincipal,
                                originalAuthData != null ? originalAuthData : authenticationData);
                return isOriginalPrincipalAuthorized.thenCombine(isAuthRoleAuthorized,
                        (originalPrincipal, authRole) -> originalPrincipal && authRole);
            } else {
                return isAuthRoleAuthorized;
            }
        } else {
            return CompletableFuture.completedFuture(true);
        }
    }

    private CompletableFuture<Boolean> verifyTxnOwnership(TxnID txnID) {
        assert ctx.executor().inEventLoop();
        return service.pulsar().getTransactionMetadataStoreService()
                .verifyTxnOwnership(txnID, getPrincipal())
                .thenComposeAsync(isOwner -> {
                    if (isOwner) {
                        return CompletableFuture.completedFuture(true);
                    }
                    if (service.isAuthenticationEnabled() && service.isAuthorizationEnabled()) {
                        return isSuperUser();
                    } else {
                        return CompletableFuture.completedFuture(false);
                    }
                }, ctx.executor());
    }

    @Override
    protected void handleEndTxnOnPartition(CommandEndTxnOnPartition command) {
        checkArgument(state == State.Connected);
        final long requestId = command.getRequestId();
        final String topic = command.getTopic();
        final int txnAction = command.getTxnAction().getValue();
        final TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        final long lowWaterMark = command.getTxnidLeastBitsOfLowWatermark();

        if (log.isDebugEnabled()) {
            log.debug("[{}] handleEndTxnOnPartition txnId: [{}], txnAction: [{}]", topic,
                    txnID, txnAction);
        }
        CompletableFuture<Optional<Topic>> topicFuture = service.getTopicIfExists(TopicName.get(topic).toString());
        topicFuture.thenAcceptAsync(optionalTopic -> {
            if (optionalTopic.isPresent()) {
                // we only accept superuser because this endpoint is reserved for tc to broker communication
                isSuperUser()
                        .thenCompose(isOwner -> {
                            if (!isOwner) {
                                return failedFutureTxnTcNotAllowed(txnID);
                            }
                            return optionalTopic.get().endTxn(txnID, txnAction, lowWaterMark);
                        })
                        .whenComplete((ignored, throwable) -> {
                            if (throwable != null) {
                                throwable = FutureUtil.unwrapCompletionException(throwable);
                                log.error("handleEndTxnOnPartition fail!, topic {}, txnId: [{}], "
                                        + "txnAction: [{}]", topic, txnID, TxnAction.valueOf(txnAction), throwable);
                                writeAndFlush(Commands.newEndTxnOnPartitionResponse(
                                        requestId, BrokerServiceException.getClientErrorCode(throwable),
                                        throwable.getMessage(),
                                        txnID.getLeastSigBits(), txnID.getMostSigBits()));
                                return;
                            }
                            writeAndFlush(Commands.newEndTxnOnPartitionResponse(requestId,
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
                                writeAndFlush(Commands.newEndTxnOnPartitionResponse(requestId,
                                        ServerError.ServiceNotReady,
                                        "The topic " + topic + " does not exist in broker.",
                                        txnID.getLeastSigBits(), txnID.getMostSigBits()));
                            } else {
                                log.warn("handleEndTxnOnPartition fail ! The topic {} has not been created, "
                                                + "txnId: [{}], txnAction: [{}]",
                                        topic, txnID, TxnAction.valueOf(txnAction));
                                writeAndFlush(Commands.newEndTxnOnPartitionResponse(requestId,
                                        txnID.getLeastSigBits(), txnID.getMostSigBits()));
                            }
                        }).exceptionally(e -> {
                            log.error("handleEndTxnOnPartition fail ! topic {}, "
                                            + "txnId: [{}], txnAction: [{}]", topic, txnID,
                                    TxnAction.valueOf(txnAction), e.getCause());
                            writeAndFlush(Commands.newEndTxnOnPartitionResponse(
                                    requestId, ServerError.ServiceNotReady,
                                    e.getMessage(), txnID.getLeastSigBits(), txnID.getMostSigBits()));
                            return null;
                });
            }
        }, ctx.executor()).exceptionally(e -> {
            log.error("handleEndTxnOnPartition fail ! topic {}, "
                            + "txnId: [{}], txnAction: [{}]", topic, txnID,
                    TxnAction.valueOf(txnAction), e.getCause());
            writeAndFlush(Commands.newEndTxnOnPartitionResponse(
                    requestId, ServerError.ServiceNotReady,
                    e.getMessage(), txnID.getLeastSigBits(), txnID.getMostSigBits()));
            return null;
        });
    }

    @Override
    protected void handleEndTxnOnSubscription(CommandEndTxnOnSubscription command) {
        checkArgument(state == State.Connected);
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
        topicFuture.thenAcceptAsync(optionalTopic -> {
            if (optionalTopic.isPresent()) {
                Subscription subscription = optionalTopic.get().getSubscription(subName);
                if (subscription == null) {
                    log.warn("handleEndTxnOnSubscription fail! "
                                    + "topic {} subscription {} does not exist. txnId: [{}], txnAction: [{}]",
                            optionalTopic.get().getName(), subName, txnID, TxnAction.valueOf(txnAction));
                    writeAndFlush(
                            Commands.newEndTxnOnSubscriptionResponse(requestId, txnidLeastBits, txnidMostBits));
                    return;
                }
                // we only accept super user becase this endpoint is reserved for tc to broker communication
                isSuperUser()
                        .thenCompose(isOwner -> {
                            if (!isOwner) {
                                return failedFutureTxnTcNotAllowed(txnID);
                            }
                            return subscription.endTxn(txnidMostBits, txnidLeastBits, txnAction, lowWaterMark);
                        }).whenComplete((ignored, e) -> {
                            if (e != null) {
                                e = FutureUtil.unwrapCompletionException(e);
                                log.error("handleEndTxnOnSubscription fail ! topic: {}, subscription: {}"
                                                + "txnId: [{}], txnAction: [{}]", topic, subName,
                                        txnID, TxnAction.valueOf(txnAction), e.getCause());
                                writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                                        requestId, txnidLeastBits, txnidMostBits,
                                        BrokerServiceException.getClientErrorCode(e),
                                        "Handle end txn on subscription failed: " + e.getMessage()));
                                return;
                            }
                            writeAndFlush(
                                    Commands.newEndTxnOnSubscriptionResponse(requestId, txnidLeastBits, txnidMostBits));
                        });
            } else {
                getBrokerService().getManagedLedgerFactory()
                        .asyncExists(TopicName.get(topic).getPersistenceNamingEncoding())
                        .thenAccept((b) -> {
                            if (b) {
                                log.error("handleEndTxnOnSubscription fail! The topic {} does not exist in broker, "
                                                + "subscription: {}, txnId: [{}], txnAction: [{}]", topic, subName,
                                        txnID, TxnAction.valueOf(txnAction));
                                writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                                        requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(),
                                        ServerError.ServiceNotReady,
                                        "The topic " + topic + " does not exist in broker."));
                            } else {
                                log.warn("handleEndTxnOnSubscription fail ! The topic {} has not been created, "
                                                + "subscription: {} txnId: [{}], txnAction: [{}]",
                                        topic, subName, txnID, TxnAction.valueOf(txnAction));
                                writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(requestId,
                                        txnID.getLeastSigBits(), txnID.getMostSigBits()));
                            }
                        }).exceptionally(e -> {
                            log.error("handleEndTxnOnSubscription fail ! topic {}, subscription: {}"
                                            + "txnId: [{}], txnAction: [{}]", topic, subName,
                                    txnID, TxnAction.valueOf(txnAction), e.getCause());
                            writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                                    requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(),
                                    ServerError.ServiceNotReady, e.getMessage()));
                            return null;
                });
            }
        }, ctx.executor()).exceptionally(e -> {
            log.error("handleEndTxnOnSubscription fail ! topic: {}, subscription: {}"
                            + "txnId: [{}], txnAction: [{}]", topic, subName,
                    txnID, TxnAction.valueOf(txnAction), e.getCause());
            writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                    requestId, txnidLeastBits, txnidMostBits,
                    ServerError.ServiceNotReady,
                    "Handle end txn on subscription failed: " + e.getMessage()));
            return null;
        });
    }

    private CompletableFuture<SchemaVersion> tryAddSchema(Topic topic, SchemaData schema) {
        if (schema != null) {
            return topic.addSchema(schema);
        } else {
            return topic.hasSchema().thenCompose((hasSchema) -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] {} configured with schema {}", remoteAddress, topic.getName(), hasSchema);
                }
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
        checkArgument(state == State.Connected);
        final TxnID txnID = new TxnID(command.getTxnidMostBits(), command.getTxnidLeastBits());
        final long requestId = command.getRequestId();
        final List<org.apache.pulsar.common.api.proto.Subscription> subscriptionsList = new ArrayList<>();
        for (org.apache.pulsar.common.api.proto.Subscription sub : command.getSubscriptionsList()) {
            subscriptionsList.add(new org.apache.pulsar.common.api.proto.Subscription().copyFrom(sub));
        }
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

        verifyTxnOwnership(txnID)
                .thenCompose(isOwner -> {
                    if (!isOwner) {
                        return failedFutureTxnNotOwned(txnID);
                    }
                    return transactionMetadataStoreService.addAckedPartitionToTxn(txnID,
                            MLTransactionMetadataStore.subscriptionToTxnSubscription(subscriptionsList));
                })
                .whenComplete((v, ex) -> {
                    if (ex == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Send response success for add published partition to txn request {}",
                                    requestId);
                        }
                        writeAndFlush(Commands.newAddSubscriptionToTxnResponse(requestId,
                                txnID.getLeastSigBits(), txnID.getMostSigBits()));
                    } else {
                        ex = handleTxnException(ex, BaseCommand.Type.ADD_SUBSCRIPTION_TO_TXN.name(), requestId);
                        writeAndFlush(
                                Commands.newAddSubscriptionToTxnResponse(requestId, txnID.getLeastSigBits(),
                                txnID.getMostSigBits(), BrokerServiceException.getClientErrorCode(ex),
                                ex.getMessage()));
                        transactionMetadataStoreService.handleOpFail(ex, tcId);
                    }
                });
    }

    @Override
    protected void handleCommandWatchTopicList(CommandWatchTopicList commandWatchTopicList) {
        checkArgument(state == State.Connected);
        final long requestId = commandWatchTopicList.getRequestId();
        final long watcherId = commandWatchTopicList.getWatcherId();
        final NamespaceName namespaceName = NamespaceName.get(commandWatchTopicList.getNamespace());

        Pattern topicsPattern = Pattern.compile(commandWatchTopicList.hasTopicsPattern()
                ? commandWatchTopicList.getTopicsPattern() : TopicList.ALL_TOPICS_PATTERN);
        String topicsHash = commandWatchTopicList.hasTopicsHash()
                ? commandWatchTopicList.getTopicsHash() : null;

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            isNamespaceOperationAllowed(namespaceName, NamespaceOperation.GET_TOPICS).thenApply(isAuthorized -> {
                if (isAuthorized) {
                    topicListService.handleWatchTopicList(namespaceName, watcherId, requestId, topicsPattern,
                            topicsHash, lookupSemaphore);
                } else {
                    final String msg = "Proxy Client is not authorized to watchTopicList";
                    log.warn("[{}] {} with role {} on namespace {}", remoteAddress, msg, getPrincipal(), namespaceName);
                    commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, msg);
                    lookupSemaphore.release();
                }
                return null;
            }).exceptionally(ex -> {
                logNamespaceNameAuthException(remoteAddress, "watchTopicList", getPrincipal(),
                        Optional.of(namespaceName), ex);
                final String msg = "Exception occurred while trying to handle command WatchTopicList";
                commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, msg);
                lookupSemaphore.release();
                return null;
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed WatchTopicList due to too many lookup-requests {}", remoteAddress,
                        namespaceName);
            }
            commandSender.sendErrorResponse(requestId, ServerError.TooManyRequests,
                    "Failed due to too many pending lookup requests");
        }
    }

    @Override
    protected void handleCommandWatchTopicListClose(CommandWatchTopicListClose commandWatchTopicListClose) {
        checkArgument(state == State.Connected);
        topicListService.handleWatchTopicListClose(commandWatchTopicListClose);
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
        if (brokerInterceptor != null) {
            brokerInterceptor.onPulsarCommand(command, this);
        }
    }

    @Override
    public void closeProducer(Producer producer) {
        assert ctx.executor().inEventLoop();
        // removes producer-connection from map and send close command to producer
        safelyRemoveProducer(producer);
        if (getRemoteEndpointProtocolVersion() >= v5.getValue()) {
            writeAndFlush(Commands.newCloseProducer(producer.getProducerId(), -1L));
            // The client does not necessarily know that the producer is closed, but the connection is still
            // active, and there could be messages in flight already. We want to ignore these messages for a time
            // because they are expected. Once the interval has passed, the client should have received the
            // CloseProducer command and should not send any additional messages until it sends a create Producer
            // command.
            final long epoch = producer.getEpoch();
            final long producerId = producer.getProducerId();
            recentlyClosedProducers.put(producerId, epoch);
            ctx.executor().schedule(() -> {
                recentlyClosedProducers.remove(producerId, epoch);
            }, service.getKeepAliveIntervalSeconds(), TimeUnit.SECONDS);
        } else {
            close();
        }

    }

    @Override
    public void closeConsumer(Consumer consumer) {
        // removes consumer-connection from map and send close command to consumer
        safelyRemoveConsumer(consumer);
        if (getRemoteEndpointProtocolVersion() >= v5.getValue()) {
            writeAndFlush(Commands.newCloseConsumer(consumer.consumerId(), -1L));
        } else {
            close();
        }
    }

    /**
     * It closes the connection with client which triggers {@code channelInactive()} which clears all producers and
     * consumers from connection-map.
     */
    protected void close() {
        if (ctx != null) {
            ctx.close();
        }
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
            disableCnxAutoRead();
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

    private void recordRateLimitMetrics(ConcurrentLongHashMap<CompletableFuture<Producer>> producers) {
        producers.forEach((key, producerFuture) -> {
            if (producerFuture != null && producerFuture.isDone()) {
                Producer p = producerFuture.getNow(null);
                if (p != null && p.getTopic() != null) {
                    p.getTopic().increasePublishLimitedTimes();
                }
            }
        });
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
            throttledConnections.dec();
        }
    }

    @Override
    public void disableCnxAutoRead() {
        if (ctx != null && ctx.channel().config().isAutoRead()) {
            ctx.channel().config().setAutoRead(false);
            recordRateLimitMetrics(producers);
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
        return getErrorCodeWithErrorLog(future, false, null);
    }

    private <T> ServerError getErrorCodeWithErrorLog(CompletableFuture<T> future, boolean logIfError,
                                                     String errorMessageIfLog) {
        ServerError error = ServerError.UnknownError;
        try {
            future.getNow(null);
        } catch (Exception e) {
            if (e.getCause() instanceof BrokerServiceException) {
                error = BrokerServiceException.getClientErrorCode(e.getCause());
            }
            if (logIfError){
                String finalErrorMessage = StringUtils.isNotBlank(errorMessageIfLog)
                        ? errorMessageIfLog : "Unknown Error";
                log.error(finalErrorMessage, e);
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
                        this.toString());
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
                writeAndFlush(Commands.newLookupErrorResponse(ServerError.InvalidTopicName,
                        "Invalid topic name: " + t.getMessage(), requestId));
            } else if (requestCommand instanceof CommandPartitionedTopicMetadata) {
                writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.InvalidTopicName,
                        "Invalid topic name: " + t.getMessage(), requestId));
            } else {
                writeAndFlush(Commands.newError(requestId, ServerError.InvalidTopicName,
                        "Invalid topic name: " + t.getMessage()));
            }

            return null;
        }
    }

    public ByteBufPair newMessageAndIntercept(long consumerId, long ledgerId, long entryId, int partition,
            int redeliveryCount, ByteBuf metadataAndPayload, long[] ackSet, String topic, long epoch) {
        BaseCommand command = Commands.newMessageCommand(consumerId, ledgerId, entryId, partition, redeliveryCount,
                ackSet, epoch);
        ByteBufPair res = Commands.serializeCommandMessageWithSize(command, metadataAndPayload);
        if (brokerInterceptor != null) {
            try {
                brokerInterceptor.onPulsarCommand(command, this);
                CompletableFuture<Consumer> consumerFuture = consumers.get(consumerId);
                if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
                    Consumer consumer = consumerFuture.getNow(null);
                    brokerInterceptor.messageDispatched(this, consumer, ledgerId, entryId, metadataAndPayload);
                }
            } catch (Exception e) {
                log.error("Exception occur when intercept messages.", e);
            }
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

    /**
     * Demo: [id: 0x2561bcd1, L:/10.0.136.103:6650 ! R:/240.240.0.5:58038] [SR:/240.240.0.5:58038].
     * L: local Address.
     * R: remote address.
     * SR: source remote address. It is the source address when enabled "haProxyProtocolEnabled".
     */
    @Override
    public String toString() {
        ChannelHandlerContext ctx = ctx();
        // ctx.channel(): 96.
        // clientSourceAddress: 5 + 46(ipv6).
        // state: 19.
        // Len = 166.
        StringBuilder buf = new StringBuilder(166);
        if (ctx == null) {
            buf.append("[ctx: null]");
        } else {
            buf.append(ctx.channel().toString());
        }
        String clientSourceAddr = clientSourceAddress();
        buf.append(" [SR:").append(clientSourceAddr == null ? "-" : clientSourceAddr)
                .append(", state:").append(state).append("]");
        return buf.toString();
    }

    @Override
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

    boolean supportsPartialProducer() {
        return features != null && features.isSupportsPartialProducer();
    }

    @Override
    public String getClientVersion() {
        return clientVersion;
    }

    @Override
    public String getProxyVersion() {
        return proxyVersion;
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
        AuthenticationDataSource authenticationDataSource = this.getAuthData();
        if (proxyMessage != null) {
            return proxyMessage.sourceAddress();
        } else if (remoteAddress instanceof InetSocketAddress) {
            InetSocketAddress inetAddress = (InetSocketAddress) remoteAddress;
            return inetAddress.getAddress().getHostAddress();
        } else {
            return null;
        }
    }

    CompletableFuture<Boolean> connectionCheckInProgress;

    @Override
    public CompletableFuture<Boolean> checkConnectionLiveness() {
        if (!isActive()) {
            return CompletableFuture.completedFuture(false);
        }
        if (connectionLivenessCheckTimeoutMillis > 0) {
            return NettyFutureUtil.toCompletableFuture(ctx.executor().submit(() -> {
                if (!isActive()) {
                    return CompletableFuture.completedFuture(false);
                }
                if (connectionCheckInProgress != null) {
                    return connectionCheckInProgress;
                } else {
                    final CompletableFuture<Boolean> finalConnectionCheckInProgress =
                            new CompletableFuture<>();
                    connectionCheckInProgress = finalConnectionCheckInProgress;
                    ctx.executor().schedule(() -> {
                        if (!isActive()) {
                            finalConnectionCheckInProgress.complete(false);
                            return;
                        }
                        if (finalConnectionCheckInProgress.isDone()) {
                            return;
                        }
                        if (finalConnectionCheckInProgress == connectionCheckInProgress) {
                            /**
                             * {@link #connectionCheckInProgress} will be completed when
                             * {@link #channelInactive(ChannelHandlerContext)} event occurs, so skip set it here.
                             */
                            log.warn("[{}] Connection check timed out. Closing connection.", this.toString());
                            ctx.close();
                        } else {
                            log.error("[{}] Reached unexpected code block. Completing connection check.",
                                    this.toString());
                            finalConnectionCheckInProgress.complete(true);
                        }
                    }, connectionLivenessCheckTimeoutMillis, TimeUnit.MILLISECONDS);
                    sendPing();
                    return finalConnectionCheckInProgress;
                }
            })).thenCompose(java.util.function.Function.identity());
        } else {
            // check is disabled
            return CompletableFuture.completedFuture((Boolean) null);
        }
    }

    @Override
    protected void messageReceived() {
        super.messageReceived();
        if (connectionCheckInProgress != null && !connectionCheckInProgress.isDone()) {
            connectionCheckInProgress.complete(true);
            connectionCheckInProgress = null;
        }
    }

    private static void logAuthException(SocketAddress remoteAddress, String operation,
                                         String principal, Optional<TopicName> topic, Throwable ex) {
        String topicString = topic.map(t -> ", topic=" + t.toString()).orElse("");
        Throwable actEx = FutureUtil.unwrapCompletionException(ex);
        if (actEx instanceof AuthenticationException) {
            log.info("[{}] Failed to authenticate: operation={}, principal={}{}, reason={}",
                    remoteAddress, operation, principal, topicString, actEx.getMessage());
            return;
        } else if (actEx instanceof WebApplicationException restException){
            // Do not print error log if users tries to access a not found resource.
            if (restException.getResponse().getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                log.info("[{}] Trying to authenticate for a topic which under a namespace not exists: operation={},"
                                + " principal={}{}, reason: {}",
                        remoteAddress, operation, principal, topicString, actEx.getMessage());
                return;
            }
        }
        log.error("[{}] Error trying to authenticate: operation={}, principal={}{}",
                remoteAddress, operation, principal, topicString, ex);
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

    @VisibleForTesting
    protected String getOriginalPrincipal() {
        return originalPrincipal;
    }

    @VisibleForTesting
    protected AuthenticationDataSource getAuthData() {
        return authenticationData;
    }

    @VisibleForTesting
    protected AuthenticationDataSource getOriginalAuthData() {
        return originalAuthData;
    }

    @VisibleForTesting
    protected AuthenticationState getOriginalAuthState() {
        return originalAuthState;
    }

    @VisibleForTesting
    protected void setAuthRole(String authRole) {
        this.authRole = authRole;
    }
}
