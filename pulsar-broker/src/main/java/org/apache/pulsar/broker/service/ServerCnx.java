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
import static org.apache.pulsar.broker.service.ServerCnxThrottleTracker.ThrottleType;
import static org.apache.pulsar.broker.service.persistent.PersistentTopic.getMigratedClusterUrl;
import static org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorage.ignoreUnrecoverableBKException;
import static org.apache.pulsar.common.api.proto.ProtocolVersion.v5;
import static org.apache.pulsar.common.naming.Constants.WEBSOCKET_DUMMY_ORIGINAL_PRINCIPLE;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.apache.pulsar.common.protocol.Commands.newLookupErrorResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.github.merlimat.slog.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import lombok.Getter;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.AckSetStateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.limiter.ConnectionController;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.namespace.LookupOptions;
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
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.broker.topiclistlimit.TopicListMemoryLimiter;
import org.apache.pulsar.broker.topiclistlimit.TopicListSizeResultCache;
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
import org.apache.pulsar.common.configuration.anonymizer.DefaultAuthenticationRoleLoggingAnonymizer;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.Metadata;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.ClusterPolicies.ClusterUrl;
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
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterImpl;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.StringInterner;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.common.util.netty.NettyChannelUtil;
import org.apache.pulsar.common.util.netty.NettyFutureUtil;
import org.apache.pulsar.functions.utils.Exceptions;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.apache.pulsar.utils.TimedSingleThreadRateLimiter;

/**
 * Channel handler for the Pulsar broker.
 * <p>
 * Please see {@link org.apache.pulsar.common.protocol.PulsarDecoder} javadoc for important details about handle* method
 * parameter instance lifecycle.
 */
public class ServerCnx extends PulsarHandler implements TransportCnx {

    private static final Logger LOG = Logger.get(ServerCnx.class);
    private Logger log = LOG;

    private static final Logger PAUSE_RECEIVING_LOG = Logger.get(ServerCnx.class.getName() + ".pauseReceiving");
    private final BrokerService service;
    private final SchemaRegistryService schemaService;
    private final String listenerName;
    private final Map<Long, Long> recentlyClosedProducers;
    private final ConcurrentLongHashMap<CompletableFuture<Producer>> producers;
    private final ConcurrentLongHashMap<CompletableFuture<Consumer>> consumers;
    private final boolean enableSubscriptionPatternEvaluation;
    private final boolean enableTopicListWatcher;
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
    private final DefaultAuthenticationRoleLoggingAnonymizer authenticationRoleLoggingAnonymizer;

    // Max number of pending requests per connections. If multiple producers are sharing the same connection the flow
    // control done by a single producer might not be enough to prevent write spikes on the broker.
    private final int maxPendingSendRequests;
    private final int resumeReadsThreshold;
    private int pendingSendRequest = 0;
    private final String replicatorPrefix;
    private String clientVersion = null;
    private String proxyVersion = null;
    private String clientSourceAddressAndPort;
    private int nonPersistentPendingMessages = 0;
    private final int maxNonPersistentPendingMessages;
    private String originalPrincipal = null;
    private final boolean schemaValidationEnforced;
    private String authMethod = "none";
    private final int maxMessageSize;
    private boolean preciseDispatcherFlowControl;

    private boolean encryptionRequireOnProducer;

    @Getter
    private FeatureFlags features;

    private PulsarCommandSender commandSender;
    private final ConnectionController connectionController;

    private static final KeySharedMeta emptyKeySharedMeta = new KeySharedMeta()
            .setKeySharedMode(KeySharedMode.AUTO_SPLIT);

    private final long maxPendingBytesPerThread;
    private final long resumeThresholdPendingBytesPerThread;

    private final long connectionLivenessCheckTimeoutMillis;
    private final TopicsPattern.RegexImplementation topicsPatternImplementation;
    private final boolean pauseReceivingRequestsIfUnwritable;
    private final TimedSingleThreadRateLimiter requestRateLimiter;
    private final int pauseReceivingCooldownMilliSeconds;
    private boolean pausedDueToRateLimitation = false;
    private AsyncDualMemoryLimiterImpl maxTopicListInFlightLimiter;

    // Tracks and limits number of bytes pending to be published from a single specific IO thread.
    static final class PendingBytesPerThreadTracker {
        private static final FastThreadLocal<PendingBytesPerThreadTracker> pendingBytesPerThread =
                new FastThreadLocal<>() {
                    @Override
                    protected PendingBytesPerThreadTracker initialValue() throws Exception {
                        return new PendingBytesPerThreadTracker();
                    }
                };

        private long pendingBytes;
        private boolean limitExceeded;

        public static PendingBytesPerThreadTracker getInstance() {
            return pendingBytesPerThread.get();
        }

        public void incrementPublishBytes(long bytes, long maxPendingBytesPerThread) {
            pendingBytes += bytes;
            // when the limit is exceeded we throttle all connections that are sharing the same thread
            if (maxPendingBytesPerThread > 0 && pendingBytes > maxPendingBytesPerThread
                    && !limitExceeded) {
                limitExceeded = true;
                cnxsPerThread.get().forEach(cnx -> cnx.throttleTracker.markThrottled(
                        ThrottleType.IOThreadMaxPendingPublishBytesExceeded));
            }
        }

        public void decrementPublishBytes(long bytes, long resumeThresholdPendingBytesPerThread) {
            pendingBytes -= bytes;
            // when the limit has been exceeded, and we are below the resume threshold
            // we resume all connections sharing the same thread
            if (limitExceeded && pendingBytes <= resumeThresholdPendingBytesPerThread) {
                limitExceeded = false;
                cnxsPerThread.get().forEach(cnx -> cnx.throttleTracker.unmarkThrottled(
                        ThrottleType.IOThreadMaxPendingPublishBytesExceeded));
            }
        }
    }


    // A set of connections tied to the current thread
    private static final FastThreadLocal<Set<ServerCnx>> cnxsPerThread = new FastThreadLocal<>() {
        @Override
        protected Set<ServerCnx> initialValue() throws Exception {
            return Collections.newSetFromMap(new IdentityHashMap<>());
        }
    };

    enum State {
        Start, Connected, Failed, Connecting
    }

    @Getter
    private final ServerCnxThrottleTracker throttleTracker;

    public ServerCnx(PulsarService pulsar) {
        this(pulsar, null);
    }

    public ServerCnx(PulsarService pulsar, String listenerName) {
        // pulsar.getBrokerService() can sometimes be null in unit tests when using mocks
        // the null check is a workaround for #13620
        super(pulsar.getBrokerService() != null ? pulsar.getBrokerService().getKeepAliveIntervalSeconds() : 0,
                TimeUnit.SECONDS);
        this.pauseReceivingRequestsIfUnwritable =
                pulsar.getConfig().isPulsarChannelPauseReceivingRequestsIfUnwritable();
        this.requestRateLimiter = new TimedSingleThreadRateLimiter(
                pulsar.getConfig().getPulsarChannelPauseReceivingCooldownRateLimitPermits(),
                pulsar.getConfig().getPulsarChannelPauseReceivingCooldownRateLimitPeriodMs(),
                TimeUnit.MILLISECONDS);
        this.pauseReceivingCooldownMilliSeconds =
                pulsar.getConfig().getPulsarChannelPauseReceivingCooldownMs();
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
        this.recentlyClosedProducers = new ConcurrentHashMap<>();
        this.replicatorPrefix = conf.getReplicatorPrefix();
        this.maxNonPersistentPendingMessages = conf.getMaxConcurrentNonPersistentMessagePerConnection();
        this.schemaValidationEnforced = conf.isSchemaValidationEnforced();
        this.maxMessageSize = conf.getMaxMessageSize();
        this.maxPendingSendRequests = conf.getMaxPendingPublishRequestsPerConnection();
        this.resumeReadsThreshold = maxPendingSendRequests / 2;
        this.preciseDispatcherFlowControl = conf.isPreciseDispatcherFlowControl();
        this.encryptionRequireOnProducer = conf.isEncryptionRequireOnProducer();
        // Assign a portion of max-pending bytes to each IO thread
        this.maxPendingBytesPerThread = conf.getMaxMessagePublishBufferSizeInMB() * 1024L * 1024L
                / conf.getNumIOThreads();
        this.resumeThresholdPendingBytesPerThread = this.maxPendingBytesPerThread / 2;
        this.connectionController = new ConnectionController.DefaultConnectionController(
                conf.getBrokerMaxConnections(),
                conf.getBrokerMaxConnectionsPerIp());
        this.maxTopicListInFlightLimiter = pulsar.getBrokerService().getMaxTopicListInFlightLimiter();
        this.enableSubscriptionPatternEvaluation = conf.isEnableBrokerSideSubscriptionPatternEvaluation();
        this.enableTopicListWatcher = conf.isEnableBrokerTopicListWatcher();
        this.maxSubscriptionPatternLength = conf.getSubscriptionPatternMaxLength();
        this.topicListService = new TopicListService(pulsar, this,
                enableSubscriptionPatternEvaluation, maxSubscriptionPatternLength);
        this.brokerInterceptor = this.service != null ? this.service.getInterceptor() : null;
        this.throttleTracker = new ServerCnxThrottleTracker(this);
        topicsPatternImplementation = conf.getTopicsPatternRegexImplementation();
        this.authenticationRoleLoggingAnonymizer = new DefaultAuthenticationRoleLoggingAnonymizer(
                conf.getAuthenticationRoleLoggingAnonymizer());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.log = LOG.with()
                .attr("localAddress", ctx.channel().localAddress())
                .attr("remoteAddress", ctx.channel().remoteAddress())
                .build();
        ConnectionController.State state = connectionController.increaseConnection(remoteAddress);
        if (!state.equals(ConnectionController.State.OK)) {
            final ByteBuf msg = Commands.newError(-1, ServerError.NotAllowedError,
                    state.equals(ConnectionController.State.REACH_MAX_CONNECTION)
                            ? "Reached the maximum number of connections"
                            : "Reached the maximum number of connections on address" + remoteAddress);
            NettyChannelUtil.writeAndFlushWithClosePromise(ctx, msg);
            return;
        }
        // Connection information is logged after a successful Connect command is processed.
        log.debug("New connection");
        this.ctx = ctx;
        this.commandSender =
                new PulsarCommandSenderImpl(brokerInterceptor, this, this.service.getMaxTopicListInFlightLimiter());
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
        log.info("Closed connection");
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
                    log.warn().attr("consumer", consumer).exceptionMessage(e)
                            .log("Consumer was already closed");
                }
            }
        });
        this.topicListService.inactivate();
        this.service.getPulsarStats().recordConnectionClose();

        // complete possible pending connection check future
        if (connectionCheckInProgress != null && !connectionCheckInProgress.isDone()) {
            connectionCheckInProgress.complete(Optional.of(false));
        }
    }

    private void checkPauseReceivingRequestsAfterResumeRateLimit(BaseCommand cmd) {
        if (!pauseReceivingRequestsIfUnwritable
                || pauseReceivingCooldownMilliSeconds <= 0 || cmd.getType() == BaseCommand.Type.PONG
                || cmd.getType() == BaseCommand.Type.PING) {
            return;
        }
        final ChannelOutboundBuffer outboundBuffer =
                ctx.channel().unsafe().outboundBuffer();
        if (outboundBuffer != null) {
            PAUSE_RECEIVING_LOG.debug()
                    .attr("type", cmd.getType())
                    .attr("totalPendingWriteBytes",
                            outboundBuffer.totalPendingWriteBytes())
                    .attr("isWritable", ctx.channel().isWritable())
                    .log("Start to handle request");
        } else {
            PAUSE_RECEIVING_LOG.debug()
                    .attr("type", cmd.getType())
                    .attr("isWritable", ctx.channel().isWritable())
                    .log("Start to handle request");
        }
        // "requestRateLimiter" will return the permits that you acquired if it is not opening(has been called
        // "timingOpen(duration)").
        if (requestRateLimiter.acquire(1) == 0 && !pausedDueToRateLimitation) {
            log.warn("Reached rate limitation");
            // Stop receiving requests.
            pausedDueToRateLimitation = true;
            getThrottleTracker().markThrottled(ThrottleType.ConnectionPauseReceivingCooldownRateLimit);
            // Resume after 1 second.
            ctx.channel().eventLoop().schedule(() -> {
                if (pausedDueToRateLimitation) {
                    log.info("Resuming connection after rate limitation");
                    getThrottleTracker().unmarkThrottled(ThrottleType.ConnectionPauseReceivingCooldownRateLimit);
                    pausedDueToRateLimitation = false;
                }
            }, requestRateLimiter.getPeriodAtMs(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (pauseReceivingRequestsIfUnwritable && ctx.channel().isWritable()) {
            log.info("Channel is writable, turning on auto-read");
            getThrottleTracker().unmarkThrottled(ThrottleType.ConnectionOutboundBufferFull);
            requestRateLimiter.timingOpen(pauseReceivingCooldownMilliSeconds, TimeUnit.MILLISECONDS);
        } else if (pauseReceivingRequestsIfUnwritable && !ctx.channel().isWritable()) {
            final ChannelOutboundBuffer outboundBuffer =
                    ctx.channel().unsafe().outboundBuffer();
            if (outboundBuffer != null) {
                PAUSE_RECEIVING_LOG.debug()
                        .attr("cnx", this)
                        .attr("totalPendingWriteBytes",
                                outboundBuffer.totalPendingWriteBytes())
                        .log("Not writable, turn off channel"
                                + " auto-read");
            } else {
                PAUSE_RECEIVING_LOG.debug()
                        .attr("cnx", this)
                        .log("Not writable, turn off channel"
                                + " auto-read");
            }
            getThrottleTracker().markThrottled(ThrottleType.ConnectionOutboundBufferFull);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (state != State.Failed) {
            // No need to report stack trace for known exceptions that happen in disconnections
            log.warn()
                    .attr("cause", ClientCnx.isKnownException(cause)
                            ? cause.toString()
                            : ExceptionUtils.getStackTrace(cause))
                    .log("Got exception");
            state = State.Failed;
            log.debug()
                    .attr("state", State.Failed.name())
                    .log("Connect state changed");
        } else {
            // At default info level, suppress all subsequent exceptions that are thrown when the connection has already
            // failed
            log.debug()
                    .attr("cause", ClientCnx.isKnownException(cause)
                            ? cause.toString()
                            : ExceptionUtils.getStackTrace(cause))
                    .log("Got exception");
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
        CompletableFuture<Boolean> result = service.getAuthorizationService().allowTopicOperationAsync(
                topicName, operation, originalPrincipal, authRole,
                originalAuthDataSource != null ? originalAuthDataSource : authDataSource, authDataSource);
        result.thenAccept(isAuthorized -> {
            if (!isAuthorized) {
                log.warn()
                        .attr("authRole", authRole)
                        .attr("originalPrincipal", originalPrincipal)
                        .attr("operation", operation)
                        .attr("topic", topicName)
                        .log("Role or OriginalRole is not authorized to perform operation on topic");
            }
        });
        return result;
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
    protected void handleLookup(CommandLookupTopic lookupParam) {
        checkArgument(state == State.Connected);

        // Make a copy since the command is handled asynchronously
        CommandLookupTopic lookup = new CommandLookupTopic().copyFrom(lookupParam);

        final long requestId = lookup.getRequestId();
        final boolean authoritative = lookup.isAuthoritative();

        // use the connection-specific listener name by default.
        final String advertisedListenerName =
                lookup.hasAdvertisedListenerName() && StringUtils.isNotBlank(lookup.getAdvertisedListenerName())
                        ? lookup.getAdvertisedListenerName() : this.listenerName;
        log.debug()
                .attr("topic", lookup.getTopic())
                .attr("requestId", requestId)
                .attr("advertisedListenerName",
                        StringUtils.isNotBlank(advertisedListenerName) ? advertisedListenerName : "(none)")
                .log("Received Lookup request");

        TopicName topicName = validateTopicName(lookup.getTopic(), requestId, lookup);
        if (topicName == null) {
            return;
        }

        if (!this.service.getPulsar().isRunning()) {
            log.debug()
                    .attr("topic", topicName)
                    .attr("state", service.getPulsar().getState())
                    .log("Failed lookup topic due to pulsar service is not ready");
            writeAndFlush(newLookupErrorResponse(ServerError.ServiceNotReady,
                    "Failed due to pulsar service is not ready", requestId));
            return;
        }

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            isTopicOperationAllowed(topicName, TopicOperation.LOOKUP, authenticationData, originalAuthData).thenApply(
                    isAuthorized -> {
                if (isAuthorized) {
                    final Map<String, String> properties;
                    if (lookup.getPropertiesCount() > 0) {
                        properties = new HashMap<>();
                        for (int i = 0; i < lookup.getPropertiesCount(); i++) {
                            final var keyValue = lookup.getPropertyAt(i);
                            properties.put(keyValue.getKey(), keyValue.getValue());
                        }
                    } else {
                        properties = Collections.emptyMap();
                    }
                    lookupTopicAsync(getBrokerService().pulsar(), topicName, authoritative,
                            authRole, originalPrincipal, authenticationData,
                            originalAuthData != null ? originalAuthData : authenticationData,
                            requestId, advertisedListenerName, properties).handle((lookupResponse, ex) -> {
                                if (ex == null) {
                                    writeAndFlush(lookupResponse);
                                } else {
                                    // it should never happen
                                    log.warn()
                                            .attr("topic", topicName)

                                            .exception(ex)
                                            .log("lookup failed with error");
                                    writeAndFlush(newLookupErrorResponse(ServerError.ServiceNotReady,
                                            ex.getMessage(), requestId));
                                }
                                lookupSemaphore.release();
                                return null;
                            });
                } else {
                    final String msg = "Client is not authorized to Lookup";
                    log.warn()
                            .attr("principal", getPrincipal())
                            .attr("topic", topicName)
                            .log(msg);
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
            log.debug()
                    .attr("topic", topicName)
                    .log("Failed lookup due to too many lookup-requests");
            writeAndFlush(newLookupErrorResponse(ServerError.TooManyRequests,
                    "Failed due to too many pending lookup requests", requestId));
        }
    }

    private void writeAndFlush(ByteBuf cmd) {
        NettyChannelUtil.writeAndFlushWithVoidPromise(ctx, cmd);
    }

    @Override
    protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata partitionMetadataParam) {
        checkArgument(state == State.Connected);

        // Make a copy since the command is handled asynchronously
        CommandPartitionedTopicMetadata partitionMetadata =
                new CommandPartitionedTopicMetadata().copyFrom(partitionMetadataParam);

        final long requestId = partitionMetadata.getRequestId();
        log.debug()
                .attr("topic", partitionMetadata.getTopic())
                .attr("requestId", requestId)
                .log("Received PartitionMetadataLookup from for");

        TopicName topicName = validateTopicName(partitionMetadata.getTopic(), requestId, partitionMetadata);
        if (topicName == null) {
            return;
        }

        if (!this.service.getPulsar().isRunning()) {
            log.debug()
                    .attr("topic", partitionMetadata.getTopic())
                    .attr("requestId", requestId)
                    .attr("state", service.getPulsar().getState())
                    .log("Failed PartitionMetadataLookup due to pulsar service is not ready");
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
                            namespaceService.checkTopicExistsAsync(topicName).thenAccept(topicExistsInfo -> {
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
                                log.error()
                                        .attr("topic", topicName)
                                        .exception(ex)
                                        .log("Failed to get partition metadata");
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
                                            log.warn()
                                                    .attr("role", getRole())
                                                    .attr("topic", topicName)
                                                    .exceptionMessage(ex)
                                                    .log("Failed to authorize on topic");
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
                                                log.info()
                                                        .attr("topic", topicName)
                                                        .exceptionMessage(ex)
                                                        .log("Trying to get Partitioned"
                                                                + " Metadata for"
                                                                + " nonexistent resource");
                                            } else {
                                                log.warn()
                                                        .attr("topic", topicName)

                                                        .exception(ex)
                                                        .log("Failed to get Partitioned Metadata");
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
                    log.warn()
                            .attr("principal", getPrincipal())
                            .attr("topic", topicName)
                            .log(msg);
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
            log.debug()
                    .attr("topic", topicName)
                    .log("Failed Partition-Metadata lookup due to too many lookup-requests");
            commandSender.sendPartitionMetadataResponse(ServerError.TooManyRequests,
                    "Failed due to too many pending lookup requests", requestId);
        }
    }

    @Override
    protected void handleConsumerStats(CommandConsumerStats commandConsumerStats) {
        checkArgument(state == State.Connected);
        log.debug("Received CommandConsumerStats call");

        final long requestId = commandConsumerStats.getRequestId();
        final long consumerId = commandConsumerStats.getConsumerId();
        CompletableFuture<Consumer> consumerFuture = consumers.get(consumerId);
        Consumer consumer = consumerFuture.getNow(null);
        ByteBuf msg = null;

        if (consumer == null) {
            log.error()
                    .attr("requestId", requestId)
                    .attr("consumerId", consumerId)
                    .log("Failed to get consumer-stats response - Consumer not found");
            msg = Commands.newConsumerStatsResponse(ServerError.ConsumerNotFound,
                    "Consumer " + consumerId + " not found", requestId);
        } else {
            log.debug()
                    .attr("requestId", requestId)
                    .attr("consumer", consumer)
                    .log("CommandConsumerStats");
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
        writeAndFlush(Commands.newConnected(clientProtoVersion, maxMessageSize, enableTopicListWatcher));
        state = State.Connected;
        service.getPulsarStats().recordConnectionCreateSuccess();
        log.debug()
                .attr("state", State.Connected.name())
                .log("connect state change to");
        setRemoteEndpointProtocolVersion(clientProtoVersion);
        if (isNotBlank(clientVersion)) {
            this.clientVersion = StringInterner.intern(clientVersion);
        }
        if (!service.isAuthenticationEnabled()) {
            log.info()
                    .attr("clientVersion", clientVersion)
                    .attr("clientProtoVersion", clientProtoVersion)
                    .attr("proxyVersion", proxyVersion)
                    .log("connected");
        } else if (originalPrincipal != null) {
            log.info()
                    .attr("authRole", authenticationRoleLoggingAnonymizer.anonymize(authRole))
                    .attr("originalAuthRole", authenticationRoleLoggingAnonymizer.anonymize(originalPrincipal))
                    .attr("authMethod", authMethod)
                    .attr("clientVersion", clientVersion)
                    .attr("clientProtoVersion", clientProtoVersion)
                    .attr("proxyVersion", proxyVersion)
                    .log("connected with original auth role");
        } else {
            log.info()
                    .attr("authRole", authenticationRoleLoggingAnonymizer.anonymize(authRole))
                    .attr("authMethod", authMethod)
                    .attr("clientVersion", clientVersion)
                    .attr("clientProtoVersion", clientProtoVersion)
                    .attr("proxyVersion", proxyVersion)
                    .log("connected");
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
        log.debug()
                .attr("useOriginalAuthState", useOriginalAuthState)
                .attr("authRole", authRole)
                .log("Authenticate using original auth state");
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
                            log.warn()
                                    .attr("expectedAuthRole", authRole)
                                    .attr("newAuthRole", newAuthRole)
                                    .log("Principal cannot change during an authentication refresh");
                            ctx.close();
                        } else {
                            log.info()
                                    .attr("authRole", authRole)
                                    .log("Refreshed authentication credentials for role");
                        }
                    }
                }
            } else {
                // auth not complete, continue auth with client side.
                ctx.writeAndFlush(Commands.newAuthChallenge(authMethod, authChallenge, clientProtocolVersion));
                log.debug()
                        .attr("authMethod", authMethod)
                        .log("Authentication in progress client by method.");
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
                            log.debug()
                                    .attr("originalPrincipal", originalPrincipal)
                                    .log("Authenticated original role (forwarded from proxy)");
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
            log.info("Cannot revalidate user credential when using proxy and not forwarding the credentials, "
                    + "closing connection");
            ctx.close();
            return;
        }

        if (!supportsAuthenticationRefresh()) {
            log.warn("Closing connection because client doesn't support auth credentials refresh");
            ctx.close();
            return;
        }

        if (pendingAuthChallengeResponse) {
            log.warn("Closing connection after timeout on refreshing auth credentials");
            ctx.close();
            return;
        }

        log.info()
                .attr("originalPrincipal", originalPrincipal)
                .attr("authRole", this.authRole)
                .log("Refreshing authentication credentials for originalPrincipal and authRole");
        try {
            AuthData brokerData = authState.refreshAuthentication();

            writeAndFlush(Commands.newAuthChallenge(authMethod, brokerData,
                    getRemoteEndpointProtocolVersion()));
            log.debug()
                    .attr("authMethod", authMethod)
                    .log("Sent auth challenge to client to refresh credentials");

            pendingAuthChallengeResponse = true;

        } catch (AuthenticationException e) {
            log.warn().exceptionMessage(e)
                    .log("Failed to refresh authentication");
            ctx.close();
        }
    }

    private static final byte[] emptyArray = new byte[0];

    @Override
    @SuppressWarnings("deprecation")
    protected void handleConnect(CommandConnect connect) {
        checkArgument(state == State.Start);

        log.debug()
                .attr("isAuthenticationEnabled", service.isAuthenticationEnabled())
                .attr("hasOriginalPrincipal", connect.hasOriginalPrincipal())
                .attr("originalPrincipal", connect.hasOriginalPrincipal() ? connect.getOriginalPrincipal() : null)
                .log("Received CONNECT");

        if (!this.service.getPulsar().isRunning()) {
            log.debug()
                    .attr("state", service.getPulsar().getState())
                    .log("Failed CONNECT due to pulsar service is not ready");
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

            log.debug().attr("role", () -> (authState != null && authState.isComplete())
                    ? authState.getAuthRole() : "authentication incomplete or null")
                    .log("Authenticate role");

            if (connect.hasOriginalPrincipal() && service.getPulsar().getConfig().isAuthenticateOriginalAuthData()
                    && !WEBSOCKET_DUMMY_ORIGINAL_PRINCIPLE.equals(connect.getOriginalPrincipal())) {
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

                log.debug()
                        .attr("originalPrincipal", originalPrincipal)
                        .log("Setting original role (forwarded from proxy)");
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

        log.debug()
                .attr("authMethodName", authResponse.getResponse().getAuthMethodName())
                .log("Received AuthResponse from, auth method");

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

        log.debug()
                .attr("authRole", authenticationRoleLoggingAnonymizer.anonymize(authRole))
                .attr("originalAuthRole", authenticationRoleLoggingAnonymizer.anonymize(originalPrincipal))
                .log("Handle subscribe command");

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
        final Boolean isReplicated =
                subscribe.hasReplicateSubscriptionState() ? subscribe.isReplicateSubscriptionState() : null;
        final boolean forceTopicCreation = subscribe.isForceTopicCreation();
        final KeySharedMeta keySharedMeta = subscribe.hasKeySharedMeta()
              ? new KeySharedMeta().copyFrom(subscribe.getKeySharedMeta())
              : emptyKeySharedMeta;
        final long consumerEpoch = subscribe.hasConsumerEpoch() ? subscribe.getConsumerEpoch() : DEFAULT_CONSUMER_EPOCH;
        final Optional<Map<String, String>> subscriptionProperties = SubscriptionOption.getPropertiesMap(
                subscribe.getSubscriptionPropertiesList());

        log.debug()
                .attr("topic", topicName)
                .attr("subscription", subscriptionName)
                .attr("schema", schema == null ? "absent" : "present")
                .log("Subscribe request received");

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
        isAuthorizedFuture.thenApplyAsync(isAuthorized -> {
            if (isAuthorized) {
                log.debug()
                        .attr("principal", getPrincipal())
                        .log("Client is authorized to subscribe with role");

                log.info()
                        .attr("cnx", this.toString())
                        .attr("topic", topicName)
                        .attr("subscription", subscriptionName)
                        .attr("consumerId", consumerId)
                        .attr("principal", getPrincipal())
                        .log("Subscribing on topic");
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
                        log.warn()
                                .attr("topic", topicName)
                                .attr("subscription", subscriptionName)
                                .attr("consumerId", consumerId)
                                .log("Consumer with id is already present on the connection," + "consumerId");
                        commandSender.sendErrorResponse(requestId, ServerError.ServiceNotReady,
                                "Consumer is already present on the connection");
                    } else if (existingConsumerFuture.isCompletedExceptionally()){
                        log.warn()
                                .attr("topic", topicName)
                                .attr("subscription", subscriptionName)
                                .attr("consumerId", consumerId)
                                .log("A failed consumer with id is already present on the connection," + "consumerId");
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
                        log.warn()
                                .attr("consumerId", consumerId)
                                .attr("consumer", consumer)
                                .log("Consumer with the same id is already created:" + "consumerId=, consumer");
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
                                log.warn()
                                        .attr("topic", topic)
                                        .log("Attempting to add consumer to topic which reached max"
                                                + "consumers limit");
                                Throwable t =
                                        new ConsumerBusyException("Topic reached max consumers limit");
                                return FutureUtil.failedFuture(t);
                            }
                            return service.isAllowAutoSubscriptionCreationAsync(topicName)
                                    .thenCompose(isAllowedAutoSubscriptionCreation -> {
                                        boolean subscriptionExists =
                                                topic.getSubscriptions().containsKey(subscriptionName);
                                        // If subscription is as "a/b". The url of HTTP API that defined as
                                        // "{tenant}/{namespace}/{topic}/{subscription}" will be like below:
                                        // "public/default/tp/a/b", then the broker will assume it is a topic that
                                        // using the old rule "{tenant}/{cluster}/{namespace}/{topic}/{subscription}".
                                        // So denied to create a subscription that contains "/".
                                        if (getBrokerService().pulsar().getConfig().isStrictlyVerifySubscriptionName()
                                                && !subscriptionExists
                                                && !NamedEntity.isAllowed(subscriptionName)) {
                                            return FutureUtil.failedFuture(
                                                new BrokerServiceException.NamingException(
                                                 "Please let the subscription only contains '/w(a-zA-Z_0-9)' or '_',"
                                                 + " the current value is " + subscriptionName));
                                        }

                                        boolean rejectSubscriptionIfDoesNotExist = isDurable
                                                && !isAllowedAutoSubscriptionCreation
                                                && !subscriptionExists
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
                                            return ignoreUnrecoverableBKException
                                                    (topic.addSchemaIfIdleOrCheckCompatible(schema))
                                                    .thenCompose(v -> topic.subscribe(option));
                                        } else {
                                            return topic.subscribe(option);
                                        }
                                    });
                        })
                        .thenAcceptAsync(consumer -> {
                            if (consumer.checkAndApplyTopicMigration()) {
                                log.info()
                                        .attr("consumerId", consumerId)
                                        .attr("subscription", subscriptionName)
                                        .attr("topic", topicName)
                                        .log("Disconnecting consumer on migrated subscription on topic");
                                consumers.remove(consumerId, consumerFuture);
                                return;
                            }

                            if (consumerFuture.complete(consumer)) {
                                log.info()
                                        .attr("topic", topicName)
                                        .attr("subscription", subscriptionName)
                                        .log("Created subscription on topic");
                                commandSender.sendSuccessResponse(requestId);
                                if (brokerInterceptor != null) {
                                    try {
                                        brokerInterceptor.consumerCreated(this, consumer, metadata);
                                    } catch (Throwable t) {
                                        log.error()
                                                .exception(t)
                                                .log("Exception occur when intercept consumer created.");
                                    }
                                }
                            } else {
                                // The consumer future was completed before by a close command
                                try {
                                    consumer.close();
                                    log.info()
                                            .attr("consumer", consumer)
                                            .log("Cleared consumer created after timeout on client side");
                                } catch (BrokerServiceException e) {
                                    log.warn()
                                            .attr("consumer", consumer)
                                            .exceptionMessage(e)
                                            .log("Error closing consumer created after timeout on client side");
                                }
                                consumers.remove(consumerId, consumerFuture);
                            }

                        }, ctx.executor())
                        .exceptionallyAsync(exception -> {
                            if (exception.getCause() instanceof ConsumerBusyException) {
                                log.debug()
                                        .attr("topic", topicName)
                                        .attr("subscription", subscriptionName)
                                        .exceptionMessage(exception.getCause())
                                        .log("Failed to create consumer because exclusive consumer "
                                                + "is already connected");
                            } else if (exception.getCause() instanceof BrokerServiceException.TopicMigratedException) {
                                Optional<ClusterUrl> clusterURL = getMigratedClusterUrl(service.getPulsar(),
                                        topicName.toString());
                                if (clusterURL.isPresent()) {
                                    log.info()
                                            .attr("topic", topicName)
                                            .attr("consumerId", consumerId)
                                            .attr("subscription", subscriptionName)
                                            .exceptionMessage(exception.getCause())
                                            .log("Redirect migrated consumer");
                                    boolean msgSent = commandSender.sendTopicMigrated(ResourceType.Consumer, consumerId,
                                            clusterURL.get().getBrokerServiceUrl(),
                                            clusterURL.get().getBrokerServiceUrlTls());
                                    if (!msgSent) {
                                        log.info()
                                                .attr("topic", topicName)
                                                .attr("consumerId", consumerId)
                                                .log("Consumer client doesn't support topic migration handling");
                                    }
                                    consumers.remove(consumerId, consumerFuture);
                                    closeConsumer(consumerId, Optional.empty());
                                    return null;
                                }
                            } else if (exception.getCause() instanceof BrokerServiceException) {
                                log.warn()
                                        .attr("topic", topicName)
                                        .attr("subscription", subscriptionName)
                                        .attr("consumerId", consumerId)
                                        .exceptionMessage(exception.getCause())
                                        .log("Failed to create consumer");
                            } else {
                                log.warn()
                                        .attr("topic", topicName)
                                        .attr("subscription", subscriptionName)
                                        .attr("consumerId", consumerId)
                                        .exceptionMessage(exception.getCause())
                                        .exception(exception)
                                        .log("Failed to create consumer");
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

                        }, ctx.executor());
            } else {
                String msg = "Client is not authorized to subscribe";
                log.warn()
                        .attr("principal", getPrincipal())
                        .log(msg);
                consumers.remove(consumerId, consumerFuture);
                writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
            }
            return null;
        }, ctx.executor()).exceptionallyAsync(ex -> {
            logAuthException(remoteAddress, "subscribe", getPrincipal(), Optional.of(topicName), ex);
            consumers.remove(consumerId, consumerFuture);
            commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, ex.getMessage());
            return null;
        }, ctx.executor());
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

        isAuthorizedFuture.thenApplyAsync(isAuthorized -> {
            if (!isAuthorized) {
                String msg = "Client is not authorized to Produce";
                log.warn()
                        .attr("principal", getPrincipal())
                        .log(msg);
                writeAndFlush(Commands.newError(requestId, ServerError.AuthorizationError, msg));
                return null;
            }

            log.debug()
                    .attr("principal", getPrincipal())
                    .log("Client is authorized to Produce");
            CompletableFuture<Producer> producerFuture = new CompletableFuture<>();
            CompletableFuture<Producer> existingProducerFuture = producers.putIfAbsent(producerId, producerFuture);

            if (existingProducerFuture != null) {
                if (!existingProducerFuture.isDone()) {
                    // There was an early request to create a producer with same producerId.
                    // This can happen when client timeout is lower than the broker timeouts.
                    // We need to wait until the previous producer creation request
                    // either complete or fails.
                    log.warn()
                            .attr("topic", topicName)
                            .attr("producerId", producerId)
                            .log("Producer with id is already present on the connection, producerId");
                    commandSender.sendErrorResponse(requestId, ServerError.ServiceNotReady,
                            "Producer is already present on the connection");
                } else if (existingProducerFuture.isCompletedExceptionally()) {
                    // remove producer with producerId as it's already completed with exception
                    log.warn()
                            .attr("topic", topicName)
                            .attr("producerId", producerId)
                            .log("Producer with id is failed to register present on the connection, producerId");
                    ServerError error = getErrorCode(existingProducerFuture);
                    producers.remove(producerId, existingProducerFuture);
                    commandSender.sendErrorResponse(requestId, error,
                            "Producer is already failed to register present on the connection");
                } else {
                    Producer producer = existingProducerFuture.getNow(null);
                    log.info()
                            .attr("topic", topicName)
                            .attr("producerId", producerId)
                            .attr("producer", producer)
                            .log("Producer with the same id is already created:" + "producerId=, producer");
                    commandSender.sendProducerSuccessResponse(requestId, producer.getProducerName(),
                            producer.getSchemaVersion());
                }
                return null;
            }

            log.debug()
                    .attr("topic", topicName)
                    .attr("producerId", producerId)
                    .attr("producerName", producerName)
                    .attr("schema", schema == null ? "absent" : "present")
                    .log("Creating producer");

            service.getOrCreateTopic(topicName.toString()).thenComposeAsync((Topic topic) -> {
                // Check max producer limitation to avoid unnecessary ops wasting resources. For example: the new
                // producer reached max producer limitation, but pulsar did schema check first, it would waste CPU
                if (((AbstractTopic) topic).isProducersExceeded(producerName)) {
                    log.warn()
                            .attr("topic", topic)
                            .log("Attempting to add producer to topic which reached max producers limit");
                    String errorMsg = "Topic '" + topicName.toString() + "' reached max producers limit";
                    Throwable t = new BrokerServiceException.ProducerBusyException(errorMsg);
                    return CompletableFuture.failedFuture(t);
                }

                // Before creating producer, check if backlog quota exceeded
                // on topic for size based limit and time based limit
                CompletableFuture<Void> backlogQuotaCheckFuture = CompletableFuture.allOf(
                        topic.checkBacklogQuotaExceeded(producerName, BacklogQuotaType.destination_storage),
                        topic.checkBacklogQuotaExceeded(producerName, BacklogQuotaType.message_age));

                backlogQuotaCheckFuture.thenRunAsync(() -> {
                    // Check whether the producer will publish encrypted messages or not
                    if ((topic.isEncryptionRequired() || encryptionRequireOnProducer)
                            && !isEncrypted
                            && !SystemTopicNames.isSystemTopic(topicName)) {
                        String msg = String.format("Encryption is required in %s", topicName);
                        log.warn().attr("msg", msg).log("");
                        if (producerFuture.completeExceptionally(new ServerMetadataException(msg))) {
                            commandSender.sendErrorResponse(requestId, ServerError.MetadataError, msg);
                        }
                        producers.remove(producerId, producerFuture);
                        return;
                    }

                    disableTcpNoDelayIfNeeded(topicName.toString(), producerName);

                    CompletableFuture<SchemaVersion> schemaVersionFuture = tryAddSchema(topic, schema);

                    schemaVersionFuture.exceptionallyAsync(exception -> {
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
                        if (cause instanceof IncompatibleSchemaException) {
                            // ignore it
                        } else if (cause instanceof InvalidSchemaDataException) {
                            log.warn()
                                    .attr("topic", topicName)
                                    .attr("producerId", producerId)
                                    .log("Try add schema failed due to invalid schema data, "
                                            + "remote address, topic, producerId");
                        } else {
                            log.error()
                                    .attr("topic", topicName)
                                    .attr("producerId", producerId)
                                    .exception(exception)
                                    .log("Try add schema failed, remote address, topic, producerId");
                        }
                        producers.remove(producerId, producerFuture);
                        return null;
                    }, ctx.executor());

                    schemaVersionFuture.thenAcceptAsync(schemaVersion -> {
                        CompletionStage<Subscription> createInitSubFuture;
                        if (!Strings.isNullOrEmpty(initialSubscriptionName)
                                && topic.isPersistent()
                                && !topic.getSubscriptions().containsKey(initialSubscriptionName)) {
                            createInitSubFuture = service.isAllowAutoSubscriptionCreationAsync(topicName)
                                    .thenCompose(isAllowAutoSubscriptionCreation -> {
                                        if (!isAllowAutoSubscriptionCreation) {
                                            return CompletableFuture.failedFuture(
                                                    new BrokerServiceException.NotAllowedException(
                                                            "Could not create the initial subscription due to the "
                                                                    + "auto subscription creation is not allowed."));
                                        }
                                        return topic.createSubscription(initialSubscriptionName,
                                                InitialPosition.Earliest, false, null);
                                    });
                        } else {
                            createInitSubFuture = CompletableFuture.completedFuture(null);
                        }

                        createInitSubFuture.whenCompleteAsync((sub, ex) -> {
                            if (ex != null) {
                                final Throwable rc = FutureUtil.unwrapCompletionException(ex);
                                if (rc instanceof BrokerServiceException.NotAllowedException) {
                                    log.warn()
                                            .exceptionMessage(rc)
                                            .attr("initialSubscriptionName", initialSubscriptionName)
                                            .attr("topic", topicName)
                                            .log("Failed to create initial subscription");
                                    if (producerFuture.completeExceptionally(rc)) {
                                        commandSender.sendErrorResponse(requestId,
                                                ServerError.NotAllowedError, rc.getMessage());
                                    }
                                    producers.remove(producerId, producerFuture);
                                    return;
                                }
                                String msg =
                                        "Failed to create the initial subscription: " + ex.getCause().getMessage();
                                log.warn()
                                        .attr("msg", msg)
                                        .attr("initialSubscriptionName", initialSubscriptionName)
                                        .attr("topic", topicName)
                                        .log("Failed to create initial subscription");
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
                        }, ctx.executor());
                    }, ctx.executor());
                }, ctx.executor());
                return backlogQuotaCheckFuture;
            }, ctx.executor()).exceptionallyAsync(exception -> {
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
                } else if (cause instanceof BrokerServiceException.TopicMigratedException) {
                    Optional<ClusterUrl> clusterURL = getMigratedClusterUrl(service.getPulsar(), topicName.toString());
                    if (clusterURL.isPresent()) {
                        log.info()
                                .attr("topic", topicName)
                                .attr("producerId", producerId)
                                .attr("producerName", producerName)
                                .exceptionMessage(cause)
                                .log("redirect migrated producer to topic: " + "producerId=, producerName");
                        boolean msgSent = commandSender.sendTopicMigrated(ResourceType.Producer, producerId,
                                clusterURL.get().getBrokerServiceUrl(), clusterURL.get().getBrokerServiceUrlTls());
                        if (!msgSent) {
                            log.info()
                                    .attr("topic", topicName)
                                    .attr("producerId", producerId)
                                    .log("client doesn't support topic migration handling");
                        }
                        producers.remove(producerId, producerFuture);
                        closeProducer(producerId, -1L, Optional.empty());
                        return null;
                    }
                }

                // Do not print stack traces for expected exceptions
                if (cause instanceof NoSuchElementException) {
                    cause = new TopicNotFoundException(String.format("Topic not found %s", topicName.toString()));
                    log.warn()
                            .attr("topic", topicName)
                            .attr("producerId", producerId)
                            .log("Failed to load topic, producerId=: Topic not found");
                } else if (!Exceptions.areExceptionsPresentInChain(cause,
                        ServiceUnitNotReadyException.class, ManagedLedgerException.class,
                        BrokerServiceException.ProducerBusyException.class)) {
                    log.error()
                            .attr("topic", topicName)
                            .attr("producerId", producerId)
                            .exception(exception)
                            .log("Failed to create topic, producerId");
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
            }, ctx.executor());
            return null;
        }, ctx.executor()).exceptionallyAsync(ex -> {
            logAuthException(remoteAddress, "producer", getPrincipal(), Optional.of(topicName), ex);
            commandSender.sendErrorResponse(requestId, ServerError.AuthorizationError, ex.getMessage());
            return null;
        }, ctx.executor());
    }

    private void buildProducerAndAddTopic(Topic topic, long producerId, String producerName, long requestId,
                             boolean isEncrypted, Map<String, String> metadata, SchemaVersion schemaVersion, long epoch,
                             boolean userProvidedProducerName, TopicName topicName,
                             ProducerAccessMode producerAccessMode,
                             Optional<Long> topicEpoch, boolean supportsPartialProducer,
                             CompletableFuture<Producer> producerFuture){
        if (producerFuture.isCompletedExceptionally()) {
            log.info()
                    .attr("producerId", producerId)
                    .attr("producerName", producerName)
                    .log("Skipped producer creation after timeout on client side");
            producers.remove(producerId, producerFuture);
            return;
        }

        CompletableFuture<Void> producerQueuedFuture = new CompletableFuture<>();
        Producer producer = new Producer(topic, ServerCnx.this, producerId, producerName,
                getPrincipal(), isEncrypted, metadata, schemaVersion, epoch,
                userProvidedProducerName, producerAccessMode, topicEpoch, supportsPartialProducer);

        topic.addProducer(producer, producerQueuedFuture).thenAcceptAsync(newTopicEpoch -> {
            if (isActive()) {
                if (producerFuture.complete(producer)) {
                    log.info()
                            .attr("producer", producer)
                            .attr("principal", getPrincipal())
                            .log("Created new producer");
                    commandSender.sendProducerSuccessResponse(requestId, producerName,
                            producer.getLastSequenceId(), producer.getSchemaVersion(),
                            newTopicEpoch, true /* producer is ready now */);
                    if (brokerInterceptor != null) {
                        try {
                            brokerInterceptor.producerCreated(this, producer, metadata);
                        } catch (Throwable t) {
                            log.error().exception(t).log("Exception occur when intercept producer created.");
                        }
                    }
                    return;
                } else {
                    // The producer's future was completed before by
                    // a close command
                    producer.closeNow(true);
                    log.info()
                            .attr("producer", producer)
                            .log("Cleared producer created after" + "timeout on client side");
                }
            } else {
                producer.closeNow(true);
                log.info()
                        .attr("producer", producer)
                        .log("Cleared producer created after connection was closed");
                producerFuture.completeExceptionally(
                        new IllegalStateException(
                                "Producer created after connection was closed"));
            }

            producers.remove(producerId, producerFuture);
        }, ctx.executor()).exceptionallyAsync(ex -> {
            if (ex.getCause() instanceof BrokerServiceException.TopicMigratedException) {
                Optional<ClusterUrl> clusterURL = getMigratedClusterUrl(service.getPulsar(), topic.getName());
                if (clusterURL.isPresent()) {
                    if (!topic.shouldProducerMigrate()) {
                        log.info()
                                .attr("topic", topicName)
                                .attr("producerId", producerId)
                                .attr("producerName", producerName)
                                .exceptionMessage(ex.getCause())
                                .log("Topic is migrated but replication backlog exist: "
                                        + "producerId =, producerName");
                    } else {
                        log.info()
                                .attr("topic", topicName)
                                .attr("producerId", producerId)
                                .attr("producerName", producerName)
                                .exceptionMessage(ex.getCause())
                                .log("redirect migrated producer to topic: " + "producerId=, producerName");
                        boolean msgSent = commandSender.sendTopicMigrated(ResourceType.Producer, producerId,
                                clusterURL.get().getBrokerServiceUrl(), clusterURL.get().getBrokerServiceUrlTls());
                        if (!msgSent) {
                            log.info()
                                    .attr("topic", topic)
                                    .attr("producerId", producerId)
                                    .log("client doesn't support topic migration handling");
                        }
                        closeProducer(producer);
                        return null;
                    }
                } else {
                    log.warn()
                            .attr("topic", topicName)
                            .attr("producerId", producerId)
                            .exceptionMessage(ex.getCause())
                            .log("failed producer because migration url not configured topic: producerId");
                }
            } else if (ex.getCause() instanceof BrokerServiceException.ProducerFencedException) {
                log.debug()
                        .attr("topic", topicName)
                        .attr("producerId", producerId)
                        .exceptionMessage(ex.getCause())
                        .log("Failed to add producer to topic: producerId");
            } else {
                log.warn()
                        .attr("topic", topicName)
                        .attr("producerId", producerId)
                        .exceptionMessage(ex.getCause())
                        .log("Failed to add producer to topic: producerId");
            }

            producer.closeNow(true);
            if (producerFuture.completeExceptionally(ex)) {
                commandSender.sendErrorResponse(requestId,
                        BrokerServiceException.getClientErrorCode(ex), ex.getMessage());
            }
            return null;
        }, ctx.executor());

        producerQueuedFuture.thenRunAsync(() -> {
            // If the producer is queued waiting, we will get an immediate notification
            // that we need to pass to client
            if (isActive()) {
                log.info()
                        .attr("producer", producer)
                        .log("Producer is waiting in queue");
                commandSender.sendProducerSuccessResponse(requestId, producerName,
                        producer.getLastSequenceId(), producer.getSchemaVersion(),
                        Optional.empty(), false/* producer is not ready now */);
                if (brokerInterceptor != null) {
                    brokerInterceptor.
                            producerCreated(this, producer, metadata);
                }
            }
        }, ctx.executor());
    }
    @Override
    protected void handleSend(CommandSend send, ByteBuf headersAndPayload) {
        checkArgument(state == State.Connected);

        CompletableFuture<Producer> producerFuture = producers.get(send.getProducerId());

        if (producerFuture == null || !producerFuture.isDone() || producerFuture.isCompletedExceptionally()) {
            if (recentlyClosedProducers.containsKey(send.getProducerId())) {
                log.debug()
                        .attr("producerId", send.getProducerId())
                        .log("Received message, but the producer was recently closed :. Ignoring message.");
                // We expect these messages because we recently closed the producer. Do not close the connection.
                return;
            }
            log.warn()
                    .attr("producerId", send.getProducerId())
                    .log("Received message, but the producer is not ready :. Closing the connection.");
            close();
            return;
        }

        Producer producer = producerFuture.getNow(null);
        printSendCommandDebug(send, headersAndPayload);

        // New messages are silently ignored during topic transfer. Note that the transferring flag is only set when the
        // Extensible Load Manager is enabled.
        if (producer.getTopic().isTransferring()) {
            var pulsar = getBrokerService().pulsar();
            var ignoredMsgCount = send.getNumMessages();
            var ignoredSendMsgTotalCount = ExtensibleLoadManagerImpl.get(pulsar).getIgnoredSendMsgCount().
                    addAndGet(ignoredMsgCount);
            log.debug()
                    .attr("ignoredMsgCount", ignoredMsgCount)
                    .attr("producerId", send.getProducerId())
                    .attr("name", producer.getTopic().getName())
                    .attr("ignoredSendMsgTotalCount", ignoredSendMsgTotalCount)
                    .log("Ignoring messages from:: to fenced topic: while transferring."
                            + "Total ignored message count:.");
            return;
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

        increasePendingSendRequestsAndPublishBytes(headersAndPayload.readableBytes());

        if (send.hasTxnidMostBits() && send.hasTxnidLeastBits()) {
            TxnID txnID = new TxnID(send.getTxnidMostBits(), send.getTxnidLeastBits());
            producer.publishTxnMessage(txnID, producer.getProducerId(), send.getSequenceId(),
                    send.getHighestSequenceId(), headersAndPayload, send.getNumMessages(), send.isIsChunk(),
                    send.isMarker());
            return;
        }

        // This position is only used for shadow replicator
        Position position = send.hasMessageId()
                ? PositionFactory.create(send.getMessageId().getLedgerId(), send.getMessageId().getEntryId()) : null;

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
        log.debug()
                .attr("producerId", send.getProducerId())
                .attr("sendSequenceId", send.getSequenceId())
                .attr("producerName", msgMetadata.getProducerName())
                .attr("metadataSequenceId", msgMetadata.getSequenceId())
                .attr("readableBytes", headersAndPayload.readableBytes())
                .attr("partitionKey", msgMetadata.hasPartitionKey() ? msgMetadata.getPartitionKey() : null)
                .attr("orderingKey", msgMetadata.hasOrderingKey() ? msgMetadata.getOrderingKey() : null)
                .attr("uncompressedSize", msgMetadata.getUncompressedSize())
                .log("Received send message request");
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
            Subscription subscription = consumer.getSubscription();
            // Message acks are silently ignored during topic transfer. Note that the transferring flag is only set when
            // the Extensible Load Manager is enabled.
            if (subscription.getTopic().isTransferring()) {
                var pulsar = getBrokerService().getPulsar();
                var ignoredAckCount = ack.getMessageIdsCount();
                var ignoredAckTotalCount = ExtensibleLoadManagerImpl.get(pulsar).getIgnoredAckCount().
                        addAndGet(ignoredAckCount);
                log.debug()
                        .attr("subscription", subscription)
                        .attr("consumerId", consumerId)
                        .attr("ignoredAckCount", ignoredAckCount)
                        .attr("ignoredAckTotalCount", ignoredAckTotalCount)
                        .log("Ignoring message acks during topic transfer. Total ignored ack count");
                return;
            }
            consumer.messageAcked(ack).thenRun(() -> {
                if (hasRequestId) {
                    writeAndFlush(Commands.newAckResponse(
                            requestId, null, null, consumerId));
                }
                if (brokerInterceptor != null) {
                    try {
                        brokerInterceptor.messageAcked(this, consumer, copyOfAckForInterceptor);
                    } catch (Throwable t) {
                        log.error().exception(t).log("Exception occur when intercept message acked.");
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
            log.debug()
                    .attr("consumerId", ack.getConsumerId())
                    .attr("messageIdsCount", ack.getMessageIdsCount())
                    .log("Consumer future is not complete (not complete or error), discarding received command ack");
        }
    }

    @Override
    protected void handleFlow(CommandFlow flow) {
        checkArgument(state == State.Connected);
        log.debug()
                .attr("consumerId", flow.getConsumerId())
                .attr("messagePermits", flow.getMessagePermits())
                .log("Received flow from consumer permits");

        CompletableFuture<Consumer> consumerFuture = consumers.get(flow.getConsumerId());

        if (consumerFuture != null && consumerFuture.isDone() && !consumerFuture.isCompletedExceptionally()) {
            Consumer consumer = consumerFuture.getNow(null);
            if (consumer != null) {
                consumer.flowPermits(flow.getMessagePermits());
            } else {
                log.info()
                        .attr("consumerId", flow.getConsumerId())
                        .log("Couldn't find consumer");
            }
        }
    }

    @Override
    protected void handleRedeliverUnacknowledged(CommandRedeliverUnacknowledgedMessages redeliver) {
        checkArgument(state == State.Connected);
        log.debug()
                .attr("consumerId", redeliver.getConsumerId())
                .attr("consumerEpoch", redeliver.hasConsumerEpoch() ? redeliver.getConsumerEpoch() : null)
                .log("redeliverUnacknowledged from consumer");

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
            consumerFuture.getNow(null).doUnsubscribe(unsubscribe.getRequestId(), unsubscribe.isForce());
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

            Position position = AckSetStateUtil.createPositionWithAckSet(msgIdData.getLedgerId(),
                    msgIdData.getEntryId(), ackSet);


            subscription.resetCursor(position).thenRun(() -> {
                log.info()
                        .attr("topic", subscription.getTopic().getName())
                        .attr("subscription", subscription.getName())
                        .attr("position", position)
                        .log("Reset subscription to message id");
                commandSender.sendSuccessResponse(requestId);
            }).exceptionally(ex -> {
                log.warn()
                        .attr("subscription", subscription)
                        .exception(ex)
                        .log("Failed to reset subscription");
                commandSender.sendErrorResponse(requestId, ServerError.UnknownError,
                        "Error when resetting subscription: " + ex.getCause().getMessage());
                return null;
            });
        } else if (consumerCreated && seek.hasMessagePublishTime()){
            Consumer consumer = consumerFuture.getNow(null);
            Subscription subscription = consumer.getSubscription();
            long timestamp = seek.getMessagePublishTime();

            subscription.resetCursor(timestamp).thenRun(() -> {
                log.info()
                        .attr("topic", subscription.getTopic().getName())
                        .attr("subscription", subscription.getName())
                        .attr("timestamp", timestamp)
                        .log("Reset subscription to publish time");
                commandSender.sendSuccessResponse(requestId);
            }).exceptionally(ex -> {
                log.warn()
                        .attr("subscription", subscription)
                        .exception(ex)
                        .log("Failed to reset subscription");
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
            log.info()
                    .attr("producerId", producerId)
                    .log("Producer was not registered on the connection");
            writeAndFlush(Commands.newSuccess(requestId));
            return;
        }

        if (!producerFuture.isDone() && producerFuture
                .completeExceptionally(new IllegalStateException("Closed producer before creation was complete"))) {
            // We have received a request to close the producer before it was actually completed, we have marked the
            // producer future as failed and we can tell the client the close operation was successful.
            log.info()
                    .attr("producerId", producerId)
                    .log("Closed producer before its creation was completed. producerId");
            commandSender.sendSuccessResponse(requestId);
            producers.remove(producerId, producerFuture);
            return;
        } else if (producerFuture.isCompletedExceptionally()) {
            log.info()
                    .attr("producerId", producerId)
                    .log("Closed producer that already failed to be created. producerId");
            commandSender.sendSuccessResponse(requestId);
            producers.remove(producerId, producerFuture);
            return;
        }

        // Proceed with normal close, the producer
        Producer producer = producerFuture.getNow(null);
        log.info()
                .attr("topic", producer.getTopic())
                .attr("producerName", producer.getProducerName())
                .attr("producerId", producerId)
                .log("Closing producer on cnx. producerId");

        producer.close(true).thenAcceptAsync(v -> {
            log.info()
                    .attr("topic", producer.getTopic())
                    .attr("producerName", producer.getProducerName())
                    .attr("producerId", producerId)
                    .log("Closed producer on cnx. producerId");
            commandSender.sendSuccessResponse(requestId);
            producers.remove(producerId, producerFuture);
            if (brokerInterceptor != null) {
                brokerInterceptor.producerClosed(this, producer, producer.getMetadata());
            }
        }, ctx.executor());
    }

    @Override
    protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
        checkArgument(state == State.Connected);
        log.info()
                .attr("consumerId", closeConsumer.getConsumerId())
                .log("Closing consumer: consumerId");

        long requestId = closeConsumer.getRequestId();
        long consumerId = closeConsumer.getConsumerId();

        CompletableFuture<Consumer> consumerFuture = consumers.get(consumerId);
        if (consumerFuture == null) {
            log.info()
                    .attr("consumerId", consumerId)
                    .log("Consumer was not registered on the connection");
            writeAndFlush(Commands.newSuccess(requestId));
            return;
        }

        if (!consumerFuture.isDone() && consumerFuture
                .completeExceptionally(new IllegalStateException("Closed consumer before creation was complete"))) {
            // We have received a request to close the consumer before it was actually completed, we have marked the
            // consumer future as failed and we can tell the client the close operation was successful. When the actual
            // create operation will complete, the new consumer will be discarded.
            log.info()
                    .attr("consumerId", consumerId)
                    .log("Closed consumer before its creation was completed. consumerId");
            commandSender.sendSuccessResponse(requestId);
            return;
        }

        if (consumerFuture.isCompletedExceptionally()) {
            log.info()
                    .attr("consumerId", consumerId)
                    .log("Closed consumer that already failed to be created. consumerId");
            commandSender.sendSuccessResponse(requestId);
            return;
        }

        // Proceed with normal consumer close
        Consumer consumer = consumerFuture.getNow(null);
        try {
            consumer.close();
            consumers.remove(consumerId, consumerFuture);
            commandSender.sendSuccessResponse(requestId);
            log.info()
                    .attr("consumerId", consumerId)
                    .log("Closed consumer, consumerId");
            if (brokerInterceptor != null) {
                brokerInterceptor.consumerClosed(this, consumer, consumer.getMetadata());
            }
        } catch (BrokerServiceException e) {
            log.warn()
                    .attr("consumer", consumer)
                    .exception(e)
                    .log("[{]] Error closing consumer");
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
            topic.checkIfTransactionBufferRecoverCompletely()
                 .thenCompose(__ -> topic.getLastDispatchablePosition())
                 .thenApply(lastPosition -> {
                     int partitionIndex = TopicName.getPartitionIndex(topic.getName());

                     Position markDeletePosition = PositionFactory.EARLIEST;
                     if (consumer.getSubscription() instanceof PersistentSubscription) {
                         markDeletePosition = ((PersistentSubscription) consumer.getSubscription()).getCursor()
                                 .getMarkDeletedPosition();
                     }

                     getLargestBatchIndexWhenPossible(
                             topic,
                             lastPosition,
                             markDeletePosition,
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
            Position lastPosition,
            Position markDeletePosition,
            int partitionIndex,
            long requestId,
            String subscriptionName,
            boolean readCompacted) {

        PersistentTopic persistentTopic = (PersistentTopic) topic;
        ManagedLedger ml = persistentTopic.getManagedLedger();

        // If it's not pointing to a valid entry, respond messageId of the current position.
        // If the compaction cursor reach the end of the topic, respond messageId from compacted ledger
        CompletableFuture<Position> compactionHorizonFuture = readCompacted
                ? persistentTopic.getTopicCompactionService().getLastCompactedPosition() :
                CompletableFuture.completedFuture(null);

        compactionHorizonFuture.whenComplete((compactionHorizon, ex) -> {
            if (ex != null) {
                log.error().exception(ex).log("Failed to get compactionHorizon.");
                writeAndFlush(Commands.newError(requestId, ServerError.MetadataError, ex.getMessage()));
                return;
            }


            if (lastPosition.getEntryId() == -1 || !ml.getLedgersInfo().containsKey(lastPosition.getLedgerId())) {
                // there is no entry in the original topic
                if (compactionHorizon != null) {
                    // if readCompacted is true, we need to read the last entry from compacted topic
                    handleLastMessageIdFromCompactionService(persistentTopic, requestId, partitionIndex,
                            markDeletePosition);
                } else {
                    // if readCompacted is false, we need to return MessageId.earliest
                    writeAndFlush(Commands.newGetLastMessageIdResponse(requestId, -1, -1, partitionIndex, -1,
                            markDeletePosition.getLedgerId(), markDeletePosition.getEntryId()));
                }
                return;
            }

            if (compactionHorizon != null && lastPosition.compareTo(compactionHorizon) <= 0) {
                handleLastMessageIdFromCompactionService(persistentTopic, requestId, partitionIndex,
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

                @Override
                public String toString() {
                    return String.format("ServerCnx [%s] get largest batch index when possible",
                            ServerCnx.this.toString());
                }
            }, null);

            CompletableFuture<Integer> batchSizeFuture = entryFuture.thenApply(entry -> {
                MessageMetadata metadata = entry.getMessageMetadata();
                if (metadata == null) {
                    metadata = Commands.parseMessageMetadata(entry.getDataBuffer());
                }
                int batchSize = metadata.getNumMessagesInBatch();
                entry.release();
                return metadata.hasNumMessagesInBatch() ? batchSize : -1;
            });

            batchSizeFuture.whenComplete((batchSize, e) -> {
                if (e != null) {
                    if (e.getCause() instanceof ManagedLedgerException.NonRecoverableLedgerException
                            && readCompacted) {
                        handleLastMessageIdFromCompactionService(persistentTopic, requestId, partitionIndex,
                                markDeletePosition);
                    } else {
                        writeAndFlush(Commands.newError(
                                requestId, ServerError.MetadataError,
                                "Failed to get batch size for entry " + e.getMessage()));
                    }
                } else {
                    int largestBatchIndex = batchSize > 0 ? batchSize - 1 : -1;

                    log.debug()
                            .attr("topic", topic.getName())
                            .attr("subscription", subscriptionName)
                            .attr("lastPosition", lastPosition)
                            .attr("partitionIndex", partitionIndex)
                            .log("Get LastMessageId partitionIndex");

                    writeAndFlush(Commands.newGetLastMessageIdResponse(requestId, lastPosition.getLedgerId(),
                            lastPosition.getEntryId(), partitionIndex, largestBatchIndex,
                            markDeletePosition.getLedgerId(), markDeletePosition.getEntryId()));
                }
            });
        });
    }

    private void handleLastMessageIdFromCompactionService(PersistentTopic persistentTopic, long requestId,
                                                          int partitionIndex, Position markDeletePosition) {
        persistentTopic.getTopicCompactionService().getLastMessagePosition().thenAccept(position ->
                writeAndFlush(Commands.newGetLastMessageIdResponse(requestId, position.ledgerId(), position.entryId(),
                        partitionIndex, position.batchIndex(), markDeletePosition.getLedgerId(),
                        markDeletePosition.getEntryId()))
        ).exceptionally(ex -> {
            writeAndFlush(Commands.newError(
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
                    namespaceName, operation, originalPrincipal, originalAuthData);
        } else {
            isProxyAuthorizedFuture = CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> isAuthorizedFuture = service.getAuthorizationService().allowNamespaceOperationAsync(
                namespaceName, operation, authRole, authenticationData);
        return isProxyAuthorizedFuture.thenCombine(isAuthorizedFuture, (isProxyAuthorized, isAuthorized) -> {
            if (!isProxyAuthorized) {
                log.warn()
                        .attr("originalPrincipal", authenticationRoleLoggingAnonymizer.anonymize(originalPrincipal))
                        .attr("operation", operation)
                        .attr("namespace", namespaceName)
                        .log("OriginalRole is not authorized to perform operation on namespace");
            }
            if (!isAuthorized) {
                log.warn()
                        .attr("authRole", authenticationRoleLoggingAnonymizer.anonymize(authRole))
                        .attr("operation", operation)
                        .attr("namespace", namespaceName)
                        .log("Role is not authorized to perform operation on namespace");
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
        final Map<String, String> properties = new HashMap<>();
        for (KeyValue keyValue : commandGetTopicsOfNamespace.getPropertiesList()) {
            properties.put(keyValue.getKey(), keyValue.getValue());
        }

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            isNamespaceOperationAllowed(namespaceName, NamespaceOperation.GET_TOPICS).thenApply(isAuthorized -> {
                if (isAuthorized) {
                    internalHandleGetTopicsOfNamespace(namespace, namespaceName, requestId, mode, topicsPattern,
                            topicsHash, properties, lookupSemaphore);
                } else {
                    final String msg = "Client is not authorized to GetTopicsOfNamespace";
                    log.warn()
                            .attr("principal", getPrincipal())
                            .attr("namespace", namespaceName)
                            .log(msg);
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
            log.debug()
                    .attr("namespace", namespaceName)
                    .log("Failed GetTopicsOfNamespace lookup due to too many lookup-requests");
            commandSender.sendErrorResponse(requestId, ServerError.TooManyRequests,
                    "Failed due to too many pending lookup requests");
        }
    }

    private void internalHandleGetTopicsOfNamespace(String namespace, NamespaceName namespaceName, long requestId,
                                                    CommandGetTopicsOfNamespace.Mode mode,
                                                    Optional<String> topicsPattern, Optional<String> topicsHash,
                                                    Map<String, String> properties,
                                                    Semaphore lookupSemaphore) {
        BooleanSupplier isPermitRequestCancelled = () -> !ctx().channel().isActive();
        TopicListSizeResultCache.ResultHolder
                listSizeHolder = service.getTopicListSizeResultCache().getTopicListSize(namespaceName.toString(), mode);
        listSizeHolder.getSizeAsync().thenAccept(initialSize -> {
            maxTopicListInFlightLimiter.withAcquiredPermits(initialSize,
                    AsyncDualMemoryLimiter.LimitType.HEAP_MEMORY, isPermitRequestCancelled, initialPermits -> {
                        return getBrokerService().pulsar().getNamespaceService()
                                .getListOfUserTopicsByProperties(namespaceName, mode, properties)
                                .thenCompose(topics -> {
                                    long actualSize = TopicListMemoryLimiter.estimateTopicListSize(topics);
                                    listSizeHolder.updateSize(actualSize);
                                    return maxTopicListInFlightLimiter.withUpdatedPermits(initialPermits, actualSize,
                                            isPermitRequestCancelled, permits -> {
                                                boolean filterTopics = false;
                                                // filter system topic
                                                List<String> filteredTopics = topics;

                                                if (enableSubscriptionPatternEvaluation && topicsPattern.isPresent()) {
                                                    if (topicsPattern.get().length() <= maxSubscriptionPatternLength) {
                                                        filterTopics = true;
                                                        filteredTopics = TopicList.filterTopics(filteredTopics,
                                                                topicsPattern.get(),
                                                                topicsPatternImplementation);
                                                    } else {
                                                        log.info()
                                                                .attr("get", topicsPattern.get())
                                                                .attr("maxSubscriptionPatternLength",
                                                                        maxSubscriptionPatternLength)
                                                                .log("Subscription pattern provided was longer "
                                                                        + "than maximum.");
                                                    }
                                                }
                                                String hash = TopicList.calculateHash(filteredTopics);
                                                boolean hashUnchanged =
                                                        topicsHash.isPresent() && topicsHash.get().equals(hash);
                                                if (hashUnchanged) {
                                                    filteredTopics = Collections.emptyList();
                                                }
                                                log.debug()
                                                        .attr("namespace", namespace)
                                                        .attr("requestId", requestId)
                                                        .attr("size", topics.size())
                                                        .log("Received CommandGetTopicsOfNamespace for namespace "
                                                                + "[// by, size");
                                                return commandSender.sendGetTopicsOfNamespaceResponse(filteredTopics,
                                                        hash,
                                                        filterTopics, !hashUnchanged, requestId, ex -> {
                                                            log.warn()
                                                                    .exceptionMessage(ex)
                                                                    .log("Failed to acquire direct memory permits for "
                                                                            + "GetTopicsOfNamespace");
                                                            commandSender.sendErrorResponse(requestId,
                                                                    ServerError.TooManyRequests,
                                                                    "Cannot acquire permits for direct memory");
                                                            return CompletableFuture.completedFuture(null);
                                                        });
                                            }, t -> {
                                                log.warn()
                                                        .exceptionMessage(t)
                                                        .log("Failed to acquire heap memory permits for "
                                                                + "GetTopicsOfNamespace");
                                                writeAndFlush(Commands.newError(requestId, ServerError.TooManyRequests,
                                                        "Failed due to heap memory limit exceeded"));
                                                return CompletableFuture.completedFuture(null);
                                            });
                                }).whenComplete((__, ___) -> {
                                    lookupSemaphore.release();
                                }).exceptionally(ex -> {
                                    log.warn()
                                            .attr("namespace", namespace)
                                            .attr("requestId", requestId)
                                            .log("Error GetTopicsOfNamespace for namespace [// by");
                                    listSizeHolder.resetIfInitializing();
                                    commandSender.sendErrorResponse(requestId,
                                            BrokerServiceException.getClientErrorCode(new ServerMetadataException(ex)),
                                            ex.getMessage());
                                    return null;
                                });
                    }, t -> {
                        log.warn()
                                .exceptionMessage(t)
                                .log("Failed to acquire initial heap memory permits for GetTopicsOfNamespace");
                        listSizeHolder.resetIfInitializing();
                        writeAndFlush(Commands.newError(requestId, ServerError.TooManyRequests,
                                "Failed due to heap memory limit exceeded"));
                        lookupSemaphore.release();
                        return CompletableFuture.completedFuture(null);
                    });
        });
    }


    @Override
    protected void handleGetSchema(CommandGetSchema commandGetSchema) {
        checkArgument(state == State.Connected);
        if (commandGetSchema.hasSchemaVersion()) {
            log.debug()
                    .attr("schemaVersion", new String(commandGetSchema.getSchemaVersion()))
                    .attr("topic", commandGetSchema.getTopic())
                    .attr("requestId", commandGetSchema.getRequestId())
                    .log("Received CommandGetSchema");
        } else {
            log.debug()
                    .attr("topic", commandGetSchema.getTopic())
                    .attr("requestId", commandGetSchema.getRequestId())
                    .log("Received CommandGetSchema");
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
        log.debug("Received CommandGetOrCreateSchema call");
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
        log.debug()
                .attr("requestId", requestId)
                .attr("tcId", tcId)
                .log("Receive tc client connect request to transaction meta store from.");

        if (!checkTransactionEnableAndSendError(requestId)) {
            return;
        }

        TransactionMetadataStoreService transactionMetadataStoreService =
                service.pulsar().getTransactionMetadataStoreService();

        transactionMetadataStoreService.handleTcClientConnect(tcId).thenAccept(connection -> {
            log.debug()
                    .attr("requestId", requestId)
                    .attr("tcId", tcId)
                    .log("Handle tc client connect request to transaction meta store from success.");
            commandSender.sendTcClientConnectResponse(requestId);
        }).exceptionally(e -> {
            log.error()
                    .attr("requestId", requestId)
                    .attr("tcId", tcId)
                    .log("Handle tc client connect request to transaction meta store from fail.");
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
            log.debug().attr("op", op).log("The Coordinator was not found for the request");
            return cause;
        }
        if (cause instanceof ManagedLedgerException.ManagedLedgerFencedException) {
            log.debug()
                    .attr("op", op)
                    .log("Throw a CoordinatorNotFoundException to client "
                            + "with the message got from a ManagedLedgerFencedException for the request");
            return new CoordinatorException.CoordinatorNotFoundException(cause.getMessage());

        }
        log.error()
                .attr("op", op)
                .attr("requestId", requestId)
                .exception(cause)
                .log("Send response error for request.");
        return cause;
    }
    @Override
    protected void handleNewTxn(CommandNewTxn command) {
        checkArgument(state == State.Connected);
        final long requestId = command.getRequestId();
        final TransactionCoordinatorID tcId = TransactionCoordinatorID.get(command.getTcId());
        log.debug()
                .attr("requestId", requestId)
                .attr("tcId", tcId)
                .log("Receive new txn request to transaction meta store from.");

        if (!checkTransactionEnableAndSendError(requestId)) {
            return;
        }

        TransactionMetadataStoreService transactionMetadataStoreService =
                service.pulsar().getTransactionMetadataStoreService();
        final String owner = getPrincipal();
        transactionMetadataStoreService.newTransaction(tcId, command.getTxnTtlSeconds(), owner)
            .whenComplete(((txnID, ex) -> {
                if (ex == null) {
                    log.debug()
                            .attr("txnID", txnID)
                            .attr("requestId", requestId)
                            .log("Send response for new txn request");
                    commandSender.sendNewTxnResponse(requestId, txnID, tcId.getId());
                } else {
                    if (ex instanceof CoordinatorException.ReachMaxActiveTxnException) {
                        // if new txn throw ReachMaxActiveTxnException, don't return any response to client,
                        // otherwise client will retry, it will wast o lot of resources
                        // link https://github.com/apache/pulsar/issues/15133
                        log.warn()
                                .attr("tcId", tcId.getId())
                                .attr("requestId", requestId)
                                .exception(ex)
                                .log("New txn op reached max active transactions");
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
        partitionsList.forEach(partition ->
                log.debug()
                        .attr("requestId", requestId)
                        .attr("txnID", txnID)
                        .attr("partition", partition)
                        .log("Receive add published partition to txn request " + "from with txnId, topic"));

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
                        log.debug()
                                .attr("requestId", requestId)
                                .log("Send response success for add published partition to txn request");
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
        log.warn().attr("msg", msg).log("");
        return CompletableFuture.failedFuture(new CoordinatorException.TransactionNotFoundException(msg));
    }

    private CompletableFuture<Void> failedFutureTxnTcNotAllowed(TxnID txnID) {
        String msg = String.format(
                "TC client (%s) is not a super user, and is not allowed to operate on transaction %s",
                getPrincipal(), txnID
        );
        log.warn().attr("msg", msg).log("");
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

        log.debug()
                .attr("topic", topic)
                .attr("txnID", txnID)
                .attr("txnAction", txnAction)
                .log("handleEndTxnOnPartition");
        TopicName topicName = TopicName.get(topic);
        CompletableFuture<Optional<Topic>> topicFuture = service.getTopicIfExists(topicName.toString());
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
                                log.error()
                                        .attr("topic", topic)
                                        .attr("txnID", txnID)
                                        .attr("txnAction", TxnAction.valueOf(txnAction))
                                        .exception(throwable)
                                        .log("handleEndTxnOnPartition fail");
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
                getBrokerService().getManagedLedgerFactoryForTopic(topicName)
                        .thenCompose(managedLedgerFactory -> {
                            return managedLedgerFactory.asyncExists(topicName.getPersistenceNamingEncoding())
                                    .thenAccept((b) -> {
                                        if (b) {
                                            log.error()
                                                    .attr("topic", topic)
                                                    .attr("txnID", txnID)
                                                    .attr("txnAction", TxnAction.valueOf(txnAction))
                                                    .log("handleEndTxnOnPartition fail ! The topic does not exist in "
                                                            + "broker");
                                            writeAndFlush(Commands.newEndTxnOnPartitionResponse(requestId,
                                                    ServerError.ServiceNotReady,
                                                    "The topic " + topic + " does not exist in broker.",
                                                    txnID.getLeastSigBits(), txnID.getMostSigBits()));
                                        } else {
                                            log.warn()
                                                    .attr("topic", topic)
                                                    .attr("txnID", txnID)
                                                    .attr("txnAction", TxnAction.valueOf(txnAction))
                                                    .log("handleEndTxnOnPartition: topic not created");
                                            writeAndFlush(Commands.newEndTxnOnPartitionResponse(requestId,
                                                    txnID.getLeastSigBits(), txnID.getMostSigBits()));
                                        }
                                    });
                        }).exceptionally(e -> {
                            log.error()
                                    .attr("topic", topic)
                                    .attr("txnID", txnID)
                                    .attr("txnAction", TxnAction.valueOf(txnAction))
                                    .log("handleEndTxnOnPartition fail");
                            writeAndFlush(Commands.newEndTxnOnPartitionResponse(
                                    requestId, ServerError.ServiceNotReady,
                                    e.getMessage(), txnID.getLeastSigBits(), txnID.getMostSigBits()));
                            return null;

                        });
            }
        }, ctx.executor()).exceptionally(e -> {
            log.error()
                    .attr("topic", topic)
                    .attr("txnID", txnID)
                    .attr("txnAction", TxnAction.valueOf(txnAction))
                    .log("handleEndTxnOnPartition fail");
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

        log.debug()
                .attr("topic", topic)
                .attr("subscription", subName)
                .attr("txnID", new TxnID(txnidMostBits, txnidLeastBits))
                .attr("txnAction", txnAction)
                .log("handleEndTxnOnSubscription");

        TopicName topicName = TopicName.get(topic);
        CompletableFuture<Optional<Topic>> topicFuture = service.getTopicIfExists(topicName.toString());
        topicFuture.thenAcceptAsync(optionalTopic -> {
            if (optionalTopic.isPresent()) {
                Subscription subscription = optionalTopic.get().getSubscription(subName);
                if (subscription == null) {
                    log.warn()
                            .attr("topic", optionalTopic.get().getName())
                            .attr("subscription", subName)
                            .attr("txnID", txnID)
                            .attr("txnAction", TxnAction.valueOf(txnAction))
                            .log("handleEndTxnOnSubscription fail! topic subscription does not exist");
                    writeAndFlush(
                            Commands.newEndTxnOnSubscriptionResponse(requestId, txnidLeastBits, txnidMostBits));
                    return;
                }
                // we only accept super user because this endpoint is reserved for tc to broker communication
                isSuperUser()
                        .thenCompose(isOwner -> {
                            if (!isOwner) {
                                return failedFutureTxnTcNotAllowed(txnID);
                            }
                            return subscription.endTxn(txnidMostBits, txnidLeastBits, txnAction, lowWaterMark);
                        }).whenComplete((ignored, e) -> {
                            if (e != null) {
                                e = FutureUtil.unwrapCompletionException(e);
                                log.error()
                                        .attr("topic", topic)
                                        .attr("subscription", subName)
                                        .attr("txnID", txnID)
                                        .attr("txnAction", TxnAction.valueOf(txnAction))
                                        .log("handleEndTxnOnSubscription failed");
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
                getBrokerService().getManagedLedgerFactoryForTopic(topicName)
                        .thenCompose(managedLedgerFactory -> {
                            return managedLedgerFactory.asyncExists(topicName.getPersistenceNamingEncoding())
                                    .thenAccept((b) -> {
                                        if (b) {
                                            log.error()
                                                    .attr("topic", topic)
                                                    .attr("subscription", subName)
                                                    .attr("txnID", txnID)
                                                    .attr("txnAction", TxnAction.valueOf(txnAction))
                                                    .log("handleEndTxnOnSubscription failed: "
                                                            + "the topic does not exist in broker");
                                            writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                                                    requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(),
                                                    ServerError.ServiceNotReady,
                                                    "The topic " + topic + " does not exist in broker."));
                                        } else {
                                            log.warn()
                                                    .attr("topic", topic)
                                                    .attr("subscription", subName)
                                                    .attr("txnID", txnID)
                                                    .attr("txnAction", TxnAction.valueOf(txnAction))
                                                    .log("handleEndTxnOnSubscription failed: "
                                                            + "the topic has not been created");
                                            writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(requestId,
                                                    txnID.getLeastSigBits(), txnID.getMostSigBits()));
                                        }
                                    });
                        }).exceptionally(e -> {
                            log.error()
                                    .attr("topic", topic)
                                    .attr("subscription", subName)
                                    .attr("txnID", txnID)
                                    .attr("txnAction", TxnAction.valueOf(txnAction))
                                    .log("handleEndTxnOnSubscription failed");
                            writeAndFlush(Commands.newEndTxnOnSubscriptionResponse(
                                    requestId, txnID.getLeastSigBits(), txnID.getMostSigBits(),
                                    ServerError.ServiceNotReady, e.getMessage()));
                            return null;
                        });
            }
        }, ctx.executor()).exceptionally(e -> {
            log.error()
                    .attr("topic", topic)
                    .attr("subscription", subName)
                    .attr("txnID", txnID)
                    .attr("txnAction", TxnAction.valueOf(txnAction))
                    .log("handleEndTxnOnSubscription failed");
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
                log.debug()
                        .attr("topic", topic.getName())
                        .attr("hasSchema", hasSchema)
                        .log("configured with schema");
                CompletableFuture<SchemaVersion> result = new CompletableFuture<>();
                if (hasSchema && (schemaValidationEnforced || topic.getSchemaValidationEnforced())) {
                    result.completeExceptionally(new IncompatibleSchemaException(
                            "Producers cannot connect or send message without a schema to topics with a schema"
                         + "when SchemaValidationEnforced is enabled"));
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
        log.debug()
                .attr("requestId", requestId)
                .attr("txnID", txnID)
                .log("Receive add published partition to txn request from with txnId");

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
                        log.debug()
                                .attr("requestId", requestId)
                                .log("Send response success for add published partition to txn request");
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
    protected void handleCommandWatchTopicList(CommandWatchTopicList commandWatchTopicListParam) {
        checkArgument(state == State.Connected);

        // make a copy since command is handled asynchronously
        CommandWatchTopicList commandWatchTopicList = new CommandWatchTopicList().copyFrom(commandWatchTopicListParam);

        final long requestId = commandWatchTopicList.getRequestId();
        final long watcherId = commandWatchTopicList.getWatcherId();
        final NamespaceName namespaceName = NamespaceName.get(commandWatchTopicList.getNamespace());

        final Semaphore lookupSemaphore = service.getLookupRequestSemaphore();
        if (lookupSemaphore.tryAcquire()) {
            isNamespaceOperationAllowed(namespaceName, NamespaceOperation.GET_TOPICS).thenApply(isAuthorized -> {
                if (isAuthorized) {
                    String topicsPatternString = commandWatchTopicList.hasTopicsPattern()
                            ? commandWatchTopicList.getTopicsPattern() : TopicList.ALL_TOPICS_PATTERN;
                    String topicsHash = commandWatchTopicList.hasTopicsHash()
                            ? commandWatchTopicList.getTopicsHash() : null;
                    topicListService.handleWatchTopicList(namespaceName, watcherId, requestId, topicsPatternString,
                            topicsPatternImplementation, topicsHash, lookupSemaphore);
                } else {
                    final String msg = "Proxy Client is not authorized to watchTopicList";
                    log.warn()
                            .attr("principal", getPrincipal())
                            .attr("namespace", namespaceName)
                            .log(msg);
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
            log.debug()
                    .attr("namespace", namespaceName)
                    .log("Failed WatchTopicList due to too many lookup-requests");
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
        // removes producer-connection from map and send close command to producer
        safelyRemoveProducer(producer);
        closeProducer(producer.getProducerId(), producer.getEpoch(), Optional.empty());
    }

    @Override
    public void closeProducer(Producer producer, Optional<BrokerLookupData> assignedBrokerLookupData) {
        // removes producer-connection from map and send close command to producer
        safelyRemoveProducer(producer);
        closeProducer(producer.getProducerId(), producer.getEpoch(), assignedBrokerLookupData);
    }

    private LookupData getLookupData(BrokerLookupData lookupData) {
        LookupOptions.LookupOptionsBuilder builder = LookupOptions.builder();
        if (StringUtils.isNotBlank((listenerName))) {
            builder.advertisedListenerName(listenerName);
        }
        try {
            return lookupData.toLookupResult(builder.build()).getLookupData();
        } catch (PulsarServerException e) {
            log.error().exception(e).log("Failed to get lookup data");
            throw new RuntimeException(e);
        }
    }

    private void closeProducer(long producerId, long epoch, Optional<BrokerLookupData> assignedBrokerLookupData) {
        if (getRemoteEndpointProtocolVersion() >= v5.getValue()) {
            assignedBrokerLookupData.ifPresentOrElse(lookup -> {
                        LookupData lookupData = getLookupData(lookup);
                        writeAndFlush(Commands.newCloseProducer(producerId, -1L,
                                lookupData.getBrokerUrl(),
                                lookupData.getBrokerUrlTls()));
                    },
                    () -> writeAndFlush(Commands.newCloseProducer(producerId, -1L)));

            // The client does not necessarily know that the producer is closed, but the connection is still
            // active, and there could be messages in flight already. We want to ignore these messages for a time
            // because they are expected. Once the interval has passed, the client should have received the
            // CloseProducer command and should not send any additional messages until it sends a create Producer
            // command.
            recentlyClosedProducers.put(producerId, epoch);
            ctx.executor().schedule(() -> {
                recentlyClosedProducers.remove(producerId, epoch);
            }, service.getKeepAliveIntervalSeconds(), TimeUnit.SECONDS);
        } else {
            close();
        }

    }

    @Override
    public void closeConsumer(Consumer consumer, Optional<BrokerLookupData> assignedBrokerLookupData) {
        // removes consumer-connection from map and send close command to consumer
        safelyRemoveConsumer(consumer);
        closeConsumer(consumer.consumerId(), assignedBrokerLookupData);
    }

    private void closeConsumer(long consumerId, Optional<BrokerLookupData> assignedBrokerLookupData) {
        if (getRemoteEndpointProtocolVersion() >= v5.getValue()) {
            assignedBrokerLookupData.ifPresentOrElse(lookup -> {
                        LookupData lookupData = getLookupData(lookup);
                        writeCloseConsumerAndCloseConnectionOnFailure(Commands.newCloseConsumer(consumerId, -1L,
                                lookupData.getBrokerUrl(),
                                lookupData.getBrokerUrlTls()), consumerId);
                    },
                    () -> writeCloseConsumerAndCloseConnectionOnFailure(
                            Commands.newCloseConsumer(consumerId, -1L, null, null), consumerId));
        } else {
            close();
        }
    }

    private void writeCloseConsumerAndCloseConnectionOnFailure(ByteBuf cmd, long consumerId) {
        ctx.writeAndFlush(cmd).addListener(future -> {
            if (!future.isSuccess()) {
                log.warn()
                        .attr("consumerId", consumerId)
                        .exception(future.cause())
                        .log("Forcing connection to close since cannot send close consumer command");
                close();
            }
        });
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
        log.debug()
                .attr("producerId", producerId)
                .attr("producer", producer)
                .log("Removed producer");
        CompletableFuture<Producer> future = producers.get(producerId);
        if (future != null) {
            future.whenCompleteAsync((producer2, exception) -> {
                    if (exception != null || producer2 == producer) {
                        producers.remove(producerId, future);
                    }
                }, ctx.executor());
        }
    }

    private void safelyRemoveConsumer(Consumer consumer) {
        long consumerId = consumer.consumerId();
        log.debug()
                .attr("consumerId", consumerId)
                .attr("consumer", consumer)
                .log("Removed consumer");
        CompletableFuture<Consumer> future = consumers.get(consumerId);
        if (future != null) {
            future.whenCompleteAsync((consumer2, exception) -> {
                    if (exception != null || consumer2 == consumer) {
                        consumers.remove(consumerId, future);
                    }
                }, ctx.executor());
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

    // handle throttling based on pending send requests in the same connection
    // or the pending publish bytes
    private void increasePendingSendRequestsAndPublishBytes(int msgSize) {
        if (++pendingSendRequest == maxPendingSendRequests) {
            throttleTracker.markThrottled(ThrottleType.ConnectionMaxPendingPublishRequestsExceeded);
        }
        PendingBytesPerThreadTracker.getInstance().incrementPublishBytes(msgSize, maxPendingBytesPerThread);
    }


    /**
     * Increase the throttling metric for the topic when a producer is throttled.
     */
    void increasePublishLimitedTimesForTopics() {
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
        PendingBytesPerThreadTracker.getInstance().decrementPublishBytes(msgSize, resumeThresholdPendingBytesPerThread);

        if (--pendingSendRequest == resumeReadsThreshold) {
            throttleTracker.unmarkThrottled(ThrottleType.ConnectionMaxPendingPublishRequestsExceeded);
        }

        if (isNonPersistentTopic) {
            nonPersistentPendingMessages--;
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
                log.error().exception(e).log(finalErrorMessage);
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
                log.warn()
                        .attr("topic", topic)
                        .attr("producerName", producerName)
                        .log("Failed to remove TCP no-delay property on client cnx");
            }
        }
    }

    private TopicName validateTopicName(String topic, long requestId, Object requestCommand) {
        try {
            return TopicName.get(topic);
        } catch (Throwable t) {
            log.debug()
                    .attr("topic", topic)
                    .exception(t)
                    .log("Failed to parse topic name ''");

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
                log.error().exception(e).log("Exception occur when intercept messages.");
            }
        }
        return res;
    }
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

    @Override
    public String clientSourceAddressAndPort() {
        if (clientSourceAddressAndPort == null) {
            if (hasHAProxyMessage()) {
                clientSourceAddressAndPort =
                        getHAProxyMessage().sourceAddress() + ":" + getHAProxyMessage().sourcePort();
            } else {
                clientSourceAddressAndPort = clientAddress().toString();
            }
        }
        return clientSourceAddressAndPort;
    }

    CompletableFuture<Optional<Boolean>> connectionCheckInProgress;

    @Override
    public CompletableFuture<Optional<Boolean>> checkConnectionLiveness() {
        if (!isActive()) {
            return CompletableFuture.completedFuture(Optional.of(false));
        }
        if (connectionLivenessCheckTimeoutMillis > 0) {
            return NettyFutureUtil.toCompletableFuture(ctx.executor().submit(() -> {
                if (!isActive()) {
                    return CompletableFuture.completedFuture(Optional.of(false));
                }
                if (connectionCheckInProgress != null) {
                    return connectionCheckInProgress;
                } else {
                    final CompletableFuture<Optional<Boolean>> finalConnectionCheckInProgress =
                            new CompletableFuture<>();
                    connectionCheckInProgress = finalConnectionCheckInProgress;
                    ctx.executor().schedule(() -> {
                        if (!isActive()) {
                            finalConnectionCheckInProgress.complete(Optional.of(false));
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
                            log.warn("Connection check timed out, closing connection");
                            ctx.close();
                        } else {
                            log.error("Reached unexpected code block, completing connection check");
                            finalConnectionCheckInProgress.complete(Optional.of(true));
                        }
                    }, connectionLivenessCheckTimeoutMillis, TimeUnit.MILLISECONDS);
                    sendPing();
                    return finalConnectionCheckInProgress;
                }
            })).thenCompose(java.util.function.Function.identity());
        } else {
            // check is disabled
            return CompletableFuture.completedFuture(Optional.empty());
        }
    }

    @Override
    protected void messageReceived(BaseCommand cmd) {
        checkPauseReceivingRequestsAfterResumeRateLimit(cmd);
        super.messageReceived(cmd);
        if (connectionCheckInProgress != null && !connectionCheckInProgress.isDone()) {
            connectionCheckInProgress.complete(Optional.of(true));
            connectionCheckInProgress = null;
        }
    }

    private static void logAuthException(SocketAddress remoteAddress, String operation,
                                         String principal, Optional<TopicName> topic, Throwable ex) {
        String topicString = topic.map(t -> ", topic=" + t.toString()).orElse("");
        Throwable actEx = FutureUtil.unwrapCompletionException(ex);
        if (actEx instanceof AuthenticationException) {
            LOG.info()
                    .attr("remoteAddress", remoteAddress)
                    .attr("operation", operation)
                    .attr("principal", principal)
                    .attr("topicString", topicString)
                    .exceptionMessage(actEx)
                    .log("Failed to authenticate");
            return;
        } else if (actEx instanceof WebApplicationException restException){
            // Do not print error log if users tries to access a not found resource.
            if (restException.getResponse().getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                LOG.info()
                        .attr("remoteAddress", remoteAddress)
                        .attr("operation", operation)
                        .attr("principal", principal)
                        .attr("topicString", topicString)
                        .exceptionMessage(actEx)
                        .log("Trying to authenticate for a topic which under a namespace not exists");
                return;
            }
        }
        LOG.error()
                .attr("remoteAddress", remoteAddress)
                .attr("operation", operation)
                .attr("principal", principal)
                .attr("topicString", topicString)
                .exception(ex)
                .log("Error trying to authenticate");
    }

    private static void logNamespaceNameAuthException(SocketAddress remoteAddress, String operation,
                                         String principal, Optional<NamespaceName> namespaceName, Throwable ex) {
        String namespaceNameString = namespaceName.map(t -> ", namespace=" + t.toString()).orElse("");
        if (ex instanceof AuthenticationException) {
            LOG.info()
                    .attr("remoteAddress", remoteAddress)
                    .attr("operation", operation)
                    .attr("principal", principal)
                    .attr("namespaceNameString", namespaceNameString)
                    .exceptionMessage(ex)
                    .log("Failed to authenticate");
        } else {
            LOG.error()
                    .attr("remoteAddress", remoteAddress)
                    .attr("operation", operation)
                    .attr("principal", principal)
                    .attr("namespaceNameString", namespaceNameString)
                    .exception(ex)
                    .log("Error trying to authenticate");
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

    @VisibleForTesting
    void setAuthState(AuthenticationState authState) {
        this.authState = authState;
    }
}
