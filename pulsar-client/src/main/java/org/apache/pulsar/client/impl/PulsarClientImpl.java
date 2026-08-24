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

import static org.apache.commons.lang3.StringUtils.isBlank;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.NameResolver;
import io.netty.util.Timer;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServicesAware;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.client.impl.auth.v5.DefaultClientAuthenticationServices;
import org.apache.pulsar.client.impl.auth.v5.FrameworkHttpClientFactory;
import org.apache.pulsar.client.impl.auth.v5.V5AuthenticationLoader;
import org.apache.pulsar.client.impl.auth.v5.V5BinaryAuthenticationDriver;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.impl.metrics.MemoryBufferStats;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.client.impl.tls.ClientTlsFactorySupport;
import org.apache.pulsar.client.impl.transaction.TransactionBuilderImpl;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.DnsResolverUtil;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.jspecify.annotations.Nullable;

@CustomLog
public class PulsarClientImpl implements PulsarClient {
    private static final int CLOSE_TIMEOUT_SECONDS = 60;
    protected static final double THRESHOLD_FOR_CONSUMER_RECEIVER_QUEUE_SIZE_SHRINKING = 0.95;

    // default limits for producers when memory limit controller is disabled
    private static final int NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES = 1000;
    private static final int NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS = 50000;

    protected final ClientConfigurationData conf;
    private final boolean createdExecutorProviders;

    private final boolean createdScheduledProviders;
    private final boolean createdLookupProviders;
    private volatile LookupService lookup;
    private Map<String, LookupService> urlLookupMap = new ConcurrentHashMap<>();
    private final ConnectionPool cnxPool;
    @Getter
    private final Timer timer;
    private boolean needStopTimer;
    private boolean serviceUrlProviderInitialized;
    private final ExecutorProvider externalExecutorProvider;
    private final ExecutorProvider internalExecutorProvider;
    private final ExecutorProvider lookupExecutorProvider;

    private final ScheduledExecutorProvider scheduledExecutorProvider;

    // PIP-478: AutoClusterFailover mutates the TLS material/identity on `conf` at runtime
    // (updateTlsTrustCertsFilePath / updateAuthentication). The client TLS factory is composed once from that
    // material, so a mutation would otherwise never reach it. Rebuild-not-mutate: rebuild the factory from the
    // updated conf and swap it into the binary connection pool so NEW connections use the per-target trust /
    // identity. Only the framework-built default file-based factory is rebuildable — a v5-adopted custom
    // factory / policy map owns its own material and is left untouched. Superseded factories are retained until
    // client close (a shared HTTP-lookup subscription may still read the previous factory, and existing binary
    // connections keep their contexts alive via their own refcount), so they are not closed eagerly.
    private final Object tlsFactoryRebuildLock = new Object();
    private volatile boolean clientTlsFactoryRebuildable;
    private volatile boolean clientComposedTlsFactory;
    private final List<PulsarTlsFactory> supersededTlsFactories = new CopyOnWriteArrayList<>();

    // PIP-478: a stable id for the owning client (logging correlation), plus a small bounded,
    // cached blocking executor created on demand and reaped when idle. It off-loads potentially-blocking
    // authentication work (credential I/O, legacy v4 plugin calls) so it never runs on the Netty event
    // loop. Created only when the configured auth actually wants framework services.
    private static final AtomicLong CLIENT_INSTANCE_ID_GENERATOR = new AtomicLong();
    private static final int AUTH_BLOCKING_MAX_THREADS = 16;
    private final String clientInstanceId = "pulsar-client-" + CLIENT_INSTANCE_ID_GENERATOR.incrementAndGet();
    private volatile ExecutorService blockingAuthExecutor;
    // PIP-478: the framework HTTP client factory handed to the authentication the client drives. Built for
    // every client, because the driver is resolved for every client; it holds only suppliers until an auth
    // plugin actually asks for a client. Closed with the client, and on each updateAuthentication rebind.
    private volatile FrameworkHttpClientFactory authHttpClientFactory;
    // PIP-478: the framework services handed to the authentication the client drives, rebuilt alongside
    // authHttpClientFactory on every updateAuthentication rebind.
    private volatile ClientAuthenticationServices authServices;

    private final boolean createdEventLoopGroup;
    private final boolean createdCnxPool;
    private final DnsResolverGroupImpl dnsResolverGroupLocalInstance;
    @Getter
    private final AddressResolver<InetSocketAddress> addressResolver;

    public enum State {
        Open, Closing, Closed
    }

    private final AtomicReference<State> state = new AtomicReference<>();
    // These sets are updated from multiple threads, so they require a threadsafe data structure
    private final Set<ProducerBase<?>> producers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<ConsumerBase<?>> consumers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final AtomicLong producerIdGenerator = new AtomicLong();
    private final AtomicLong consumerIdGenerator = new AtomicLong();
    private final AtomicLong topicListWatcherIdGenerator = new AtomicLong();
    private final AtomicLong requestIdGenerator =
            new AtomicLong(ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE / 2));

    protected final EventLoopGroup eventLoopGroup;
    private final MemoryLimitController memoryLimitController;
    private final MemoryBufferStats memoryBufferStats;

    private final LoadingCache<String, SchemaInfoProvider> schemaProviderLoadingCache =
            CacheBuilder.newBuilder().maximumSize(100000)
                    .expireAfterAccess(30, TimeUnit.MINUTES)
                    .build(new CacheLoader<String, SchemaInfoProvider>() {

                        @Override
                        public SchemaInfoProvider load(String topicName) {
                            return newSchemaProvider(topicName);
                        }
                    });

    private final Clock clientClock;

    private final InstrumentProvider instrumentProvider;

    @Getter
    private TransactionCoordinatorClientImpl tcClient;

    private final Runnable memoryLimitTrigger = this::reduceConsumerReceiverQueueSize;

    public PulsarClientImpl(ClientConfigurationData conf) throws PulsarClientException {
        this(conf, null, null, null, null, null, null, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this(conf, eventLoopGroup, null, null, null, null, null, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool)
            throws PulsarClientException {
        this(conf, eventLoopGroup, cnxPool, null, null, null, null, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool,
                            Timer timer)
            throws PulsarClientException {
        this(conf, eventLoopGroup, cnxPool, timer, null, null, null, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool connectionPool,
                            Timer timer, ExecutorProvider externalExecutorProvider,
                            ExecutorProvider internalExecutorProvider,
                            ScheduledExecutorProvider scheduledExecutorProvider)
            throws PulsarClientException {
        this(conf, eventLoopGroup, connectionPool, timer, externalExecutorProvider, internalExecutorProvider,
                scheduledExecutorProvider, null, null, null);
    }

    @Builder(builderClassName = "PulsarClientImplBuilder")
    PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool connectionPool,
                     Timer timer, ExecutorProvider externalExecutorProvider,
                     ExecutorProvider internalExecutorProvider,
                     ScheduledExecutorProvider scheduledExecutorProvider,
                     ExecutorProvider lookupExecutorProvider,
                     DnsResolverGroupImpl dnsResolverGroup,
                     MemoryLimitController memoryLimitController) throws PulsarClientException {
        EventLoopGroup eventLoopGroupReference = null;
        ConnectionPool connectionPoolReference = null;
        try {
            if (conf == null || isBlank(conf.getServiceUrl())) {
                throw new PulsarClientException.InvalidConfigurationException("Invalid client configuration");
            }
            this.conf = conf;
            this.createdEventLoopGroup = eventLoopGroup == null;
            this.createdCnxPool = connectionPool == null;
            if ((externalExecutorProvider == null) != (internalExecutorProvider == null)) {
                throw new IllegalArgumentException(
                        "Both externalExecutorProvider and internalExecutorProvider must be specified or unspecified.");
            }
            this.createdExecutorProviders = externalExecutorProvider == null;
            this.createdScheduledProviders = scheduledExecutorProvider == null;
            this.createdLookupProviders = lookupExecutorProvider == null;
            eventLoopGroupReference = eventLoopGroup != null ? eventLoopGroup :
                    PulsarClientResourcesConfigurer.createEventLoopGroup(conf);
            this.eventLoopGroup = eventLoopGroupReference;
            this.instrumentProvider = new InstrumentProvider(conf.getOpenTelemetry());
            clientClock = conf.getClock();
            this.scheduledExecutorProvider = scheduledExecutorProvider != null ? scheduledExecutorProvider :
                    PulsarClientResourcesConfigurer.createScheduledExecutorProvider(conf);
            // PIP-478: resolve the client-side TLS SPI factory (new path) before the connection
            // pool and HTTP lookup are created — both read conf.getTlsFactory() to branch onto it.
            setupClientTlsFactory();
            if (connectionPool != null) {
                connectionPoolReference = connectionPool;
                dnsResolverGroupLocalInstance = null;
                addressResolver = dnsResolverGroup != null
                        ? dnsResolverGroup.createAddressResolver(eventLoopGroupReference) : null;
            } else {
                DnsResolverGroupImpl dnsResolverGroupReference;
                if (dnsResolverGroup == null) {
                    dnsResolverGroupReference =
                            dnsResolverGroupLocalInstance =
                                    PulsarClientResourcesConfigurer.createDnsResolverGroup(conf
                                    );
                } else {
                    dnsResolverGroupReference = dnsResolverGroup;
                    dnsResolverGroupLocalInstance = null;
                }
                addressResolver = dnsResolverGroupReference.createAddressResolver(eventLoopGroupReference);
                connectionPoolReference = ConnectionPool.builder()
                        .instrumentProvider(instrumentProvider)
                        .conf(conf)
                        .eventLoopGroup(eventLoopGroupReference)
                        .addressResolverSupplier(Optional.of(() -> addressResolver))
                        .scheduledExecutorService(
                                (ScheduledExecutorService) this.scheduledExecutorProvider.getExecutor())
                        .build();
            }
            this.cnxPool = connectionPoolReference;
            this.externalExecutorProvider = externalExecutorProvider != null ? externalExecutorProvider :
                    PulsarClientResourcesConfigurer.createExternalExecutorProvider(conf);
            this.internalExecutorProvider = internalExecutorProvider != null ? internalExecutorProvider :
                    PulsarClientResourcesConfigurer.createInternalExecutorProvider(conf);
            this.lookupExecutorProvider = lookupExecutorProvider != null ? lookupExecutorProvider :
                    PulsarClientResourcesConfigurer.createLookupExecutorProvider();
            if (timer == null) {
                this.timer = PulsarClientResourcesConfigurer.createTimer();
                needStopTimer = true;
            } else {
                this.timer = timer;
            }
            // PIP-478: late-bind the client's framework services (scheduler, bounded blocking
            // executor, framework HTTP client factory, client instance id) into the authentication driver and
            // start it — AFTER the shared event loop, timer, DNS resolver and TLS factory exist, so a plugin
            // whose start()/initializeAsync(...) reaches for a framework HTTP client (OAuth2, Athenz) gets one
            // backed by the fully-shared resources. bind precedes start() so the plugin sees the services from
            // its first call. Nothing between the config above and here consumes started authentication.
            bindAuthenticationServices(conf.getAuthentication());
            conf.getAuthentication().start();
            resolveV5Authentication();
            lookup = createLookup(conf.getServiceUrl());

            if (conf.getServiceUrlProvider() != null) {
                conf.getServiceUrlProvider().initialize(this);
                serviceUrlProviderInitialized = true;
            }

            if (conf.isEnableTransaction()) {
                tcClient = new TransactionCoordinatorClientImpl(this);
                try {
                    tcClient.start();
                } catch (Throwable e) {
                    log.error().exception(e).log("Start transactionCoordinatorClient error.");
                    throw new PulsarClientException(e);
                }
            }

            if (memoryLimitController == null) {
                this.memoryLimitController = new MemoryLimitController(conf.getMemoryLimitBytes(),
                        (long) (conf.getMemoryLimitBytes() * THRESHOLD_FOR_CONSUMER_RECEIVER_QUEUE_SIZE_SHRINKING),
                        this.memoryLimitTrigger);
            } else {
                this.memoryLimitController = memoryLimitController;
                this.memoryLimitController.registerTrigger(this.memoryLimitTrigger);
            }
            // Only create memory buffer metrics if memory limit controller is local and memory limiting is enabled.
            if (memoryLimitController == null && this.memoryLimitController.isMemoryLimited()) {
                this.memoryBufferStats = new MemoryBufferStats(this.instrumentProvider, this.memoryLimitController);
            } else {
                this.memoryBufferStats = null;
            }
            state.set(State.Open);
        } catch (Throwable t) {
            // Log the exception first, or it could be missed if there are any subsequent exceptions in the
            // shutdown sequence
            log.error().exception(t).log("Failed to create Pulsar client instance.");
            shutdown();
            shutdownEventLoopGroup(eventLoopGroupReference);
            closeCnxPool(connectionPoolReference);
            throw t;
        }
    }

    /**
     * Late-bind the client's framework services into the configured authentication driver if it wants
     * them (PIP-478). Called before {@code Authentication.start()} so the plugin's
     * {@code initializeAsync(...)} sees real services. A plain v4 plugin that does not implement
     * {@link ClientAuthenticationServicesAware} is left untouched.
     *
     * @param authentication the configured authentication (never {@code null};
     *        {@code AuthenticationDisabled} when unset)
     */
    private void bindAuthenticationServices(Authentication authentication) {
        // Do not (re)bind into a client that is shutting down: a rebind racing close() would install a
        // FrameworkHttpClientFactory that close() has already passed, leaking it. state is null during
        // construction (the first bind), which must proceed.
        State currentState = state.get();
        if (currentState == State.Closing || currentState == State.Closed) {
            return;
        }
        // Close a previously-bound factory before overwriting it, else each updateAuthentication rebind
        // (AutoClusterFailover / ControlledClusterFailover) leaks one FrameworkHttpClientFactory.
        FrameworkHttpClientFactory previous = this.authHttpClientFactory;
        if (previous != null) {
            try {
                previous.close();
            } catch (Throwable t) {
                log.warn().exception(t)
                        .log("Failed to close previous framework HTTP client factory during auth rebind");
            }
        }
        // PIP-478: the framework HTTP client factory shares the client's event loop, timer, DNS
        // resolver and TLS factory, resolved lazily at newHttpClient() time via these suppliers.
        this.authHttpClientFactory = new FrameworkHttpClientFactory(
                () -> eventLoopGroup, () -> timer, this::getNameResolver, conf::getTlsFactory,
                conf, clientInstanceId);
        ClientAuthenticationServices services = new DefaultClientAuthenticationServices(
                authHttpClientFactory,
                (ScheduledExecutorService) scheduledExecutorProvider.getExecutor(),
                blockingAuthExecutor(),
                clientClock,
                conf.getOpenTelemetry() != null ? conf.getOpenTelemetry() : OpenTelemetry.noop(),
                clientInstanceId);
        this.authServices = services;
        if (authentication instanceof ClientAuthenticationServicesAware aware) {
            aware.bindClientAuthenticationServices(services);
        }
    }

    /**
     * Resolve the authentication the client drives (PIP-478). The client drives the v5 model natively, so
     * exactly one v5 {@code Authentication} is resolved per client and every connection opens an exchange
     * against it.
     *
     * <p>The v5 builder may already have configured a v5 plugin; otherwise the v4 slot is translated. A
     * built-in shim hands over its v5-native body (no bridge, no executor hop for a credential it holds in
     * memory); anything else is wrapped by {@link LegacyV4AuthenticationAdapter}. The wrap is the
     * already-started variant because the v4 instance stays in {@code conf} and this client both started it
     * above and closes it in {@link #shutdown()} — the adapter must not run that lifecycle a second time.
     */
    private void resolveV5Authentication() throws PulsarClientException {
        // The v5 slot holds only what was explicitly configured through the v5 builder. A body derived from
        // the v4 slot is deliberately NOT written back: build() hands this client the builder's own
        // configuration object, so a derived body stored here would outlive the build and be reused by the
        // next client from the same (or a cloned) builder — which, after authentication(...) was called
        // again, would drive the previous credential while conf.getAuthentication() showed the new one.
        // Deriving on every resolution keeps the answer a function of the current configuration.
        org.apache.pulsar.client.api.v5.auth.Authentication v5 = conf.getV5Authentication();
        boolean configuredDirectly = v5 != null;
        if (v5 == null) {
            v5 = V5AuthenticationLoader.forStartedV4Plugin(conf.getAuthentication());
        }
        V5BinaryAuthenticationDriver driver = new V5BinaryAuthenticationDriver(v5, authServices);
        if (configuredDirectly) {
            // A plugin configured through the v5 builder is initialized here, on the application thread, so a
            // misconfiguration fails the build (PIP-478). The plugins the client derives from the v4 slot
            // stay lazy: initializing a bridged plugin runs v4 code, which the v4 client does not do at
            // build time either.
            requireUsableForBinaryTransport(v5, driver);
        }
        conf.setV5AuthenticationDriver(driver);
    }

    /**
     * Fail the client build when the configured authentication cannot authenticate a binary connection.
     *
     * <p>The binary transport requires {@code BinaryAuthDataProvider} (PIP-478 binary routing rule 1). Without
     * this check the plugin builds fine and every connection attempt fails the same way for the client's
     * lifetime, with the reason buried in a connection failure rather than stated where the mistake was made.
     * Capabilities are only meaningful once the plugin has initialized, so the check follows initialization
     * rather than preceding it.
     */
    private void requireUsableForBinaryTransport(org.apache.pulsar.client.api.v5.auth.Authentication v5,
            V5BinaryAuthenticationDriver driver) throws PulsarClientException {
        try {
            driver.initializedAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
        if (v5.capability(BinaryAuthDataProvider.class).isEmpty()) {
            throw new PulsarClientException.UnsupportedAuthenticationException(
                    "authentication plugin " + v5.getClass().getName() + " does not expose "
                            + "BinaryAuthDataProvider; the Pulsar binary transport requires it (PIP-478)");
        }
    }

    /**
     * Resolve the client-side TLS SPI factory (PIP-478) and stash it on the configuration so the connection
     * pool ({@code PulsarChannelInitializer}) and the HTTP lookup ({@code HttpClient}) build engines from it.
     * This is the only client TLS path since the PIP-337 removal. It normally runs only when broker TLS is
     * enabled (a plaintext client leaves {@code conf.getTlsFactory()} null, and its transports never request
     * TLS); {@link #needsClientTlsFactory()} lists the two configurations that also need it on an otherwise
     * plaintext client. On the v5-builder path a fail-fast probe of {@code CLIENT_DEFAULT} runs, so a
     * bad configuration fails the client build.
     *
     * @throws PulsarClientException if the TLS factory cannot be built / initialized / probed
     */
    private void setupClientTlsFactory() throws PulsarClientException {
        // Rebuildable (PIP-478) only when the framework composes the default file-based factory from the
        // conf tls* fields; a v5-adopted custom factory or an explicit policy map self-manages its material and
        // must not be rebuilt from the conf fields on an AutoClusterFailover material update. Determined even
        // when no factory is composed now, so a later updateAuthentication that introduces OAuth2 IdP TLS
        // material (a failover swap from a non-OAuth2 cluster) can still build one.
        this.clientTlsFactoryRebuildable = conf.getTlsFactory() == null && conf.getTlsPolicyMap() == null;
        // Whether the factory in the slot is one this client composed, as opposed to one the application
        // adopted through the v5 builder. Only a composed factory may be cleared from the configuration on
        // shutdown — see the note there.
        this.clientComposedTlsFactory = conf.getTlsFactory() == null;
        if (!needsClientTlsFactory()) {
            return;
        }
        try {
            ScheduledExecutorService executor = (ScheduledExecutorService) scheduledExecutorProvider.getExecutor();
            var factory = ClientTlsFactorySupport.resolveClientTlsFactory(conf, executor, executor,
                    conf.getOpenTelemetry() != null ? conf.getOpenTelemetry() : OpenTelemetry.noop());
            conf.setTlsFactory(factory);
        } catch (Exception e) {
            throw new PulsarClientException.InvalidConfigurationException(e);
        }
    }

    /**
     * Whether a client TLS factory must be composed. Broker TLS is the usual trigger, but two configurations
     * need the factory even on a plaintext broker connection, because they describe a trust domain other than
     * the binary transport that would otherwise fall back silently to the platform default:
     *
     * <ul>
     *   <li>an explicit v5 {@code tlsPolicy(purpose, policy)} map — a policy for a non-transport purpose
     *       (notably {@code CLIENT_OAUTH2}) deliberately leaves {@code useTls} false, so gating on the
     *       transport flag alone would drop the very policy the caller configured; and</li>
     *   <li>an OAuth2 plugin carrying its own IdP TLS material (see {@link #hasOAuth2IdpTlsMaterial()}),
     *       which is the same trust domain reached through the plugin rather than the builder.</li>
     * </ul>
     *
     * <p>The broker connection stays plaintext in both cases: binary-transport TLS is gated on
     * {@code conf.isUseTls()} separately.
     *
     * @return whether the client TLS factory must be composed
     */
    private boolean needsClientTlsFactory() {
        // An adopted factory counts too. Resolution is what initializes it and puts it under this client's
        // ownership, so skipping it would leave a factory the client still closes on shutdown but never
        // initialized — and would serve an uninitialized factory to anything that asked. The admin path
        // applies the same rule (AsyncHttpConnector.needsTlsFactory), and pip-478.md states it for both.
        return conf.isUseTls() || hasExplicitTlsPolicies() || hasOAuth2IdpTlsMaterial()
                || conf.getTlsFactory() != null;
    }

    /**
     * Whether the caller configured TLS policies explicitly. The map is populated only by the v5 builder's
     * {@code tlsPolicy(...)}, so a non-empty map means the configuration carries trust material the client
     * must honour regardless of whether the broker transport uses TLS.
     *
     * @return whether an explicit TLS policy map is configured
     */
    private boolean hasExplicitTlsPolicies() {
        Map<TlsPurpose, TlsPolicy> policies = conf.getTlsPolicyMap();
        return policies != null && !policies.isEmpty();
    }

    /**
     * Whether the configured authentication is an OAuth2 plugin whose HTTPS IdP carries its own TLS material
     * (private-CA trust or mTLS identity) folded into {@code CLIENT_OAUTH2}. When true the client TLS factory
     * must be composed even on a plaintext broker connection, so the framework HTTP client that fetches IdP
     * metadata / tokens honours that trust instead of the platform default (v4 ran an independent OAuth2 client
     * that honoured IdP TLS regardless of broker TLS). The flow is created during {@code configure()} before
     * construction, so {@code idpTlsPolicy()} is resolvable here and after an {@link #updateAuthentication} swap.
     *
     * @return whether OAuth2 IdP TLS material is present to fold
     */
    private boolean hasOAuth2IdpTlsMaterial() {
        return conf.getAuthentication() instanceof AuthenticationOAuth2 oauth2 && oauth2.idpTlsPolicy().isPresent();
    }

    /**
     * Rebuild the client TLS factory from the current (just-mutated) {@link ClientConfigurationData} and swap
     * it into the binary connection pool so NEW connections pick up per-target trust roots / TLS identity
     * (PIP-478). Invoked when AutoClusterFailover updates the client's TLS material at runtime
     * ({@link #updateTlsTrustCertsFilePath} / {@link #updateTlsTrustStorePathAndPassword} /
     * {@link #updateAuthentication}). This is the rebuild-not-mutate counterpart of the once-composed factory:
     * without it the mutation never reaches the transport (a v4-parity regression).
     *
     * <p>No-op unless the factory is the framework-built default (a v5-adopted custom factory / policy map owns
     * its own material) and {@link #needsClientTlsFactory()} still holds — in practice that means broker TLS is
     * enabled or an OAuth2 plugin carries IdP TLS material to fold, since the explicit-policy arm of that
     * predicate implies a policy map, which is exactly what makes the factory non-rebuildable —
     * so a plaintext-broker OAuth2 swap (a different-CA issuer on cluster failover) still recomposes the folded
     * {@code CLIENT_OAUTH2} trust for the framework HTTP client. The superseded factory is retained until the
     * client closes rather than closed here: existing binary connections keep their already-acquired contexts
     * alive via their own refcount, and a shared HTTP-lookup subscription may still read the previous factory
     * (the HTTP client's TLS was fixed at build in v4 too, so it keeps the old material — consistent
     * behaviour). On a rebuild failure the previous factory is kept so the client stays usable; connections to
     * the new target then fail loudly at handshake rather than silently using the wrong trust.
     */
    private void rebuildClientTlsFactory() {
        if (!needsClientTlsFactory() || !clientTlsFactoryRebuildable) {
            return;
        }
        synchronized (tlsFactoryRebuildLock) {
            PulsarTlsFactory previous = conf.getTlsFactory();
            // Clear so resolveClientTlsFactory rebuilds from the updated conf rather than re-adopting `previous`.
            conf.setTlsFactory(null);
            PulsarTlsFactory rebuilt;
            try {
                ScheduledExecutorService executor =
                        (ScheduledExecutorService) scheduledExecutorProvider.getExecutor();
                rebuilt = ClientTlsFactorySupport.resolveClientTlsFactory(conf, executor, executor,
                        conf.getOpenTelemetry() != null ? conf.getOpenTelemetry() : OpenTelemetry.noop());
            } catch (Exception e) {
                // Keep the previous factory so the client stays functional; log loudly.
                conf.setTlsFactory(previous);
                log.error().exception(e).log("Failed to rebuild the client TLS factory after a runtime TLS "
                        + "material update; keeping the previous factory");
                return;
            }
            conf.setTlsFactory(rebuilt);
            cnxPool.updateClientTlsFactory(rebuilt);
            if (previous != null) {
                supersededTlsFactories.add(previous);
            }
        }
    }

    /**
     * Lazily create the bounded blocking executor for authentication credential I/O (PIP-478):
     * a small cached pool with zero core threads (created on demand, reaped after 60s idle) and a hard
     * upper bound. It is created only when an auth driver actually asks for framework services, and is
     * shut down when the client closes.
     *
     * @return the blocking executor
     */
    private synchronized Executor blockingAuthExecutor() {
        if (blockingAuthExecutor == null) {
            // Queue rather than reject. Every caller is a connection attempt or an authenticated request, so
            // a saturated pool must slow them down, not fail them: a SynchronousQueue with the default abort
            // policy turns a reconnect storm against a slow identity provider into failed connections. Core
            // threads are allowed to time out, so an unused pool still costs nothing.
            ThreadPoolExecutor executor = new ThreadPoolExecutor(AUTH_BLOCKING_MAX_THREADS,
                    AUTH_BLOCKING_MAX_THREADS, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                    new ExecutorProvider.ExtendedThreadFactory("pulsar-client-auth-blocking", true));
            executor.allowCoreThreadTimeOut(true);
            blockingAuthExecutor = executor;
        }
        return blockingAuthExecutor;
    }

    private void reduceConsumerReceiverQueueSize() {
        for (ConsumerBase<?> consumer : consumers) {
            consumer.reduceCurrentReceiverQueueSize();
        }
    }

    public ClientConfigurationData getConfiguration() {
        return conf;
    }

    public Clock getClientClock() {
        return clientClock;
    }

    public AtomicReference<State> getState() {
        return state;
    }

    @Override
    public ProducerBuilder<byte[]> newProducer() {
        return new ProducerBuilderImpl<>(this, Schema.BYTES);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
        ProducerBuilderImpl<T> producerBuilder = new ProducerBuilderImpl<>(this, schema);
        if (!memoryLimitController.isMemoryLimited()) {
            // set default limits for producers when memory limit controller is disabled
            producerBuilder.maxPendingMessages(NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES);
            producerBuilder.maxPendingMessagesAcrossPartitions(
                    NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS);
        }
        return producerBuilder;
    }

    @Override
    public ConsumerBuilder<byte[]> newConsumer() {
        return new ConsumerBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> ConsumerBuilder<T> newConsumer(Schema<T> schema) {
        return new ConsumerBuilderImpl<>(this, schema);
    }

    @Override
    public ReaderBuilder<byte[]> newReader() {
        return new ReaderBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> ReaderBuilder<T> newReader(Schema<T> schema) {
        return new ReaderBuilderImpl<>(this, schema);
    }

    /**
     * @deprecated use {@link #newTableView(Schema)} instead.
     */
    @Override
    @Deprecated
    public <T> TableViewBuilder<T> newTableViewBuilder(Schema<T> schema) {
        return new TableViewBuilderImpl<>(this, schema);
    }

    @Override
    public TableViewBuilder<byte[]> newTableView() {
        return new TableViewBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> TableViewBuilder<T> newTableView(Schema<T> schema) {
        return new TableViewBuilderImpl<>(this, schema);
    }

    public CompletableFuture<Producer<byte[]>> createProducerAsync(ProducerConfigurationData conf) {
        return createProducerAsync(conf, Schema.BYTES, null);
    }

    public <T> CompletableFuture<Producer<T>> createProducerAsync(ProducerConfigurationData conf,  Schema<T> schema) {
        return createProducerAsync(conf, schema, null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> CompletableFuture<Producer<T>> createProducerAsync(ProducerConfigurationData conf, Schema<T> schema,
                                                                  ProducerInterceptors interceptors) {
        if (conf == null) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidConfigurationException("Producer configuration undefined"));
        }

        if (schema instanceof AutoConsumeSchema) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException(
                            "AutoConsumeSchema is only used by consumers to detect schemas automatically"));
        }

        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Client already closed : state = " + state.get()));
        }

        String topic = conf.getTopicName();

        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }
        if (isScalableDomain(topic)) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidTopicNameException(
                    "Scalable topics (topic://) require the V5 client SDK."
                    + " Topic: '" + topic + "'"));
        }

        if (schema instanceof AutoProduceBytesSchema) {
            AutoProduceBytesSchema autoProduceBytesSchema = (AutoProduceBytesSchema) schema;
            if (autoProduceBytesSchema.hasUserProvidedSchema()) {
                return createProducerAsync(topic, conf, schema, interceptors);
            }
            return reloadSchemaForAutoProduceProducer(topic, autoProduceBytesSchema)
                    .thenCompose(schemaInfoOptional -> createProducerAsync(topic, conf, schema, interceptors));
        } else {
            return createProducerAsync(topic, conf, schema, interceptors);
        }

    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> reloadSchemaForAutoProduceProducer(String topic, AutoProduceBytesSchema autoSchema) {
        return lookup.getSchema(TopicName.get(topic)).thenAccept(schemaInfoOptional -> {
            if (schemaInfoOptional.isPresent()) {
                SchemaInfo schemaInfo = schemaInfoOptional.get();
                if (schemaInfo.getType() == SchemaType.PROTOBUF) {
                    autoSchema.setSchema(new GenericAvroSchema(schemaInfo));
                } else {
                    autoSchema.setSchema(Schema.getSchema(schemaInfo));
                }
            } else {
                autoSchema.setSchema(Schema.BYTES);
            }
        });
    }

    private CompletableFuture<Integer> checkPartitions(String topic, boolean forceNoPartitioned,
                                                       @Nullable String producerNameForLog) {
        CompletableFuture<Integer> checkPartitions = new CompletableFuture<>();
        getPartitionedTopicMetadata(topic, !forceNoPartitioned, true).thenAccept(metadata -> {
            if (forceNoPartitioned && metadata.partitions > 0) {
                String errorMsg = String.format("Can not create the producer[%s] for the topic[%s] that contains %s"
                                + " partitions b,ut the producer does not support for a partitioned topic.",
                        producerNameForLog, topic, metadata.partitions);
                log.error(errorMsg);
                checkPartitions.completeExceptionally(
                        new PulsarClientException.NotConnectedException(errorMsg));
            } else {
                checkPartitions.complete(metadata.partitions);
            }
        }).exceptionally(ex -> {
            Throwable actEx = FutureUtil.unwrapCompletionException(ex);
            if (forceNoPartitioned && (actEx instanceof PulsarClientException.NotFoundException
                    || actEx instanceof PulsarClientException.TopicDoesNotExistException
                    || actEx instanceof PulsarAdminException.NotFoundException)) {
                checkPartitions.complete(0);
            } else {
                checkPartitions.completeExceptionally(ex);
            }
            return null;
        });
        return checkPartitions;
    }

    /**
     * Create a producer bypassing the scalable domain check.
     * This is intended for internal use by the V5 client to create segment producers.
     */
    public <T> CompletableFuture<Producer<T>> createSegmentProducerAsync(
            ProducerConfigurationData conf, Schema<T> schema) {
        if (conf == null) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidConfigurationException("Producer configuration undefined"));
        }
        String topic = conf.getTopicName();
        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }
        return createProducerAsync(topic, conf, schema, null);
    }

    /**
     * Create a reader against a segment topic bypassing the scalable domain check.
     * This is intended for internal use by the V5 {@code CheckpointConsumer} to read each
     * segment's underlying {@code segment://} topic.
     */
    public <T> CompletableFuture<Reader<T>> createSegmentReaderAsync(ReaderConfigurationData<T> conf,
                                                                      Schema<T> schema) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Reader configuration undefined"));
        }
        if (conf.getTopicNames().size() != 1) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException(
                            "createSegmentReaderAsync requires exactly one topic, got "
                                    + conf.getTopicNames().size()));
        }
        String topic = conf.getTopicName();
        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }
        if (conf.getStartMessageId() == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Invalid startMessageId"));
        }
        return preProcessSchemaBeforeSubscribe(this, schema, topic)
                .thenCompose(schemaClone -> createSingleTopicReaderAsync(conf, schemaClone));
    }

    /**
     * Subscribe to a segment topic bypassing the scalable domain check.
     * This is intended for internal use by the V5 client to subscribe to per-segment v4
     * topics for the {@code segment://} backing topics it owns.
     */
    public <T> CompletableFuture<Consumer<T>> subscribeSegmentAsync(ConsumerConfigurationData<T> conf,
                                                                     Schema<T> schema) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }
        if (conf.getTopicNames().size() != 1) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException(
                            "subscribeSegmentAsync requires exactly one topic, got " + conf.getTopicNames().size()));
        }
        String topic = conf.getSingleTopic();
        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }
        if (isBlank(conf.getSubscriptionName())) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Empty subscription name"));
        }
        return singleTopicSubscribeAsync(conf, schema, null);
    }

    /**
     * Reject {@code topic://} (PIP-460 scalable topics) and {@code segment://} (the internal
     * backing-topic domain used by V5 scalable topics). Users on the V4 SDK must switch to the
     * V5 SDK for either.
     */
    private static boolean isScalableDomain(String topic) {
        TopicName topicName = TopicName.get(topic);
        return topicName.isScalable() || topicName.isSegment();
    }

    private <T> CompletableFuture<Producer<T>> createProducerAsync(String topic,
                                                                   ProducerConfigurationData conf,
                                                                   Schema<T> schema,
                                                                   ProducerInterceptors interceptors) {
        CompletableFuture<Producer<T>> producerCreatedFuture = new CompletableFuture<>();



        checkPartitions(topic, conf.isNonPartitionedTopicExpected(), conf.getProducerName()).thenAccept(partitions -> {
                log.debug().attr("topic", topic)
                        .attr("partitions", partitions)
                        .log("Received topic metadata. partitions");

            ProducerBase<T> producer;
            if (partitions > 0) {
                producer = newPartitionedProducerImpl(topic, conf, schema, interceptors, producerCreatedFuture,
                        partitions);
            } else {
                producer = newProducerImpl(topic, -1, conf, schema, interceptors, producerCreatedFuture,
                        Optional.empty());
            }
            producers.add(producer);
        }).exceptionally(ex -> {
            log.warn().attr("topic", topic).exceptionMessage(ex).log("Failed to get partitioned topic metadata");
            producerCreatedFuture.completeExceptionally(ex);
            return null;
        });

        return producerCreatedFuture;
    }

    /**
     * Factory method for creating PartitionedProducerImpl instance.
     *
     * Allows overriding the PartitionedProducerImpl instance in tests.
     *
     * @param topic topic name
     * @param conf producer configuration
     * @param schema topic schema
     * @param interceptors producer interceptors
     * @param producerCreatedFuture future for signaling completion of async producer creation
     * @param <T> message type class
     * @return new PartitionedProducerImpl instance
     */
    protected <T> PartitionedProducerImpl<T> newPartitionedProducerImpl(String topic,
                                                                        ProducerConfigurationData conf,
                                                                        Schema<T> schema,
                                                                        ProducerInterceptors interceptors,
                                                                        CompletableFuture<Producer<T>>
                                                                                producerCreatedFuture,
                                                                        int partitions) {
        return new PartitionedProducerImpl<>(PulsarClientImpl.this, topic, conf, partitions,
                producerCreatedFuture, schema, interceptors);
    }

    /**
     * Factory method for creating ProducerImpl instance.
     *
     * Allows overriding the ProducerImpl instance in tests.
     *
     * @param topic topic name
     * @param partitionIndex partition index of a partitioned topic. the value -1 is used for non-partitioned topics.
     * @param conf producer configuration
     * @param schema topic schema
     * @param interceptors producer interceptors
     * @param producerCreatedFuture future for signaling completion of async producer creation
     * @param <T> message type class
     *
     * @return a producer instance
     */
    @SuppressWarnings("unchecked")
    protected <T> ProducerImpl<T> newProducerImpl(String topic, int partitionIndex,
                                                  ProducerConfigurationData conf,
                                                  Schema<T> schema,
                                                  ProducerInterceptors interceptors,
                                                  CompletableFuture<Producer<T>> producerCreatedFuture,
                                                  Optional<String> overrideProducerName) {
        if (conf.isReplProducer()) {
            return new GeoReplicationProducerImpl(PulsarClientImpl.this, topic, conf, producerCreatedFuture,
                    partitionIndex, schema, interceptors, overrideProducerName);
        }
        return new ProducerImpl<>(PulsarClientImpl.this, topic, conf, producerCreatedFuture, partitionIndex, schema,
                interceptors, overrideProducerName);
    }

    public CompletableFuture<Consumer<byte[]>> subscribeAsync(ConsumerConfigurationData<byte[]> conf) {
        return subscribeAsync(conf, Schema.BYTES, null);
    }

    public <T> CompletableFuture<Consumer<T>> subscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema,
                                                             ConsumerInterceptors<T> interceptors) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        for (String topic : conf.getTopicNames()) {
            if (!TopicName.isValid(topic)) {
                return FutureUtil.failedFuture(
                        new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
            }
            if (isScalableDomain(topic)) {
                return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidTopicNameException(
                        "Scalable topics (topic://) require the V5 client SDK."
                        + " Topic: '" + topic + "'"));
            }
        }

        if (isBlank(conf.getSubscriptionName())) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Empty subscription name"));
        }

        if (conf.isReadCompacted() && (!conf.getTopicNames().stream()
                .allMatch(topic -> TopicName.get(topic).getDomain() == TopicDomain.persistent)
                || (conf.getSubscriptionType() != SubscriptionType.Exclusive
                        && conf.getSubscriptionType() != SubscriptionType.Failover))) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Read compacted can only be used with exclusive or failover persistent subscriptions"));
        }

        if (conf.getConsumerEventListener() != null && conf.getSubscriptionType() != SubscriptionType.Failover) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Active consumer listener is only supported for failover subscription"));
        }

        if (conf.getTopicsPattern() != null) {
            // If use topicsPattern, we should not use topic(), and topics() method.
            if (!conf.getTopicNames().isEmpty()){
                return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic names list must be null when use topicsPattern"));
            }
            return patternTopicSubscribeAsync(conf, schema, interceptors);
        } else if (conf.getTopicNames().size() == 1) {
            return singleTopicSubscribeAsync(conf, schema, interceptors);
        } else {
            return multiTopicSubscribeAsync(conf, schema, interceptors);
        }
    }

    private <T> CompletableFuture<Consumer<T>> singleTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
                                                                         Schema<T> schema,
                                                                         ConsumerInterceptors<T> interceptors) {
        return preProcessSchemaBeforeSubscribe(this, schema, conf.getSingleTopic())
                .thenCompose(schemaClone -> doSingleTopicSubscribeAsync(conf, schemaClone, interceptors));
    }

    private <T> CompletableFuture<Consumer<T>> doSingleTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
                                                                           Schema<T> schema,
                                                                           ConsumerInterceptors<T> interceptors) {
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

        String topic = conf.getSingleTopic();

        getPartitionedTopicMetadata(topic, true, false).thenAccept(metadata -> {
                log.debug().attr("topic", topic)
                        .attr("partitions", metadata.partitions)
                        .log("Received topic metadata. partitions");

            ConsumerBase<T> consumer;
            if (metadata.partitions > 0) {
                consumer = MultiTopicsConsumerImpl.createPartitionedConsumer(PulsarClientImpl.this, conf,
                        externalExecutorProvider, consumerSubscribedFuture, metadata.partitions, schema, interceptors);
            } else {
                int partitionIndex = TopicName.getPartitionIndex(topic);
                consumer = ConsumerImpl.newConsumerImpl(PulsarClientImpl.this, topic, conf, externalExecutorProvider,
                        partitionIndex, false, consumerSubscribedFuture, null, schema, interceptors,
                        true /* createTopicIfDoesNotExist */);
            }
            consumers.add(consumer);
        }).exceptionally(ex -> {
            log.warn().attr("topic", topic).exception(ex).log("Failed to get partitioned topic metadata");
            consumerSubscribedFuture.completeExceptionally(ex);
            return null;
        });

        return consumerSubscribedFuture;
    }

    private <T> CompletableFuture<Consumer<T>> multiTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
                                                                        Schema<T> schema,
                                                                        ConsumerInterceptors<T> interceptors) {
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

        ConsumerBase<T> consumer = new MultiTopicsConsumerImpl<>(PulsarClientImpl.this, conf,
                externalExecutorProvider, consumerSubscribedFuture, schema, interceptors,
                true /* createTopicIfDoesNotExist */);

        consumers.add(consumer);

        return consumerSubscribedFuture;
    }

    public CompletableFuture<Consumer<byte[]>> patternTopicSubscribeAsync(ConsumerConfigurationData<byte[]> conf) {
        return patternTopicSubscribeAsync(conf, Schema.BYTES, null);
    }

    private <T> CompletableFuture<Consumer<T>> patternTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
            Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        String regex = conf.getTopicsPattern().pattern();
        Mode subscriptionMode = convertRegexSubscriptionMode(conf.getRegexSubscriptionMode());
        TopicName destination = TopicName.get(regex);
        NamespaceName namespaceName = destination.getNamespaceObject();
        TopicsPattern pattern = TopicsPatternFactory.create(conf.getTopicsPattern());

        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
        lookup.getTopicsUnderNamespace(namespaceName, subscriptionMode, regex, null, conf.getProperties())
            .thenAccept(getTopicsResult -> {
                log.debug().attr("subscriptionName", conf.getSubscriptionName())
                        .attr("namespace", namespaceName)
                        .attr("topicsCount", getTopicsResult.getTopics().size())
                        .attr("topicsHash", getTopicsResult.getTopicsHash())
                        .attr("changed", getTopicsResult.isChanged())
                        .attr("filtered", getTopicsResult.isFiltered())
                        .log("Pattern consumer get topics under namespace");
                getTopicsResult.getTopics().forEach(topicName ->
                        log.debug().attr("subscriptionName", conf.getSubscriptionName())
                                .attr("namespace", namespaceName)
                                .attr("topic", topicName)
                                .log("Pattern consumer get topics under namespace"));

                List<String> topicsList;
                if (!getTopicsResult.isFiltered()) {
                   topicsList = TopicList.filterTopics(getTopicsResult.getTopics(), pattern);
                } else {
                    // deduplicate java.lang.String instances using TopicName's cache
                    topicsList = getTopicsResult.getTopics().stream()
                            .map(TopicName::get).map(TopicName::toString)
                            .collect(Collectors.toList());
                }
                conf.getTopicNames().addAll(topicsList);

                log.debug().attr("subscriptionName", conf.getSubscriptionName())
                        .attr("topics", () -> getTopicsResult.getNonPartitionedOrPartitionTopics())
                        .log("Pattern consumer initialize topics.");

                // Pattern consumer has his unique check mechanism, so do not need the feature "autoUpdatePartitions".
                conf.setAutoUpdatePartitions(false);
                ConsumerBase<T> consumer = new PatternMultiTopicsConsumerImpl<>(pattern,
                        PulsarClientImpl.this,
                        conf,
                        externalExecutorProvider,
                        consumerSubscribedFuture,
                        schema, subscriptionMode, interceptors);

                consumers.add(consumer);
            })
            .exceptionally(ex -> {
                log.warn().attr("namespaceName", namespaceName).log("Failed to get topics under namespace");
                consumerSubscribedFuture.completeExceptionally(ex);
                return null;
            });

        return consumerSubscribedFuture;
    }

    public CompletableFuture<Reader<byte[]>> createReaderAsync(ReaderConfigurationData<byte[]> conf) {
        return createReaderAsync(conf, Schema.BYTES);
    }

    public <T> CompletableFuture<Reader<T>> createReaderAsync(ReaderConfigurationData<T> conf, Schema<T> schema) {
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }
        for (String topic : conf.getTopicNames()) {
            if (!TopicName.isValid(topic)) {
                return FutureUtil.failedFuture(new PulsarClientException
                        .InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
            }
            if (isScalableDomain(topic)) {
                return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidTopicNameException(
                        "Scalable topics (topic://) require the V5 client SDK."
                        + " Topic: '" + topic + "'"));
            }
        }

        if (conf.getStartMessageId() == null) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Invalid startMessageId"));
        }

        if (conf.getTopicNames().size() == 1) {
            return preProcessSchemaBeforeSubscribe(this, schema, conf.getTopicName())
                    .thenCompose(schemaClone -> createSingleTopicReaderAsync(conf, schemaClone));
        }
        return createMultiTopicReaderAsync(conf, schema);
    }

    protected <T> CompletableFuture<Reader<T>> createMultiTopicReaderAsync(
            ReaderConfigurationData<T> conf, Schema<T> schema) {
        CompletableFuture<Reader<T>> readerFuture = new CompletableFuture<>();
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
        MultiTopicsReaderImpl<T> reader = new MultiTopicsReaderImpl<>(this,
                conf, externalExecutorProvider, consumerSubscribedFuture, schema);
        ConsumerBase<T> consumer = reader.getMultiTopicsConsumer();
        consumers.add(consumer);
        consumerSubscribedFuture.thenRun(() -> readerFuture.complete(reader))
                .exceptionally(ex -> {
                    log.warn().exception(ex).log("Failed to create multiTopicReader");
                    readerFuture.completeExceptionally(ex);
                    return null;
                });
        return readerFuture;
    }

    protected <T> CompletableFuture<Reader<T>> createSingleTopicReaderAsync(
            ReaderConfigurationData<T> conf, Schema<T> schema) {
        String topic = conf.getTopicName();

        CompletableFuture<Reader<T>> readerFuture = new CompletableFuture<>();

        getPartitionedTopicMetadata(topic, true, false).thenAccept(metadata -> {
            log.debug().attr("topic", topic)
                    .attr("partitions", metadata.partitions)
                    .log("Received topic metadata. partitions");
            if (metadata.partitions > 0
                    && MultiTopicsConsumerImpl.isIllegalMultiTopicsMessageId(conf.getStartMessageId())) {
                readerFuture.completeExceptionally(
                        new PulsarClientException("The partitioned topic startMessageId is illegal"));
                return;
            }
            CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
            Reader<T> reader;
            ConsumerBase<T> consumer;
            if (metadata.partitions > 0) {
                reader = new MultiTopicsReaderImpl<>(PulsarClientImpl.this,
                        conf, externalExecutorProvider, consumerSubscribedFuture, schema);
                consumer = ((MultiTopicsReaderImpl<T>) reader).getMultiTopicsConsumer();
            } else {
                reader = new ReaderImpl<>(PulsarClientImpl.this, conf, externalExecutorProvider,
                        consumerSubscribedFuture, schema);
                consumer = ((ReaderImpl<T>) reader).getConsumer();
            }

            consumers.add(consumer);

            consumerSubscribedFuture.thenRun(() -> readerFuture.complete(reader)).exceptionally(ex -> {
                log.warn().attr("topic", topic).exception(ex).log("Failed to get create topic reader");
                readerFuture.completeExceptionally(ex);
                return null;
            });
        }).exceptionally(ex -> {
            log.warn().attr("topic", topic).exception(ex).log("Failed to get partitioned topic metadata");
            readerFuture.completeExceptionally(ex);
            return null;
        });

        return readerFuture;
    }

    /**
     * Read the schema information for a given topic.
     *
     * If the topic does not exist or it has no schema associated, it will return an empty response
     */
    public CompletableFuture<Optional<SchemaInfo>> getSchema(String topic) {
        TopicName topicName;
        try {
            topicName = TopicName.get(topic);
        } catch (Throwable t) {
            return FutureUtil
                    .failedFuture(
                            new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }

        return lookup.getSchema(topicName);
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            PulsarClientException unwrapped = PulsarClientException.unwrap(e);
            if (unwrapped instanceof PulsarClientException.AlreadyClosedException) {
                // this is not a problem
                return;
            }
            throw unwrapped;
        }
    }

    private void closeUrlLookupMap() {
        Map<String, LookupService> closedUrlLookupServices = new HashMap<>(urlLookupMap.size());
        urlLookupMap.entrySet().forEach(e -> {
            try {
                e.getValue().close();
            } catch (Exception ex) {
                log.error().attr("service", e.getKey()).exception(ex).log("Error closing lookup service");
            }
            closedUrlLookupServices.put(e.getKey(), e.getValue());
        });
        closedUrlLookupServices.entrySet().forEach(e -> {
            urlLookupMap.remove(e.getKey(), e.getValue());
        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        log.info().attr("url", lookup.getServiceUrl()).log("Client closing. URL");
        if (!state.compareAndSet(State.Open, State.Closing)) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        closeUrlLookupMap();

        producers.forEach(p -> futures.add(p.closeAsync().handle((__, t) -> {
            if (t != null) {
                log.error().attr("producer", p).exception(t).log("Error closing producer");
            }
            return null;
        })));
        consumers.forEach(c -> futures.add(c.closeAsync().handle((__, t) -> {
            if (t != null) {
                log.error().attr("consumer", c).exception(t).log("Error closing consumer");
            }
            return null;
        })));

        // Need to run the shutdown sequence in a separate thread to prevent deadlocks
        // If there are consumers or producers that need to be shutdown we cannot use the same thread
        // to shutdown the EventLoopGroup as well as that would be trying to shutdown itself thus a deadlock
        // would happen
        CompletableFuture<Void> combinedFuture = FutureUtil.waitForAll(futures);
        ScheduledExecutorService shutdownExecutor = Executors.newSingleThreadScheduledExecutor(
                new ExecutorProvider.ExtendedThreadFactory("pulsar-client-shutdown-timeout-scheduler"));
        FutureUtil.addTimeoutHandling(combinedFuture, Duration.ofSeconds(CLOSE_TIMEOUT_SECONDS),
                shutdownExecutor, () -> FutureUtil.createTimeoutException("Closing producers and consumers timed out.",
                        PulsarClientImpl.class, "closeAsync"));
        combinedFuture.handle((__, t) -> {
            if (t != null) {
                log.error().exception(t).log("Closing producers and consumers failed. Continuing with shutdown.");
            }
            new Thread(() -> {
                shutdownExecutor.shutdownNow();
                // All producers & consumers are now closed, we can stop the client safely
                try {
                    shutdown();
                } catch (PulsarClientException e) {
                    log.error().exception(e).log("Shutdown failed. Ignoring the exception.");
                }
                state.set(State.Closed);
                closeFuture.complete(null);
            }, "pulsar-client-shutdown-thread").start();
            return null;
        });
        return closeFuture;
    }

    @Override
    public void shutdown() throws PulsarClientException {
        try {
            // We will throw the last thrown exception only, though logging all of them.
            Throwable throwable = null;
            // PIP-478: close the framework HTTP client factory (and every client it still owns)
            // first — its AsyncHttpClient instances share the client's event loop, timer and DNS resolver and
            // hold TLS subscriptions, so they must be released before those shared resources and the TLS
            // factory are torn down below. A plugin's own close() later is idempotent on an already-closed
            // client.
            if (authHttpClientFactory != null) {
                try {
                    authHttpClientFactory.close();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to close framework HTTP client factory");
                    throwable = t;
                }
            }
            if (lookup != null) {
                try {
                    lookup.close();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to shutdown lookup");
                    throwable = t;
                }
            }
            if (tcClient != null) {
                try {
                    tcClient.close();
                } catch (Throwable t) {
                    log.warn("Failed to close tcClient");
                    throwable = t;
                }
            }

            // close the service url provider allocated resource.
            if (conf != null && conf.getServiceUrlProvider() != null && serviceUrlProviderInitialized) {
                conf.getServiceUrlProvider().close();
            }

            if (addressResolver != null) {
                try {
                    addressResolver.close();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to close addressResolver");
                    throwable = t;
                }
            }

            if (dnsResolverGroupLocalInstance != null) {
                try {
                    dnsResolverGroupLocalInstance.close();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to close dnsResolverGroup");
                    throwable = t;
                }
            }

            try {
                // Shutting down eventLoopGroup separately because in some cases, cnxPool might be using different
                // eventLoopGroup.
                shutdownEventLoopGroup(eventLoopGroup);
            } catch (PulsarClientException e) {
                log.warn().exception(e).log("Failed to shutdown eventLoopGroup");
                throwable = e;
            }
            try {
                closeCnxPool(cnxPool);
            } catch (PulsarClientException e) {
                log.warn().exception(e).log("Failed to shutdown cnxPool");
                throwable = e;
            }
            if (timer != null && needStopTimer) {
                try {
                    timer.stop();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to shutdown timer");
                    throwable = t;
                }
            }
            try {
                shutdownExecutors();
            } catch (PulsarClientException e) {
                throwable = e;
            }

            if (memoryBufferStats != null) {
                try {
                    memoryBufferStats.close();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to close memoryBufferStats");
                    throwable = t;
                }
            }

            if (memoryLimitController != null) {
                memoryLimitController.deregisterTrigger(memoryLimitTrigger);
            }

            if (conf != null && conf.getV5Authentication() != null) {
                // PIP-478: a v5-native plugin configured through the v5 builder lives only in this slot —
                // the v4 slot still holds AuthenticationDisabled — so closing the v4 slot alone would leak
                // whatever it holds open. Closing is safe for the other cases too: a body handed over by a
                // built-in shim owns nothing, and a bridged adapter created for an already-started plugin
                // deliberately leaves that plugin to its owner, which is the v4 close below.
                try {
                    conf.getV5Authentication().close();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to close v5 authentication");
                    throwable = t;
                }
            }
            if (conf != null && conf.getAuthentication() != null) {
                try {
                    conf.getAuthentication().close();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to close authentication");
                    throwable = t;
                }
            }
            // PIP-478: release the auth blocking executor if one was created.
            if (blockingAuthExecutor != null) {
                blockingAuthExecutor.shutdownNow();
            }
            // PIP-478: close the TLS factory this client owns. Unconditional on both arms, because adoption
            // is a hand-over: PulsarTlsFactory.close() says a factory supplied to the v5 builder "is adopted
            // and closed with the client", tlsFactory(...) says the same on the public builder, and
            // resolveClientTlsFactory already closes an adopted instance when the fail-fast probe fails.
            if (conf != null && conf.getTlsFactory() != null) {
                try {
                    conf.getTlsFactory().close();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to close TLS factory");
                    throwable = t;
                } finally {
                    // Only a factory this client composed is cleared, because only that one is this client's
                    // own artefact written into a configuration the caller may still hold. Both builders now
                    // hand over a copy, so the one configuration that outlives its client is the one passed
                    // to PulsarService.createClientImpl directly: the broker's own internal client, and the
                    // per-cluster replication / peer-lookup clients that hand it a ClientBuilderImpl's
                    // configuration. There the client composes the factory itself unless
                    // brokerClientTlsFactoryClassName put one in the slot first, and a composed factory left
                    // behind would be mistaken for an adopted one on the next construction from that
                    // configuration, re-initialized while closed, and fail that build.
                    //
                    // An adopted factory stays in the slot on purpose. The slot is then the caller's own
                    // statement of which factory this configuration uses, so clearing it would silently
                    // reroute the next client built from that configuration onto a factory composed from the
                    // tls* fields — different trust material, no error. Leaving the closed instance means
                    // such a reuse hits a closed factory instead. No builder can get there: the v5 builder,
                    // the only one with a tlsFactory(...) seam, refuses to hand the same instance twice.
                    if (clientComposedTlsFactory) {
                        conf.setTlsFactory(null);
                    }
                }
            }
            // PIP-478: close any TLS factories superseded by an AutoClusterFailover rebuild; they were kept
            // alive (not closed eagerly) so in-flight connections / a shared HTTP subscription stayed valid.
            for (PulsarTlsFactory superseded : supersededTlsFactories) {
                try {
                    superseded.close();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to close a superseded TLS factory");
                    throwable = t;
                }
            }
            supersededTlsFactories.clear();
            if (throwable != null) {
                throw throwable;
            }
        } catch (Throwable t) {
            log.warn().exception(t).log("Failed to shutdown Pulsar client");
            throw PulsarClientException.unwrap(t);
        }
    }

    private void closeCnxPool(ConnectionPool cnxPool) throws PulsarClientException {
        if (createdCnxPool && cnxPool != null) {
            try {
                cnxPool.close();
            } catch (Throwable t) {
                throw PulsarClientException.unwrap(t);
            }
        }
    }

    private void shutdownEventLoopGroup(EventLoopGroup eventLoopGroup) throws PulsarClientException {
        if (createdEventLoopGroup && eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
            try {
                eventLoopGroup.shutdownGracefully().get(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (Throwable t) {
                throw PulsarClientException.unwrap(t);
            }
        }
    }

    private void shutdownExecutors() throws PulsarClientException {
        PulsarClientException pulsarClientException = null;
        if (createdExecutorProviders) {

            if (externalExecutorProvider != null && !externalExecutorProvider.isShutdown()) {
                try {
                    externalExecutorProvider.shutdownNow();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to shutdown externalExecutorProvider");
                    pulsarClientException = PulsarClientException.unwrap(t);
                }
            }
            if (internalExecutorProvider != null && !internalExecutorProvider.isShutdown()) {
                try {
                    internalExecutorProvider.shutdownNow();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to shutdown internalExecutorService");
                    pulsarClientException = PulsarClientException.unwrap(t);
                }
            }
        }
        if (createdScheduledProviders && scheduledExecutorProvider != null && !scheduledExecutorProvider.isShutdown()) {
            try {
                scheduledExecutorProvider.shutdownNow();
            } catch (Throwable t) {
                log.warn().exception(t).log("Failed to shutdown scheduledExecutorProvider");
                pulsarClientException = PulsarClientException.unwrap(t);
            }
        }

        if (createdLookupProviders && lookupExecutorProvider != null && !lookupExecutorProvider.isShutdown()) {
            try {
                lookupExecutorProvider.shutdownNow();
            } catch (Throwable t) {
                log.warn().exception(t).log("Failed to shutdown lookupExecutorProvider");
                pulsarClientException = PulsarClientException.unwrap(t);
            }
        }

        if (pulsarClientException != null) {
            throw pulsarClientException;
        }
    }

    @Override
    public boolean isClosed() {
        State currentState = state.get();
        return currentState == State.Closed || currentState == State.Closing;
    }

    @Override
    public synchronized void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        log.info().attr("serviceUrl", serviceUrl).log("Updating service URL");

        conf.setServiceUrl(serviceUrl);
        lookup.updateServiceUrl(serviceUrl);
        cnxPool.closeAllConnections();
    }

    public void updateAuthentication(Authentication authentication) throws IOException {
        log.info().attr("authentication", authentication).log("Updating authentication");
        if (conf.getAuthentication() != null) {
            conf.getAuthentication().close();
        }
        conf.setAuthentication(authentication);
        // PIP-478: bind framework services into the swapped-in auth before starting it.
        bindAuthenticationServices(conf.getAuthentication());
        // PIP-478: a swapped-in auth may carry TLS client identity (e.g. AuthenticationTls) or OAuth2 IdP TLS
        // material, so rebuild the client TLS factory to fold the new material BEFORE start(): start() may
        // eagerly fetch IdP metadata over the framework HTTP client, which reads conf.getTlsFactory() — a
        // stale factory would fetch under the OLD (wrong-CA) CLIENT_OAUTH2 trust and leave the new OAuth2 HTTP
        // client subscribed to the superseded factory. This mirrors the construction path (setupClientTlsFactory
        // runs before start()). rebuildClientTlsFactory folds idpTlsPolicy from the already-set new
        // authentication and does not depend on the auth being started.
        rebuildClientTlsFactory();
        conf.getAuthentication().start();
        // PIP-478: the client drives the v5 model, so the swap is not complete until what it drives has been
        // re-resolved. Without this the previous body and driver survive in the configuration and every new
        // connection keeps presenting the OLD credential — from a plugin this method has just closed.
        conf.setV5Authentication(null);
        conf.setV5AuthenticationDriver(null);
        resolveV5Authentication();
    }

    public void updateTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        log.info().attr("tlsTrustCertsFilePath", tlsTrustCertsFilePath).log("Updating tlsTrustCertsFilePath");
        conf.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
        // PIP-478: rebuild the client TLS factory so the updated trust roots reach new connections.
        rebuildClientTlsFactory();
    }

    public void updateTlsTrustStorePathAndPassword(String tlsTrustStorePath, String tlsTrustStorePassword) {
        log.info().attr("tlsTrustStorePath", tlsTrustStorePath)
                .log("Updating tlsTrustStorePath to, tlsTrustStorePassword to *****");
        conf.setTlsTrustStorePath(tlsTrustStorePath);
        conf.setTlsTrustStorePassword(tlsTrustStorePassword);
        // PIP-478: rebuild the client TLS factory so the updated keystore trust reaches new connections.
        rebuildClientTlsFactory();
    }

    public CompletableFuture<Pair<ClientCnx, Boolean>> getConnection(String topic, int randomKeyForSelectConnection) {
        CompletableFuture<LookupTopicResult> lookupTopicResult = lookup.getBroker(TopicName.get(topic));
        CompletableFuture<Boolean> isUseProxy = lookupTopicResult.thenApply(LookupTopicResult::isUseProxy);
        return lookupTopicResult.thenCompose(lookupResult -> getConnection(lookupResult.getLogicalAddress(),
                        lookupResult.getPhysicalAddress(), randomKeyForSelectConnection)).
                thenCombine(isUseProxy, Pair::of);
    }

    /**
     * Only for test.
     */
    @VisibleForTesting
    public CompletableFuture<ClientCnx> getConnection(final String topic) {
        return getConnection(topic, cnxPool.genRandomKeyToSelectCon()).thenApply(Pair::getLeft);
    }

    public CompletableFuture<ClientCnx> getConnection(final String topic, final String url) {
        TopicName topicName = TopicName.get(topic);
        return getLookup(url).getBroker(topicName)
                .thenCompose(lookupResult -> getConnection(lookupResult.getLogicalAddress(),
                        lookupResult.getPhysicalAddress(), cnxPool.genRandomKeyToSelectCon()));
    }

    public LookupService getLookup(String serviceUrl) {
        return urlLookupMap.computeIfAbsent(serviceUrl, url -> {
            if (isClosed()) {
                throw new IllegalStateException("Pulsar client has been closed, can not build LookupService when"
                        + " calling get lookup with an url");
            }
            try {
                return createLookup(serviceUrl);
            } catch (PulsarClientException e) {
                log.warn().attr("service", url).exceptionMessage(e).log("Failed to update url to lookup service");
                throw new IllegalStateException("Failed to update url " + url);
            }
        });
    }

    public CompletableFuture<ClientCnx> getConnectionToServiceUrl() {
        if (!lookup.isBinaryProtoLookupService()) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidServiceURL(
                    "Can't get client connection to HTTP service URL", null));
        }
        InetSocketAddress address = lookup.resolveHost();
        return getConnection(address, address, cnxPool.genRandomKeyToSelectCon());
    }

    /**
     * Open a connection to the proxy and ask it to pair the connection to any broker it selects
     * (an empty proxyToBrokerUrl). Used for control-plane operations that aren't tied to a specific
     * broker (e.g. scalable-topic subscribe/namespace-watch) when connecting through a proxy.
     */
    public CompletableFuture<ClientCnx> getAnyBrokerProxyConnection() {
        if (!lookup.isBinaryProtoLookupService()) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidServiceURL(
                    "Can't pair to any broker through an HTTP service URL", null));
        }
        return getConnection(PulsarChannelInitializer.PROXY_TO_ANY_BROKER, lookup.resolveHost(),
                cnxPool.genRandomKeyToSelectCon());
    }

    public CompletableFuture<ClientCnx> getProxyConnection(final InetSocketAddress logicalAddress,
                                                           final int randomKeyForSelectConnection) {
        if (!lookup.isBinaryProtoLookupService()) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidServiceURL(
                    "Cannot proxy connection through HTTP service URL", null));
        }
        return getConnection(logicalAddress, lookup.resolveHost(), randomKeyForSelectConnection);
    }

    public CompletableFuture<ClientCnx> getConnection(final InetSocketAddress logicalAddress,
                                                      final InetSocketAddress physicalAddress,
                                                      final int randomKeyForSelectConnection) {
        return cnxPool.getConnection(logicalAddress, physicalAddress, randomKeyForSelectConnection);
    }

    /** visible for pulsar-functions. **/
    public Timer timer() {
        return timer;
    }

    public ExecutorProvider externalExecutorProvider() {
        return externalExecutorProvider;
    }

    long newProducerId() {
        return producerIdGenerator.getAndIncrement();
    }

    public long newConsumerId() {
        return consumerIdGenerator.getAndIncrement();
    }

    long newTopicListWatcherId() {
        return topicListWatcherIdGenerator.getAndIncrement();
    }

    public long newRequestId() {
        return requestIdGenerator.getAndIncrement();
    }

    public ConnectionPool getCnxPool() {
        return cnxPool;
    }

    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    @VisibleForTesting
    public void setLookup(LookupService lookup) {
        this.lookup = lookup;
    }

    public LookupService getLookup() {
        return lookup;
    }

    public void reloadLookUp() throws PulsarClientException {
        LookupService previousLookup = lookup;
        lookup = createLookup(conf.getServiceUrl());
        // close the previous lookup after the new lookup is created successfully
        if (previousLookup != null && previousLookup != lookup) {
            try {
                previousLookup.close();
            } catch (Exception e) {
                log.warn().exception(e).log("Failed to close previous lookup service");
            }
        }
    }

    public LookupService createLookup(String url) throws PulsarClientException {
        LookupService lookupService;
        if (url.startsWith("http")) {
            lookupService = new HttpLookupService(instrumentProvider, conf, eventLoopGroup, timer, getNameResolver(),
                    blockingAuthExecutor());
        } else {
            lookupService = new BinaryProtoLookupService(this, url, conf.getListenerName(), conf.isUseTls(),
                    this.scheduledExecutorProvider.getExecutor(), this.lookupExecutorProvider.getExecutor());
        }
        return new InProgressDeduplicationDecoratorLookupService(lookupService,
                () -> getConfiguration().getLookupProperties());
    }

    /**
     * @param useFallbackForNonPIP344Brokers <p>If true, fallback to the prior behavior of the method
     *                                       getPartitionedTopicMetadata if the broker does not support the PIP-344
     *                                       feature 'supports_get_partitioned_metadata_without_auto_creation'. This
     *                                       parameter only affects the behavior when
     *                                       {@param metadataAutoCreationEnabled} is false.</p>
     */
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(
            String topic, boolean metadataAutoCreationEnabled, boolean useFallbackForNonPIP344Brokers) {

        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();

        try {
            TopicName topicName = TopicName.get(topic);
            // Segment topics are internal storage units of a scalable topic and are never
            // partitioned. Skip the broker partitioned-metadata lookup — the standard path
            // isn't set up for the 4-component segment://tenant/ns/parent/descriptor name
            // and will time out.
            if (topicName.isSegment()) {
                metadataFuture.complete(new PartitionedTopicMetadata(0));
                return metadataFuture;
            }
            AtomicLong opTimeoutMs = new AtomicLong(conf.getLookupTimeoutMs());
            Backoff backoff = Backoff.builder()
                    .initialDelay(Duration.ofNanos(conf.getInitialBackoffIntervalNanos()))
                    .mandatoryStop(Duration.ofMillis(opTimeoutMs.get() * 2))
                    .maxBackoff(Duration.ofNanos(conf.getMaxBackoffIntervalNanos()))
                    .build();
            getPartitionedTopicMetadata(topicName, backoff, opTimeoutMs, metadataFuture,
                    new AtomicInteger(0),
                    metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);
        } catch (IllegalArgumentException e) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(e.getMessage()));
        }
        return metadataFuture;
    }

    private void getPartitionedTopicMetadata(TopicName topicName,
                                             Backoff backoff,
                                             AtomicLong remainingTime,
                                             CompletableFuture<PartitionedTopicMetadata> future,
                                             AtomicInteger previousExceptionCount,
                                             boolean metadataAutoCreationEnabled,
                                             boolean useFallbackForNonPIP344Brokers) {
        long startTime = System.nanoTime();
        CompletableFuture<PartitionedTopicMetadata> queryFuture = lookup.getPartitionedTopicMetadata(topicName,
                metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);
        queryFuture.thenAccept(future::complete).exceptionally(e -> {
            remainingTime.addAndGet(-1 * TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
            long nextDelay = Math.min(backoff.next().toMillis(), remainingTime.get());
            // skip retry scheduler when set lookup throttle in client or server side which will lead to
            // `TooManyRequestsException`
            boolean isLookupThrottling = !PulsarClientException.isRetriableError(e.getCause())
                || e.getCause() instanceof PulsarClientException.AuthenticationException
                || e.getCause() instanceof PulsarClientException.NotFoundException;
            if (nextDelay <= 0 || isLookupThrottling) {
                PulsarClientException.setPreviousExceptionCount(e, previousExceptionCount);
                future.completeExceptionally(e);
                return null;
            }
            previousExceptionCount.getAndIncrement();

            ((ScheduledExecutorService) scheduledExecutorProvider.getExecutor()).schedule(() -> {
                log.warn().attr("topic", topicName)
                        .attr("nextDelayMs", nextDelay)
                        .log("Could not get connection while getting partitioned topic metadata, will retry");
                remainingTime.addAndGet(-nextDelay);
                getPartitionedTopicMetadata(topicName, backoff, remainingTime, future, previousExceptionCount,
                        metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);
            }, nextDelay, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    @Override
    public CompletableFuture<List<String>> getPartitionsForTopic(String topic, boolean metadataAutoCreationEnabled) {
        return getPartitionedTopicMetadata(topic, metadataAutoCreationEnabled, true).thenApply(metadata -> {
            if (metadata.partitions > 0) {
                TopicName topicName = TopicName.get(topic);
                List<String> partitions = new ArrayList<>(metadata.partitions);
                for (int i = 0; i < metadata.partitions; i++) {
                    partitions.add(topicName.getPartition(i).toString());
                }
                return partitions;
            } else {
                return Collections.singletonList(topic);
            }
        });
    }

    void cleanupProducer(ProducerBase<?> producer) {
        producers.remove(producer);
    }

    void cleanupConsumer(ConsumerBase<?> consumer) {
        consumers.remove(consumer);
    }

    @VisibleForTesting
    int producersCount() {
        return producers.size();
    }

    @VisibleForTesting
    int consumersCount() {
        return consumers.size();
    }

    private static Mode convertRegexSubscriptionMode(RegexSubscriptionMode regexSubscriptionMode) {
        switch (regexSubscriptionMode) {
        case PersistentOnly:
            return Mode.PERSISTENT;
        case NonPersistentOnly:
            return Mode.NON_PERSISTENT;
        case AllTopics:
            return Mode.ALL;
        default:
            return null;
        }
    }

    private SchemaInfoProvider newSchemaProvider(String topicName) {
        return new MultiVersionSchemaInfoProvider(TopicName.get(topicName), this);
    }

    public LoadingCache<String, SchemaInfoProvider> getSchemaProviderLoadingCache() {
        return schemaProviderLoadingCache;
    }

    public MemoryLimitController getMemoryLimitController() {
        return memoryLimitController;
    }

    @SuppressWarnings("unchecked")
    protected <T> CompletableFuture<Schema<T>> preProcessSchemaBeforeSubscribe(PulsarClientImpl pulsarClientImpl,
                                                                      Schema<T> schema,
                                                                      String topicName) {
        if (schema != null && schema.supportSchemaVersioning()) {
            final SchemaInfoProvider schemaInfoProvider;
            String schemaTopicName = TopicName.getPartitionedTopicName(topicName).toString();
            try {
                schemaInfoProvider = pulsarClientImpl.getSchemaProviderLoadingCache().get(schemaTopicName);
            } catch (ExecutionException e) {
                log.error().attr("topic", schemaTopicName)
                        .exception(e)
                        .log("Failed to load schema info provider for topic");
                return FutureUtil.failedFuture(e.getCause());
            }
            schema = schema.clone();
            if (schema.requireFetchingSchemaInfo()) {
                @SuppressWarnings("rawtypes") Schema finalSchema = schema;
                return schemaInfoProvider.getLatestSchema().thenCompose(schemaInfo -> {
                    if (null == schemaInfo) {
                        if (!(finalSchema instanceof AutoConsumeSchema)
                            && !(finalSchema instanceof KeyValueSchema)) {
                            // no schema info is found
                            return FutureUtil.failedFuture(
                                    new PulsarClientException.NotFoundException(
                                            "No latest schema found for topic " + topicName));
                        }
                    }
                    try {
                        log.info().attr("topic", topicName)
                                .attr("schemaInfo", schemaInfo)
                                .log("Configuring schema for topic");
                        finalSchema.configureSchemaInfo(topicName, "topic", schemaInfo);
                    } catch (RuntimeException re) {
                        return FutureUtil.failedFuture(re);
                    }
                    finalSchema.setSchemaInfoProvider(schemaInfoProvider);
                    return CompletableFuture.completedFuture(finalSchema);
                });
            } else {
                schema.setSchemaInfoProvider(schemaInfoProvider);
            }
        }
        return CompletableFuture.completedFuture(schema);
    }

    public ExecutorService getInternalExecutorService() {
        return internalExecutorProvider.getExecutor();
    }

    public ScheduledExecutorProvider getScheduledExecutorProvider() {
        return scheduledExecutorProvider;
    }

    public InstrumentProvider instrumentProvider() {
        return instrumentProvider;
    }


    //
    // Transaction related API
    //

    // This method should be exposed in the PulsarClient interface. Only expose it when all the transaction features
    // are completed.
    // @Override
    public TransactionBuilder newTransaction() {
        return new TransactionBuilderImpl(this, tcClient);
    }

    NameResolver<InetAddress> getNameResolver() {
        return DnsResolverUtil.adaptToNameResolver(addressResolver);
    }
}
