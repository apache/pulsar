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
package org.apache.pulsar.client.admin.internal;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.OpenTelemetry;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import java.io.IOException;
import java.net.URL;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.Bookies;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.MetadataMigration;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.NonPersistentTopics;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.ProxyStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.ResourceGroups;
import org.apache.pulsar.client.admin.ResourceQuotas;
import org.apache.pulsar.client.admin.ScalableTopics;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Sink;
import org.apache.pulsar.client.admin.Sinks;
import org.apache.pulsar.client.admin.Source;
import org.apache.pulsar.client.admin.Sources;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.admin.Transactions;
import org.apache.pulsar.client.admin.Worker;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnectorProvider;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServicesAware;
import org.apache.pulsar.client.impl.PulsarClientSharedResourcesImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.client.impl.auth.v5.DefaultClientAuthenticationServices;
import org.apache.pulsar.client.impl.auth.v5.FrameworkHttpClientFactory;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.tls.ClientTlsFactorySupport;
import org.apache.pulsar.common.net.ServiceURI;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

/**
 * Pulsar client admin API client.
 */
@SuppressWarnings("deprecation")
@CustomLog
public class PulsarAdminImpl implements PulsarAdmin {

    public static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 300;

    // PIP-478: ceiling for the admin's blocking authentication pool. Smaller than the client's 16 —
    // an admin issues REST calls, not a fan-out of connection attempts against many brokers.
    private static final int AUTH_BLOCKING_MAX_THREADS = 8;

    private final Clusters clusters;
    private final Brokers brokers;
    private final BrokerStats brokerStats;
    private final ProxyStats proxyStats;
    private final Tenants tenants;
    private final ResourceGroups resourcegroups;
    private final Namespaces namespaces;
    private final Bookies bookies;
    private final TopicsImpl topics;
    private final TopicPolicies localTopicPolicies;
    private final TopicPolicies globalTopicPolicies;
    private final NonPersistentTopics nonPersistentTopics;
    private final ResourceQuotas resourceQuotas;
    private final ClientConfigurationData clientConfigData;
    private final Client client;
    @Getter
    private final AsyncHttpConnector asyncHttpConnector;
    private final String serviceUrl;
    private final Lookup lookups;
    private final Functions functions;
    private final Sources sources;
    private final Sinks sinks;
    private final Worker worker;
    private final Schemas schemas;
    private final Packages packages;
    private final Transactions transactions;
    private final MetadataMigration metadataMigration;
    private final ScalableTopics scalableTopics;
    protected final WebTarget root;
    protected final Authentication auth;
    // PIP-478: the framework HTTP client factory bound into a services-aware auth plugin
    // (e.g. OAuth2) so an admin-only client acquires tokens over the framework client; null when the auth
    // plugin does not implement ClientAuthenticationServicesAware. Closed with this admin.
    private FrameworkHttpClientFactory authHttpClientFactory;
    // PIP-478: the admin's own bounded executor for potentially-blocking authentication work — the v4
    // credential composition on every request, and whatever a services-aware plugin off-loads. Shut down
    // with this admin. Constructing it costs one object: a ThreadPoolExecutor starts no threads until a
    // task arrives, and with core threads timing out an admin that issues no authenticated request keeps
    // none.
    private final ThreadPoolExecutor blockingAuthExecutor;
    @Getter
    private AsyncHttpConnectorProvider asyncConnectorProvider;

    public PulsarAdminImpl(String serviceUrl, ClientConfigurationData clientConfigData,
                           ClassLoader clientBuilderClassLoader) throws PulsarClientException {
        this(serviceUrl, clientConfigData, clientBuilderClassLoader, true, null);
    }

    public PulsarAdminImpl(String serviceUrl, ClientConfigurationData clientConfigData,
                           ClassLoader clientBuilderClassLoader, boolean acceptGzipCompression,
                           PulsarClientSharedResourcesImpl sharedResources)
            throws PulsarClientException {
        checkArgument(StringUtils.isNotBlank(serviceUrl), "Service URL needs to be specified");

        this.clientConfigData = clientConfigData;
        this.auth = clientConfigData != null ? clientConfigData.getAuthentication() : new AuthenticationDisabled();
        log.debug().attr("serviceUrl", serviceUrl)
                .attr("authMethodName", auth.getAuthMethodName())
                .log("created");

        if (clientConfigData != null && StringUtils.isBlank(clientConfigData.getServiceUrl())) {
            clientConfigData.setServiceUrl(serviceUrl);
        }

        // Built before the authentication is bound and started, because both may already need the TLS
        // factory it owns: an OAuth2 plugin's start() fetches IdP metadata over HTTPS through the framework
        // HTTP client, which resolves CLIENT_OAUTH2 against this provider's factory. Resolving the service
        // URL first is part of that — the provider reads it to decide whether a factory is needed at all.
        asyncConnectorProvider = new AsyncHttpConnectorProvider(clientConfigData,
                clientConfigData.getAutoCertRefreshSeconds(), acceptGzipCompression);

        this.blockingAuthExecutor = newBlockingAuthExecutor();

        boolean constructed = false;
        try {
        bindAuthenticationServices(clientConfigData);
        this.auth.start();

        ClientConfig httpConfig = new ClientConfig();
        httpConfig.property(ClientProperties.FOLLOW_REDIRECTS, true);
        httpConfig.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 8);
        httpConfig.register(MultiPartFeature.class);
        httpConfig.connectorProvider(asyncConnectorProvider);

        ClassLoader originalCtxLoader = null;
        if (clientBuilderClassLoader != null) {
            originalCtxLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(clientBuilderClassLoader);
        }

        ClientBuilder clientBuilder = ClientBuilder.newBuilder()
                .withConfig(httpConfig)
                .connectTimeout(this.clientConfigData.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(this.clientConfigData.getReadTimeoutMs(), TimeUnit.MILLISECONDS)
                .register(JacksonConfigurator.class).register(JacksonFeature.class);

        boolean useTls = clientConfigData.getServiceUrl().startsWith("https://");

        this.client = clientBuilder.build();

        this.serviceUrl = serviceUrl;
        ServiceURI serviceUri = ServiceURI.create(serviceUrl);
        root = client.target(serviceUri.selectOne());

        this.asyncHttpConnector = asyncConnectorProvider.getConnector(
                Math.toIntExact(clientConfigData.getConnectionTimeoutMs()),
                Math.toIntExact(clientConfigData.getReadTimeoutMs()),
                Math.toIntExact(clientConfigData.getRequestTimeoutMs()),
                clientConfigData.getAutoCertRefreshSeconds(), sharedResources);

        long requestTimeoutMs = clientConfigData.getRequestTimeoutMs();
        this.clusters = lendAuthExecutor(new ClustersImpl(root, auth, requestTimeoutMs));
        this.brokers = lendAuthExecutor(new BrokersImpl(root, auth, requestTimeoutMs));
        this.brokerStats = lendAuthExecutor(new BrokerStatsImpl(root, auth, requestTimeoutMs));
        this.proxyStats = lendAuthExecutor(new ProxyStatsImpl(root, auth, requestTimeoutMs));
        this.tenants = lendAuthExecutor(new TenantsImpl(root, auth, requestTimeoutMs));
        this.resourcegroups = lendAuthExecutor(new ResourceGroupsImpl(root, auth, requestTimeoutMs));
        this.namespaces = lendAuthExecutor(new NamespacesImpl(root, auth, requestTimeoutMs));
        this.topics = lendAuthExecutor(new TopicsImpl(root, auth, requestTimeoutMs));
        this.localTopicPolicies = lendAuthExecutor(new TopicPoliciesImpl(root, auth, requestTimeoutMs, false));
        this.globalTopicPolicies = lendAuthExecutor(new TopicPoliciesImpl(root, auth, requestTimeoutMs, true));
        this.nonPersistentTopics = lendAuthExecutor(new NonPersistentTopicsImpl(root, auth, requestTimeoutMs));
        this.resourceQuotas = lendAuthExecutor(new ResourceQuotasImpl(root, auth, requestTimeoutMs));
        this.lookups = lendAuthExecutor(new LookupImpl(root, auth, useTls, requestTimeoutMs, topics));
        this.functions = lendAuthExecutor(new FunctionsImpl(root, auth, asyncHttpConnector, requestTimeoutMs));
        this.sources = lendAuthExecutor(new SourcesImpl(root, auth, asyncHttpConnector, requestTimeoutMs));
        this.sinks = lendAuthExecutor(new SinksImpl(root, auth, asyncHttpConnector, requestTimeoutMs));
        this.worker = lendAuthExecutor(new WorkerImpl(root, auth, requestTimeoutMs));
        this.schemas = lendAuthExecutor(new SchemasImpl(root, auth, requestTimeoutMs));
        this.bookies = lendAuthExecutor(new BookiesImpl(root, auth, requestTimeoutMs));
        this.packages = lendAuthExecutor(new PackagesImpl(root, auth, asyncHttpConnector, requestTimeoutMs));
        this.transactions = lendAuthExecutor(new TransactionsImpl(root, auth, requestTimeoutMs));
        this.metadataMigration = lendAuthExecutor(new MetadataMigrationImpl(root, auth, requestTimeoutMs));
        this.scalableTopics = lendAuthExecutor(new ScalableTopicsImpl(root, auth, requestTimeoutMs));

        if (originalCtxLoader != null) {
            Thread.currentThread().setContextClassLoader(originalCtxLoader);
        }
        constructed = true;
        } finally {
            if (!constructed) {
                // close() is unreachable when the constructor throws, and the provider owns live resources
                // from the moment its factory is resolved — the TLS factory itself plus a non-daemon
                // "pulsar-admin-tls-factory" rotation thread. Without this, every failed build() (a bad
                // trustCertsFilePath being the common case) leaks one of each. The connectors release only
                // their own borrowed handles, which is exactly why this has to happen here.
                asyncConnectorProvider.close();
                // A services-aware plugin's start() may already have run work here before the failure.
                blockingAuthExecutor.shutdown();
            }
        }
    }

    /**
     * Construct a new Pulsar Admin client object.
     * <p/>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar service URL (eg. 'http://my-broker.example.com:8080')
     * @param auth
     *            the Authentication object to be used to talk with Pulsar
     * @deprecated Since 2.0. Use {@link #builder()} to construct a new {@link PulsarAdmin} instance.
     */
    @Deprecated
    public PulsarAdminImpl(URL serviceUrl, Authentication auth) throws PulsarClientException {
        this(serviceUrl.toString(), getConfigData(auth), null);
    }

    private static ClientConfigurationData getConfigData(Authentication auth) {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setAuthentication(auth);
        return conf;
    }

    /**
     * Construct a new Pulsar Admin client object.
     * <p/>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar URL (eg. 'http://my-broker.example.com:8080')
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @deprecated Since 2.0. Use {@link #builder()} to construct a new {@link PulsarAdmin} instance.
     */
    @Deprecated
    public PulsarAdminImpl(URL serviceUrl, String authPluginClassName, String authParamsString)
            throws PulsarClientException {
        this(serviceUrl, AuthenticationFactory.create(authPluginClassName, authParamsString));
    }

    /**
     * Construct a new Pulsar Admin client object.
     * <p/>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar URL (eg. 'http://my-broker.example.com:8080')
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @deprecated Since 2.0. Use {@link #builder()} to construct a new {@link PulsarAdmin} instance.
     */
    @Deprecated
    public PulsarAdminImpl(URL serviceUrl, String authPluginClassName, Map<String, String> authParams)
            throws PulsarClientException {
        this(serviceUrl, AuthenticationFactory.create(authPluginClassName, authParams));
    }

    /**
     * @return the clusters management object
     */
    public Clusters clusters() {
        return clusters;
    }

    /**
     * @return the brokers management object
     */
    public Brokers brokers() {
        return brokers;
    }

    /**
     * @return the tenants management object
     */
    public Tenants tenants() {
        return tenants;
    }

    /**
     * @return the resourcegroups management object
     */
    public ResourceGroups resourcegroups() {
        return resourcegroups;
    }

    /**
     * @return the namespaces management object
     */
    public Namespaces namespaces() {
        return namespaces;
    }

    public Topics topics() {
        return topics;
    }

    @Override
    public TopicPolicies topicPolicies() {
        return localTopicPolicies;
    }

    @Override
    public TopicPolicies topicPolicies(boolean isGlobal) {
        return isGlobal ? globalTopicPolicies : localTopicPolicies;
    }

    /**
     * @return the bookies management object
     */
    public Bookies bookies() {
        return bookies;
    }

    /**
     * @return the persistentTopics management object
     * @deprecated Since 2.0. See {@link #topics()}
     */
    @Deprecated
    public NonPersistentTopics nonPersistentTopics() {
        return nonPersistentTopics;
    }

    /**
     * @return the resource quota management object
     */
    public ResourceQuotas resourceQuotas() {
        return resourceQuotas;
    }

    /**
     * @return does a looks up for the broker serving the topic
     */
    public Lookup lookups() {
        return lookups;
    }

    /**
     *
     * @return the functions management object
     */
    public Functions functions() {
        return functions;
    }

    /**
     * @return the sources management object
     * @deprecated in favor of {@link #sources()}
     */
    @Deprecated
    public Source source() {
        return (Source) sources;
    }

    public Sources sources() {
        return sources;
    }

    /**
     * @return the sinks management object
     * @deprecated in favor of {@link #sinks}
     */
    @Deprecated
    public Sink sink() {
        return (Sink) sinks;
    }

    /**
     * @return the sinks management object
     */
    public Sinks sinks() {
        return sinks;
    }

    /**
     * @return the Worker stats
     */
    public Worker worker() {
        return worker;
    }

    /**
     * @return the broker statics
     */
    public BrokerStats brokerStats() {
        return brokerStats;
    }

    /**
     * @return the proxy statics
     */
    public ProxyStats proxyStats() {
        return proxyStats;
    }

    /**
     * @return the service HTTP URL that is being used
     */
    public String getServiceUrl() {
        return serviceUrl;
    }

    /**
     * @return the client Configuration Data that is being used
     */
    public ClientConfigurationData getClientConfigData() {
        return clientConfigData;
    }

    /**
     * @return the schemas
     */
    public Schemas schemas() {
        return schemas;
    }

    /**
     * @return the packages management object
     */
    public Packages packages() {
        return packages;
    }

    @Override
    public Transactions transactions() {
        return transactions;
    }

    @Override
    public MetadataMigration metadataMigration() {
        return metadataMigration;
    }

    @Override
    public ScalableTopics scalableTopics() {
        return scalableTopics;
    }

    /**
     * Close the Pulsar admin client to release all the resources.
     */
    /**
     * PIP-478: bind the framework HTTP client factory into a services-aware auth plugin (e.g.
     * OAuth2) before {@code auth.start()}, which eagerly acquires a token via the flow's HTTP client. A
     * {@code PulsarAdmin} used without a {@code PulsarClient} previously rode the now-removed private OAuth2
     * {@code AsyncHttpClient}; this mirrors {@code PulsarClientImpl.bindAuthenticationServices} so admin-only
     * OAuth2 acquires tokens over the framework client. The admin transport is HTTP-only, so the shared event
     * loop / timer / DNS resolver are not threaded here (AsyncHttpClient provisions its own, as the removed
     * private client did); the TLS factory supplier reads the admin's attached factory (broker admin path)
     * when present.
     */
    /**
     * Ensure the admin's TLS factory can serve {@link TlsPurpose#CLIENT_OAUTH2} when the configured OAuth2
     * plugin carries its own IdP TLS material (PIP-478, issue #24944).
     *
     * <p>This mirrors what {@code PulsarClientImpl} does: the IdP policy is folded into the client's own
     * factory rather than the plugin being left to self-provision a standalone one. Folding is the better of
     * the two remedies because the standalone factory is built from a fresh
     * {@code ClientConfigurationData} — losing the admin's SOCKS5 proxy scope and address, so IdP traffic
     * stopped being proxied — and because it duplicates a {@code FileBasedTlsFactory} and its refresh
     * scheduler per admin.
     *
     * <p>Called before the framework HTTP client factory is bound, so that factory has something to resolve
     * {@code CLIENT_OAUTH2} against. A policy the caller supplied explicitly wins: the fold is
     * {@code putIfAbsent}.
     *
     * <p>The fold writes into a map this admin owns, never into one it was handed (#26398).
     * {@code PulsarAdminBuilderImpl.build()} passes a {@code clone()} of the builder's configuration, but
     * {@code ClientConfigurationData.clone()} is {@code super.clone()} — a copy still shares the
     * {@code tlsPolicyMap} <em>instance</em>. Inserting into that shared map would put the first admin's IdP
     * policy back in the builder's reach, and the next admin's {@code putIfAbsent} would find it already
     * there and keep it: two admins with different OAuth2 credentials, the second one trusting the first's
     * IdP material. Copying here rather than deepening {@code clone()} keeps the rule where the mutation is.
     *
     * @param conf the admin client configuration, given this admin's own policy map carrying the IdP policy
     */
    private void foldOAuth2IdpPolicy(ClientConfigurationData conf) {
        if (!(auth instanceof AuthenticationOAuth2 oauth2)) {
            return;
        }
        // PIP-478: inherit the admin's own provider pins onto the IdP leg, on each axis the OAuth2 parameters
        // do not pin themselves. This fold runs before ClientTlsFactorySupport.composePolicies and writes into
        // the same policy map, so its putIfAbsent there can never replace what is inserted here — the
        // inheritance has to happen at this site or not at all.
        TlsPolicy clientDefault = ClientTlsFactorySupport.effectiveClientDefaultPolicy(conf);
        oauth2.idpTlsPolicy(clientDefault.jsseProvider(), clientDefault.jcaProvider()).ifPresent(policy -> {
            Map<TlsPurpose, TlsPolicy> policies = conf.getTlsPolicyMap() == null
                    ? new LinkedHashMap<>() : new LinkedHashMap<>(conf.getTlsPolicyMap());
            policies.putIfAbsent(TlsPurpose.CLIENT_OAUTH2, policy);
            conf.setTlsPolicyMap(policies);
        });
    }

    /**
     * The TLS factory the authentication plugin's framework HTTP client resolves purposes against: the one
     * adopted onto the configuration when present, else the one this admin's {@link AsyncHttpConnector}
     * composed. Returning {@code null} leaves the framework client on platform-default trust, which is correct
     * only when nothing configured a trust domain for it.
     *
     * @param conf this admin's client configuration
     * @return the TLS factory, or {@code null} when none was composed
     */
    private PulsarTlsFactory authTlsFactory(ClientConfigurationData conf) {
        PulsarTlsFactory adopted = conf.getTlsFactory();
        if (adopted != null) {
            return adopted;
        }
        // From the provider rather than a connector: this is called during auth start() as well as per
        // request, and at start() time no connector exists yet. The provider resolves lazily and memoizes,
        // so asking it here composes the one factory this admin uses rather than an extra one.
        return asyncConnectorProvider.tlsFactory();
    }

    /**
     * Whether {@link #bindAuthenticationServices} actually bound the framework HTTP client factory into the
     * authentication plugin.
     *
     * <p>Exists so a test can observe the decision at its real call site rather than by calling the
     * predicate directly: review demonstrated that deleting the guard's invocation here left every test
     * asserting {@code leaveOAuth2Standalone(...)} green, because none of them ran this method
     * (VisibleForTesting).
     *
     * @return whether the framework HTTP client factory was bound
     */
    boolean boundAuthHttpClientFactoryForTest() {
        return authHttpClientFactory != null;
    }

    /**
     * The bound framework HTTP client factory, so a test can issue the IdP request through exactly the client
     * the authentication plugin uses (VisibleForTesting).
     *
     * @return the framework HTTP client factory, or {@code null} when none was bound
     */
    FrameworkHttpClientFactory authHttpClientFactoryForTest() {
        return authHttpClientFactory;
    }

    private void bindAuthenticationServices(ClientConfigurationData conf) {
        if (conf == null || !(auth instanceof ClientAuthenticationServicesAware aware)) {
            return;
        }
        foldOAuth2IdpPolicy(conf);
        // Resolve the shared factory here, not on first use. First use is the OAuth2 plugin's start(), which
        // runs inside the flow's own lock and blocks on material loading; resolving under that lock invites
        // the loading path to deadlock against it. Doing it here — after the fold, so CLIENT_OAUTH2 is in the
        // policy map, and before anything plugin-owned is locked — also surfaces a bad TLS configuration at
        // PulsarAdmin build time rather than at the first authenticated request. No-op when nothing needs it.
        asyncConnectorProvider.tlsFactory();
        String clientInstanceId = "pulsar-admin-" + Integer.toHexString(System.identityHashCode(this));
        // The TLS factory the framework HTTP client resolves its purposes against — notably the CLIENT_OAUTH2
        // policy folded in just above. An adopted factory (the broker's admin-client attach) is already on the
        // configuration; otherwise the one the connector composes is the admin's single factory, and reading it
        // from there rather than composing a second one is what keeps the fold meaningful. The supplier is
        // resolved lazily, at the first authenticated request, because the connector is created later in this
        // constructor.
        this.authHttpClientFactory = new FrameworkHttpClientFactory(
                () -> null, () -> null, () -> null, () -> authTlsFactory(conf), conf, clientInstanceId);
        OpenTelemetry openTelemetry = conf.getOpenTelemetry() != null ? conf.getOpenTelemetry()
                : OpenTelemetry.noop();
        // No scheduler: nothing on the admin path schedules periodic authentication work, and a plugin that
        // does schedule some gets the framework's shared one — binding a scheduled pool per admin to sit idle
        // would cost more than it buys.
        //
        // The blocking executor *is* the admin's own — the same pool BaseResource runs the deprecated v4
        // composition on. This is where the SASL-over-HTTP challenge rounds put their GSSAPI work, so it must
        // never be the admin's request threads — a slow KDC would consume them — but leaving it unbound is
        // not right either: the plugin then shares one process-wide pool with every other client in the JVM,
        // so a stalled identity provider reached by one admin throttles authentication for all of them. A
        // small bounded pool per admin keeps that blast radius inside the admin that owns the plugin, and is
        // shut down with it.
        ClientAuthenticationServices services = new DefaultClientAuthenticationServices(
                authHttpClientFactory, null, blockingAuthExecutor, Clock.systemDefaultZone(), openTelemetry,
                clientInstanceId);
        aware.bindClientAuthenticationServices(services);
    }

    /**
     * Lend a resource this admin's blocking authentication executor, so the deprecated v4 credential
     * composition it runs per request stays on the admin's own pool (PIP-478).
     *
     * @param resource the freshly constructed resource
     * @param <T> the resource type
     * @return the same resource
     */
    private <T extends BaseResource> T lendAuthExecutor(T resource) {
        resource.setBlockingAuthExecutor(blockingAuthExecutor);
        return resource;
    }

    /**
     * Build the admin's bounded executor for potentially-blocking authentication work (PIP-478).
     *
     * <p>Queues rather than rejects, matching the framework's shared pool: every caller is an authenticated
     * request, so a saturated pool must slow requests down rather than fail them — and a queued task is far
     * smaller than what the caller already retains to produce it (a synchronous admin call is a parked
     * thread, bounded by its own {@code requestTimeoutMs}). Core threads time out, so an admin whose plugin
     * never blocks holds no threads. Shut down in {@link #close()}.
     *
     * @return the blocking authentication executor
     */
    private static ThreadPoolExecutor newBlockingAuthExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(AUTH_BLOCKING_MAX_THREADS,
                AUTH_BLOCKING_MAX_THREADS, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable, "pulsar-admin-auth-blocking");
                    thread.setDaemon(true);
                    return thread;
                });
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    @Override
    public void close() {
        try {
            auth.close();
        } catch (IOException e) {
            log.error().exception(e).log("Failed to close the authentication service");
        }
        client.close();

        asyncHttpConnector.close();
        if (authHttpClientFactory != null) {
            authHttpClientFactory.close();
        }
        // PIP-478: last, because everything above holds subscriptions on the factory this closes — the
        // connectors' CLIENT_DEFAULT subscription and the auth HTTP clients' CLIENT_OAUTH2 one. Closing the
        // factory first would tear it down while those subscriptions are still live, which a custom factory
        // is entitled to treat as an error.
        asyncConnectorProvider.close();
        // PIP-478: after auth.close(), so a plugin shutting down over this executor still has it. shutdown()
        // rather than shutdownNow(): queued work is a credential call for a request already in flight, and
        // the threads are daemons, so a straggler cannot hold the JVM up.
        blockingAuthExecutor.shutdown();
    }

    @VisibleForTesting
     WebTarget getRoot() {
        return root;
    }
}
