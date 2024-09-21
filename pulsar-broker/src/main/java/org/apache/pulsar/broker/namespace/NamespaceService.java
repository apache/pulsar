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
package org.apache.pulsar.broker.namespace;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.client.api.PulsarClientException.FailedFeatureCheck.SupportsGetPartitionedMetadataWithoutAutoCreation;
import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import com.google.common.hash.Hashing;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.prometheus.client.Counter;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.manager.RedirectManager;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.internal.PropertiesUtils;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.BundleSplitOption;
import org.apache.pulsar.common.naming.FlowOrQpsEquallyDivideBundleSplitOption;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.NamespaceIsolationPolicy;
import org.apache.pulsar.common.policies.data.BrokerAssignment;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.stats.MetricsUtil;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.opentelemetry.annotations.PulsarDeprecatedMetric;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>NamespaceService</code> provides resource ownership lookup as well as resource ownership claiming services
 * for the <code>PulsarService</code>.
 * <p/>
 * The <code>PulsarService</code> relies on this service for resource ownership operations.
 * <p/>
 * The focus of this phase is to bring up the system and be able to iterate and improve the services effectively.
 * <p/>
 *
 * @see org.apache.pulsar.broker.PulsarService
 */
@Slf4j
public class NamespaceService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(NamespaceService.class);

    private final ServiceConfiguration config;
    private final AtomicReference<LoadManager> loadManager;
    private final PulsarService pulsar;
    private final OwnershipCache ownershipCache;
    private final MetadataCache<LocalBrokerData> localBrokerDataCache;
    private final NamespaceBundleFactory bundleFactory;
    private final String host;

    public static final int BUNDLE_SPLIT_RETRY_LIMIT = 7;
    public static final String SLA_NAMESPACE_PROPERTY = "sla-monitor";
    public static final Pattern HEARTBEAT_NAMESPACE_PATTERN = Pattern.compile("pulsar/[^/]+/([^:]+:\\d+)");
    public static final Pattern HEARTBEAT_NAMESPACE_PATTERN_V2 = Pattern.compile("pulsar/([^:]+:\\d+)");
    public static final Pattern SLA_NAMESPACE_PATTERN = Pattern.compile(SLA_NAMESPACE_PROPERTY + "/[^/]+/([^:]+:\\d+)");
    public static final String HEARTBEAT_NAMESPACE_FMT = "pulsar/%s/%s";
    public static final String HEARTBEAT_NAMESPACE_FMT_V2 = "pulsar/%s";
    public static final String SLA_NAMESPACE_FMT = SLA_NAMESPACE_PROPERTY + "/%s/%s";

    private final Map<ClusterDataImpl, PulsarClientImpl> namespaceClients = new ConcurrentHashMap<>();

    private final List<NamespaceBundleOwnershipListener> bundleOwnershipListeners;

    private final List<NamespaceBundleSplitListener> bundleSplitListeners;


    private final RedirectManager redirectManager;

    public static final String LOOKUP_REQUEST_DURATION_METRIC_NAME = "pulsar.broker.request.topic.lookup.duration";

    private static final AttributeKey<String> PULSAR_LOOKUP_RESPONSE_ATTRIBUTE =
            AttributeKey.stringKey("pulsar.lookup.response");
    public static final Attributes PULSAR_LOOKUP_RESPONSE_BROKER_ATTRIBUTES = Attributes.builder()
            .put(PULSAR_LOOKUP_RESPONSE_ATTRIBUTE, "broker")
            .build();
    public static final Attributes PULSAR_LOOKUP_RESPONSE_REDIRECT_ATTRIBUTES = Attributes.builder()
            .put(PULSAR_LOOKUP_RESPONSE_ATTRIBUTE, "redirect")
            .build();
    public static final Attributes PULSAR_LOOKUP_RESPONSE_FAILURE_ATTRIBUTES = Attributes.builder()
            .put(PULSAR_LOOKUP_RESPONSE_ATTRIBUTE, "failure")
            .build();

    @PulsarDeprecatedMetric(newMetricName = LOOKUP_REQUEST_DURATION_METRIC_NAME)
    private static final Counter lookupRedirects = Counter.build("pulsar_broker_lookup_redirects", "-").register();

    @PulsarDeprecatedMetric(newMetricName = LOOKUP_REQUEST_DURATION_METRIC_NAME)
    private static final Counter lookupFailures = Counter.build("pulsar_broker_lookup_failures", "-").register();

    @PulsarDeprecatedMetric(newMetricName = LOOKUP_REQUEST_DURATION_METRIC_NAME)
    private static final Counter lookupAnswers = Counter.build("pulsar_broker_lookup_answers", "-").register();

    @PulsarDeprecatedMetric(newMetricName = LOOKUP_REQUEST_DURATION_METRIC_NAME)
    private static final Summary lookupLatency = Summary.build("pulsar_broker_lookup", "-")
            .quantile(0.50)
            .quantile(0.99)
            .quantile(0.999)
            .quantile(1.0)
            .register();
    private final DoubleHistogram lookupLatencyHistogram;

    private ConcurrentHashMap<String, CompletableFuture<List<String>>> inProgressQueryUserTopics =
            new ConcurrentHashMap<>();

    /**
     * Default constructor.
     */
    public NamespaceService(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.host = pulsar.getAdvertisedAddress();
        this.config = pulsar.getConfiguration();
        this.loadManager = pulsar.getLoadManager();
        this.bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());
        this.ownershipCache = new OwnershipCache(pulsar, this);
        this.bundleOwnershipListeners = new CopyOnWriteArrayList<>();
        this.bundleSplitListeners = new CopyOnWriteArrayList<>();
        this.localBrokerDataCache = pulsar.getLocalMetadataStore().getMetadataCache(LocalBrokerData.class);
        this.redirectManager = new RedirectManager(pulsar);

        this.lookupLatencyHistogram = pulsar.getOpenTelemetry().getMeter()
                .histogramBuilder(LOOKUP_REQUEST_DURATION_METRIC_NAME)
                .setDescription("The duration of topic lookup requests (either binary or HTTP)")
                .setUnit("s")
                .build();
    }

    public void initialize() {
        if (!getOwnershipCache().refreshSelfOwnerInfo()) {
            throw new RuntimeException("Failed to refresh self owner info.");
        }
    }

    public CompletableFuture<Optional<LookupResult>> getBrokerServiceUrlAsync(TopicName topic, LookupOptions options) {
        long startTime = System.nanoTime();

        CompletableFuture<Optional<LookupResult>> future = getBundleAsync(topic)
                .thenCompose(bundle -> {
                    // Do redirection if the cluster is in rollback or deploying.
                    return findRedirectLookupResultAsync(bundle).thenCompose(optResult -> {
                        if (optResult.isPresent()) {
                            LOG.info("[{}] Redirect lookup request to {} for topic {}",
                                    pulsar.getBrokerId(), optResult.get(), topic);
                            return CompletableFuture.completedFuture(optResult);
                        }
                        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
                            return loadManager.get().findBrokerServiceUrl(Optional.of(topic), bundle, options);
                        } else {
                            // TODO: Add unit tests cover it.
                            return findBrokerServiceUrl(bundle, options);
                        }
                    });
                });

        future.whenComplete((lookupResult, throwable) -> {
            var latencyNs = System.nanoTime() - startTime;
            lookupLatency.observe(latencyNs, TimeUnit.NANOSECONDS);
            Attributes attributes;
            if (throwable == null) {
                if (lookupResult.isPresent()) {
                    if (lookupResult.get().isRedirect()) {
                        lookupRedirects.inc();
                        attributes = PULSAR_LOOKUP_RESPONSE_REDIRECT_ATTRIBUTES;
                    } else {
                        lookupAnswers.inc();
                        attributes = PULSAR_LOOKUP_RESPONSE_BROKER_ATTRIBUTES;
                    }
                } else {
                    // No lookup result, default to reporting as failure.
                    attributes = PULSAR_LOOKUP_RESPONSE_FAILURE_ATTRIBUTES;
                }
            } else {
                lookupFailures.inc();
                attributes = PULSAR_LOOKUP_RESPONSE_FAILURE_ATTRIBUTES;
            }
            lookupLatencyHistogram.record(MetricsUtil.convertToSeconds(latencyNs, TimeUnit.NANOSECONDS), attributes);
        });

        return future;
    }

    private CompletableFuture<Optional<LookupResult>> findRedirectLookupResultAsync(ServiceUnitId bundle) {
        if (isSLAOrHeartbeatNamespace(bundle.getNamespaceObject().toString())) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return redirectManager.findRedirectLookupResultAsync();
    }

    public CompletableFuture<NamespaceBundle> getBundleAsync(TopicName topic) {
        return bundleFactory.getBundlesAsync(topic.getNamespaceObject())
                .thenApply(bundles -> bundles.findBundle(topic));
    }

    public Optional<NamespaceBundle> getBundleIfPresent(TopicName topicName) {
        Optional<NamespaceBundles> bundles = bundleFactory.getBundlesIfPresent(topicName.getNamespaceObject());
        return bundles.map(b -> b.findBundle(topicName));
    }

    public NamespaceBundle getBundle(TopicName topicName) {
        return bundleFactory.getBundles(topicName.getNamespaceObject()).findBundle(topicName);
    }

    public int getBundleCount(NamespaceName namespace) throws Exception {
        return bundleFactory.getBundles(namespace).size();
    }

    private NamespaceBundle getFullBundle(NamespaceName fqnn) throws Exception {
        return bundleFactory.getFullBundle(fqnn);
    }

    private CompletableFuture<NamespaceBundle> getFullBundleAsync(NamespaceName fqnn) {
        return bundleFactory.getFullBundleAsync(fqnn);
    }

    /**
     * Return the URL of the broker who's owning a particular service unit in asynchronous way.
     * <p>
     * If the service unit is not owned, return a CompletableFuture with empty optional.
     */
    public CompletableFuture<Optional<URL>> getWebServiceUrlAsync(ServiceUnitId suName, LookupOptions options) {
        if (suName instanceof TopicName name) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Getting web service URL of topic: {} - options: {}", name, options);
            }
            return getBundleAsync(name)
                    .thenCompose(namespaceBundle ->
                            internalGetWebServiceUrl(name, namespaceBundle, options));
        }

        if (suName instanceof NamespaceName namespaceName) {
            return getFullBundleAsync(namespaceName)
                    .thenCompose(namespaceBundle ->
                            internalGetWebServiceUrl(null, namespaceBundle, options));
        }

        if (suName instanceof NamespaceBundle namespaceBundle) {
            return internalGetWebServiceUrl(null, namespaceBundle, options);
        }

        throw new IllegalArgumentException("Unrecognized class of NamespaceBundle: " + suName.getClass().getName());
    }

    /**
     * Return the URL of the broker who's owning a particular service unit.
     * <p>
     * If the service unit is not owned, return an empty optional
     */
    public Optional<URL> getWebServiceUrl(ServiceUnitId suName, LookupOptions options) throws Exception {
        return getWebServiceUrlAsync(suName, options)
                .get(pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
    }

    private CompletableFuture<Optional<URL>> internalGetWebServiceUrl(@Nullable ServiceUnitId topic,
                                                                      NamespaceBundle bundle,
                                                                      LookupOptions options) {
        return findRedirectLookupResultAsync(bundle).thenCompose(optResult -> {
            if (optResult.isPresent()) {
                LOG.info("[{}] Redirect lookup request to {} for topic {}",
                        pulsar.getBrokerId(), optResult.get(), topic);
                try {
                    LookupData lookupData = optResult.get().getLookupData();
                    final String redirectUrl = options.isRequestHttps()
                            ? lookupData.getHttpUrlTls() : lookupData.getHttpUrl();
                    return CompletableFuture.completedFuture(Optional.of(new URL(redirectUrl)));
                } catch (Exception e) {
                    // just log the exception, nothing else to do
                    LOG.warn("internalGetWebServiceUrl [{}]", e.getMessage(), e);
                }
                return CompletableFuture.completedFuture(Optional.empty());
            }
            CompletableFuture<Optional<LookupResult>> future =
                    ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)
                    ? loadManager.get().findBrokerServiceUrl(Optional.ofNullable(topic), bundle, options) :
                    findBrokerServiceUrl(bundle, options);

            return future.thenApply(lookupResult -> {
                if (lookupResult.isPresent()) {
                    try {
                        LookupData lookupData = lookupResult.get().getLookupData();
                        final String redirectUrl = options.isRequestHttps()
                                ? lookupData.getHttpUrlTls() : lookupData.getHttpUrl();
                        return Optional.of(new URL(redirectUrl));
                    } catch (Exception e) {
                        // just log the exception, nothing else to do
                        LOG.warn("internalGetWebServiceUrl [{}]", e.getMessage(), e);
                    }
                }
                return Optional.empty();
            });
        });
    }

    /**
     * Register all the bootstrap name spaces including the heartbeat namespace.
     *
     * @throws PulsarServerException if an unexpected error occurs
     */
    public void registerBootstrapNamespaces() throws PulsarServerException {
        String brokerId = pulsar.getBrokerId();
        // ensure that we own the heartbeat namespace
        if (registerNamespace(getHeartbeatNamespace(brokerId, config), true)) {
            LOG.info("added heartbeat namespace name in local cache: ns={}",
                    getHeartbeatNamespace(brokerId, config));
        }

        // ensure that we own the heartbeat namespace
        if (registerNamespace(getHeartbeatNamespaceV2(brokerId, config), true)) {
            LOG.info("added heartbeat namespace name in local cache: ns={}",
                    getHeartbeatNamespaceV2(brokerId, config));
        }

        // we may not need strict ownership checking for bootstrap names for now
        for (String namespace : config.getBootstrapNamespaces()) {
            if (registerNamespace(NamespaceName.get(namespace), false)) {
                LOG.info("added bootstrap namespace name in local cache: ns={}", namespace);
            }
        }
    }

    /**
     * Tries to register a namespace to this instance.
     *
     * @param nsname namespace name
     * @param ensureOwned sets the behavior when the namespace is already owned by another broker.
     *                    If this flag is set to true, then the method will throw an exception.
     *                    If this flag is set to false, then the method will return false.
     * @return true if the namespace was successfully registered, false otherwise
     * @throws PulsarServerException if an error occurs when registering the namespace
     */
    public boolean registerNamespace(NamespaceName nsname, boolean ensureOwned) throws PulsarServerException {
        try {
            // all pre-registered namespace is assumed to have bundles disabled
            NamespaceBundle nsFullBundle = bundleFactory.getFullBundle(nsname);
            // v2 namespace will always use full bundle object
            final NamespaceEphemeralData otherData;
            if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
                ExtensibleLoadManagerImpl loadManager = ExtensibleLoadManagerImpl.get(this.loadManager.get());
                otherData = loadManager.tryAcquiringOwnership(nsFullBundle).get();
            } else {
                otherData = ownershipCache.tryAcquiringOwnership(nsFullBundle).get();
            }

            if (StringUtils.equals(pulsar.getBrokerServiceUrl(), otherData.getNativeUrl())
                || StringUtils.equals(pulsar.getBrokerServiceUrlTls(), otherData.getNativeUrlTls())) {
                if (nsFullBundle != null) {
                    // preload heartbeat namespace
                    pulsar.loadNamespaceTopics(nsFullBundle);
                }
                return true;
            }

            String msg = String.format("namespace already owned by other broker : ns=%s expected=%s actual=%s",
                    nsname,
                    StringUtils.defaultString(pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls()),
                    StringUtils.defaultString(otherData.getNativeUrl(), otherData.getNativeUrlTls()));

            // ignore if not be owned for now
            if (!ensureOwned) {
                LOG.info(msg);
                return false;
            }

            // should not happen
            throw new IllegalStateException(msg);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        }
    }

    private final Map<NamespaceBundle, CompletableFuture<Optional<LookupResult>>>
            findingBundlesAuthoritative = new ConcurrentHashMap<>();
    private final Map<NamespaceBundle, CompletableFuture<Optional<LookupResult>>>
            findingBundlesNotAuthoritative = new ConcurrentHashMap<>();

    /**
     * Main internal method to lookup and setup ownership of service unit to a broker.
     *
     * @param bundle the namespace bundle
     * @param options the lookup options
     * @return the lookup result
     */
    private CompletableFuture<Optional<LookupResult>> findBrokerServiceUrl(
            NamespaceBundle bundle, LookupOptions options) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("findBrokerServiceUrl: {} - options: {}", bundle, options);
        }

        Map<NamespaceBundle, CompletableFuture<Optional<LookupResult>>> targetMap;
        if (options.isAuthoritative()) {
            targetMap = findingBundlesAuthoritative;
        } else {
            targetMap = findingBundlesNotAuthoritative;
        }

        return targetMap.computeIfAbsent(bundle, (k) -> {
            CompletableFuture<Optional<LookupResult>> future = new CompletableFuture<>();

            // First check if we or someone else already owns the bundle
            ownershipCache.getOwnerAsync(bundle).thenAccept(nsData -> {
                if (nsData.isEmpty()) {
                    // No one owns this bundle

                    if (options.isReadOnly()) {
                        // Do not attempt to acquire ownership
                        future.complete(Optional.empty());
                    } else {
                        // Now, no one owns the namespace yet. Hence, we will try to dynamically assign it
                        pulsar.getExecutor().execute(() -> searchForCandidateBroker(bundle, future, options));
                    }
                } else if (nsData.get().isDisabled()) {
                    future.completeExceptionally(
                            new IllegalStateException(String.format("Namespace bundle %s is being unloaded", bundle)));
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Namespace bundle {} already owned by {} ", bundle, nsData);
                    }
                    // find the target
                    if (options.hasAdvertisedListenerName()) {
                        AdvertisedListener listener =
                                nsData.get().getAdvertisedListeners().get(options.getAdvertisedListenerName());
                        if (listener == null) {
                            future.completeExceptionally(
                                    new PulsarServerException("the broker do not have "
                                            + options.getAdvertisedListenerName() + " listener"));
                        } else {
                            URI url = listener.getBrokerServiceUrl();
                            URI urlTls = listener.getBrokerServiceUrlTls();
                            future.complete(Optional.of(new LookupResult(nsData.get(),
                                    url == null ? null : url.toString(),
                                    urlTls == null ? null : urlTls.toString())));
                        }
                    } else {
                        future.complete(Optional.of(new LookupResult(nsData.get())));
                    }
                }
            }).exceptionally(exception -> {
                LOG.warn("Failed to check owner for bundle {}: {}", bundle, exception.getMessage(), exception);
                future.completeExceptionally(exception);
                return null;
            });

            future.whenComplete((r, t) -> pulsar.getExecutor().execute(
                () -> targetMap.remove(bundle)
            ));

            return future;
        });
    }

    /**
     * Check if this is Heartbeat or SLAMonitor namespace and return the broker id.
     *
     * @param serviceUnit the service unit
     * @param isBrokerActive the function to check if the broker is active
     * @return the broker id
     */
    public CompletableFuture<String> getHeartbeatOrSLAMonitorBrokerId(
            ServiceUnitId serviceUnit, Function<String, CompletableFuture<Boolean>> isBrokerActive) {
        String candidateBroker = NamespaceService.checkHeartbeatNamespace(serviceUnit);
        if (candidateBroker != null) {
            return CompletableFuture.completedFuture(candidateBroker);
        }
        candidateBroker = NamespaceService.checkHeartbeatNamespaceV2(serviceUnit);
        if (candidateBroker != null) {
            return CompletableFuture.completedFuture(candidateBroker);
        }
        candidateBroker = NamespaceService.getSLAMonitorBrokerName(serviceUnit);
        if (candidateBroker != null) {
            // Check if the broker is available
            final String finalCandidateBroker = candidateBroker;
            return isBrokerActive.apply(candidateBroker).thenApply(isActive -> {
                if (isActive) {
                    return finalCandidateBroker;
                } else {
                    return null;
                }
            });
        }
        return CompletableFuture.completedFuture(null);
    }

    private void searchForCandidateBroker(NamespaceBundle bundle,
                                          CompletableFuture<Optional<LookupResult>> lookupFuture,
                                          LookupOptions options) {
        String candidateBroker;
        LeaderElectionService les = pulsar.getLeaderElectionService();
        if (les == null) {
            LOG.warn("The leader election has not yet been completed! NamespaceBundle[{}]", bundle);
            lookupFuture.completeExceptionally(
                    new IllegalStateException("The leader election has not yet been completed!"));
            return;
        }

        boolean authoritativeRedirect = les.isLeader();

        try {
            // check if this is Heartbeat or SLAMonitor namespace
            candidateBroker = getHeartbeatOrSLAMonitorBrokerId(bundle, cb ->
                    CompletableFuture.completedFuture(isBrokerActive(cb)))
                    .get(config.getMetadataStoreOperationTimeoutSeconds(), SECONDS);

            if (candidateBroker == null) {
                Optional<LeaderBroker> currentLeader = pulsar.getLeaderElectionService().getCurrentLeader();

                if (options.isAuthoritative()) {
                    // leader broker already assigned the current broker as owner
                    candidateBroker = pulsar.getBrokerId();
                } else {
                    LoadManager loadManager = this.loadManager.get();
                    boolean makeLoadManagerDecisionOnThisBroker = !loadManager.isCentralized() || les.isLeader();
                    if (!makeLoadManagerDecisionOnThisBroker) {
                        // If leader is not active, fallback to pick the least loaded from current broker loadmanager
                        boolean leaderBrokerActive = currentLeader.isPresent()
                                && isBrokerActive(currentLeader.get().getBrokerId());
                        if (!leaderBrokerActive) {
                            makeLoadManagerDecisionOnThisBroker = true;
                            if (currentLeader.isEmpty()) {
                                LOG.warn(
                                        "The information about the current leader broker wasn't available. "
                                                + "Handling load manager decisions in a decentralized way. "
                                                + "NamespaceBundle[{}]",
                                        bundle);
                            } else {
                                LOG.warn(
                                        "The current leader broker {} isn't active. "
                                                + "Handling load manager decisions in a decentralized way. "
                                                + "NamespaceBundle[{}]",
                                        currentLeader.get(), bundle);
                            }
                        }
                    }
                    if (makeLoadManagerDecisionOnThisBroker) {
                        Optional<String> availableBroker = getLeastLoadedFromLoadManager(bundle);
                        if (availableBroker.isEmpty()) {
                            LOG.warn("Load manager didn't return any available broker. "
                                            + "Returning empty result to lookup. NamespaceBundle[{}]",
                                    bundle);
                            lookupFuture.complete(Optional.empty());
                            return;
                        }
                        candidateBroker = availableBroker.get();
                        authoritativeRedirect = true;
                    } else {
                        // forward to leader broker to make assignment
                        candidateBroker = currentLeader.get().getBrokerId();
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Error when searching for candidate broker to acquire {}: {}", bundle, e.getMessage(), e);
            lookupFuture.completeExceptionally(e);
            return;
        }

        try {
            Objects.requireNonNull(candidateBroker);

            if (candidateBroker.equals(pulsar.getBrokerId())) {
                // Load manager decided that the local broker should try to become the owner
                ownershipCache.tryAcquiringOwnership(bundle).thenAccept(ownerInfo -> {
                    if (ownerInfo.isDisabled()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Namespace bundle {} is currently being unloaded", bundle);
                        }
                        lookupFuture.completeExceptionally(new IllegalStateException(
                                String.format("Namespace bundle %s is currently being unloaded", bundle)));
                    } else {
                        // Found owner for the namespace bundle

                        if (options.isLoadTopicsInBundle()) {
                            // Schedule the task to preload topics
                            pulsar.loadNamespaceTopics(bundle);
                        }
                        // find the target
                        if (options.hasAdvertisedListenerName()) {
                            AdvertisedListener listener =
                                    ownerInfo.getAdvertisedListeners().get(options.getAdvertisedListenerName());
                            if (listener == null) {
                                lookupFuture.completeExceptionally(
                                        new PulsarServerException("the broker do not have "
                                                + options.getAdvertisedListenerName() + " listener"));
                            } else {
                                URI url = listener.getBrokerServiceUrl();
                                URI urlTls = listener.getBrokerServiceUrlTls();
                                lookupFuture.complete(Optional.of(
                                        new LookupResult(ownerInfo,
                                                url == null ? null : url.toString(),
                                                urlTls == null ? null : urlTls.toString())));
                            }
                        } else {
                            lookupFuture.complete(Optional.of(new LookupResult(ownerInfo)));
                        }
                    }
                }).exceptionally(exception -> {
                    LOG.warn("Failed to acquire ownership for namespace bundle {}: {}", bundle, exception);
                    lookupFuture.completeExceptionally(new PulsarServerException(
                            "Failed to acquire ownership for namespace bundle " + bundle, exception));
                    return null;
                });

            } else {
                // Load managed decider some other broker should try to acquire ownership

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Redirecting to broker {} to acquire ownership of bundle {}", candidateBroker, bundle);
                }

                // Now setting the redirect url
                createLookupResult(candidateBroker, authoritativeRedirect, options.getAdvertisedListenerName())
                        .thenAccept(lookupResult -> lookupFuture.complete(Optional.of(lookupResult)))
                        .exceptionally(ex -> {
                            lookupFuture.completeExceptionally(ex);
                            return null;
                        });

            }
        } catch (Exception e) {
            LOG.warn("Error in trying to acquire namespace bundle ownership for {}: {}", bundle, e.getMessage(), e);
            lookupFuture.completeExceptionally(e);
        }
    }

    public CompletableFuture<LookupResult> createLookupResult(String candidateBroker, boolean authoritativeRedirect,
                                                                 final String advertisedListenerName) {

        CompletableFuture<LookupResult> lookupFuture = new CompletableFuture<>();
        try {
            checkArgument(StringUtils.isNotBlank(candidateBroker), "Lookup broker can't be null %s", candidateBroker);
            String path = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + candidateBroker;

            localBrokerDataCache.get(path).thenAccept(reportData -> {
                if (reportData.isPresent()) {
                    LocalBrokerData lookupData = reportData.get();
                    if (StringUtils.isNotBlank(advertisedListenerName)) {
                        AdvertisedListener listener = lookupData.getAdvertisedListeners().get(advertisedListenerName);
                        if (listener == null) {
                            lookupFuture.completeExceptionally(
                                    new PulsarServerException(
                                            "the broker do not have " + advertisedListenerName + " listener"));
                        } else {
                            URI url = listener.getBrokerServiceUrl();
                            URI urlTls = listener.getBrokerServiceUrlTls();
                            lookupFuture.complete(new LookupResult(lookupData.getWebServiceUrl(),
                                    lookupData.getWebServiceUrlTls(), url == null ? null : url.toString(),
                                    urlTls == null ? null : urlTls.toString(), authoritativeRedirect));
                        }
                    } else {
                        lookupFuture.complete(new LookupResult(lookupData.getWebServiceUrl(),
                                lookupData.getWebServiceUrlTls(), lookupData.getPulsarServiceUrl(),
                                lookupData.getPulsarServiceUrlTls(), authoritativeRedirect));
                    }
                } else {
                    lookupFuture.completeExceptionally(new MetadataStoreException.NotFoundException(path));
                }
            }).exceptionally(ex -> {
                lookupFuture.completeExceptionally(ex);
                return null;
            });
        } catch (Exception e) {
            lookupFuture.completeExceptionally(e);
        }
        return lookupFuture;
    }

    public boolean isBrokerActive(String candidateBroker) {
        Set<String> availableBrokers = getAvailableBrokers();
        if (availableBrokers.contains(candidateBroker)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Broker {} is available for.", candidateBroker);
            }
            return true;
        } else {
            LOG.warn("Broker {} couldn't be found in available brokers {}",
                    candidateBroker, String.join(",", availableBrokers));
            return false;
        }
    }

    private Set<String> getAvailableBrokers() {
        try {
            return loadManager.get().getAvailableBrokers();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper function to encapsulate the logic to invoke between old and new load manager.
     *
     * @param serviceUnit the service unit
     * @return the least loaded broker addresses
     * @throws Exception if an error occurs
     */
    private Optional<String> getLeastLoadedFromLoadManager(ServiceUnitId serviceUnit) throws Exception {
        Optional<ResourceUnit> leastLoadedBroker = loadManager.get().getLeastLoaded(serviceUnit);
        if (leastLoadedBroker.isEmpty()) {
            LOG.warn("No broker is available for {}", serviceUnit);
            return Optional.empty();
        }

        String lookupAddress = leastLoadedBroker.get().getResourceId();

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} : redirecting to the least loaded broker, lookup address={}",
                    pulsar.getBrokerId(),
                    lookupAddress);
        }
        return Optional.of(lookupAddress);
    }

    public CompletableFuture<Void> unloadNamespaceBundle(NamespaceBundle bundle) {
        return unloadNamespaceBundle(bundle, Optional.empty());
    }

    public CompletableFuture<Void> unloadNamespaceBundle(NamespaceBundle bundle, Optional<String> destinationBroker) {

        // unload namespace bundle
        return unloadNamespaceBundle(bundle, destinationBroker,
                config.getNamespaceBundleUnloadingTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> unloadNamespaceBundle(NamespaceBundle bundle,
                                                         Optional<String> destinationBroker,
                                                         long timeout,
                                                         TimeUnit timeoutUnit) {
        return unloadNamespaceBundle(bundle, destinationBroker, timeout, timeoutUnit, true);
    }

    public CompletableFuture<Void> unloadNamespaceBundle(NamespaceBundle bundle,
                                                         long timeout,
                                                         TimeUnit timeoutUnit) {
        return unloadNamespaceBundle(bundle, Optional.empty(), timeout, timeoutUnit, true);
    }

    public CompletableFuture<Void> unloadNamespaceBundle(NamespaceBundle bundle,
                                                         long timeout,
                                                         TimeUnit timeoutUnit,
                                                         boolean closeWithoutWaitingClientDisconnect) {
        return unloadNamespaceBundle(bundle, Optional.empty(), timeout,
                timeoutUnit, closeWithoutWaitingClientDisconnect);
    }

    public CompletableFuture<Void> unloadNamespaceBundle(NamespaceBundle bundle,
                                                         Optional<String> destinationBroker,
                                                         long timeout,
                                                         TimeUnit timeoutUnit,
                                                         boolean closeWithoutWaitingClientDisconnect) {
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            return ExtensibleLoadManagerImpl.get(loadManager.get())
                    .unloadNamespaceBundleAsync(bundle, destinationBroker, false, timeout, timeoutUnit);
        }
        // unload namespace bundle
        OwnedBundle ob = ownershipCache.getOwnedBundle(bundle);
        if (ob == null) {
            return FutureUtil.failedFuture(new IllegalStateException("Bundle " + bundle + " is not currently owned"));
        } else {
            return ob.handleUnloadRequest(pulsar, timeout, timeoutUnit, closeWithoutWaitingClientDisconnect);
        }
    }

    public CompletableFuture<Boolean> isNamespaceBundleOwned(NamespaceBundle bundle) {
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            ExtensibleLoadManagerImpl extensibleLoadManager = ExtensibleLoadManagerImpl.get(loadManager.get());
            return extensibleLoadManager.getOwnershipAsync(Optional.empty(), bundle)
                    .thenApply(Optional::isPresent);
        }
        return pulsar.getLocalMetadataStore().exists(ServiceUnitUtils.path(bundle));
    }

    public CompletableFuture<Map<String, NamespaceOwnershipStatus>> getOwnedNameSpacesStatusAsync() {
       return pulsar.getPulsarResources().getNamespaceResources().getIsolationPolicies()
               .getIsolationDataPoliciesAsync(pulsar.getConfiguration().getClusterName())
               .thenApply(nsIsolationPoliciesOpt -> nsIsolationPoliciesOpt.orElseGet(NamespaceIsolationPolicies::new))
               .thenCompose(namespaceIsolationPolicies -> {
                   if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
                       ExtensibleLoadManagerImpl extensibleLoadManager =
                               ExtensibleLoadManagerImpl.get(loadManager.get());
                       return extensibleLoadManager.getOwnedServiceUnitsAsync()
                               .thenApply(OwnedServiceUnits -> OwnedServiceUnits.stream()
                                       .collect(Collectors.toMap(NamespaceBundle::toString,
                                               bundle -> getNamespaceOwnershipStatus(true,
                                                       namespaceIsolationPolicies.getPolicyByNamespace(
                                                               bundle.getNamespaceObject())))));
                   }
                    Collection<CompletableFuture<OwnedBundle>> futures =
                            ownershipCache.getOwnedBundlesAsync().values();
                    return FutureUtil.waitForAll(futures)
                            .thenApply(__ -> futures.stream()
                                    .map(CompletableFuture::join)
                                    .collect(Collectors.toMap(bundle -> bundle.getNamespaceBundle().toString(),
                                            bundle -> getNamespaceOwnershipStatus(bundle.isActive(),
                                                    namespaceIsolationPolicies.getPolicyByNamespace(
                                                            bundle.getNamespaceBundle().getNamespaceObject()))
                                    ))
                            );
                });
    }

    private NamespaceOwnershipStatus getNamespaceOwnershipStatus(boolean isActive,
            NamespaceIsolationPolicy nsIsolationPolicy) {
        NamespaceOwnershipStatus nsOwnedStatus = new NamespaceOwnershipStatus(BrokerAssignment.shared, false,
                isActive);
        if (nsIsolationPolicy == null) {
            // no matching policy found, this namespace must be an uncontrolled one and using shared broker
            return nsOwnedStatus;
        }
        // found corresponding policy, set the status to controlled
        nsOwnedStatus.is_controlled = true;
        if (nsIsolationPolicy.isPrimaryBroker(pulsar.getAdvertisedAddress())) {
            nsOwnedStatus.broker_assignment = BrokerAssignment.primary;
        } else if (nsIsolationPolicy.isSecondaryBroker(pulsar.getAdvertisedAddress())) {
            nsOwnedStatus.broker_assignment = BrokerAssignment.secondary;
        }

        return nsOwnedStatus;
    }

    public boolean isNamespaceBundleDisabled(NamespaceBundle bundle) throws Exception {
        try {
            // Does ZooKeeper say that the namespace is disabled?
            CompletableFuture<Optional<NamespaceEphemeralData>> nsDataFuture = ownershipCache.getOwnerAsync(bundle);
            if (nsDataFuture != null) {
                Optional<NamespaceEphemeralData> nsData = nsDataFuture.getNow(null);
                if (nsData != null && nsData.isPresent()) {
                    return nsData.get().isDisabled();
                } else {
                    return false;
                }
            } else {
                // if namespace is not owned, it is not considered disabled
                return false;
            }
        } catch (Exception e) {
            LOG.warn("Exception in getting ownership info for service unit {}: {}", bundle, e.getMessage(), e);
        }

        return false;
    }

    /**
     * 1. split the given bundle into two bundles 2. assign ownership of both the bundles to current broker 3. update
     * policies with newly created bundles into LocalZK 4. disable original bundle and refresh the cache.
     * <p>
     * It will call splitAndOwnBundleOnceAndRetry to do the real retry work, which will retry "retryTimes".
     *
     * @param bundle the bundle to split
     * @param unload whether to unload the new split bundles
     * @param splitAlgorithm the algorithm to split the bundle
     * @param boundaries the boundaries to split the bundle
     * @return a future that will complete when the bundle is split and owned
     */
    public CompletableFuture<Void> splitAndOwnBundle(NamespaceBundle bundle, boolean unload,
                                                     NamespaceBundleSplitAlgorithm splitAlgorithm,
                                                     List<Long> boundaries) {
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            return ExtensibleLoadManagerImpl.get(loadManager.get())
                    .splitNamespaceBundleAsync(bundle, splitAlgorithm, boundaries);
        }
        final CompletableFuture<Void> unloadFuture = new CompletableFuture<>();
        final AtomicInteger counter = new AtomicInteger(BUNDLE_SPLIT_RETRY_LIMIT);
        splitAndOwnBundleOnceAndRetry(bundle, unload, counter, unloadFuture, splitAlgorithm, boundaries);

        return unloadFuture;
    }

    void splitAndOwnBundleOnceAndRetry(NamespaceBundle bundle,
                                       boolean unload,
                                       AtomicInteger counter,
                                       CompletableFuture<Void> completionFuture,
                                       NamespaceBundleSplitAlgorithm splitAlgorithm,
                                       List<Long> boundaries) {
        BundleSplitOption bundleSplitOption = getBundleSplitOption(bundle, boundaries, config);

        splitAlgorithm.getSplitBoundary(bundleSplitOption).whenComplete((splitBoundaries, ex) -> {
            CompletableFuture<List<NamespaceBundle>> updateFuture = new CompletableFuture<>();
            if (ex == null) {
                if (splitBoundaries == null || splitBoundaries.size() == 0) {
                    LOG.info("[{}] No valid boundary found in {} to split bundle {}",
                            bundle.getNamespaceObject().toString(), boundaries, bundle.getBundleRange());
                    completionFuture.complete(null);
                    return;
                }
                try {
                    bundleFactory.splitBundles(bundle, splitBoundaries.size() + 1, splitBoundaries)
                            .thenAccept(splitBundles -> {
                                // Split and updateNamespaceBundles. Update may fail because of concurrent write to
                                // Zookeeper.
                                if (splitBundles == null) {
                                    String msg = format("bundle %s not found under namespace", bundle.toString());
                                    LOG.warn(msg);
                                    updateFuture.completeExceptionally(new ServiceUnitNotReadyException(msg));
                                    return;
                                }

                                Objects.requireNonNull(splitBundles.getLeft());
                                Objects.requireNonNull(splitBundles.getRight());
                                checkArgument(splitBundles.getRight().size() == splitBoundaries.size() + 1,
                                        "bundle has to be split in " + (splitBoundaries.size() + 1) + " bundles");
                                NamespaceName nsname = bundle.getNamespaceObject();
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("[{}] splitAndOwnBundleOnce: {}, counter: {}, bundles: {}",
                                            nsname.toString(), bundle.getBundleRange(), counter.get(),
                                            splitBundles.getRight());
                                }
                                try {
                                    // take ownership of newly split bundles
                                    for (NamespaceBundle sBundle : splitBundles.getRight()) {
                                        Objects.requireNonNull(ownershipCache.tryAcquiringOwnership(sBundle));
                                    }
                                    updateNamespaceBundles(nsname, splitBundles.getLeft()).thenCompose(__ ->
                                        updateNamespaceBundlesForPolicies(nsname, splitBundles.getLeft()))
                                            .thenRun(() -> {
                                                bundleFactory.invalidateBundleCache(bundle.getNamespaceObject());
                                                updateFuture.complete(splitBundles.getRight());
                                    }).exceptionally(ex1 -> {
                                        String msg = format("failed to update namespace policies [%s], "
                                                        + "NamespaceBundle: %s due to %s",
                                                nsname.toString(), bundle.getBundleRange(), ex1.getMessage());
                                        LOG.warn(msg);
                                        updateFuture.completeExceptionally(
                                                new ServiceUnitNotReadyException(msg, ex1.getCause()));
                                        return null;
                                    });
                                } catch (Exception e) {
                                    String msg = format(
                                            "failed to acquire ownership of split bundle for namespace [%s], %s",
                                            nsname.toString(), e.getMessage());
                                    LOG.warn(msg, e);
                                    updateFuture.completeExceptionally(new ServiceUnitNotReadyException(msg, e));
                                }
                            });
                } catch (Exception e) {
                    updateFuture.completeExceptionally(e);
                }
            } else {
                updateFuture.completeExceptionally(ex);
            }

            // If success updateNamespaceBundles, then do invalidateBundleCache and unload.
            // Else retry splitAndOwnBundleOnceAndRetry.
            updateFuture.whenCompleteAsync((r, t)-> {
                if (t != null) {
                    // retry several times on BadVersion
                    if ((t.getCause() instanceof MetadataStoreException.BadVersionException)
                            && (counter.decrementAndGet() >= 0)) {
                        pulsar.getExecutor().schedule(() -> pulsar.getOrderedExecutor()
                                .execute(() -> splitAndOwnBundleOnceAndRetry(
                                        bundle, unload, counter, completionFuture, splitAlgorithm, boundaries)),
                                100, MILLISECONDS);
                    } else if (t instanceof IllegalArgumentException) {
                        completionFuture.completeExceptionally(t);
                    } else {
                        // Retry enough, or meet other exception
                        String msg2 = format(" %s not success update nsBundles, counter %d, reason %s",
                            bundle.toString(), counter.get(), t.getMessage());
                        LOG.warn(msg2);
                        completionFuture.completeExceptionally(new ServiceUnitNotReadyException(msg2));
                    }
                    return;
                }

                // success updateNamespaceBundles
                // disable old bundle in memory
                getOwnershipCache().updateBundleState(bundle, false)
                        .thenRun(() -> {
                            // update bundled_topic cache for load-report-generation
                            pulsar.getBrokerService().refreshTopicToStatsMaps(bundle);
                            loadManager.get().setLoadReportForceUpdateFlag();
                            // release old bundle from ownership cache
                            pulsar.getNamespaceService().getOwnershipCache().removeOwnership(bundle);
                            completionFuture.complete(null);
                            if (unload) {
                                // Unload new split bundles, in background. This will not
                                // affect the split operation which is already safely completed
                                r.forEach(this::unloadNamespaceBundle);
                            }
                            onNamespaceBundleSplit(bundle);
                        })
                        .exceptionally(e -> {
                            String msg1 = format(
                                    "failed to disable bundle %s under namespace [%s] with error %s",
                                    bundle.getNamespaceObject().toString(), bundle, ex.getMessage());
                            LOG.warn(msg1, e);
                            completionFuture.completeExceptionally(new ServiceUnitNotReadyException(msg1));
                            return null;
                        });
            }, pulsar.getOrderedExecutor());
        });
    }

    /**
     * Get the split boundary's.
     *
     * @param bundle The bundle to split.
     * @param boundaries The specified positions,
     *                   use for {@link org.apache.pulsar.common.naming.SpecifiedPositionsBundleSplitAlgorithm}.
     * @return A pair, left is target namespace bundle, right is split bundles.
     */
    public CompletableFuture<Pair<NamespaceBundles, List<NamespaceBundle>>> getSplitBoundary(
            NamespaceBundle bundle, NamespaceBundleSplitAlgorithm nsBundleSplitAlgorithm, List<Long> boundaries) {
        CompletableFuture<List<Long>> splitBoundary = getSplitBoundary(bundle, boundaries, nsBundleSplitAlgorithm);
        return splitBoundary.thenCompose(splitBoundaries -> {
                    if (splitBoundaries == null || splitBoundaries.size() == 0) {
                        LOG.info("[{}] No valid boundary found in {} to split bundle {}",
                                bundle.getNamespaceObject().toString(), boundaries, bundle.getBundleRange());
                        return CompletableFuture.completedFuture(null);
                    }
                    return pulsar.getNamespaceService().getNamespaceBundleFactory()
                            .splitBundles(bundle, splitBoundaries.size() + 1, splitBoundaries);
                });
    }

    public CompletableFuture<List<Long>> getSplitBoundary(
            NamespaceBundle bundle, List<Long> boundaries, NamespaceBundleSplitAlgorithm nsBundleSplitAlgorithm) {
        BundleSplitOption bundleSplitOption = getBundleSplitOption(bundle, boundaries, config);
        return nsBundleSplitAlgorithm.getSplitBoundary(bundleSplitOption);
    }

    private BundleSplitOption getBundleSplitOption(NamespaceBundle bundle,
                                                   List<Long> boundaries,
                                                   ServiceConfiguration config) {
        BundleSplitOption bundleSplitOption;
        if (config.getDefaultNamespaceBundleSplitAlgorithm()
                .equals(NamespaceBundleSplitAlgorithm.FLOW_OR_QPS_EQUALLY_DIVIDE)) {
            Map<String, TopicStatsImpl> topicStatsMap =  pulsar.getBrokerService().getTopicStats(bundle);
            bundleSplitOption = new FlowOrQpsEquallyDivideBundleSplitOption(this, bundle, boundaries,
                    topicStatsMap,
                    config.getLoadBalancerNamespaceBundleMaxMsgRate(),
                    config.getLoadBalancerNamespaceBundleMaxBandwidthMbytes(),
                    config.getFlowOrQpsDifferenceThresholdPercentage());
        } else {
            bundleSplitOption = new BundleSplitOption(this, bundle, boundaries);
        }
        return bundleSplitOption;
    }

    public NamespaceBundleSplitAlgorithm getNamespaceBundleSplitAlgorithmByName(String algorithmName) {
        NamespaceBundleSplitAlgorithm algorithm = NamespaceBundleSplitAlgorithm.of(algorithmName);
        if (algorithm == null) {
            algorithm = NamespaceBundleSplitAlgorithm.of(pulsar.getConfig().getDefaultNamespaceBundleSplitAlgorithm());
        }
        if (algorithm == null) {
            algorithm = NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO;
        }
        return algorithm;
    }

    /**
     * Update new bundle-range to admin/policies/namespace.
     * Update may fail because of concurrent write to Zookeeper.
     *
     * @param nsname the namespace name
     * @param nsBundles the new namespace bundles
     */
    public CompletableFuture<Void> updateNamespaceBundlesForPolicies(NamespaceName nsname,
                                                                      NamespaceBundles nsBundles) {
        Objects.requireNonNull(nsname);
        Objects.requireNonNull(nsBundles);

        return pulsar.getPulsarResources().getNamespaceResources().getPoliciesAsync(nsname).thenCompose(policies -> {
            if (policies.isPresent()) {
                return pulsar.getPulsarResources().getNamespaceResources().setPoliciesAsync(nsname, oldPolicies -> {
                    oldPolicies.bundles = nsBundles.getBundlesData();
                    return oldPolicies;
                });
            } else {
                LOG.error("Policies of namespace {} is not exist!", nsname);
                Policies newPolicies = new Policies();
                newPolicies.bundles = nsBundles.getBundlesData();
                return pulsar.getPulsarResources().getNamespaceResources().createPoliciesAsync(nsname, newPolicies);
            }
        });
    }


    /**
     * Update new bundle-range to LocalZk (create a new node if not present).
     * Update may fail because of concurrent write to Zookeeper.
     *
     * @param nsname the namespace name
     * @param nsBundles the new namespace bundles
     */
    public CompletableFuture<Void> updateNamespaceBundles(NamespaceName nsname, NamespaceBundles nsBundles) {
        Objects.requireNonNull(nsname);
        Objects.requireNonNull(nsBundles);

        LocalPolicies localPolicies = nsBundles.toLocalPolicies();

        return pulsar.getPulsarResources().getLocalPolicies()
                .setLocalPoliciesWithVersion(nsname, localPolicies, nsBundles.getVersion());
    }

    public OwnershipCache getOwnershipCache() {
        return ownershipCache;
    }

    public Set<NamespaceBundle> getOwnedServiceUnits() {
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            ExtensibleLoadManagerImpl extensibleLoadManager = ExtensibleLoadManagerImpl.get(loadManager.get());
            try {
                return extensibleLoadManager.getOwnedServiceUnitsAsync()
                        .get(config.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return ownershipCache.getOwnedBundles().values().stream().map(OwnedBundle::getNamespaceBundle)
                .collect(Collectors.toSet());
    }

    public boolean isServiceUnitOwned(ServiceUnitId suName) throws Exception {
        return isServiceUnitOwnedAsync(suName).get(config.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
    }

    public CompletableFuture<Boolean> isServiceUnitOwnedAsync(ServiceUnitId suName) {
        if (suName instanceof TopicName) {
            return isTopicOwnedAsync((TopicName) suName);
        }

        if (suName instanceof NamespaceName) {
            return isNamespaceOwnedAsync((NamespaceName) suName);
        }

        if (suName instanceof NamespaceBundle) {
            if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
                return loadManager.get().checkOwnershipAsync(Optional.empty(), suName);
            }
            // TODO: Add unit tests cover it.
            return CompletableFuture.completedFuture(
                    ownershipCache.isNamespaceBundleOwned((NamespaceBundle) suName));
        }

        return FutureUtil.failedFuture(
                new IllegalArgumentException("Invalid class of NamespaceBundle: " + suName.getClass().getName()));
    }

    /**
     * @deprecated This method is only used in test now.
     */
    @Deprecated
    public boolean isServiceUnitActive(TopicName topicName) {
        try {
            return isServiceUnitActiveAsync(topicName).get(pulsar.getConfig()
                    .getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.warn("Unable to find OwnedBundle for topic in time - [{}]", topicName, e);
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Boolean> isServiceUnitActiveAsync(TopicName topicName) {
        // TODO: Add unit tests cover it.
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            return getBundleAsync(topicName)
                    .thenCompose(bundle -> loadManager.get().checkOwnershipAsync(Optional.of(topicName), bundle));
        }
        return getBundleAsync(topicName).thenCompose(bundle -> {
            Optional<CompletableFuture<OwnedBundle>> optionalFuture = ownershipCache.getOwnedBundleAsync(bundle);
            if (optionalFuture.isEmpty()) {
                return CompletableFuture.completedFuture(false);
            }
            return optionalFuture.get().thenApply(ob -> ob != null && ob.isActive());
        });
    }

    private CompletableFuture<Boolean> isNamespaceOwnedAsync(NamespaceName fqnn) {
        // TODO: Add unit tests cover it.
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            return getFullBundleAsync(fqnn)
                    .thenCompose(bundle -> loadManager.get().checkOwnershipAsync(Optional.empty(), bundle));
        }
        return getFullBundleAsync(fqnn)
                .thenApply(bundle -> ownershipCache.getOwnedBundle(bundle) != null);
    }

    private CompletableFuture<Boolean> isTopicOwnedAsync(TopicName topic) {
        // TODO: Add unit tests cover it.
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            return getBundleAsync(topic)
                    .thenCompose(bundle -> loadManager.get().checkOwnershipAsync(Optional.of(topic), bundle));
        }
        return getBundleAsync(topic).thenApply(ownershipCache::isNamespaceBundleOwned);
    }

    public CompletableFuture<Boolean> checkTopicOwnership(TopicName topicName) {
        // TODO: Add unit tests cover it.
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            return getBundleAsync(topicName)
                    .thenCompose(bundle -> loadManager.get().checkOwnershipAsync(Optional.of(topicName), bundle));
        }
        return getBundleAsync(topicName)
                .thenCompose(ownershipCache::checkOwnershipAsync);
    }

    public CompletableFuture<Void> removeOwnedServiceUnitAsync(NamespaceBundle nsBundle) {
        CompletableFuture<Void> future;
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            ExtensibleLoadManagerImpl extensibleLoadManager = ExtensibleLoadManagerImpl.get(loadManager.get());
            future = extensibleLoadManager.unloadNamespaceBundleAsync(
                    nsBundle, Optional.empty(), true,
                    pulsar.getConfig().getNamespaceBundleUnloadingTimeoutMs(), TimeUnit.MILLISECONDS);
        } else {
            future = ownershipCache.removeOwnership(nsBundle);
        }
        return future.thenRun(() -> bundleFactory.invalidateBundleCache(nsBundle.getNamespaceObject()));
    }

    public void onNamespaceBundleOwned(NamespaceBundle bundle) {
        for (NamespaceBundleOwnershipListener bundleOwnedListener : bundleOwnershipListeners) {
            notifyNamespaceBundleOwnershipListener(bundle, bundleOwnedListener);
        }
    }

    public void onNamespaceBundleUnload(NamespaceBundle bundle) {
        for (NamespaceBundleOwnershipListener bundleOwnedListener : bundleOwnershipListeners) {
            try {
                if (bundleOwnedListener.test(bundle)) {
                    bundleOwnedListener.unLoad(bundle);
                }
            } catch (Throwable t) {
                LOG.error("Call bundle {} ownership listener error", bundle, t);
            }
        }
    }

    public void onNamespaceBundleSplit(NamespaceBundle bundle) {
        for (NamespaceBundleSplitListener bundleSplitListener : bundleSplitListeners) {
            try {
                if (bundleSplitListener.test(bundle)) {
                    bundleSplitListener.onSplit(bundle);
                }
            } catch (Throwable t) {
                LOG.error("Call bundle {} split listener {} error", bundle, bundleSplitListener, t);
            }
        }
    }

    public void addNamespaceBundleOwnershipListener(NamespaceBundleOwnershipListener... listeners) {
        Objects.requireNonNull(listeners);
        for (NamespaceBundleOwnershipListener listener : listeners) {
            if (listener != null) {
                bundleOwnershipListeners.add(listener);
            }
        }
        pulsar.runWhenReadyForIncomingRequests(() -> {
            try {
                getOwnedServiceUnits().forEach(bundle -> notifyNamespaceBundleOwnershipListener(bundle, listeners));
            } catch (Exception e) {
                LOG.error("Failed to notify namespace bundle ownership listener", e);
            }
        });
    }

    public void addNamespaceBundleSplitListener(NamespaceBundleSplitListener... listeners) {
        Objects.requireNonNull(listeners);
        for (NamespaceBundleSplitListener listener : listeners) {
            if (listener != null) {
                bundleSplitListeners.add(listener);
            }
        }
    }

    private void notifyNamespaceBundleOwnershipListener(NamespaceBundle bundle,
                    NamespaceBundleOwnershipListener... listeners) {
        if (listeners != null) {
            for (NamespaceBundleOwnershipListener listener : listeners) {
                try {
                    if (listener.test(bundle)) {
                        listener.onLoad(bundle);
                    }
                } catch (Throwable t) {
                    LOG.error("Call bundle {} ownership listener error", bundle, t);
                }
            }
        }
    }

    public NamespaceBundleFactory getNamespaceBundleFactory() {
        return bundleFactory;
    }

    public ServiceUnitId getServiceUnitId(TopicName topicName) throws Exception {
        return getBundle(topicName);
    }

    public CompletableFuture<List<String>> getFullListOfTopics(NamespaceName namespaceName) {
        return getListOfPersistentTopics(namespaceName)
                .thenCombine(getListOfNonPersistentTopics(namespaceName),
                        ListUtils::union);
    }

    public CompletableFuture<List<String>> getFullListOfPartitionedTopic(NamespaceName namespaceName) {
        NamespaceResources.PartitionedTopicResources partitionedTopicResources =
                pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources();
        return partitionedTopicResources.listPartitionedTopicsAsync(namespaceName, TopicDomain.persistent)
                .thenCombine(partitionedTopicResources
                                .listPartitionedTopicsAsync(namespaceName, TopicDomain.non_persistent),
                        ListUtils::union);
    }

    public CompletableFuture<List<String>> getOwnedTopicListForNamespaceBundle(NamespaceBundle bundle) {
        return getFullListOfTopics(bundle.getNamespaceObject()).thenCompose(topics ->
                CompletableFuture.completedFuture(
                        topics.stream()
                                .filter(topic -> bundle.includes(TopicName.get(topic)))
                                .collect(Collectors.toList())))
                .thenCombine(getAllPartitions(bundle.getNamespaceObject()).thenCompose(topics ->
                        CompletableFuture.completedFuture(
                                topics.stream().filter(topic -> bundle.includes(TopicName.get(topic)))
                                        .collect(Collectors.toList()))), (left, right) -> {
                    for (String topic : right) {
                        if (!left.contains(topic)) {
                            left.add(topic);
                        }
                    }
                    return left;
                });
    }

    /***
     * Check topic exists( partitioned or non-partitioned ).
     */
    public CompletableFuture<TopicExistsInfo> checkTopicExists(TopicName topic) {
        return pulsar.getBrokerService()
            .fetchPartitionedTopicMetadataAsync(TopicName.get(topic.toString()))
            .thenCompose(metadata -> {
                if (metadata.partitions > 0) {
                    return CompletableFuture.completedFuture(
                            TopicExistsInfo.newPartitionedTopicExists(metadata.partitions));
                }
                return checkNonPartitionedTopicExists(topic)
                    .thenApply(b -> b ? TopicExistsInfo.newNonPartitionedTopicExists()
                            : TopicExistsInfo.newTopicNotExists());
            });
    }

    /***
     * Check non-partitioned topic exists.
     */
    public CompletableFuture<Boolean> checkNonPartitionedTopicExists(TopicName topic) {
        if (topic.isPersistent()) {
            return pulsar.getPulsarResources().getTopicResources().persistentTopicExists(topic);
        } else {
            return checkNonPersistentNonPartitionedTopicExists(topic.toString());
        }
    }

    /**
     * Regarding non-persistent topic, we do not know whether it exists or not. Redirect the request to the ownership
     * broker of this topic. HTTP API has implemented the mechanism that redirect to ownership broker, so just call
     * HTTP API here.
     */
    public CompletableFuture<Boolean> checkNonPersistentNonPartitionedTopicExists(String topic) {
        TopicName topicName = TopicName.get(topic);
        // "non-partitioned & non-persistent" topics only exist on the owner broker.
        return checkTopicOwnership(TopicName.get(topic)).thenCompose(isOwned -> {
            // The current broker is the owner.
            if (isOwned) {
               CompletableFuture<Optional<Topic>> nonPersistentTopicFuture = pulsar.getBrokerService()
                       .getTopic(topic, false);
               if (nonPersistentTopicFuture != null) {
                   return nonPersistentTopicFuture.thenApply(Optional::isPresent);
               } else {
                   return CompletableFuture.completedFuture(false);
               }
            }

            // Forward to the owner broker.
            PulsarClientImpl pulsarClient;
            try {
                pulsarClient = (PulsarClientImpl) pulsar.getClient();
            } catch (Exception ex) {
                // This error will never occur.
                log.error("{} Failed to get partition metadata due to create internal admin client fails", topic, ex);
                return FutureUtil.failedFuture(ex);
            }
            LookupOptions lookupOptions = LookupOptions.builder().readOnly(false).authoritative(true).build();
            return getBrokerServiceUrlAsync(TopicName.get(topic), lookupOptions)
                .thenCompose(lookupResult -> {
                    if (!lookupResult.isPresent()) {
                        log.error("{} Failed to get partition metadata due can not find the owner broker", topic);
                        return FutureUtil.failedFuture(new ServiceUnitNotReadyException(
                                "No broker was available to own " + topicName));
                    }
                    LookupData lookupData = lookupResult.get().getLookupData();
                    String brokerUrl;
                    if (pulsar.getConfiguration().isBrokerClientTlsEnabled()
                            && StringUtils.isNotEmpty(lookupData.getBrokerUrlTls())) {
                        brokerUrl = lookupData.getBrokerUrlTls();
                    } else {
                        brokerUrl = lookupData.getBrokerUrl();
                    }
                    return pulsarClient.getLookup(brokerUrl)
                        .getPartitionedTopicMetadata(topicName, false)
                        .thenApply(metadata -> true)
                        .exceptionallyCompose(ex -> {
                            Throwable actEx = FutureUtil.unwrapCompletionException(ex);
                            if (actEx instanceof PulsarClientException.NotFoundException
                                    || actEx instanceof PulsarClientException.TopicDoesNotExistException
                                    || actEx instanceof PulsarAdminException.NotFoundException) {
                                return CompletableFuture.completedFuture(false);
                            } else if (actEx instanceof PulsarClientException.FeatureNotSupportedException fe){
                                if (fe.getFailedFeatureCheck() == SupportsGetPartitionedMetadataWithoutAutoCreation) {
                                    // Since the feature PIP-344 isn't supported, restore the behavior to previous
                                    // behavior before https://github.com/apache/pulsar/pull/22838 changes.
                                    log.info("{} Checking the existence of a non-persistent non-partitioned topic "
                                                    + "was performed using the behavior prior to PIP-344 changes, "
                                                    + "because the broker does not support the PIP-344 feature "
                                                    + "'supports_get_partitioned_metadata_without_auto_creation'.",
                                            topic);
                                    return CompletableFuture.completedFuture(false);
                                } else {
                                    log.error("{} Failed to get partition metadata", topic, ex);
                                    return CompletableFuture.failedFuture(ex);
                                }
                            } else {
                                log.error("{} Failed to get partition metadata", topic, ex);
                                return CompletableFuture.failedFuture(ex);
                            }
                        });
            });
        });
    }

    public CompletableFuture<List<String>> getListOfTopics(NamespaceName namespaceName, Mode mode) {
        switch (mode) {
            case ALL:
                return getFullListOfTopics(namespaceName);
            case NON_PERSISTENT:
                return getListOfNonPersistentTopics(namespaceName);
            case PERSISTENT:
            default:
                return getListOfPersistentTopics(namespaceName);
        }
    }

    public CompletableFuture<List<String>> getListOfUserTopics(NamespaceName namespaceName, Mode mode) {
        String key = String.format("%s://%s", mode, namespaceName);
        final MutableBoolean initializedByCurrentThread = new MutableBoolean();
        CompletableFuture<List<String>> queryRes = inProgressQueryUserTopics.computeIfAbsent(key, k -> {
            initializedByCurrentThread.setTrue();
            return getListOfTopics(namespaceName, mode).thenApplyAsync(list -> {
                return TopicList.filterSystemTopic(list);
            }, pulsar.getExecutor());
        });
        if (initializedByCurrentThread.getValue()) {
            queryRes.whenComplete((ignore, ex) -> {
                inProgressQueryUserTopics.remove(key, queryRes);
            });
        }
        return queryRes;
    }

    public CompletableFuture<List<String>> getAllPartitions(NamespaceName namespaceName) {
        return getPartitions(namespaceName, TopicDomain.persistent)
                .thenCombine(getPartitions(namespaceName, TopicDomain.non_persistent),
                        ListUtils::union);
    }


    public CompletableFuture<List<String>> getPartitions(NamespaceName namespaceName, TopicDomain topicDomain) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting children from partitioned-topics now: {} - {}", namespaceName, topicDomain);
        }

        return pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .listPartitionedTopicsAsync(namespaceName, topicDomain)
                .thenCompose(topics -> {
            CompletableFuture<List<String>> result = new CompletableFuture<>();
            List<String> resultPartitions = Collections.synchronizedList(new ArrayList<>());
            if (CollectionUtils.isNotEmpty(topics)) {
                List<CompletableFuture<List<String>>> futures = new ArrayList<>();
                for (String topic : topics) {
                    CompletableFuture<List<String>> future = getPartitionsForTopic(TopicName.get(topic));
                    futures.add(future);
                    future.thenAccept(resultPartitions::addAll);
                }
                FutureUtil.waitForAll(futures).whenComplete((v, ex) -> {
                    if (ex != null) {
                        result.completeExceptionally(ex);
                    } else {
                        result.complete(resultPartitions);
                    }
                });
            } else {
                result.complete(resultPartitions);
            }
            return result;
        });
    }

    private CompletableFuture<List<String>> getPartitionsForTopic(TopicName topicName) {
        return pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName).thenCompose(meta -> {
            List<String> result = new ArrayList<>();
            for (int i = 0; i < meta.partitions; i++) {
                result.add(topicName.getPartition(i).toString());
            }
            return CompletableFuture.completedFuture(result);
        });
    }

    /***
     * List persistent topics names under a namespace, the topic name contains the partition suffix.
     */
    public CompletableFuture<List<String>> getListOfPersistentTopics(NamespaceName namespaceName) {
        return pulsar.getPulsarResources().getTopicResources().listPersistentTopicsAsync(namespaceName);
    }

    public CompletableFuture<List<String>> getListOfNonPersistentTopics(NamespaceName namespaceName) {

        return PulsarWebResource.checkLocalOrGetPeerReplicationCluster(pulsar, namespaceName, true)
                .thenCompose(peerClusterData -> {
                    // if peer-cluster-data is present it means namespace is owned by that peer-cluster and request
                    // should redirect to the peer-cluster
                    if (peerClusterData != null) {
                        return getNonPersistentTopicsFromPeerCluster(peerClusterData, namespaceName);
                    } else {
                        // Non-persistent topics don't have managed ledgers. So we have to retrieve them from local
                        // cache.
                        List<String> topics = new ArrayList<>();
                        synchronized (pulsar.getBrokerService().getMultiLayerTopicsMap()) {
                            if (pulsar.getBrokerService().getMultiLayerTopicsMap()
                                    .containsKey(namespaceName.toString())) {
                                pulsar.getBrokerService().getMultiLayerTopicsMap().get(namespaceName.toString())
                                        .forEach((__, bundle) -> bundle.forEach((topicName, topic) -> {
                                            if (topic instanceof NonPersistentTopic
                                                    && ((NonPersistentTopic) topic).isActive()) {
                                                topics.add(topicName);
                                            }
                                        }));
                            }
                        }

                        topics.sort(null);
                        return CompletableFuture.completedFuture(topics);
                    }
                });
    }

    private CompletableFuture<List<String>> getNonPersistentTopicsFromPeerCluster(ClusterDataImpl peerClusterData,
                                                                                  NamespaceName namespace) {
        PulsarClientImpl client = getNamespaceClient(peerClusterData);
        return client.getLookup().getTopicsUnderNamespace(namespace, Mode.NON_PERSISTENT, null, null)
                .thenApply(GetTopicsResult::getTopics);
    }

    public PulsarClientImpl getNamespaceClient(ClusterDataImpl cluster) {
        PulsarClientImpl client = namespaceClients.get(cluster);
        if (client != null) {
            return client;
        }

        return namespaceClients.computeIfAbsent(cluster, key -> {
            try {
                ClientBuilder clientBuilder = PulsarClient.builder()
                        .memoryLimit(0, SizeUnit.BYTES)
                        .enableTcpNoDelay(false)
                        .statsInterval(0, TimeUnit.SECONDS);

                // Apply all arbitrary configuration. This must be called before setting any fields annotated as
                // @Secret on the ClientConfigurationData object because of the way they are serialized.
                // See https://github.com/apache/pulsar/issues/8509 for more information.
                clientBuilder.loadConf(PropertiesUtils.filterAndMapProperties(config.getProperties(), "brokerClient_"));

                // Disabled auto release useless connection.
                clientBuilder.connectionMaxIdleSeconds(-1);

                if (pulsar.getConfiguration().isAuthenticationEnabled()) {
                    clientBuilder.authentication(pulsar.getConfiguration().getBrokerClientAuthenticationPlugin(),
                        pulsar.getConfiguration().getBrokerClientAuthenticationParameters());
                }

                if (pulsar.getConfiguration().isTlsEnabled()) {
                    clientBuilder
                        .serviceUrl(isNotBlank(cluster.getBrokerServiceUrlTls())
                            ? cluster.getBrokerServiceUrlTls() : cluster.getServiceUrlTls())
                        .enableTls(true)
                        .tlsTrustCertsFilePath(pulsar.getConfiguration().getBrokerClientTrustCertsFilePath())
                        .allowTlsInsecureConnection(pulsar.getConfiguration().isTlsAllowInsecureConnection())
                        .enableTlsHostnameVerification(pulsar.getConfiguration().isTlsHostnameVerificationEnabled())
                        .sslFactoryPlugin(pulsar.getConfiguration().getBrokerClientSslFactoryPlugin())
                        .sslFactoryPluginParams(pulsar.getConfiguration().getBrokerClientSslFactoryPluginParams());
                } else {
                    clientBuilder.serviceUrl(isNotBlank(cluster.getBrokerServiceUrl())
                        ? cluster.getBrokerServiceUrl() : cluster.getServiceUrl());
                }

                // Share all the IO threads across broker and client connections
                ClientConfigurationData conf = ((ClientBuilderImpl) clientBuilder).getClientConfigurationData();
                return pulsar.createClientImpl(conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public CompletableFuture<Optional<NamespaceEphemeralData>> getOwnerAsync(NamespaceBundle bundle) {
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            ExtensibleLoadManagerImpl extensibleLoadManager = ExtensibleLoadManagerImpl.get(loadManager.get());
            return extensibleLoadManager.getOwnershipWithLookupDataAsync(bundle)
                    .thenCompose(lookupData -> lookupData
                        .map(brokerLookupData ->
                            CompletableFuture.completedFuture(Optional.of(brokerLookupData.toNamespaceEphemeralData())))
                        .orElseGet(() -> CompletableFuture.completedFuture(Optional.empty())));
        }
        return ownershipCache.getOwnerAsync(bundle);
    }

    public boolean checkOwnershipPresent(NamespaceBundle bundle) throws Exception {
        return checkOwnershipPresentAsync(bundle).get(pulsar.getConfiguration()
                        .getMetadataStoreOperationTimeoutSeconds(), SECONDS);
    }

    public CompletableFuture<Boolean> checkOwnershipPresentAsync(NamespaceBundle bundle) {
        if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)) {
            ExtensibleLoadManagerImpl extensibleLoadManager = ExtensibleLoadManagerImpl.get(loadManager.get());
            return extensibleLoadManager.getOwnershipAsync(Optional.empty(), bundle)
                    .thenApply(Optional::isPresent);
        }
        return getOwnerAsync(bundle).thenApply(Optional::isPresent);
    }

    public void unloadSLANamespace() throws Exception {
        NamespaceName namespaceName = getSLAMonitorNamespace(pulsar.getBrokerId(), config);

        LOG.info("Checking owner for SLA namespace {}", namespaceName);

        NamespaceBundle nsFullBundle = getFullBundle(namespaceName);
        if (!checkOwnershipPresent(nsFullBundle)) {
            // No one owns the namespace so no point trying to unload it
            // Next lookup will assign the bundle to this broker.
            return;
        }

        LOG.info("Trying to unload SLA namespace {}", namespaceName);
        PulsarAdmin adminClient = pulsar.getAdminClient();
        adminClient.namespaces().unload(namespaceName.toString());
        LOG.info("Namespace {} unloaded successfully", namespaceName);
    }

    public static NamespaceName getHeartbeatNamespace(String lookupBroker, ServiceConfiguration config) {
        return NamespaceName.get(String.format(HEARTBEAT_NAMESPACE_FMT, config.getClusterName(), lookupBroker));
    }

    public static NamespaceName getHeartbeatNamespaceV2(String lookupBroker, ServiceConfiguration config) {
        return NamespaceName.get(String.format(HEARTBEAT_NAMESPACE_FMT_V2, lookupBroker));
    }

    public static NamespaceName getSLAMonitorNamespace(String lookupBroker, ServiceConfiguration config) {
        return NamespaceName.get(String.format(SLA_NAMESPACE_FMT, config.getClusterName(), lookupBroker));
    }

    public static String checkHeartbeatNamespace(ServiceUnitId ns) {
        Matcher m = HEARTBEAT_NAMESPACE_PATTERN.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            LOG.debug("Heartbeat namespace matched the lookup namespace {}", ns.getNamespaceObject().toString());
            return m.group(1);
        } else {
            return null;
        }
    }

    public static String checkHeartbeatNamespaceV2(ServiceUnitId ns) {
        Matcher m = HEARTBEAT_NAMESPACE_PATTERN_V2.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            LOG.debug("Heartbeat namespace v2 matched the lookup namespace {}", ns.getNamespaceObject().toString());
            return m.group(1);
        } else {
            return null;
        }
    }

    public static String getSLAMonitorBrokerName(ServiceUnitId ns) {
        Matcher m = SLA_NAMESPACE_PATTERN.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            return m.group(1);
        } else {
            return null;
        }
    }

    public static boolean isSystemServiceNamespace(String namespace) {
        return SYSTEM_NAMESPACE.toString().equals(namespace)
                || SLA_NAMESPACE_PATTERN.matcher(namespace).matches()
                || HEARTBEAT_NAMESPACE_PATTERN.matcher(namespace).matches()
                || HEARTBEAT_NAMESPACE_PATTERN_V2.matcher(namespace).matches();
    }

    /**
     * used for filtering bundles in special namespace.
     * @param namespace the namespace name
     * @return True if namespace is HEARTBEAT_NAMESPACE or SLA_NAMESPACE
     */
    public static boolean isSLAOrHeartbeatNamespace(String namespace) {
        return SLA_NAMESPACE_PATTERN.matcher(namespace).matches()
                || HEARTBEAT_NAMESPACE_PATTERN.matcher(namespace).matches()
                || HEARTBEAT_NAMESPACE_PATTERN_V2.matcher(namespace).matches();
    }

    public static boolean isHeartbeatNamespace(ServiceUnitId ns) {
        String namespace = ns.getNamespaceObject().toString();
        return HEARTBEAT_NAMESPACE_PATTERN.matcher(namespace).matches()
                || HEARTBEAT_NAMESPACE_PATTERN_V2.matcher(namespace).matches();
    }

    public boolean registerSLANamespace() throws PulsarServerException {
        String brokerId = pulsar.getBrokerId();
        boolean isNameSpaceRegistered = registerNamespace(getSLAMonitorNamespace(brokerId, config), false);
        if (isNameSpaceRegistered) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Added SLA Monitoring namespace name in local cache: ns={}",
                        getSLAMonitorNamespace(brokerId, config));
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("SLA Monitoring not owned by the broker: ns={}",
                    getSLAMonitorNamespace(brokerId, config));
        }
        return isNameSpaceRegistered;
    }

    @Override
    public void close() {
        namespaceClients.forEach((cluster, client) -> {
            try {
                client.shutdown();
            } catch (PulsarClientException e) {
                LOG.warn("Error shutting down namespace client for cluster {}", cluster, e);
            }
        });
    }
}
