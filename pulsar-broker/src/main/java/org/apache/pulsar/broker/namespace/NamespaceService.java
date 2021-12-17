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
package org.apache.pulsar.broker.namespace;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import io.netty.channel.EventLoopGroup;
import io.prometheus.client.Counter;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.data.LookupData;
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
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreException;
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
public class NamespaceService implements AutoCloseable {

    public enum AddressType {
        BROKER_URL, LOOKUP_URL
    }

    private static final Logger LOG = LoggerFactory.getLogger(NamespaceService.class);

    private final ServiceConfiguration config;

    private final MetadataCache<LocalPolicies> localPoliciesCache;

    private final AtomicReference<LoadManager> loadManager;

    private final PulsarService pulsar;

    private final OwnershipCache ownershipCache;
    private final MetadataCache<LocalBrokerData> localBrokerDataCache;

    private final NamespaceBundleFactory bundleFactory;

    private int uncountedNamespaces;

    private final String host;

    private static final int BUNDLE_SPLIT_RETRY_LIMIT = 7;

    public static final String SLA_NAMESPACE_PROPERTY = "sla-monitor";
    public static final Pattern HEARTBEAT_NAMESPACE_PATTERN = Pattern.compile("pulsar/[^/]+/([^:]+:\\d+)");
    public static final Pattern HEARTBEAT_NAMESPACE_PATTERN_V2 = Pattern.compile("pulsar/([^:]+:\\d+)");
    public static final Pattern SLA_NAMESPACE_PATTERN = Pattern.compile(SLA_NAMESPACE_PROPERTY + "/[^/]+/([^:]+:\\d+)");
    public static final String HEARTBEAT_NAMESPACE_FMT = "pulsar/%s/%s:%s";
    public static final String HEARTBEAT_NAMESPACE_FMT_V2 = "pulsar/%s:%s";
    public static final String SLA_NAMESPACE_FMT = SLA_NAMESPACE_PROPERTY + "/%s/%s:%s";

    private final ConcurrentOpenHashMap<ClusterDataImpl, PulsarClientImpl> namespaceClients;

    private final List<NamespaceBundleOwnershipListener> bundleOwnershipListeners;


    private static final Counter lookupRedirects = Counter.build("pulsar_broker_lookup_redirects", "-").register();
    private static final Counter lookupFailures = Counter.build("pulsar_broker_lookup_failures", "-").register();
    private static final Counter lookupAnswers = Counter.build("pulsar_broker_lookup_answers", "-").register();

    private static final Summary lookupLatency = Summary.build("pulsar_broker_lookup", "-")
            .quantile(0.50)
            .quantile(0.99)
            .quantile(0.999)
            .quantile(1.0)
            .register();


    /**
     * Default constructor.
     *
     * @throws PulsarServerException
     */
    public NamespaceService(PulsarService pulsar) {
        this.pulsar = pulsar;
        host = pulsar.getAdvertisedAddress();
        this.config = pulsar.getConfiguration();
        this.loadManager = pulsar.getLoadManager();
        this.bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());
        this.ownershipCache = new OwnershipCache(pulsar, bundleFactory, this);
        this.namespaceClients = new ConcurrentOpenHashMap<>();
        this.bundleOwnershipListeners = new CopyOnWriteArrayList<>();
        this.localBrokerDataCache = pulsar.getLocalMetadataStore().getMetadataCache(LocalBrokerData.class);
        this.localPoliciesCache = pulsar.getLocalMetadataStore().getMetadataCache(LocalPolicies.class);
    }

    public void initialize() {
        if (!getOwnershipCache().refreshSelfOwnerInfo()) {
            throw new RuntimeException("Failed to refresh self owner info.");
        }
    }

    public CompletableFuture<Optional<LookupResult>> getBrokerServiceUrlAsync(TopicName topic, LookupOptions options) {
        long startTime = System.nanoTime();

        CompletableFuture<Optional<LookupResult>> future = getBundleAsync(topic)
                .thenCompose(bundle -> findBrokerServiceUrl(bundle, options));

        future.thenAccept(optResult -> {
            lookupLatency.observe(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            if (optResult.isPresent()) {
                if (optResult.get().isRedirect()) {
                    lookupRedirects.inc();
                } else {
                    lookupAnswers.inc();
                }
            }
        }).exceptionally(ex -> {
            lookupFailures.inc();
            return null;
        });

        return future;
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
     *
     * If the service unit is not owned, return a CompletableFuture with empty optional.
     */
    public CompletableFuture<Optional<URL>> getWebServiceUrlAsync(ServiceUnitId suName, LookupOptions options) {
        if (suName instanceof TopicName) {
            TopicName name = (TopicName) suName;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Getting web service URL of topic: {} - options: {}", name, options);
            }
            return getBundleAsync(name)
                    .thenCompose(namespaceBundle -> internalGetWebServiceUrl(namespaceBundle, options));
        }

        if (suName instanceof NamespaceName) {
            return getFullBundleAsync((NamespaceName) suName)
                    .thenCompose(namespaceBundle -> internalGetWebServiceUrl(namespaceBundle, options));
        }

        if (suName instanceof NamespaceBundle) {
            return internalGetWebServiceUrl((NamespaceBundle) suName, options);
        }

        throw new IllegalArgumentException("Unrecognized class of NamespaceBundle: " + suName.getClass().getName());
    }

    /**
     * Return the URL of the broker who's owning a particular service unit.
     *
     * If the service unit is not owned, return an empty optional
     */
    public Optional<URL> getWebServiceUrl(ServiceUnitId suName, LookupOptions options) throws Exception {
        return getWebServiceUrlAsync(suName, options)
                .get(pulsar.getConfiguration().getZooKeeperOperationTimeoutSeconds(), SECONDS);
    }

    private CompletableFuture<Optional<URL>> internalGetWebServiceUrl(NamespaceBundle bundle, LookupOptions options) {

        return findBrokerServiceUrl(bundle, options).thenApply(lookupResult -> {
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
    }

    /**
     * Register all the bootstrap name spaces including the heartbeat namespace.
     *
     * @return
     * @throws PulsarServerException
     */
    public void registerBootstrapNamespaces() throws PulsarServerException {

        // ensure that we own the heartbeat namespace
        if (registerNamespace(getHeartbeatNamespace(host, config), true)) {
            this.uncountedNamespaces++;
            LOG.info("added heartbeat namespace name in local cache: ns={}", getHeartbeatNamespace(host, config));
        }

        // ensure that we own the heartbeat namespace
        if (registerNamespace(getHeartbeatNamespaceV2(host, config), true)) {
            this.uncountedNamespaces++;
            LOG.info("added heartbeat namespace name in local cache: ns={}", getHeartbeatNamespaceV2(host, config));
        }

        // we may not need strict ownership checking for bootstrap names for now
        for (String namespace : config.getBootstrapNamespaces()) {
            if (registerNamespace(NamespaceName.get(namespace), false)) {
                LOG.info("added bootstrap namespace name in local cache: ns={}", namespace);
            }
        }
    }

    /**
     * Tried to registers a namespace to this instance.
     *
     * @param nsname
     * @param ensureOwned
     * @return
     * @throws PulsarServerException
     * @throws Exception
     */
    public boolean registerNamespace(NamespaceName nsname, boolean ensureOwned) throws PulsarServerException {
        try {
            NamespaceBundle nsFullBundle = null;

            // all pre-registered namespace is assumed to have bundles disabled
            nsFullBundle = bundleFactory.getFullBundle(nsname);
            // v2 namespace will always use full bundle object
            NamespaceEphemeralData otherData = ownershipCache.tryAcquiringOwnership(nsFullBundle).get();
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

    private final ConcurrentOpenHashMap<NamespaceBundle, CompletableFuture<Optional<LookupResult>>>
            findingBundlesAuthoritative = new ConcurrentOpenHashMap<>();
    private final ConcurrentOpenHashMap<NamespaceBundle, CompletableFuture<Optional<LookupResult>>>
            findingBundlesNotAuthoritative = new ConcurrentOpenHashMap<>();

    /**
     * Main internal method to lookup and setup ownership of service unit to a broker.
     *
     * @param bundle
     * @param options
     * @return
     * @throws PulsarServerException
     */
    private CompletableFuture<Optional<LookupResult>> findBrokerServiceUrl(
            NamespaceBundle bundle, LookupOptions options) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("findBrokerServiceUrl: {} - options: {}", bundle, options);
        }

        ConcurrentOpenHashMap<NamespaceBundle, CompletableFuture<Optional<LookupResult>>> targetMap;
        if (options.isAuthoritative()) {
            targetMap = findingBundlesAuthoritative;
        } else {
            targetMap = findingBundlesNotAuthoritative;
        }

        return targetMap.computeIfAbsent(bundle, (k) -> {
            CompletableFuture<Optional<LookupResult>> future = new CompletableFuture<>();

            // First check if we or someone else already owns the bundle
            ownershipCache.getOwnerAsync(bundle).thenAccept(nsData -> {
                if (!nsData.isPresent()) {
                    // No one owns this bundle

                    if (options.isReadOnly()) {
                        // Do not attempt to acquire ownership
                        future.complete(Optional.empty());
                    } else {
                        // Now, no one owns the namespace yet. Hence, we will try to dynamically assign it
                        pulsar.getExecutor().execute(() -> {
                            searchForCandidateBroker(bundle, future, options);
                        });
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
                        return;
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

    private void searchForCandidateBroker(NamespaceBundle bundle,
                                          CompletableFuture<Optional<LookupResult>> lookupFuture,
                                          LookupOptions options) {
        if (null == pulsar.getLeaderElectionService()) {
            LOG.warn("The leader election has not yet been completed! NamespaceBundle[{}]", bundle);
            lookupFuture.completeExceptionally(
                    new IllegalStateException("The leader election has not yet been completed!"));
            return;
        }
        String candidateBroker = null;

        LeaderElectionService les = pulsar.getLeaderElectionService();
        if (les == null) {
            // The leader election service was not initialized yet. This can happen because the broker service is
            // initialized first and it might start receiving lookup requests before the leader election service is
            // fully initialized.
            LOG.warn("Leader election service isn't initialized yet. "
                            + "Returning empty result to lookup. NamespaceBundle[{}]",
                    bundle);
            lookupFuture.complete(Optional.empty());
            return;
        }

        boolean authoritativeRedirect = les.isLeader();

        try {
            // check if this is Heartbeat or SLAMonitor namespace
            candidateBroker = checkHeartbeatNamespace(bundle);
            if (candidateBroker == null) {
                candidateBroker = checkHeartbeatNamespaceV2(bundle);
            }
            if (candidateBroker == null) {
                String broker = getSLAMonitorBrokerName(bundle);
                // checking if the broker is up and running
                if (broker != null && isBrokerActive(broker)) {
                    candidateBroker = broker;
                }
            }

            if (candidateBroker == null) {
                Optional<LeaderBroker> currentLeader = pulsar.getLeaderElectionService().getCurrentLeader();

                if (options.isAuthoritative()) {
                    // leader broker already assigned the current broker as owner
                    candidateBroker = pulsar.getSafeWebServiceAddress();
                } else {
                    LoadManager loadManager = this.loadManager.get();
                    boolean makeLoadManagerDecisionOnThisBroker = !loadManager.isCentralized() || les.isLeader();
                    if (!makeLoadManagerDecisionOnThisBroker) {
                        // If leader is not active, fallback to pick the least loaded from current broker loadmanager
                        boolean leaderBrokerActive = currentLeader.isPresent()
                                && isBrokerActive(currentLeader.get().getServiceUrl());
                        if (!leaderBrokerActive) {
                            makeLoadManagerDecisionOnThisBroker = true;
                            if (!currentLeader.isPresent()) {
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
                        if (!availableBroker.isPresent()) {
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
                        candidateBroker = currentLeader.get().getServiceUrl();
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Error when searching for candidate broker to acquire {}: {}", bundle, e.getMessage(), e);
            lookupFuture.completeExceptionally(e);
            return;
        }

        try {
            checkNotNull(candidateBroker);

            if (candidateBroker.equals(pulsar.getSafeWebServiceAddress())) {
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
                            // Schedule the task to pre-load topics
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
                                return;
                            } else {
                                URI url = listener.getBrokerServiceUrl();
                                URI urlTls = listener.getBrokerServiceUrlTls();
                                lookupFuture.complete(Optional.of(
                                        new LookupResult(ownerInfo,
                                                url == null ? null : url.toString(),
                                                urlTls == null ? null : urlTls.toString())));
                                return;
                            }
                        } else {
                            lookupFuture.complete(Optional.of(new LookupResult(ownerInfo)));
                            return;
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

    protected CompletableFuture<LookupResult> createLookupResult(String candidateBroker, boolean authoritativeRedirect,
                                                                 final String advertisedListenerName) {

        CompletableFuture<LookupResult> lookupFuture = new CompletableFuture<>();
        try {
            checkArgument(StringUtils.isNotBlank(candidateBroker), "Lookup broker can't be null %s", candidateBroker);
            String path = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + parseHostAndPort(candidateBroker);

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

    private boolean isBrokerActive(String candidateBroker) {
        String candidateBrokerHostAndPort = parseHostAndPort(candidateBroker);
        Set<String> availableBrokers = getAvailableBrokers();
        if (availableBrokers.contains(candidateBrokerHostAndPort)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Broker {} ({}) is available for.", candidateBroker, candidateBrokerHostAndPort);
            }
            return true;
        } else {
            LOG.warn("Broker {} ({}) couldn't be found in available brokers {}",
                    candidateBroker, candidateBrokerHostAndPort,
                    availableBrokers.stream().collect(Collectors.joining(",")));
            return false;
        }
    }

    private static String parseHostAndPort(String candidateBroker) {
        int uriSeparatorPos = candidateBroker.indexOf("://");
        if (uriSeparatorPos == -1) {
            throw new IllegalArgumentException("'" + candidateBroker + "' isn't an URI.");
        }
        String candidateBrokerHostAndPort = candidateBroker.substring(uriSeparatorPos + 3);
        return candidateBrokerHostAndPort;
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
     * @return
     * @throws Exception
     */
    private Optional<String> getLeastLoadedFromLoadManager(ServiceUnitId serviceUnit) throws Exception {
        Optional<ResourceUnit> leastLoadedBroker = loadManager.get().getLeastLoaded(serviceUnit);
        if (!leastLoadedBroker.isPresent()) {
            LOG.warn("No broker is available for {}", serviceUnit);
            return Optional.empty();
        }

        String lookupAddress = leastLoadedBroker.get().getResourceId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} : redirecting to the least loaded broker, lookup address={}",
                    pulsar.getSafeWebServiceAddress(),
                    lookupAddress);
        }
        return Optional.of(lookupAddress);
    }

    public CompletableFuture<Void> unloadNamespaceBundle(NamespaceBundle bundle) {
        // unload namespace bundle
        return unloadNamespaceBundle(bundle, config.getNamespaceBundleUnloadingTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> unloadNamespaceBundle(NamespaceBundle bundle, long timeout, TimeUnit timeoutUnit) {
        // unload namespace bundle
        OwnedBundle ob = ownershipCache.getOwnedBundle(bundle);
        if (ob == null) {
            return FutureUtil.failedFuture(new IllegalStateException("Bundle " + bundle + " is not currently owned"));
        } else {
            return ob.handleUnloadRequest(pulsar, timeout, timeoutUnit);
        }
    }

    public CompletableFuture<Boolean> isNamespaceBundleOwned(NamespaceBundle bundle) {
        return pulsar.getLocalMetadataStore().exists(ServiceUnitUtils.path(bundle));
    }

    public Map<String, NamespaceOwnershipStatus> getOwnedNameSpacesStatus() throws Exception {
        NamespaceIsolationPolicies nsIsolationPolicies = this.getLocalNamespaceIsolationPolicies();
        Map<String, NamespaceOwnershipStatus> ownedNsStatus = new HashMap<String, NamespaceOwnershipStatus>();
        for (OwnedBundle nsObj : this.ownershipCache.getOwnedBundles().values()) {
            NamespaceOwnershipStatus nsStatus = this.getNamespaceOwnershipStatus(nsObj,
                    nsIsolationPolicies.getPolicyByNamespace(nsObj.getNamespaceBundle().getNamespaceObject()));
            ownedNsStatus.put(nsObj.getNamespaceBundle().toString(), nsStatus);
        }

        return ownedNsStatus;
    }

    private NamespaceOwnershipStatus getNamespaceOwnershipStatus(OwnedBundle nsObj,
            NamespaceIsolationPolicy nsIsolationPolicy) {
        NamespaceOwnershipStatus nsOwnedStatus = new NamespaceOwnershipStatus(BrokerAssignment.shared, false,
                nsObj.isActive());
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

    private NamespaceIsolationPolicies getLocalNamespaceIsolationPolicies() throws Exception {
        String localCluster = pulsar.getConfiguration().getClusterName();
        return pulsar.getPulsarResources().getNamespaceResources().getIsolationPolicies()
                .getIsolationDataPolicies(localCluster)
                .orElseGet(() -> {
                    // the namespace isolation policies are empty/undefined = an empty object
                    return new NamespaceIsolationPolicies();
                });
    }

    public boolean isNamespaceBundleDisabled(NamespaceBundle bundle) throws Exception {
        try {
            // Does ZooKeeper says that the namespace is disabled?
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
     *
     * It will call splitAndOwnBundleOnceAndRetry to do the real retry work, which will retry "retryTimes".
     *
     * @param bundle
     * @return
     * @throws Exception
     */
    public CompletableFuture<Void> splitAndOwnBundle(NamespaceBundle bundle, boolean unload,
                                                     NamespaceBundleSplitAlgorithm splitAlgorithm) {

        final CompletableFuture<Void> unloadFuture = new CompletableFuture<>();
        final AtomicInteger counter = new AtomicInteger(BUNDLE_SPLIT_RETRY_LIMIT);
        splitAndOwnBundleOnceAndRetry(bundle, unload, counter, unloadFuture, splitAlgorithm);

        return unloadFuture;
    }

    void splitAndOwnBundleOnceAndRetry(NamespaceBundle bundle,
                                       boolean unload,
                                       AtomicInteger counter,
                                       CompletableFuture<Void> completionFuture,
                                       NamespaceBundleSplitAlgorithm splitAlgorithm) {
        splitAlgorithm.getSplitBoundary(this, bundle).whenComplete((splitBoundary, ex) -> {
            CompletableFuture<List<NamespaceBundle>> updateFuture = new CompletableFuture<>();
            if (ex == null) {
                try {
                    bundleFactory.splitBundles(bundle,
                        2 /* by default split into 2 */, splitBoundary)
                            .thenAccept(splittedBundles -> {
                                // Split and updateNamespaceBundles. Update may fail because of concurrent write to
                                // Zookeeper.
                                if (splittedBundles == null) {
                                    String msg = format("bundle %s not found under namespace", bundle.toString());
                                    LOG.warn(msg);
                                    updateFuture.completeExceptionally(new ServiceUnitNotReadyException(msg));
                                    return;
                                }

                                checkNotNull(splittedBundles.getLeft());
                                checkNotNull(splittedBundles.getRight());
                                checkArgument(splittedBundles.getRight().size() == 2,
                                        "bundle has to be split in two bundles");
                                NamespaceName nsname = bundle.getNamespaceObject();
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("[{}] splitAndOwnBundleOnce: {}, counter: {},  2 bundles: {}, {}",
                                            nsname.toString(), bundle.getBundleRange(), counter.get(),
                                            splittedBundles.getRight().get(0).getBundleRange(),
                                            splittedBundles.getRight().get(1).getBundleRange());
                                }
                                try {
                                    // take ownership of newly split bundles
                                    for (NamespaceBundle sBundle : splittedBundles.getRight()) {
                                        checkNotNull(ownershipCache.tryAcquiringOwnership(sBundle));
                                    }
                                    updateNamespaceBundles(nsname, splittedBundles.getLeft())
                                            .thenRun(() -> {
                                                bundleFactory.invalidateBundleCache(bundle.getNamespaceObject());
                                                updateFuture.complete(splittedBundles.getRight());
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
                        pulsar.getOrderedExecutor()
                                .execute(() -> splitAndOwnBundleOnceAndRetry(
                                        bundle, unload, counter, completionFuture, splitAlgorithm));
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
                            completionFuture.complete(null);
                            if (unload) {
                                // Unload new split bundles, in background. This will not
                                // affect the split operation which is already safely completed
                                r.forEach(this::unloadNamespaceBundle);
                            }
                        })
                        .exceptionally(e -> {
                            String msg1 = format(
                                    "failed to disable bundle %s under namespace [%s] with error %s",
                                    bundle.getNamespaceObject().toString(), bundle.toString(), ex.getMessage());
                            LOG.warn(msg1, e);
                            completionFuture.completeExceptionally(new ServiceUnitNotReadyException(msg1));
                            return null;
                        });
            }, pulsar.getOrderedExecutor());
        });
    }

    /**
     * Update new bundle-range to LocalZk (create a new node if not present).
     * Update may fail because of concurrent write to Zookeeper.
     *
     * @param nsname
     * @param nsBundles
     * @throws Exception
     */
    private CompletableFuture<Void> updateNamespaceBundles(NamespaceName nsname, NamespaceBundles nsBundles) {
        checkNotNull(nsname);
        checkNotNull(nsBundles);

        LocalPolicies localPolicies = nsBundles.toLocalPolicies();

        return pulsar.getPulsarResources().getLocalPolicies()
                .setLocalPoliciesWithVersion(nsname, localPolicies, nsBundles.getVersion());
    }

    public OwnershipCache getOwnershipCache() {
        return ownershipCache;
    }

    public int getTotalServiceUnitsLoaded() {
        return ownershipCache.getOwnedBundles().size() - this.uncountedNamespaces;
    }

    public Set<NamespaceBundle> getOwnedServiceUnits() {
        return ownershipCache.getOwnedBundles().values().stream().map(OwnedBundle::getNamespaceBundle)
                .collect(Collectors.toSet());
    }

    public boolean isServiceUnitOwned(ServiceUnitId suName) throws Exception {
        if (suName instanceof TopicName) {
            return isTopicOwnedAsync((TopicName) suName).get();
        }

        if (suName instanceof NamespaceName) {
            return isNamespaceOwned((NamespaceName) suName);
        }

        if (suName instanceof NamespaceBundle) {
            return ownershipCache.isNamespaceBundleOwned((NamespaceBundle) suName);
        }

        throw new IllegalArgumentException("Invalid class of NamespaceBundle: " + suName.getClass().getName());
    }

    public CompletableFuture<Boolean> isServiceUnitOwnedAsync(ServiceUnitId suName) {
        if (suName instanceof TopicName) {
            return isTopicOwnedAsync((TopicName) suName);
        }

        if (suName instanceof NamespaceName) {
            return isNamespaceOwnedAsync((NamespaceName) suName);
        }

        if (suName instanceof NamespaceBundle) {
            return CompletableFuture.completedFuture(
                    ownershipCache.isNamespaceBundleOwned((NamespaceBundle) suName));
        }

        return FutureUtil.failedFuture(
                new IllegalArgumentException("Invalid class of NamespaceBundle: " + suName.getClass().getName()));
    }

    public boolean isServiceUnitActive(TopicName topicName) {
        try {
            return ownershipCache.getOwnedBundle(getBundle(topicName)).isActive();
        } catch (Exception e) {
            LOG.warn("Unable to find OwnedBundle for topic - [{}]", topicName);
            return false;
        }
    }

    private boolean isNamespaceOwned(NamespaceName fqnn) throws Exception {
        return ownershipCache.getOwnedBundle(getFullBundle(fqnn)) != null;
    }

    private CompletableFuture<Boolean> isNamespaceOwnedAsync(NamespaceName fqnn) {
        return getFullBundleAsync(fqnn)
                .thenApply(bundle -> ownershipCache.getOwnedBundle(bundle) != null);
    }

    private CompletableFuture<Boolean> isTopicOwnedAsync(TopicName topic) {
        return getBundleAsync(topic).thenApply(bundle -> ownershipCache.isNamespaceBundleOwned(bundle));
    }

    public CompletableFuture<Boolean> checkTopicOwnership(TopicName topicName) {
        return getBundleAsync(topicName)
                .thenApply(ownershipCache::checkOwnership);
    }

    public void removeOwnedServiceUnit(NamespaceBundle nsBundle) throws Exception {
        ownershipCache.removeOwnership(nsBundle).get(pulsar.getConfiguration().getZooKeeperOperationTimeoutSeconds(),
                SECONDS);
        bundleFactory.invalidateBundleCache(nsBundle.getNamespaceObject());
    }

    protected void onNamespaceBundleOwned(NamespaceBundle bundle) {
        for (NamespaceBundleOwnershipListener bundleOwnedListener : bundleOwnershipListeners) {
            notifyNamespaceBundleOwnershipListener(bundle, bundleOwnedListener);
        }
    }

    protected void onNamespaceBundleUnload(NamespaceBundle bundle) {
        for (NamespaceBundleOwnershipListener bundleOwnedListener : bundleOwnershipListeners) {
            try {
                if (bundleOwnedListener.test(bundle)) {
                    bundleOwnedListener.unLoad(bundle);
                }
            } catch (Throwable t) {
                LOG.error("Call bundle {} ownership lister error", bundle, t);
            }
        }
    }

    public void addNamespaceBundleOwnershipListener(NamespaceBundleOwnershipListener... listeners) {
        checkNotNull(listeners);
        for (NamespaceBundleOwnershipListener listener : listeners) {
            if (listener != null) {
                bundleOwnershipListeners.add(listener);
            }
        }
        getOwnedServiceUnits().forEach(bundle -> notifyNamespaceBundleOwnershipListener(bundle, listeners));
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
                    LOG.error("Call bundle {} ownership lister error", bundle, t);
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
                        (persistentTopics, nonPersistentTopics) -> {
                            return ListUtils.union(persistentTopics, nonPersistentTopics);
                        });
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

    public CompletableFuture<Boolean> checkTopicExists(TopicName topic) {
        if (topic.isPersistent()) {
            return pulsar.getPulsarResources().getTopicResources().persistentTopicExists(topic);
        } else {
            return pulsar.getBrokerService()
                    .getTopicIfExists(topic.toString())
                    .thenApply(optTopic -> optTopic.isPresent());
        }
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
            List<String> resultPartitions = Collections.synchronizedList(Lists.newArrayList());
            if (CollectionUtils.isNotEmpty(topics)) {
                List<CompletableFuture<List<String>>> futures = Lists.newArrayList();
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
            List<String> result = Lists.newArrayList();
            for (int i = 0; i < meta.partitions; i++) {
                result.add(topicName.getPartition(i).toString());
            }
            return CompletableFuture.completedFuture(result);
        });
    }

    public CompletableFuture<List<String>> getListOfPersistentTopics(NamespaceName namespaceName) {
        return pulsar.getPulsarResources().getTopicResources().listPersistentTopicsAsync(namespaceName);
    }

    public CompletableFuture<List<String>> getListOfNonPersistentTopics(NamespaceName namespaceName) {

        return PulsarWebResource.checkLocalOrGetPeerReplicationCluster(pulsar, namespaceName)
                .thenCompose(peerClusterData -> {
                    // if peer-cluster-data is present it means namespace is owned by that peer-cluster and request
                    // should be redirect to the peer-cluster
                    if (peerClusterData != null) {
                        return getNonPersistentTopicsFromPeerCluster(peerClusterData, namespaceName);
                    } else {
                        // Non-persistent topics don't have managed ledgers so we have to retrieve them from local
                        // cache.
                        List<String> topics = Lists.newArrayList();
                        synchronized (pulsar.getBrokerService().getMultiLayerTopicMap()) {
                            if (pulsar.getBrokerService().getMultiLayerTopicMap()
                                    .containsKey(namespaceName.toString())) {
                                pulsar.getBrokerService().getMultiLayerTopicMap().get(namespaceName.toString())
                                        .forEach((__, bundle) -> {
                                            bundle.forEach((topicName, topic) -> {
                                                if (topic instanceof NonPersistentTopic
                                                        && ((NonPersistentTopic) topic).isActive()) {
                                                    topics.add(topicName);
                                                }
                                            });
                                        });
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
        return client.getLookup().getTopicsUnderNamespace(namespace, Mode.NON_PERSISTENT);
    }

    public PulsarClientImpl getNamespaceClient(ClusterDataImpl cluster) {
        PulsarClientImpl client = namespaceClients.get(cluster);
        if (client != null) {
            return client;
        }

        return namespaceClients.computeIfAbsent(cluster, key -> {
            try {
                ClientBuilder clientBuilder = PulsarClient.builder()
                    .enableTcpNoDelay(false)
                    .statsInterval(0, TimeUnit.SECONDS);

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
                        .allowTlsInsecureConnection(pulsar.getConfiguration().isTlsAllowInsecureConnection());
                } else {
                    clientBuilder.serviceUrl(isNotBlank(cluster.getBrokerServiceUrl())
                        ? cluster.getBrokerServiceUrl() : cluster.getServiceUrl());
                }

                // Share all the IO threads across broker and client connections
                ClientConfigurationData conf = ((ClientBuilderImpl) clientBuilder).getClientConfigurationData();
                return new PulsarClientImpl(conf, (EventLoopGroup) pulsar.getBrokerService().executor());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Optional<NamespaceEphemeralData> getOwner(NamespaceBundle bundle) throws Exception {
        // if there is no znode for the service unit, it is not owned by any broker
        return getOwnerAsync(bundle).get(pulsar.getConfiguration().getZooKeeperOperationTimeoutSeconds(), SECONDS);
    }

    public CompletableFuture<Optional<NamespaceEphemeralData>> getOwnerAsync(NamespaceBundle bundle) {
        return ownershipCache.getOwnerAsync(bundle);
    }

    public void unloadSLANamespace() throws Exception {
        PulsarAdmin adminClient = null;
        NamespaceName namespaceName = getSLAMonitorNamespace(host, config);

        LOG.info("Checking owner for SLA namespace {}", namespaceName);

        NamespaceBundle nsFullBundle = getFullBundle(namespaceName);
        if (!getOwner(nsFullBundle).isPresent()) {
            // No one owns the namespace so no point trying to unload it
            // Next lookup will assign the bundle to this broker.
            return;
        }

        LOG.info("Trying to unload SLA namespace {}", namespaceName);
        adminClient = pulsar.getAdminClient();
        adminClient.namespaces().unload(namespaceName.toString());
        LOG.info("Namespace {} unloaded successfully", namespaceName);
    }

    public static NamespaceName getHeartbeatNamespace(String host, ServiceConfiguration config) {
        Integer port = null;
        if (config.getWebServicePort().isPresent()) {
            port = config.getWebServicePort().get();
        } else if (config.getWebServicePortTls().isPresent()) {
            port = config.getWebServicePortTls().get();
        }
        return NamespaceName.get(String.format(HEARTBEAT_NAMESPACE_FMT, config.getClusterName(), host, port));
    }

    public static NamespaceName getHeartbeatNamespaceV2(String host, ServiceConfiguration config) {
        Integer port = null;
        if (config.getWebServicePort().isPresent()) {
            port = config.getWebServicePort().get();
        } else if (config.getWebServicePortTls().isPresent()) {
            port = config.getWebServicePortTls().get();
        }
        return NamespaceName.get(String.format(HEARTBEAT_NAMESPACE_FMT_V2, host, port));
    }

    public static NamespaceName getSLAMonitorNamespace(String host, ServiceConfiguration config) {
        Integer port = null;
        if (config.getWebServicePort().isPresent()) {
            port = config.getWebServicePort().get();
        } else if (config.getWebServicePortTls().isPresent()) {
            port = config.getWebServicePortTls().get();
        }
        return NamespaceName.get(String.format(SLA_NAMESPACE_FMT, config.getClusterName(), host, port));
    }

    public static String checkHeartbeatNamespace(ServiceUnitId ns) {
        Matcher m = HEARTBEAT_NAMESPACE_PATTERN.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            LOG.debug("Heartbeat namespace matched the lookup namespace {}", ns.getNamespaceObject().toString());
            return String.format("http://%s", m.group(1));
        } else {
            return null;
        }
    }

    public static String checkHeartbeatNamespaceV2(ServiceUnitId ns) {
        Matcher m = HEARTBEAT_NAMESPACE_PATTERN_V2.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            LOG.debug("Heartbeat namespace v2 matched the lookup namespace {}", ns.getNamespaceObject().toString());
            return String.format("http://%s", m.group(1));
        } else {
            return null;
        }
    }

    public static String getSLAMonitorBrokerName(ServiceUnitId ns) {
        Matcher m = SLA_NAMESPACE_PATTERN.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            return String.format("http://%s", m.group(1));
        } else {
            return null;
        }
    }

    public static boolean isSystemServiceNamespace(String namespace) {
        return HEARTBEAT_NAMESPACE_PATTERN.matcher(namespace).matches()
                || HEARTBEAT_NAMESPACE_PATTERN_V2.matcher(namespace).matches()
                || SLA_NAMESPACE_PATTERN.matcher(namespace).matches();
    }

    public boolean registerSLANamespace() throws PulsarServerException {
        boolean isNameSpaceRegistered = registerNamespace(getSLAMonitorNamespace(host, config), false);
        if (isNameSpaceRegistered) {
            this.uncountedNamespaces++;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Added SLA Monitoring namespace name in local cache: ns={}",
                        getSLAMonitorNamespace(host, config));
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("SLA Monitoring not owned by the broker: ns={}", getSLAMonitorNamespace(host, config));
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
