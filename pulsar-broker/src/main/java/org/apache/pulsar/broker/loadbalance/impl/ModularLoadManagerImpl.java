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
package org.apache.pulsar.broker.loadbalance.impl;

import static org.apache.pulsar.broker.admin.AdminResource.jsonMapper;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.web.PulsarWebResource.path;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageBrokerData;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.BundleSplitStrategy;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.broker.loadbalance.ModularLoadManager;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared.BrokerTopicLoadingPredicate;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;
import org.apache.pulsar.zookeeper.ZooKeeperCacheListener;
import org.apache.pulsar.zookeeper.ZooKeeperChildrenCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModularLoadManagerImpl implements ModularLoadManager, ZooKeeperCacheListener<LocalBrokerData> {
    private static final Logger log = LoggerFactory.getLogger(ModularLoadManagerImpl.class);

    // Path to ZNode whose children contain BundleData jsons for each bundle (new API version of ResourceQuota).
    public static final String BUNDLE_DATA_ZPATH = "/loadbalance/bundle-data";

    // Default message rate to assume for unseen bundles.
    public static final double DEFAULT_MESSAGE_RATE = 50;

    // Default message throughput to assume for unseen bundles.
    // Note that the default message size is implicitly defined as DEFAULT_MESSAGE_THROUGHPUT / DEFAULT_MESSAGE_RATE.
    public static final double DEFAULT_MESSAGE_THROUGHPUT = 50000;

    // The number of effective samples to keep for observing long term data.
    public static final int NUM_LONG_SAMPLES = 1000;

    // The number of effective samples to keep for observing short term data.
    public static final int NUM_SHORT_SAMPLES = 10;

    // Path to ZNode whose children contain ResourceQuota jsons.
    public static final String RESOURCE_QUOTA_ZPATH = "/loadbalance/resource-quota/namespace";

    // Path to ZNode containing TimeAverageBrokerData jsons for each broker.
    public static final String TIME_AVERAGE_BROKER_ZPATH = "/loadbalance/broker-time-average";

    // ZooKeeper Cache of the currently available active brokers.
    // availableActiveBrokers.get() will return a set of the broker names without an http prefix.
    private ZooKeeperChildrenCache availableActiveBrokers;

    // Set of broker candidates to reuse so that object creation is avoided.
    private final Set<String> brokerCandidateCache;

    // ZooKeeper cache of the local broker data, stored in LoadManager.LOADBALANCE_BROKER_ROOT.
    private ZooKeeperDataCache<LocalBrokerData> brokerDataCache;

    // Broker host usage object used to calculate system resource usage.
    private BrokerHostUsage brokerHostUsage;

    // Map from brokers to namespaces to the bundle ranges in that namespace assigned to that broker.
    // Used to distribute bundles within a namespace evenly across brokers.
    private final ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>> brokerToNamespaceToBundleRange;

    // Path to the ZNode containing the LocalBrokerData json for this broker.
    private String brokerZnodePath;

    // Strategy to use for splitting bundles.
    private BundleSplitStrategy bundleSplitStrategy;

    // Service configuration belonging to the pulsar service.
    private ServiceConfiguration conf;

    // The default bundle stats which are used to initialize historic data.
    // This data is overridden after the bundle receives its first sample.
    private final NamespaceBundleStats defaultStats;

    // Used to filter brokers from being selected for assignment.
    private final List<BrokerFilter> filterPipeline;

    // Timestamp of last invocation of updateBundleData.
    private long lastBundleDataUpdate;

    // LocalBrokerData available before most recent update.
    private LocalBrokerData lastData;

    // Pipeline used to determine what namespaces, if any, should be unloaded.
    private final List<LoadSheddingStrategy> loadSheddingPipeline;

    // Local data for the broker this is running on.
    private LocalBrokerData localData;

    // Load data comprising data available for each broker.
    private final LoadData loadData;

    // Used to determine whether a bundle is preallocated.
    private final Map<String, String> preallocatedBundleToBroker;

    // Strategy used to determine where new topics should be placed.
    private ModularLoadManagerStrategy placementStrategy;

    // Policies used to determine which brokers are available for particular namespaces.
    private SimpleResourceAllocationPolicies policies;

    // Pulsar service used to initialize this.
    private PulsarService pulsar;

    // Executor service used to regularly update broker data.
    private final ScheduledExecutorService scheduler;

    // ZooKeeper belonging to the pulsar service.
    private ZooKeeper zkClient;

    // check if given broker can load persistent/non-persistent topic
    private final BrokerTopicLoadingPredicate brokerTopicLoadingPredicate;

    private Map<String, String> brokerToFailureDomainMap;

    private static final Deserializer<LocalBrokerData> loadReportDeserializer = (key, content) -> jsonMapper()
            .readValue(content, LocalBrokerData.class);

    // record load balancing metrics
    private AtomicReference<List<Metrics>> loadBalancingMetrics = new AtomicReference<>();
    // record bundle unload metrics
    private AtomicReference<List<Metrics>> bundleUnloadMetrics = new AtomicReference<>();
    // record bundle split metrics
    private AtomicReference<List<Metrics>> bundleSplitMetrics = new AtomicReference<>();

    private long bundleSplitCount = 0;
    private long unloadBrokerCount = 0;
    private long unloadBundleCount = 0;

    /**
     * Initializes fields which do not depend on PulsarService. initialize(PulsarService) should subsequently be called.
     */
    public ModularLoadManagerImpl() {
        brokerCandidateCache = new HashSet<>();
        brokerToNamespaceToBundleRange = new ConcurrentOpenHashMap<>();
        defaultStats = new NamespaceBundleStats();
        filterPipeline = new ArrayList<>();
        loadData = new LoadData();
        loadSheddingPipeline = new ArrayList<>();
        preallocatedBundleToBroker = new ConcurrentHashMap<>();
        scheduler = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-modular-load-manager"));
        this.brokerToFailureDomainMap = Maps.newHashMap();

        this.brokerTopicLoadingPredicate = new BrokerTopicLoadingPredicate() {
            @Override
            public boolean isEnablePersistentTopics(String brokerUrl) {
                final BrokerData brokerData = loadData.getBrokerData().get(brokerUrl.replace("http://", ""));
                return brokerData != null && brokerData.getLocalData() != null
                        && brokerData.getLocalData().isPersistentTopicsEnabled();
            }

            @Override
            public boolean isEnableNonPersistentTopics(String brokerUrl) {
                final BrokerData brokerData = loadData.getBrokerData().get(brokerUrl.replace("http://", ""));
                return brokerData != null && brokerData.getLocalData() != null
                        && brokerData.getLocalData().isNonPersistentTopicsEnabled();
            }
        };
    }

    /**
     * Initialize this load manager using the given PulsarService. Should be called only once, after invoking the
     * default constructor.
     *
     * @param pulsar The service to initialize with.
     */
    @Override
    public void initialize(final PulsarService pulsar) {
        this.pulsar = pulsar;
        availableActiveBrokers = new ZooKeeperChildrenCache(pulsar.getLocalZkCache(),
                LoadManager.LOADBALANCE_BROKERS_ROOT);
        availableActiveBrokers.registerListener(new ZooKeeperCacheListener<Set<String>>() {
            @Override
            public void onUpdate(String path, Set<String> data, Stat stat) {
                if (log.isDebugEnabled()) {
                    log.debug("Update Received for path {}", path);
                }
                reapDeadBrokerPreallocations(data);
                scheduler.submit(ModularLoadManagerImpl.this::updateAll);
            }
        });

        brokerDataCache = new ZooKeeperDataCache<LocalBrokerData>(pulsar.getLocalZkCache()) {
            @Override
            public LocalBrokerData deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, LocalBrokerData.class);
            }
        };

        brokerDataCache.registerListener(this);

        if (SystemUtils.IS_OS_LINUX) {
            brokerHostUsage = new LinuxBrokerHostUsageImpl(pulsar);
        } else {
            brokerHostUsage = new GenericBrokerHostUsageImpl(pulsar);
        }

        bundleSplitStrategy = new BundleSplitterTask(pulsar);

        conf = pulsar.getConfiguration();

        // Initialize the default stats to assume for unseen bundles (hard-coded for now).
        defaultStats.msgThroughputIn = DEFAULT_MESSAGE_THROUGHPUT;
        defaultStats.msgThroughputOut = DEFAULT_MESSAGE_THROUGHPUT;
        defaultStats.msgRateIn = DEFAULT_MESSAGE_RATE;
        defaultStats.msgRateOut = DEFAULT_MESSAGE_RATE;

        placementStrategy = ModularLoadManagerStrategy.create(conf);
        policies = new SimpleResourceAllocationPolicies(pulsar);
        zkClient = pulsar.getZkClient();
        filterPipeline.add(new BrokerVersionFilter());

        refreshBrokerToFailureDomainMap();
        // register listeners for domain changes
        pulsar.getConfigurationCache().failureDomainListCache()
                .registerListener((path, data, stat) -> scheduler.execute(() -> refreshBrokerToFailureDomainMap()));
        pulsar.getConfigurationCache().failureDomainCache()
                .registerListener((path, data, stat) -> scheduler.execute(() -> refreshBrokerToFailureDomainMap()));

        loadSheddingPipeline.add(createLoadSheddingStrategy());
    }

    private LoadSheddingStrategy createLoadSheddingStrategy() {
        try {
            Class<?> loadSheddingClass = Class.forName(conf.getLoadBalancerLoadSheddingStrategy());
            Object loadSheddingInstance = loadSheddingClass.newInstance();
            if (loadSheddingInstance instanceof LoadSheddingStrategy) {
                return (LoadSheddingStrategy) loadSheddingInstance;
            } else {
                log.error("create load shedding strategy failed. using OverloadShedder instead.");
                return new OverloadShedder();
            }
        } catch (Exception e) {
            log.error("Error when trying to create load shedding strategy: ", e);
        }

        return new OverloadShedder();
    }

    /**
     * Initialize this load manager.
     *
     * @param pulsar
     *            Client to construct this manager from.
     */
    public ModularLoadManagerImpl(final PulsarService pulsar) {
        this();
        initialize(pulsar);
    }

    // Attempt to create a ZooKeeper path if it does not exist.
    private static void createZPathIfNotExists(final ZooKeeper zkClient, final String path) throws Exception {
        if (zkClient.exists(path, false) == null) {
            try {
                ZkUtils.createFullPathOptimistic(zkClient, path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Ignore if already exists.
            }
        }
    }

    // For each broker that we have a recent load report, see if they are still alive
    private void reapDeadBrokerPreallocations(Set<String> aliveBrokers) {
        for ( String broker : loadData.getBrokerData().keySet() ) {
            if ( !aliveBrokers.contains(broker)) {
                if ( log.isDebugEnabled() ) {
                    log.debug("Broker {} appears to have stopped; now reclaiming any preallocations", broker);
                }
                final Iterator<Map.Entry<String, String>> iterator = preallocatedBundleToBroker.entrySet().iterator();
                while ( iterator.hasNext() ) {
                    Map.Entry<String, String> entry = iterator.next();
                    final String preallocatedBundle = entry.getKey();
                    final String preallocatedBroker = entry.getValue();
                    if ( broker.equals(preallocatedBroker) ) {
                        if ( log.isDebugEnabled() ) {
                            log.debug("Removing old preallocation on dead broker {} for bundle {}",
                                    preallocatedBroker, preallocatedBundle);
                        }
                        iterator.remove();
                    }
                }
            }
        }
    }

    @Override
    public Set<String> getAvailableBrokers() {
        try {
            return availableActiveBrokers.get();
        } catch (Exception e) {
            log.warn("Error when trying to get active brokers", e);
            return loadData.getBrokerData().keySet();
        }
    }

    // Attempt to local the data for the given bundle in ZooKeeper.
    // If it cannot be found, return the default bundle data.
    private BundleData getBundleDataOrDefault(final String bundle) {
        BundleData bundleData = null;
        try {
            final String bundleZPath = getBundleDataZooKeeperPath(bundle);
            final String quotaZPath = String.format("%s/%s", RESOURCE_QUOTA_ZPATH, bundle);
            if (zkClient.exists(bundleZPath, null) != null) {
                bundleData = readJson(zkClient.getData(bundleZPath, null, null), BundleData.class);
            } else if (zkClient.exists(quotaZPath, null) != null) {
                final ResourceQuota quota = readJson(zkClient.getData(quotaZPath, null, null), ResourceQuota.class);
                bundleData = new BundleData(NUM_SHORT_SAMPLES, NUM_LONG_SAMPLES);
                // Initialize from existing resource quotas if new API ZNodes do not exist.
                final TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                final TimeAverageMessageData longTermData = bundleData.getLongTermData();

                shortTermData.setMsgRateIn(quota.getMsgRateIn());
                shortTermData.setMsgRateOut(quota.getMsgRateOut());
                shortTermData.setMsgThroughputIn(quota.getBandwidthIn());
                shortTermData.setMsgThroughputOut(quota.getBandwidthOut());

                longTermData.setMsgRateIn(quota.getMsgRateIn());
                longTermData.setMsgRateOut(quota.getMsgRateOut());
                longTermData.setMsgThroughputIn(quota.getBandwidthIn());
                longTermData.setMsgThroughputOut(quota.getBandwidthOut());

                // Assume ample history.
                shortTermData.setNumSamples(NUM_SHORT_SAMPLES);
                longTermData.setNumSamples(NUM_LONG_SAMPLES);
            }
        } catch (Exception e) {
            log.warn("Error when trying to find bundle {} on zookeeper: {}", bundle, e);
        }
        if (bundleData == null) {
            bundleData = new BundleData(NUM_SHORT_SAMPLES, NUM_LONG_SAMPLES, defaultStats);
        }
        return bundleData;
    }

    // Get the ZooKeeper path for the given bundle full name.
    private static String getBundleDataZooKeeperPath(final String bundle) {
        return BUNDLE_DATA_ZPATH + "/" + bundle;
    }

    // Use the Pulsar client to acquire the namespace bundle stats.
    private Map<String, NamespaceBundleStats> getBundleStats() {
        return pulsar.getBrokerService().getBundleStats();
    }

    // Use the thread local ObjectMapperFactory to read the given json data into an instance of the given class.
    private static <T> T readJson(final byte[] data, final Class<T> clazz) throws IOException {
        return ObjectMapperFactory.getThreadLocal().readValue(data, clazz);
    }

    private double percentChange(final double oldValue, final double newValue) {
        if (oldValue == 0) {
            if (newValue == 0) {
                // Avoid NaN
                return 0;
            }
            return Double.POSITIVE_INFINITY;
        }
        return 100 * Math.abs((oldValue - newValue) / oldValue);
    }

    // Determine if the broker data requires an update by delegating to the update condition.
    private boolean needBrokerDataUpdate() {
        final long updateMaxIntervalMillis = TimeUnit.MINUTES
                .toMillis(conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
        long timeSinceLastReportWrittenToZooKeeper = System.currentTimeMillis() - localData.getLastUpdate();
        if (timeSinceLastReportWrittenToZooKeeper > updateMaxIntervalMillis) {
            log.info("Writing local data to ZooKeeper because time since last update exceeded threshold of {} minutes",
                    conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
            // Always update after surpassing the maximum interval.
            return true;
        }
        final double maxChange = Math
                .max(100.0 * (Math.abs(lastData.getMaxResourceUsage() - localData.getMaxResourceUsage())),
                        Math.max(percentChange(lastData.getMsgRateIn() + lastData.getMsgRateOut(),
                                localData.getMsgRateIn() + localData.getMsgRateOut()),
                                Math.max(
                                        percentChange(lastData.getMsgThroughputIn() + lastData.getMsgThroughputOut(),
                                                localData.getMsgThroughputIn() + localData.getMsgThroughputOut()),
                                        percentChange(lastData.getNumBundles(), localData.getNumBundles()))));
        if (maxChange > conf.getLoadBalancerReportUpdateThresholdPercentage()) {
            log.info("Writing local data to ZooKeeper because maximum change {}% exceeded threshold {}%; " +
                    "time since last report written is {} seconds", maxChange,
                    conf.getLoadBalancerReportUpdateThresholdPercentage(), timeSinceLastReportWrittenToZooKeeper/1000.0);
            return true;
        }
        return false;
    }

    // Update both the broker data and the bundle data.
    public void updateAll() {
        if (log.isDebugEnabled()) {
            log.debug("Updating broker and bundle data for loadreport");
        }
        updateAllBrokerData();
        updateBundleData();
        // broker has latest load-report: check if any bundle requires split
        checkNamespaceBundleSplit();
    }

    // As the leader broker, update the broker data map in loadData by querying ZooKeeper for the broker data put there
    // by each broker via updateLocalBrokerData.
    private void updateAllBrokerData() {
        final Set<String> activeBrokers = getAvailableBrokers();
        final Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
        for (String broker : activeBrokers) {
            try {
                String key = String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, broker);
                final LocalBrokerData localData = brokerDataCache.get(key)
                        .orElseThrow(KeeperException.NoNodeException::new);

                if (brokerDataMap.containsKey(broker)) {
                    // Replace previous local broker data.
                    brokerDataMap.get(broker).setLocalData(localData);
                } else {
                    // Initialize BrokerData object for previously unseen
                    // brokers.
                    brokerDataMap.put(broker, new BrokerData(localData));
                }
            } catch (NoNodeException ne) {
                // it only happens if we update-brokerData before availableBrokerCache refreshed with latest data and
                // broker's delete-znode watch-event hasn't updated availableBrokerCache
                brokerDataMap.remove(broker);
                log.warn("[{}] broker load-report znode not present", broker, ne);
            } catch (Exception e) {
                log.warn("Error reading broker data from cache for broker - [{}], [{}]", broker, e.getMessage());
            }
        }
        // Remove obsolete brokers.
        for (final String broker : brokerDataMap.keySet()) {
            if (!activeBrokers.contains(broker)) {
                brokerDataMap.remove(broker);
            }
        }
    }

    // As the leader broker, use the local broker data saved on ZooKeeper to update the bundle stats so that better load
    // management decisions may be made.
    private void updateBundleData() {
        final Map<String, BundleData> bundleData = loadData.getBundleData();
        // Iterate over the broker data.
        for (Map.Entry<String, BrokerData> brokerEntry : loadData.getBrokerData().entrySet()) {
            final String broker = brokerEntry.getKey();
            final BrokerData brokerData = brokerEntry.getValue();
            final Map<String, NamespaceBundleStats> statsMap = brokerData.getLocalData().getLastStats();

            // Iterate over the last bundle stats available to the current
            // broker to update the bundle data.
            for (Map.Entry<String, NamespaceBundleStats> entry : statsMap.entrySet()) {
                final String bundle = entry.getKey();
                final NamespaceBundleStats stats = entry.getValue();
                if (bundleData.containsKey(bundle)) {
                    // If we recognize the bundle, add these stats as a new sample.
                    bundleData.get(bundle).update(stats);
                } else {
                    // Otherwise, attempt to find the bundle data on ZooKeeper.
                    // If it cannot be found, use the latest stats as the first sample.
                    BundleData currentBundleData = getBundleDataOrDefault(bundle);
                    currentBundleData.update(stats);
                    bundleData.put(bundle, currentBundleData);
                }
            }

            // Remove all loaded bundles from the preallocated maps.
            final Map<String, BundleData> preallocatedBundleData = brokerData.getPreallocatedBundleData();
            synchronized (preallocatedBundleData) {
                for (String preallocatedBundleName : brokerData.getPreallocatedBundleData().keySet()) {
                    if (brokerData.getLocalData().getBundles().contains(preallocatedBundleName)) {
                        final Iterator<Map.Entry<String, BundleData>> preallocatedIterator = preallocatedBundleData.entrySet()
                                .iterator();
                        while (preallocatedIterator.hasNext()) {
                            final String bundle = preallocatedIterator.next().getKey();

                            if (bundleData.containsKey(bundle)) {
                                preallocatedIterator.remove();
                                preallocatedBundleToBroker.remove(bundle);
                            }
                        }
                    }

                    // This is needed too in case a broker which was assigned a bundle dies and comes back up.
                    preallocatedBundleToBroker.remove(preallocatedBundleName);
                }
            }

            // Using the newest data, update the aggregated time-average data for the current broker.
            brokerData.getTimeAverageData().reset(statsMap.keySet(), bundleData, defaultStats);
            final ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>> namespaceToBundleRange = brokerToNamespaceToBundleRange
                    .computeIfAbsent(broker, k -> new ConcurrentOpenHashMap<>());
            synchronized (namespaceToBundleRange) {
                namespaceToBundleRange.clear();
                LoadManagerShared.fillNamespaceToBundlesMap(statsMap.keySet(), namespaceToBundleRange);
                LoadManagerShared.fillNamespaceToBundlesMap(preallocatedBundleData.keySet(), namespaceToBundleRange);
            }
        }
    }

    /**
     * As any broker, disable the broker this manager is running on.
     *
     * @throws PulsarServerException
     *             If ZooKeeper failed to disable the broker.
     */
    @Override
    public void disableBroker() throws PulsarServerException {
        if (StringUtils.isNotEmpty(brokerZnodePath)) {
            try {
                pulsar.getZkClient().delete(brokerZnodePath, -1);
            } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                throw new PulsarServerException.NotFoundException(e);
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }
    }

    /**
     * As the leader broker, select bundles for the namespace service to unload so that they may be reassigned to new
     * brokers.
     */
    @Override
    public synchronized void doLoadShedding() {
        if (!LoadManagerShared.isLoadSheddingEnabled(pulsar)) {
            return;
        }
        if (getAvailableBrokers().size() <= 1) {
            log.info("Only 1 broker available: no load shedding will be performed");
            return;
        }
        // Remove bundles who have been unloaded for longer than the grace period from the recently unloaded map.
        final long timeout = System.currentTimeMillis()
                - TimeUnit.MINUTES.toMillis(conf.getLoadBalancerSheddingGracePeriodMinutes());
        final Map<String, Long> recentlyUnloadedBundles = loadData.getRecentlyUnloadedBundles();
        recentlyUnloadedBundles.keySet().removeIf(e -> recentlyUnloadedBundles.get(e) < timeout);

        for (LoadSheddingStrategy strategy : loadSheddingPipeline) {
            final Multimap<String, String> bundlesToUnload = strategy.findBundlesForUnloading(loadData, conf);

            bundlesToUnload.asMap().forEach((broker, bundles) -> {
                bundles.forEach(bundle -> {
                    final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
                    final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
                    if (!shouldAntiAffinityNamespaceUnload(namespaceName, bundleRange, broker)) {
                        return;
                    }

                    log.info("[Overload shedder] Unloading bundle: {} from broker {}", bundle, broker);
                    try {
                        pulsar.getAdminClient().namespaces().unloadNamespaceBundle(namespaceName, bundleRange);
                        loadData.getRecentlyUnloadedBundles().put(bundle, System.currentTimeMillis());
                    } catch (PulsarServerException | PulsarAdminException e) {
                        log.warn("Error when trying to perform load shedding on {} for broker {}", bundle, broker, e);
                    }
                });
            });

            updateBundleUnloadingMetrics(bundlesToUnload);
        }
    }

    /**
     * As leader broker, update bundle unloading metrics.
     *
     * @param bundlesToUnload
     */
    private void updateBundleUnloadingMetrics(Multimap<String, String> bundlesToUnload) {
        unloadBrokerCount += bundlesToUnload.keySet().size();
        unloadBundleCount += bundlesToUnload.values().size();

        List<Metrics> metrics = Lists.newArrayList();
        Map<String, String> dimensions = new HashMap<>();

        dimensions.put("metric", "bundleUnloading");

        Metrics m = Metrics.create(dimensions);
        m.put("brk_lb_unload_broker_count", unloadBrokerCount);
        m.put("brk_lb_unload_bundle_count", unloadBundleCount);
        metrics.add(m);
        this.bundleUnloadMetrics.set(metrics);
    }

    public boolean shouldAntiAffinityNamespaceUnload(String namespace, String bundle, String currentBroker) {
        try {
            Optional<Policies> nsPolicies = pulsar.getConfigurationCache().policiesCache()
                    .get(path(POLICIES, namespace));
            if (!nsPolicies.isPresent() || StringUtils.isBlank(nsPolicies.get().antiAffinityGroup)) {
                return true;
            }

            synchronized (brokerCandidateCache) {
                brokerCandidateCache.clear();
                ServiceUnitId serviceUnit = pulsar.getNamespaceService().getNamespaceBundleFactory()
                        .getBundle(namespace, bundle);
                LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache,
                        getAvailableBrokers(), brokerTopicLoadingPredicate);
                return LoadManagerShared.shouldAntiAffinityNamespaceUnload(namespace, bundle, currentBroker, pulsar,
                        brokerToNamespaceToBundleRange, brokerCandidateCache);
            }

        } catch (Exception e) {
            log.warn("Failed to check anti-affinity namespace ownership for {}/{}/{}, {}", namespace, bundle,
                    currentBroker, e.getMessage());

        }
        return true;
    }

    /**
     * As the leader broker, attempt to automatically detect and split hot namespace bundles.
     */
    @Override
    public void checkNamespaceBundleSplit() {

        if (!conf.isLoadBalancerAutoBundleSplitEnabled() || pulsar.getLeaderElectionService() == null
                || !pulsar.getLeaderElectionService().isLeader()) {
            return;
        }
        final boolean unloadSplitBundles = pulsar.getConfiguration().isLoadBalancerAutoUnloadSplitBundlesEnabled();
        synchronized (bundleSplitStrategy) {
            final Set<String> bundlesToBeSplit = bundleSplitStrategy.findBundlesToSplit(loadData, pulsar);
            NamespaceBundleFactory namespaceBundleFactory = pulsar.getNamespaceService().getNamespaceBundleFactory();
            for (String bundleName : bundlesToBeSplit) {
                try {
                    final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundleName);
                    final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundleName);
                    if (!namespaceBundleFactory
                            .canSplitBundle(namespaceBundleFactory.getBundle(namespaceName, bundleRange))) {
                        continue;
                    }

                    // Make sure the same bundle is not selected again.
                    loadData.getBundleData().remove(bundleName);
                    localData.getLastStats().remove(bundleName);
                    // Clear namespace bundle-cache
                    this.pulsar.getNamespaceService().getNamespaceBundleFactory()
                            .invalidateBundleCache(NamespaceName.get(namespaceName));
                    deleteBundleDataFromZookeeper(bundleName);

                    log.info("Load-manager splitting bundle {} and unloading {}", bundleName, unloadSplitBundles);
                    pulsar.getAdminClient().namespaces().splitNamespaceBundle(namespaceName, bundleRange,
                        unloadSplitBundles, null);

                    log.info("Successfully split namespace bundle {}", bundleName);
                } catch (Exception e) {
                    log.error("Failed to split namespace bundle {}", bundleName, e);
                }
            }

            updateBundleSplitMetrics(bundlesToBeSplit);
        }

    }

    /**
     * As leader broker, update bundle split metrics.
     *
     * @param bundlesToBeSplit
     */
    private void updateBundleSplitMetrics(Set<String> bundlesToBeSplit) {
        bundleSplitCount += bundlesToBeSplit.size();

        List<Metrics> metrics = Lists.newArrayList();
        Map<String, String> dimensions = new HashMap<>();

        dimensions.put("metric", "bundlesSplit");

        Metrics m = Metrics.create(dimensions);
        m.put("brk_lb_bundles_split_count", bundleSplitCount);
        metrics.add(m);
        this.bundleSplitMetrics.set(metrics);
    }

    /**
     * When the broker data ZooKeeper nodes are updated, update the broker data map.
     */
    @Override
    public void onUpdate(final String path, final LocalBrokerData data, final Stat stat) {
        scheduler.submit(this::updateAll);
    }

    /**
     * As the leader broker, find a suitable broker for the assignment of the given bundle.
     *
     * @param serviceUnit
     *            ServiceUnitId for the bundle.
     * @return The name of the selected broker, as it appears on ZooKeeper.
     */
    @Override
    public Optional<String> selectBrokerForAssignment(final ServiceUnitId serviceUnit) {
        // Use brokerCandidateCache as a lock to reduce synchronization.
        synchronized (brokerCandidateCache) {
            final String bundle = serviceUnit.toString();
            if (preallocatedBundleToBroker.containsKey(bundle)) {
                // If the given bundle is already in preallocated, return the selected broker.
                return Optional.of(preallocatedBundleToBroker.get(bundle));
            }
            final BundleData data = loadData.getBundleData().computeIfAbsent(bundle,
                    key -> getBundleDataOrDefault(bundle));
            brokerCandidateCache.clear();
            LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),
                    brokerTopicLoadingPredicate);

            // filter brokers which owns topic higher than threshold
            LoadManagerShared.filterBrokersWithLargeTopicCount(brokerCandidateCache, loadData,
                    conf.getLoadBalancerBrokerMaxTopics());

            // distribute namespaces to domain and brokers according to anti-affinity-group
            LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar, serviceUnit.toString(), brokerCandidateCache,
                    brokerToNamespaceToBundleRange, brokerToFailureDomainMap);
            // distribute bundles evenly to candidate-brokers

            LoadManagerShared.removeMostServicingBrokersForNamespace(serviceUnit.toString(), brokerCandidateCache,
                    brokerToNamespaceToBundleRange);
            log.info("{} brokers being considered for assignment of {}", brokerCandidateCache.size(), bundle);

            // Use the filter pipeline to finalize broker candidates.
            try {
                for (BrokerFilter filter : filterPipeline) {
                    filter.filter(brokerCandidateCache, data, loadData, conf);
                }
            } catch ( BrokerFilterException x ) {
                // restore the list of brokers to the full set
                LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),
                        brokerTopicLoadingPredicate);
            }

            if ( brokerCandidateCache.isEmpty() ) {
                // restore the list of brokers to the full set
                LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),
                        brokerTopicLoadingPredicate);
            }

            // Choose a broker among the potentially smaller filtered list, when possible
            Optional<String> broker = placementStrategy.selectBroker(brokerCandidateCache, data, loadData, conf);
            if (log.isDebugEnabled()) {
                log.debug("Selected broker {} from candidate brokers {}", broker, brokerCandidateCache);
            }

            if (!broker.isPresent()) {
                // No brokers available
                return broker;
            }

            final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
            final double maxUsage = loadData.getBrokerData().get(broker.get()).getLocalData().getMaxResourceUsage();
            if (maxUsage > overloadThreshold) {
                // All brokers that were in the filtered list were overloaded, so check if there is a better broker
                LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),
                        brokerTopicLoadingPredicate);
                broker = placementStrategy.selectBroker(brokerCandidateCache, data, loadData, conf);
            }

            // Add new bundle to preallocated.
            loadData.getBrokerData().get(broker.get()).getPreallocatedBundleData().put(bundle, data);
            preallocatedBundleToBroker.put(bundle, broker.get());

            final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
            final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
            final ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>> namespaceToBundleRange = brokerToNamespaceToBundleRange
                    .computeIfAbsent(broker.get(), k -> new ConcurrentOpenHashMap<>());
            synchronized (namespaceToBundleRange) {
                namespaceToBundleRange.computeIfAbsent(namespaceName, k -> new ConcurrentOpenHashSet<>())
                        .add(bundleRange);
            }
            return broker;
        }
    }

    /**
     * As any broker, start the load manager.
     *
     * @throws PulsarServerException
     *             If an unexpected error prevented the load manager from being started.
     */
    @Override
    public void start() throws PulsarServerException {
        try {
            // At this point, the ports will be updated with the real port number that the server was assigned
            Map<String, String> protocolData = pulsar.getProtocolDataToAdvertise();

            lastData = new LocalBrokerData(pulsar.getSafeWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                    pulsar.getSafeBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls(), pulsar.getAdvertisedListeners());
            lastData.setProtocols(protocolData);
            // configure broker-topic mode
            lastData.setPersistentTopicsEnabled(pulsar.getConfiguration().isEnablePersistentTopics());
            lastData.setNonPersistentTopicsEnabled(pulsar.getConfiguration().isEnableNonPersistentTopics());

            localData = new LocalBrokerData(pulsar.getSafeWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                    pulsar.getSafeBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls(), pulsar.getAdvertisedListeners());
            localData.setProtocols(protocolData);
            localData.setBrokerVersionString(pulsar.getBrokerVersion());
            // configure broker-topic mode
            localData.setPersistentTopicsEnabled(pulsar.getConfiguration().isEnablePersistentTopics());
            localData.setNonPersistentTopicsEnabled(pulsar.getConfiguration().isEnableNonPersistentTopics());

            // Register the brokers in zk list
            createZPathIfNotExists(zkClient, LoadManager.LOADBALANCE_BROKERS_ROOT);

            String lookupServiceAddress = pulsar.getAdvertisedAddress() + ":"
                    + (conf.getWebServicePort().isPresent() ? conf.getWebServicePort().get()
                            : conf.getWebServicePortTls().get());
            brokerZnodePath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + lookupServiceAddress;
            final String timeAverageZPath = TIME_AVERAGE_BROKER_ZPATH + "/" + lookupServiceAddress;
            updateLocalBrokerData();
            try {
                if (!org.apache.pulsar.zookeeper.ZkUtils.checkNodeAndWaitExpired(
                    zkClient, brokerZnodePath,
                    pulsar.getConfig().getZooKeeperSessionTimeoutMillis())) {
                    ZkUtils.createFullPathOptimistic(zkClient, brokerZnodePath, localData.getJsonBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } else {
                    // Node may already be created by another load manager: in this case update the data.
                    zkClient.setData(brokerZnodePath, localData.getJsonBytes(), -1);
                }
            } catch (KeeperException.NodeExistsException e) {
                log.error("Broker znode - [{}] is own by different zookeeper-session", brokerZnodePath);
                throw new PulsarServerException(
                    "Broker znode - [" + brokerZnodePath + "] is owned by different zk-session");
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                // Catching exception here to print the right error message
                log.error("Interrupted at creating znode - [{}] for load balance on zookeeper ", brokerZnodePath, ie);
                throw ie;
            } catch (Exception e) {
                // Catching exception here to print the right error message
                log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
                throw e;
            }
            createZPathIfNotExists(zkClient, timeAverageZPath);
            zkClient.setData(timeAverageZPath, (new TimeAverageBrokerData()).getJsonBytes(), -1);
            updateAll();
            lastBundleDataUpdate = System.currentTimeMillis();
        } catch (Exception e) {
            log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
            throw new PulsarServerException(e);
        }
    }

    /**
     * As any broker, stop the load manager.
     *
     * @throws PulsarServerException
     *             If an unexpected error occurred when attempting to stop the load manager.
     */
    @Override
    public void stop() throws PulsarServerException {
        if (availableActiveBrokers != null) {
            availableActiveBrokers.close();
        }

        if (brokerDataCache != null) {
            brokerDataCache.close();
            brokerDataCache.clear();
        }
        scheduler.shutdown();
    }

    /**
     * As any broker, retrieve the namespace bundle stats and system resource usage to update data local to this broker.
     * @return
     */
    @Override
    public LocalBrokerData updateLocalBrokerData() {
        try {
            final SystemResourceUsage systemResourceUsage = LoadManagerShared.getSystemResourceUsage(brokerHostUsage);
            localData.update(systemResourceUsage, getBundleStats());
            updateLoadBalancingMetrics(systemResourceUsage);
        } catch (Exception e) {
            log.warn("Error when attempting to update local broker data", e);
        }
        return localData;
    }

    /**
     * As any broker, update System Resource Usage Percentage.
     *
     * @param systemResourceUsage
     */
    private void updateLoadBalancingMetrics(final SystemResourceUsage systemResourceUsage) {
        List<Metrics> metrics = Lists.newArrayList();
        Map<String, String> dimensions = new HashMap<>();

        dimensions.put("broker", conf.getAdvertisedAddress());
        dimensions.put("metric", "loadBalancing");

        Metrics m = Metrics.create(dimensions);
        m.put("brk_lb_cpu_usage", systemResourceUsage.getCpu().percentUsage());
        m.put("brk_lb_memory_usage", systemResourceUsage.getMemory().percentUsage());
        m.put("brk_lb_directMemory_usage", systemResourceUsage.getDirectMemory().percentUsage());
        m.put("brk_lb_bandwidth_in_usage", systemResourceUsage.getBandwidthIn().percentUsage());
        m.put("brk_lb_bandwidth_out_usage", systemResourceUsage.getBandwidthOut().percentUsage());
        metrics.add(m);
        this.loadBalancingMetrics.set(metrics);
    }

    /**
     * As any broker, write the local broker data to ZooKeeper.
     */
    @Override
    public void writeBrokerDataOnZooKeeper() {
        try {
            updateLocalBrokerData();
            if (needBrokerDataUpdate()) {
                localData.setLastUpdate(System.currentTimeMillis());

                try {
                    zkClient.setData(brokerZnodePath, localData.getJsonBytes(), -1);
                } catch (KeeperException.NoNodeException e) {
                    ZkUtils.createFullPathOptimistic(zkClient, brokerZnodePath, localData.getJsonBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                }

                // Clear deltas.
                localData.getLastBundleGains().clear();
                localData.getLastBundleLosses().clear();

                // Update previous data.
                lastData.update(localData);
            }
        } catch (Exception e) {
            log.warn("Error writing broker data on ZooKeeper", e);
        }
    }

    @Override
    public Deserializer<LocalBrokerData> getLoadReportDeserializer() {
        return loadReportDeserializer;
    }

    /**
     * As the leader broker, write bundle data aggregated from all brokers to ZooKeeper.
     */
    @Override
    public void writeBundleDataOnZooKeeper() {
        updateBundleData();
        // Write the bundle data to ZooKeeper.
        for (Map.Entry<String, BundleData> entry : loadData.getBundleData().entrySet()) {
            final String bundle = entry.getKey();
            final BundleData data = entry.getValue();
            try {
                final String zooKeeperPath = getBundleDataZooKeeperPath(bundle);
                createZPathIfNotExists(zkClient, zooKeeperPath);
                zkClient.setData(zooKeeperPath, data.getJsonBytes(), -1);
            } catch (Exception e) {
                log.warn("Error when writing data for bundle {} to ZooKeeper: {}", bundle, e);
            }
        }
        // Write the time average broker data to ZooKeeper.
        for (Map.Entry<String, BrokerData> entry : loadData.getBrokerData().entrySet()) {
            final String broker = entry.getKey();
            final TimeAverageBrokerData data = entry.getValue().getTimeAverageData();
            try {
                final String zooKeeperPath = TIME_AVERAGE_BROKER_ZPATH + "/" + broker;
                createZPathIfNotExists(zkClient, zooKeeperPath);
                zkClient.setData(zooKeeperPath, data.getJsonBytes(), -1);
                if (log.isDebugEnabled()) {
                    log.debug("Writing zookeeper report {}", data);
                }
            } catch (Exception e) {
                log.warn("Error when writing time average broker data for {} to ZooKeeper: {}", broker, e);
            }
        }
    }

    private void deleteBundleDataFromZookeeper(String bundle) {
        final String zooKeeperPath = getBundleDataZooKeeperPath(bundle);
        try {
            if (zkClient.exists(zooKeeperPath, null) != null) {
                zkClient.delete(zooKeeperPath, -1);
            }
        } catch (Exception e) {
            log.warn("Failed to delete bundle-data {} from zookeeper", bundle, e);
        }
    }

    private void refreshBrokerToFailureDomainMap() {
        if (!pulsar.getConfiguration().isFailureDomainsEnabled()) {
            return;
        }
        final String clusterDomainRootPath = pulsar.getConfigurationCache().CLUSTER_FAILURE_DOMAIN_ROOT;
        try {
            synchronized (brokerToFailureDomainMap) {
                Map<String, String> tempBrokerToFailureDomainMap = Maps.newHashMap();
                for (String domainName : pulsar.getConfigurationCache().failureDomainListCache().get()) {
                    try {
                        Optional<FailureDomain> domain = pulsar.getConfigurationCache().failureDomainCache()
                                .get(clusterDomainRootPath + "/" + domainName);
                        if (domain.isPresent()) {
                            for (String broker : domain.get().brokers) {
                                tempBrokerToFailureDomainMap.put(broker, domainName);
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Failed to get domain {}", domainName, e);
                    }
                }
                this.brokerToFailureDomainMap = tempBrokerToFailureDomainMap;
            }
            log.info("Cluster domain refreshed {}", brokerToFailureDomainMap);
        } catch (Exception e) {
            log.warn("Failed to get domain-list for cluster {}", e.getMessage());
        }
    }

    @Override
    public LocalBrokerData getBrokerLocalData(String broker) {
        String key = String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, broker);
        try {
            return brokerDataCache.get(key).orElse(null);
        } catch (Exception e) {
            log.warn("Failed to get local-broker data for {}",broker, e);
            return null;
        }
    }

    @Override
    public List<Metrics> getLoadBalancingMetrics() {
        List<Metrics> metricsCollection = new ArrayList<>();

        if (this.loadBalancingMetrics.get() != null) {
            metricsCollection.addAll(this.loadBalancingMetrics.get());
        }

        if (this.bundleUnloadMetrics.get() != null) {
            metricsCollection.addAll(this.bundleUnloadMetrics.get());
        }

        if (this.bundleSplitMetrics.get() != null) {
            metricsCollection.addAll(this.bundleSplitMetrics.get());
        }

        return metricsCollection;
    }
}
