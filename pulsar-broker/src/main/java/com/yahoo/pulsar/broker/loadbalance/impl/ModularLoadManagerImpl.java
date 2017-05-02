/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.loadbalance.impl;

import static com.yahoo.pulsar.broker.admin.AdminResource.jsonMapper;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.LocalBrokerData;
import com.yahoo.pulsar.broker.PulsarServerException;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.TimeAverageBrokerData;
import com.yahoo.pulsar.broker.TimeAverageMessageData;
import com.yahoo.pulsar.broker.loadbalance.BrokerFilter;
import com.yahoo.pulsar.broker.loadbalance.BrokerHostUsage;
import com.yahoo.pulsar.broker.loadbalance.LoadData;
import com.yahoo.pulsar.broker.loadbalance.LoadManager;
import com.yahoo.pulsar.broker.loadbalance.LoadSheddingStrategy;
import com.yahoo.pulsar.broker.loadbalance.ModularLoadManager;
import com.yahoo.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.common.naming.ServiceUnitId;
import com.yahoo.pulsar.common.policies.data.ResourceQuota;
import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;
import com.yahoo.pulsar.common.policies.data.loadbalancer.SystemResourceUsage;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache.Deserializer;
import com.yahoo.pulsar.zookeeper.ZooKeeperCacheListener;
import com.yahoo.pulsar.zookeeper.ZooKeeperChildrenCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

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

    // Cache of PulsarAdmins for each broker.
    private LoadingCache<String, PulsarAdmin> adminCache;

    // ZooKeeper Cache of the currently available active brokers.
    // availableActiveBrokers.get() will return a set of the broker names without an http prefix.
    private ZooKeeperChildrenCache availableActiveBrokers;

    // Set of broker candidates to reuse so that object creation is avoided.
    private final Set<String> brokerCandidateCache;

    // ZooKeeper cache of the local broker data, stored in LoadManager.LOADBALANCE_BROKER_ROOT.
    private ZooKeeperDataCache<LocalBrokerData> brokerDataCache;

    // Broker host usage object used to calculate system resource usage.
    private BrokerHostUsage brokerHostUsage;

    // Path to the ZNode containing the LocalBrokerData json for this broker.
    private String brokerZnodePath;

    // Service configuration belonging to the pulsar service.
    private ServiceConfiguration conf;

    // The default bundle stats which are used to initialize historic data.
    // This data is overriden after the bundle receives its first sample.
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

    // Cache for primary brokers according to policies.
    private final Set<String> primariesCache;

    // Executor service used to regularly update broker data.
    private final ScheduledExecutorService scheduler;

    // Cache for shard brokers according to policies.
    private final Set<String> sharedCache;

    // ZooKeeper belonging to the pulsar service.
    private ZooKeeper zkClient;

    private static final Deserializer<LocalBrokerData> loadReportDeserializer = (key, content) -> jsonMapper()
            .readValue(content, LocalBrokerData.class);

    /**
     * Initializes fields which do not depend on PulsarService. initialize(PulsarService) should subsequently be called.
     */
    public ModularLoadManagerImpl() {
        brokerCandidateCache = new HashSet<>();
        defaultStats = new NamespaceBundleStats();
        filterPipeline = new ArrayList<>();
        loadData = new LoadData();
        loadSheddingPipeline = new ArrayList<>();
        preallocatedBundleToBroker = new ConcurrentHashMap<>();
        primariesCache = new HashSet<>();
        scheduler = Executors.newScheduledThreadPool(1);
        sharedCache = new HashSet<>();
    }

    /**
     * Initialize this load manager using the given PulsarService. Should be called only once, after invoking the
     * default constructor.
     * 
     * @param pulsar
     *            The service to initialize with.
     */
    public void initialize(final PulsarService pulsar) {
        adminCache = CacheBuilder.newBuilder().removalListener(new RemovalListener<String, PulsarAdmin>() {
            public void onRemoval(RemovalNotification<String, PulsarAdmin> removal) {
                removal.getValue().close();
            }
        }).expireAfterAccess(1, TimeUnit.DAYS).build(new CacheLoader<String, PulsarAdmin>() {
            @Override
            public PulsarAdmin load(String key) throws Exception {
                // key - broker name already is valid URL, has prefix "http://"
                return new PulsarAdmin(new URL(key), pulsar.getConfiguration().getBrokerClientAuthenticationPlugin(),
                        pulsar.getConfiguration().getBrokerClientAuthenticationParameters());
            }
        });

        availableActiveBrokers = new ZooKeeperChildrenCache(pulsar.getLocalZkCache(),
                LoadManager.LOADBALANCE_BROKERS_ROOT);
        availableActiveBrokers.registerListener(new ZooKeeperCacheListener<Set<String>>() {
            @Override
            public void onUpdate(String path, Set<String> data, Stat stat) {
                if (log.isDebugEnabled()) {
                    log.debug("Update Received for path {}", path);
                }
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

        conf = pulsar.getConfiguration();

        // Initialize the default stats to assume for unseen bundles (hard-coded for now).
        defaultStats.msgThroughputIn = DEFAULT_MESSAGE_THROUGHPUT;
        defaultStats.msgThroughputOut = DEFAULT_MESSAGE_THROUGHPUT;
        defaultStats.msgRateIn = DEFAULT_MESSAGE_RATE;
        defaultStats.msgRateOut = DEFAULT_MESSAGE_RATE;

        lastData = new LocalBrokerData(pulsar.getWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls());
        localData = new LocalBrokerData(pulsar.getWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls());
        placementStrategy = ModularLoadManagerStrategy.create(conf);
        policies = new SimpleResourceAllocationPolicies(pulsar);
        this.pulsar = pulsar;
        zkClient = pulsar.getZkClient();
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
        if (System.currentTimeMillis() - localData.getLastUpdate() > updateMaxIntervalMillis) {
            log.info("Writing local data to ZooKeeper because time since last update exceeded threshold of {} minutes",
                    conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
            // Always update after surpassing the maximum interval.
            return true;
        }
        final double maxChange = Math
                .max(percentChange(lastData.getMaxResourceUsage(), localData.getMaxResourceUsage()),
                        Math.max(percentChange(lastData.getMsgRateIn() + lastData.getMsgRateOut(),
                                localData.getMsgRateIn() + localData.getMsgRateOut()),
                                Math.max(
                                        percentChange(lastData.getMsgThroughputIn() + lastData.getMsgThroughputOut(),
                                                localData.getMsgThroughputIn() + localData.getMsgThroughputOut()),
                                        percentChange(lastData.getNumBundles(), localData.getNumBundles()))));
        if (maxChange > conf.getLoadBalancerReportUpdateThresholdPercentage()) {
            log.info("Writing local data to ZooKeeper because maximum change {}% exceeded threshold {}%", maxChange,
                    conf.getLoadBalancerReportUpdateThresholdPercentage());
            return true;
        }
        return false;
    }

    // Update both the broker data and the bundle data.
    private void updateAll() {
        updateAllBrokerData();
        updateBundleData();
    }

    // As the leader broker, update the broker data map in loadData by querying ZooKeeper for the broker data put there
    // by each broker via updateLocalBrokerData.
    private void updateAllBrokerData() {
        try {
            Set<String> activeBrokers = availableActiveBrokers.get();
            final Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
            for (final String broker : activeBrokers) {
                try {
                    String key = String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, broker);
                    final LocalBrokerData localData = brokerDataCache.get(key)
                            .orElseThrow(KeeperException.NoNodeException::new);

                    if (brokerDataMap.containsKey(broker)) {
                        // Replace previous local broker data.
                        brokerDataMap.get(broker).setLocalData(localData);
                    } else {
                        // Initialize BrokerData object for previously unseen brokers.
                        brokerDataMap.put(broker, new BrokerData(localData));
                    }
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
        } catch (Exception e) {
            log.warn("Error reading active brokers list from zookeeper while updating broker data [{}]",
                    e.getMessage());
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
                    // If we recognize the bundle, add these stats as a new
                    // sample.
                    bundleData.get(bundle).update(stats);
                } else {
                    // Otherwise, attempt to find the bundle data on ZooKeeper.
                    // If it cannot be found, use the latest stats as the first
                    // sample.
                    BundleData currentBundleData = getBundleDataOrDefault(bundle);
                    currentBundleData.update(stats);
                    bundleData.put(bundle, currentBundleData);
                }
            }

            // Remove all loaded bundles from the preallocated maps.
            final Map<String, BundleData> preallocatedBundleData = brokerData.getPreallocatedBundleData();
            if (preallocatedBundleData.containsKey(broker)) {
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

            // Using the newest data, update the aggregated time-average data
            // for the current broker.
            brokerData.getTimeAverageData().reset(statsMap.keySet(), bundleData, defaultStats);
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
        for (LoadSheddingStrategy strategy : loadSheddingPipeline) {
            final Map<String, String> bundlesToUnload = strategy.findBundlesForUnloading(loadData, conf);
            if (bundlesToUnload != null && !bundlesToUnload.isEmpty()) {
                try {
                    for (Map.Entry<String, String> entry : bundlesToUnload.entrySet()) {
                        final String bundle = entry.getKey();
                        final String broker = entry.getValue();
                        adminCache.get(broker).namespaces().unloadNamespaceBundle(
                                LoadManagerShared.getNamespaceNameFromBundleName(bundle),
                                LoadManagerShared.getBundleRangeFromBundleName(bundle));
                    }
                } catch (Exception e) {
                    log.warn("Error when trying to perform load shedding: {}", e);
                }
                return;
            }
        }
    }

    /**
     * As the leader broker, attempt to automatically detect and split hot namespace bundles.
     */
    @Override
    public void doNamespaceBundleSplit() {
        // TODO?
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
    public String selectBrokerForAssignment(final ServiceUnitId serviceUnit) {
        // Use brokerCandidateCache as a lock to reduce synchronization.
        synchronized (brokerCandidateCache) {
            final String bundle = serviceUnit.toString();
            if (preallocatedBundleToBroker.containsKey(bundle)) {
                // If the given bundle is already in preallocated, return the selected broker.
                return preallocatedBundleToBroker.get(bundle);
            }
            final BundleData data = loadData.getBundleData().computeIfAbsent(bundle,
                    key -> getBundleDataOrDefault(bundle));
            brokerCandidateCache.clear();
            Set<String> activeBrokers;
            try {
                activeBrokers = availableActiveBrokers.get();
            } catch (Exception e) {
                // Try-catch block inserted because ZooKeeperChildrenCache.get throws checked exception, though we
                // should not really see this happen unless something goes very wrong.
                log.warn("Unexpected error when trying to get active brokers", e);

                // Fall back to using loadData key set.
                activeBrokers = loadData.getBrokerData().keySet();
            }
            LoadManagerShared.applyPolicies(serviceUnit, policies, brokerCandidateCache, activeBrokers);
            log.info("{} brokers being considered for assignment of {}", brokerCandidateCache.size(), bundle);

            // Use the filter pipeline to finalize broker candidates.
            for (BrokerFilter filter : filterPipeline) {
                filter.filter(brokerCandidateCache, data, loadData, conf);
            }
            final String broker = placementStrategy.selectBroker(brokerCandidateCache, data, loadData, conf);

            // Add new bundle to preallocated.
            loadData.getBrokerData().get(broker).getPreallocatedBundleData().put(bundle, data);
            preallocatedBundleToBroker.put(bundle, broker);

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
            // Register the brokers in zk list
            createZPathIfNotExists(zkClient, LoadManager.LOADBALANCE_BROKERS_ROOT);

            String lookupServiceAddress = pulsar.getAdvertisedAddress() + ":" + conf.getWebServicePort();
            brokerZnodePath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + lookupServiceAddress;
            final String timeAverageZPath = TIME_AVERAGE_BROKER_ZPATH + "/" + lookupServiceAddress;
            updateLocalBrokerData();
            try {
                ZkUtils.createFullPathOptimistic(pulsar.getZkClient(), brokerZnodePath, localData.getJsonBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.NodeExistsException e) {
                // Node may already be created by another load manager: in this case update the data.
                zkClient.setData(brokerZnodePath, localData.getJsonBytes(), -1);
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
        availableActiveBrokers.close();
        brokerDataCache.close();
        brokerDataCache.clear();
        scheduler.shutdown();
    }

    /**
     * As any broker, retrieve the namespace bundle stats and system resource usage to update data local to this broker.
     */
    @Override
    public void updateLocalBrokerData() {
        try {
            final SystemResourceUsage systemResourceUsage = LoadManagerShared.getSystemResourceUsage(brokerHostUsage);
            localData.update(systemResourceUsage, getBundleStats());
        } catch (Exception e) {
            log.warn("Error when attempting to update local broker data: {}", e);
        }
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
                zkClient.setData(brokerZnodePath, localData.getJsonBytes(), -1);

                // Clear deltas.
                localData.getLastBundleGains().clear();
                localData.getLastBundleLosses().clear();

                // Update previous data.
                lastData.update(localData);
            }
        } catch (Exception e) {
            log.warn("Error writing broker data on ZooKeeper: {}", e);
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
            } catch (Exception e) {
                log.warn("Error when writing time average broker data for {} to ZooKeeper: {}", broker, e);
            }
        }
    }
}
