package com.yahoo.pulsar.broker;

import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;
import com.yahoo.pulsar.common.policies.data.loadbalancer.ResourceUsage;
import com.yahoo.pulsar.common.policies.data.loadbalancer.ServiceLookupData;
import com.yahoo.pulsar.common.policies.data.loadbalancer.SystemResourceUsage;

import java.util.*;

/**
 * Contains all the data that is maintained locally on each broker.
 */
public class LocalBrokerData extends JSONWritable implements ServiceLookupData {

    // URLs to satisfy contract of ServiceLookupData (used by NamespaceService).
    private final String webServiceUrl;
    private final String webServiceUrlTls;
    private final String pulsarServiceUrl;
    private final String pulsarServiceUrlTls;

    // Most recently available system resource usage.
    private ResourceUsage cpu;
    private ResourceUsage memory;
    private ResourceUsage directMemory;

    // Message data from the most recent namespace bundle stats.
    private double msgThroughputIn;
    private double msgThroughputOut;
    private double msgRateIn;
    private double msgRateOut;

    // Timestamp of last update.
    private long lastUpdate;

    // The stats given in the most recent invocation of update.
    private Map<String, NamespaceBundleStats> lastStats;

    private int numTopics;
    private int numBundles;
    private int numConsumers;
    private int numProducers;

    // All bundles belonging to this broker.
    private Set<String> bundles;

    // The bundles gained since the last invocation of update.
    private Set<String> lastBundleGains;

    // The bundles lost since the last invocation of update.
    private Set<String> lastBundleLosses;

    // For JSON only.
    public LocalBrokerData(){
        this(null, null, null, null);
    }

    /**
     * Broker data constructor which takes in four URLs to satisfy the contract of ServiceLookupData.
     */
    public LocalBrokerData(final String webServiceUrl, final String webServiceUrlTls, final String pulsarServiceUrl,
                           final String pulsarServiceUrlTls) {
        this.webServiceUrl = webServiceUrl;
        this.webServiceUrlTls = webServiceUrlTls;
        this.pulsarServiceUrl = pulsarServiceUrl;
        this.pulsarServiceUrlTls = pulsarServiceUrlTls;
        lastStats = new HashMap<>();
        lastUpdate = System.currentTimeMillis();
        cpu = new ResourceUsage();
        memory = new ResourceUsage();
        directMemory = new ResourceUsage();
        bundles = new HashSet<>();
        lastBundleGains = new HashSet<>();
        lastBundleLosses = new HashSet<>();
    }

    /**
     * Using the system resource usage and bundle stats acquired from the Pulsar client, update this LocalBrokerData.
     * @param systemResourceUsage System resource usage (cpu, memory, and direct memory).
     * @param bundleStats The bundle stats retrieved from the Pulsar client.
     */
    public void update(final SystemResourceUsage systemResourceUsage,
                       final Map<String, NamespaceBundleStats> bundleStats) {
        updateSystemResourceUsage(systemResourceUsage);
        updateBundleData(bundleStats);
        lastStats = bundleStats;
        lastUpdate = System.currentTimeMillis();
    }

    // Set the cpu, memory, and direct memory to that of the new system resource usage data.
    private void updateSystemResourceUsage(final SystemResourceUsage systemResourceUsage) {
        this.cpu = systemResourceUsage.cpu;
        this.memory = systemResourceUsage.memory;
        this.directMemory = systemResourceUsage.directMemory;
    }

    // Aggregate all message, throughput, topic count, bundle count, consumer count, and producer count across the
    // given data. Also keep track of bundle gains and losses.
    private void updateBundleData(final Map<String, NamespaceBundleStats> bundleStats) {
        msgRateIn = 0;
        msgRateOut = 0;
        msgThroughputIn = 0;
        msgThroughputOut = 0;
        int totalNumTopics = 0;
        int totalNumBundles = 0;
        int totalNumConsumers = 0;
        int totalNumProducers = 0;
        lastBundleGains.clear();
        lastBundleLosses.clear();
        final Iterator<String> oldBundleIterator = bundles.iterator();
        while (oldBundleIterator.hasNext()) {
            final String bundle = oldBundleIterator.next();
            if (!bundleStats.containsKey(bundle)) {
                // If this bundle is in the old bundle set but not the new one, we lost it.
                lastBundleLosses.add(bundle);
                oldBundleIterator.remove();
            }
        }
        for (Map.Entry<String, NamespaceBundleStats> entry: bundleStats.entrySet()) {
            final String bundle = entry.getKey();
            final NamespaceBundleStats stats = entry.getValue();
            if (!bundles.contains(bundle)) {
                // If this bundle is in the new bundle set but not the old one, we gained it.
                lastBundleGains.add(bundle);
                bundles.add(bundle);
            }
            msgThroughputIn += stats.msgThroughputIn;
            msgThroughputOut += stats.msgThroughputOut;
            msgRateIn += stats.msgRateIn;
            msgRateOut += stats.msgRateOut;
            totalNumTopics += stats.topics;
            ++totalNumBundles;
            totalNumConsumers += stats.consumerCount;
            totalNumProducers += stats.producerCount;
        }
        numTopics = totalNumTopics;
        numBundles = totalNumBundles;
        numConsumers = totalNumConsumers;
        numProducers = totalNumProducers;

    }

    public ResourceUsage getCpu() {
        return cpu;
    }

    public void setCpu(ResourceUsage cpu) {
        this.cpu = cpu;
    }

    public ResourceUsage getMemory() {
        return memory;
    }

    public void setMemory(ResourceUsage memory) {
        this.memory = memory;
    }

    public ResourceUsage getDirectMemory() {
        return directMemory;
    }

    public void setDirectMemory(ResourceUsage directMemory) {
        this.directMemory = directMemory;
    }

    public Set<String> getLastBundleGains() {
        return lastBundleGains;
    }

    public void setLastBundleGains(Set<String> lastBundleGains) {
        this.lastBundleGains = lastBundleGains;
    }

    public Set<String> getLastBundleLosses() {
        return lastBundleLosses;
    }

    public void setLastBundleLosses(Set<String> lastBundleLosses) {
        this.lastBundleLosses = lastBundleLosses;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public Set<String> getBundles() {
        return bundles;
    }

    public void setBundles(Set<String> bundles) {
        this.bundles = bundles;
    }

    public Map<String, NamespaceBundleStats> getLastStats() {
        return lastStats;
    }

    public void setLastStats(Map<String, NamespaceBundleStats> lastStats) {
        this.lastStats = lastStats;
    }

    public int getNumTopics() {
        return numTopics;
    }

    public void setNumTopics(int numTopics) {
        this.numTopics = numTopics;
    }

    public int getNumBundles() {
        return numBundles;
    }

    public void setNumBundles(int numBundles) {
        this.numBundles = numBundles;
    }

    public int getNumConsumers() {
        return numConsumers;
    }

    public void setNumConsumers(int numConsumers) {
        this.numConsumers = numConsumers;
    }

    public int getNumProducers() {
        return numProducers;
    }

    public void setNumProducers(int numProducers) {
        this.numProducers = numProducers;
    }

    public double getMsgThroughputIn() {
        return msgThroughputIn;
    }

    public void setMsgThroughputIn(double msgThroughputIn) {
        this.msgThroughputIn = msgThroughputIn;
    }

    public double getMsgThroughputOut() {
        return msgThroughputOut;
    }

    public void setMsgThroughputOut(double msgThroughputOut) {
        this.msgThroughputOut = msgThroughputOut;
    }

    public double getMsgRateIn() {
        return msgRateIn;
    }

    public void setMsgRateIn(double msgRateIn) {
        this.msgRateIn = msgRateIn;
    }

    public double getMsgRateOut() {
        return msgRateOut;
    }

    public void setMsgRateOut(double msgRateOut) {
        this.msgRateOut = msgRateOut;
    }

    @Override
    public String getWebServiceUrl() {
        return webServiceUrl;
    }

    @Override
    public String getWebServiceUrlTls() {
        return webServiceUrlTls;
    }

    @Override
    public String getPulsarServiceUrl() {
        return pulsarServiceUrl;
    }

    @Override
    public String getPulsarServiceUrlTls() {
        return pulsarServiceUrlTls;
    }
}
