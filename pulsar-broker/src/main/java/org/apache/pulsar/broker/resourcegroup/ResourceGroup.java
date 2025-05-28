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
package org.apache.pulsar.broker.resourcegroup;

import static java.util.Objects.requireNonNull;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupService.ResourceGroupOpStatus;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.resource.usage.NetworkUsage;
import org.apache.pulsar.broker.service.resource.usage.ReplicatorUsage;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsage;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The resource group (RG) data structure. One of these is created internally for every resource group configured.
 *
 * The RG name is considered a key, and must be unique. Being the key, the name can't be modified after the
 * RG is created.
 *
 * Tenants and Namespaces directly reference RGs. The usage is "vertical": i.e., a tenant or namespace references the
 * same RG across all of the monitoring classes it uses (Publish/Dispatch/...), instead of referencing one RG for
 * publish, another one for dispatch, etc.
 */
public class ResourceGroup implements AutoCloseable{

    private volatile boolean isClosed = false;

    @Override
    public void close() throws Exception {
        synchronized (this) {
            isClosed = true;
            if (resourceGroupDispatchLimiter != null) {
                resourceGroupDispatchLimiter.close();
            }
            if (resourceGroupPublishLimiter != null) {
                resourceGroupPublishLimiter.close();
            }
            if (resourceGroupReplicationDispatchLimiter != null) {
                resourceGroupReplicationDispatchLimiter.close();
            }
            replicatorDispatchRateLimiterMap.forEach((k, v) -> {
                v.close();
                notifyReplicatorDispatchRateLimiterConsumer(k, null);
            });
        }
    }

    /**
     * Convenience class for bytes and messages counts, which are used together in a lot of the following code.
     */
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    public static class BytesAndMessagesCount {
        public long bytes;
        public long messages;
    }

    /**
     * Usage "sense" for resource groups: publish and dispatch for now; may be more later (e.g., storage-monitoring)
     * ToDo: "class" is vague; is there a better term to call this?
     */
    public enum ResourceGroupMonitoringClass {
        Publish,
        Dispatch,
        ReplicationDispatch,
        // Storage;  // Punt this for now, until we have a clearer idea of the usage, statistics, etc.
    }

    /**
     * The entities that might reference RGs are tenants, namespaces (and maybe topics, later).
     */
    public enum ResourceGroupRefTypes {
        Tenants,
        Namespaces,
        Topics
    }

    // Default ctor: it is not expected that anything outside of this package will need to directly
    // construct a ResourceGroup (i.e., without going through ResourceGroupService).
    protected ResourceGroup(ResourceGroupService rgs, String name,
                            org.apache.pulsar.common.policies.data.ResourceGroup rgConfig) {
        this(rgs, name, rgConfig, null, null);
    }

    // ctor for overriding the transport-manager fill/set buffer.
    // It is not expected that anything outside of this package will need to directly
    // construct a ResourceGroup (i.e., without going through ResourceGroupService).
    protected ResourceGroup(ResourceGroupService rgs, String rgName,
                            org.apache.pulsar.common.policies.data.ResourceGroup rgConfig,
                            ResourceUsagePublisher rgPublisher, ResourceUsageConsumer rgConsumer) {
        this.rgs = rgs;
        this.resourceGroupName = rgName;
        this.rgConfig = rgConfig;
        if (rgPublisher == null && rgConsumer == null) {
            this.setDefaultResourceUsageTransportHandlers();
        } else {
            this.ruPublisher = rgPublisher;
            this.ruConsumer = rgConsumer;
        }
        this.resourceGroupPublishLimiter = new ResourceGroupPublishLimiter(rgConfig, rgs.getPulsar().getExecutor());
        log.info("attaching publish rate limiter {} to {} get {}", this.resourceGroupPublishLimiter.toString(), rgName,
                this.getResourceGroupPublishLimiter());
        this.resourceGroupReplicationDispatchLimiter = ResourceGroupRateLimiterManager
                .newReplicationDispatchRateLimiter(rgConfig, rgs.getPulsar().getExecutor());
        log.info("attaching replication dispatch rate limiter {} to {}", this.resourceGroupReplicationDispatchLimiter,
                rgName);
        this.resourceGroupDispatchLimiter = ResourceGroupRateLimiterManager
                .newDispatchRateLimiter(rgConfig, rgs.getPulsar().getExecutor());
        log.info("attaching topic dispatch rate limiter {} to {}", this.resourceGroupDispatchLimiter, rgName);
    }

    protected void updateResourceGroup(org.apache.pulsar.common.policies.data.ResourceGroup rgConfig) {
        synchronized (this) {
            if (isClosed) {
                return;
            }
            updateMonitoringClassFieldsMap(rgConfig);
            BytesAndMessagesCount pubBmc = new BytesAndMessagesCount();
            pubBmc.messages = rgConfig.getPublishRateInMsgs() == null ? -1 : rgConfig.getPublishRateInMsgs();
            pubBmc.bytes = rgConfig.getPublishRateInBytes() == null ? -1 : rgConfig.getPublishRateInBytes();
            this.resourceGroupPublishLimiter.update(pubBmc);
            updateReplicationDispatchLimiters(rgConfig);
        }
    }

    private void updateReplicationDispatchLimiters(org.apache.pulsar.common.policies.data.ResourceGroup rgConfig) {
        ResourceGroupRateLimiterManager
                .updateReplicationDispatchRateLimiter(resourceGroupReplicationDispatchLimiter, rgConfig);
        replicatorDispatchRateLimiterMap.forEach((key, oldLimiter) ->
                replicatorDispatchRateLimiterMap.computeIfPresent(key,
                        (k, curr) -> updateReplicatorLimiter(k, curr, rgConfig))
        );
    }

    private ResourceGroupDispatchLimiter updateReplicatorLimiter(String key, ResourceGroupDispatchLimiter currLimiter,
                                                 org.apache.pulsar.common.policies.data.ResourceGroup rgConfig) {
        DispatchRate dispatchRate = rgConfig.getReplicatorDispatchRate().get(key);
        if (dispatchRate == null) {
            // Specific rate-limiter for this cluster is removed in the new config.
            // use the default rate-limiter, notify the rate-limiter consumers.
            if (currLimiter != resourceGroupReplicationDispatchLimiter) {
                notifyReplicatorDispatchRateLimiterConsumer(key, resourceGroupReplicationDispatchLimiter);
                return resourceGroupReplicationDispatchLimiter;
            }
        } else if (currLimiter == resourceGroupReplicationDispatchLimiter) {
            // Specific rate-limiter for this cluster is provided in the new config.
            // When rate-limiter is default, new a rate-limiter.
            ResourceGroupDispatchLimiter newLimiter = createNewReplicatorLimiter(key);
            notifyReplicatorDispatchRateLimiterConsumer(key, newLimiter);
            return newLimiter;
        } else {
            currLimiter.update(dispatchRate.getDispatchThrottlingRateInMsg(),
                    dispatchRate.getDispatchThrottlingRateInByte());
        }
        return currLimiter;
    }

    protected String getReplicatorDispatchRateLimiterKey(String remoteCluster) {
        return DispatchRateLimiter.getReplicatorDispatchRateKey(rgs.getPulsar().getConfiguration().getClusterName(),
                remoteCluster);
    }

    private ResourceGroupDispatchLimiter createNewReplicatorLimiter(String key) {
        return Optional.ofNullable(rgConfig.getReplicatorDispatchRate().get(key))
                .map(dispatchRate -> ResourceGroupRateLimiterManager.newReplicationDispatchRateLimiter(
                        rgs.getPulsar().getExecutor(),
                        dispatchRate.getDispatchThrottlingRateInMsg(),
                        dispatchRate.getDispatchThrottlingRateInByte()))
                .orElse(resourceGroupReplicationDispatchLimiter);
    }

    private void notifyReplicatorDispatchRateLimiterConsumer(
            String key, ResourceGroupDispatchLimiter limiter) {
        replicatorDispatchRateLimiterConsumerMap.computeIfPresent(key, (__, consumerSet) -> {
            consumerSet.forEach(c -> c.accept(limiter));
            return consumerSet;
        });
    }

    public void registerReplicatorDispatchRateLimiter(String remoteCluster,
                                                      Consumer<ResourceGroupDispatchLimiter> consumer) {
        String key = getReplicatorDispatchRateLimiterKey(remoteCluster);
        synchronized (this) {
            if (isClosed) {
                unregisterReplicatorDispatchRateLimiter(remoteCluster, consumer);
                // The resource group is closed, no need to register the rate limiter.
                return;
            }
            ResourceGroupDispatchLimiter limiter =
                    replicatorDispatchRateLimiterMap.computeIfAbsent(key, __ -> createNewReplicatorLimiter(key));
            consumer.accept(limiter);
            // Must use compute instead of computeIfAbsent to avoid the notifyReplicatorDispatchRateLimiterConsumer
            // concurrent access.
            replicatorDispatchRateLimiterConsumerMap.compute(key, (__, old) -> {
                if (old == null) {
                    old = ConcurrentHashMap.newKeySet();
                }
                old.add(consumer);
                return old;
            });
        }
    }

    public void unregisterReplicatorDispatchRateLimiter(String remoteCluster,
                                                        Consumer<ResourceGroupDispatchLimiter> consumer) {
        String key = getReplicatorDispatchRateLimiterKey(remoteCluster);
        synchronized (this) {
            consumer.accept(null);
            replicatorDispatchRateLimiterConsumerMap.computeIfPresent(key, (__, old) -> {
                old.remove(consumer);
                return old;
            });
        }
    }

    protected long getResourceGroupNumOfNSRefs() {
        return this.resourceGroupNamespaceRefs.size();
    }

    protected long getResourceGroupNumOfTopicRefs() {
        return this.resourceGroupTopicRefs.size();
    }

    protected long getResourceGroupNumOfTenantRefs() {
        return this.resourceGroupTenantRefs.size();
    }

    protected ResourceGroupOpStatus registerUsage(String name, ResourceGroupRefTypes refType, boolean ref,
                                                  ResourceUsageTransportManager transportManager) {
        Set<String> set;

        switch (refType) {
            default:
                return ResourceGroupOpStatus.NotSupported;
            case Tenants:
                set = this.resourceGroupTenantRefs;
                break;
            case Namespaces:
                set = this.resourceGroupNamespaceRefs;
                break;
            case Topics:
                set = this.resourceGroupTopicRefs;
                break;
        }

        if (ref) {
            // register operation
            if (set.contains(name)) {
                return ResourceGroupOpStatus.Exists;
            }
            set.add(name);

            // If this is the first ref, register with the transport manager.
            if (this.resourceGroupTenantRefs.size() + this.resourceGroupNamespaceRefs.size()
                    + this.resourceGroupTopicRefs.size() == 1) {
                if (log.isDebugEnabled()) {
                    log.debug("registerUsage for RG={}: registering with transport-mgr", this.resourceGroupName);
                }
                transportManager.registerResourceUsagePublisher(this.ruPublisher);
                transportManager.registerResourceUsageConsumer(this.ruConsumer);
            }
        } else {
            // unregister operation
            if (!set.contains(name)) {
                return ResourceGroupOpStatus.DoesNotExist;
            }
            set.remove(name);

            // If this was the last ref, unregister from the transport manager.
            if (this.resourceGroupTenantRefs.size() + this.resourceGroupNamespaceRefs.size()
                    + this.resourceGroupTopicRefs.size() == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("unRegisterUsage for RG={}: un-registering from transport-mgr", this.resourceGroupName);
                }
                transportManager.unregisterResourceUsageConsumer(this.ruConsumer);
                transportManager.unregisterResourceUsagePublisher(this.ruPublisher);
            }
        }

        return ResourceGroupOpStatus.OK;
    }

    // Transport manager mandated op.
    public String getID() {
        return this.resourceGroupName;
    }

    // Transport manager mandated op.
    public void rgFillResourceUsage(ResourceUsage resourceUsage) {
        resourceUsage.setOwner(this.getID());
        this.setUsageInMonitoredEntity(resourceUsage);
    }

    // Transport manager mandated op.
    public void rgResourceUsageListener(String broker, ResourceUsage resourceUsage) {
        this.getUsageFromMonitoredEntity(resourceUsage, broker);
    }

    private MonitoringKey getMonitoringKey(ResourceGroupMonitoringClass monClass, String remoteCluster) {
        return new MonitoringKey(monClass, remoteCluster);
    }

    protected Map<String, BytesAndMessagesCount> getConfLimits(ResourceGroupMonitoringClass monClass)
            throws PulsarAdminException {
        this.checkMonitoringClass(monClass);

        Map<String, BytesAndMessagesCount> retStats = new HashMap<>();
        monitoringClassFieldsMap.forEach(((key, monEntity) -> {
            if (key.getResourceGroupMonitoringClass() != monClass) {
                return;
            }
            BytesAndMessagesCount bytesAndMessagesCount =
                    retStats.computeIfAbsent(key.getRemoteCluster(), k -> new BytesAndMessagesCount());
            monEntity.localUsageStatsLock.lock();
            try {
                bytesAndMessagesCount.bytes += monEntity.configValuesPerPeriod.bytes;
                bytesAndMessagesCount.messages += monEntity.configValuesPerPeriod.messages;
            } finally {
                monEntity.localUsageStatsLock.unlock();
            }
        }));
        return retStats;
    }

    private PerMonitoringClassFields createPerMonitoringClassFields(ResourceGroupMonitoringClass monClass,
                                                                    String remoteCluster) {
        PerMonitoringClassFields value =
                PerMonitoringClassFields.create(getCacheDuration());
        switch (monClass) {
            case Publish:
                value.configValuesPerPeriod.bytes = rgConfig.getPublishRateInBytes() == null
                        ? -1 : rgConfig.getPublishRateInBytes();
                value.configValuesPerPeriod.messages = rgConfig.getPublishRateInMsgs() == null
                        ? -1 : rgConfig.getPublishRateInMsgs();
                break;
            case Dispatch:
                value.configValuesPerPeriod.bytes = rgConfig.getDispatchRateInBytes() == null
                        ? -1 : rgConfig.getDispatchRateInBytes();
                value.configValuesPerPeriod.messages = rgConfig.getDispatchRateInMsgs() == null
                        ? -1 : rgConfig.getDispatchRateInMsgs();
                break;
            case ReplicationDispatch:
                requireNonNull(remoteCluster, "remoteCluster cannot be null when monClass is ReplicationDispatch");
                DispatchRate dispatchRate =
                        rgConfig.getReplicatorDispatchRate().get(getReplicatorDispatchRateLimiterKey(remoteCluster));
                if (dispatchRate != null) {
                    value.configValuesPerPeriod.bytes = dispatchRate.getDispatchThrottlingRateInByte();
                    value.configValuesPerPeriod.messages =
                            dispatchRate.getDispatchThrottlingRateInMsg();
                } else {
                    value.configValuesPerPeriod.bytes = rgConfig.getReplicationDispatchRateInBytes() == null ? -1 :
                            rgConfig.getReplicationDispatchRateInBytes();
                    value.configValuesPerPeriod.messages = rgConfig.getReplicationDispatchRateInMsgs() == null ? -1 :
                            rgConfig.getReplicationDispatchRateInMsgs();
                }
                break;
        }
        return value;
    }

    protected PerMonitoringClassFields getPerMonitoringClassFields(ResourceGroupMonitoringClass monClass,
                                                                   String remoteCluster) {
        return monitoringClassFieldsMap.computeIfAbsent(getMonitoringKey(monClass, remoteCluster),
                (__) -> createPerMonitoringClassFields(monClass, remoteCluster));
    }

    protected void incrementLocalUsageStats(ResourceGroupMonitoringClass monClass, BytesAndMessagesCount stats,
                                            String remoteCluster)
            throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        synchronized (this) {
            PerMonitoringClassFields monEntity = getPerMonitoringClassFields(monClass, remoteCluster);
            monEntity.localUsageStatsLock.lock();
            try {
                monEntity.usedLocallySinceLastReport.bytes += stats.bytes;
                monEntity.usedLocallySinceLastReport.messages += stats.messages;
            } finally {
                monEntity.localUsageStatsLock.unlock();
            }
        }
    }

    protected Map<String, BytesAndMessagesCount> getLocalUsageStatsCumulative(ResourceGroupMonitoringClass monClass)
            throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        Map<String, BytesAndMessagesCount> retval = new HashMap<>();
        synchronized (this) {
            monitoringClassFieldsMap.forEach((key, monEntity) -> {
                BytesAndMessagesCount bytesAndMessagesCount =
                        retval.computeIfAbsent(key.getRemoteCluster(), k -> new BytesAndMessagesCount());
                monEntity.localUsageStatsLock.lock();
                try {
                    // If the total wasn't accumulated yet (i.e., a report wasn't sent yet), just return the
                    // partial accumulation in usedLocallySinceLastReport.
                    if (monEntity.totalUsedLocally.messages == 0) {
                        bytesAndMessagesCount.bytes = monEntity.usedLocallySinceLastReport.bytes;
                        bytesAndMessagesCount.messages = monEntity.usedLocallySinceLastReport.messages;
                    } else {
                        bytesAndMessagesCount.bytes = monEntity.totalUsedLocally.bytes;
                        bytesAndMessagesCount.messages = monEntity.totalUsedLocally.messages;
                    }
                } finally {
                    monEntity.localUsageStatsLock.unlock();
                }
            });
        }
        return retval;
    }

    protected Map<String, BytesAndMessagesCount> getLocalUsageStatsFromBrokerReports(
            ResourceGroupMonitoringClass monClass)
            throws PulsarAdminException {
        this.checkMonitoringClass(monClass);

        String myBrokerId = this.rgs.getPulsar().getBrokerServiceUrl();
        Map<String, BytesAndMessagesCount> retStats = new HashMap<>();
        monitoringClassFieldsMap.forEach(((key, monEntity) -> {
            if (key.getResourceGroupMonitoringClass() != monClass) {
                return;
            }
            BytesAndMessagesCount bytesAndMessagesCount =
                    retStats.computeIfAbsent(key.getRemoteCluster(), k -> new BytesAndMessagesCount());
            monEntity.usageFromOtherBrokersLock.lock();
            try {
                PerBrokerUsageStats pbus = monEntity.usageFromOtherBrokers.getIfPresent(myBrokerId);
                if (pbus != null) {
                    bytesAndMessagesCount.bytes += pbus.usedValues.bytes;
                    bytesAndMessagesCount.messages += pbus.usedValues.messages;
                }
            } finally {
                monEntity.usageFromOtherBrokersLock.unlock();
            }
        }));

        if (retStats.isEmpty()) {
            log.info("getLocalUsageStatsFromBrokerReports: no usage report found for broker={} and monClass={}",
                    myBrokerId, monClass);
        }
        return retStats;
    }

    protected Map<String, BytesAndMessagesCount> getGlobalUsageStats(ResourceGroupMonitoringClass monClass)
            throws PulsarAdminException {
        this.checkMonitoringClass(monClass);

        Map<String, BytesAndMessagesCount> retStats = new HashMap<>();
        monitoringClassFieldsMap.forEach(((key, monEntity) -> {
            if (key.getResourceGroupMonitoringClass() != monClass) {
                return;
            }
            BytesAndMessagesCount bytesAndMessagesCount =
                    retStats.computeIfAbsent(key.getRemoteCluster(), k -> new BytesAndMessagesCount());
            monEntity.usageFromOtherBrokersLock.lock();
            try {
                monEntity.usageFromOtherBrokers.asMap().forEach((broker, brokerUsage) -> {
                    bytesAndMessagesCount.bytes += brokerUsage.usedValues.bytes;
                    bytesAndMessagesCount.messages += brokerUsage.usedValues.messages;
                });
            } finally {
                monEntity.usageFromOtherBrokersLock.unlock();
            }
        }));
        return retStats;
    }

    protected void updateLocalQuota(ResourceGroupMonitoringClass monClass,
                                    BytesAndMessagesCount newQuota, String remoteCluster)
            throws PulsarAdminException {
        this.checkMonitoringClass(monClass);

        synchronized (this) {
            if (isClosed) {
                return;
            }
            monitoringClassFieldsMap.forEach((key, monEntity) -> {
                if (!key.getResourceGroupMonitoringClass().equals(monClass)) {
                    return;
                }
                monEntity.localUsageStatsLock.lock();
                try {
                    switch (monClass) {
                        case ReplicationDispatch:
                            String replicatorDispatchRateLimiterKey =
                                    getReplicatorDispatchRateLimiterKey(remoteCluster);
                            ResourceGroupDispatchLimiter limiter =
                                    this.replicatorDispatchRateLimiterMap.get(replicatorDispatchRateLimiterKey);
                            if (limiter != null) {
                                ResourceGroupRateLimiterManager.updateReplicationDispatchRateLimiter(limiter, newQuota);
                                notifyReplicatorDispatchRateLimiterConsumer(replicatorDispatchRateLimiterKey, limiter);
                            }
                            break;
                        case Publish:
                            this.resourceGroupPublishLimiter.update(newQuota);
                            break;
                        case Dispatch:
                            ResourceGroupRateLimiterManager.updateDispatchRateLimiter(resourceGroupDispatchLimiter,
                                    newQuota);
                            break;
                        default:
                            if (log.isDebugEnabled()) {
                                log.debug("Doing nothing for monClass={};", monClass);
                            }
                    }
                } finally {
                    monEntity.localUsageStatsLock.unlock();
                }
                if (log.isDebugEnabled()) {
                    log.debug("updateLocalQuota for RG={}: set local {} quota to bytes={}, messages={}",
                            this.resourceGroupName, monClass, newQuota.bytes, newQuota.messages);
                }
            });
        }
    }

    protected BytesAndMessagesCount getRgPublishRateLimiterValues() {
        BytesAndMessagesCount retVal;
        retVal = this.resourceGroupPublishLimiter.getResourceGroupPublishValues();
        return retVal;
    }

    // Visibility for unit testing
    protected static double getRgRemoteUsageByteCount (String rgName, String monClassName, String brokerName) {
        return rgRemoteUsageReportsBytes.labels(rgName, monClassName, brokerName).get();
    }

    // Visibility for unit testing
    protected static double getRgRemoteUsageMessageCount (String rgName, String monClassName, String brokerName) {
        return rgRemoteUsageReportsMessages.labels(rgName, monClassName, brokerName).get();
    }

    // Visibility for unit testing
    protected static double getRgUsageReportedCount (String rgName, String monClassName) {
        return rgLocalUsageReportCount.labels(rgName, monClassName).get();
    }

    // Visibility for unit testing
    protected static BytesAndMessagesCount accumulateBMCount(BytesAndMessagesCount ... bmCounts) {
        BytesAndMessagesCount retBMCount = new BytesAndMessagesCount();
        for (int ix = 0; ix < bmCounts.length; ix++) {
            retBMCount.messages += bmCounts[ix].messages;
            retBMCount.bytes += bmCounts[ix].bytes;
        }
        return retBMCount;
    }

    private void checkMonitoringClass(ResourceGroupMonitoringClass monClass) throws PulsarAdminException {
        switch (monClass) {
            case Publish:
                break;
            case Dispatch:
                break;
            case ReplicationDispatch:
                break;
            default:
                String errMesg = "Unexpected monitoring class: " + monClass;
                throw new PulsarAdminException(errMesg);
        }
    }

    // Fill usage about a particular monitoring class in the transport-manager callback
    // for reporting local stats to other brokers.
    // Returns true if something was filled.
    // Visibility for unit testing.
    protected void setUsageInMonitoredEntity(ResourceUsage resourceUsage) {
        monitoringClassFieldsMap.forEach((key, monEntity) -> {
            monEntity.localUsageStatsLock.lock();
            try {
                boolean sendReport = this.rgs.quotaCalculator.needToReportLocalUsage(
                        monEntity.usedLocallySinceLastReport.bytes,
                        monEntity.lastReportedValues.bytes,
                        monEntity.usedLocallySinceLastReport.messages,
                        monEntity.lastReportedValues.messages,
                        monEntity.lastResourceUsageFillTimeMSecsSinceEpoch);

                long bytesUsed = monEntity.usedLocallySinceLastReport.bytes;
                long messagesUsed = monEntity.usedLocallySinceLastReport.messages;
                monEntity.usedLocallySinceLastReport.bytes = monEntity.usedLocallySinceLastReport.messages = 0;
                int numSuppressions = 0;
                if (sendReport) {
                    switch (key.getResourceGroupMonitoringClass()) {
                        case Publish:
                            NetworkUsage publish = resourceUsage.setPublish();
                            publish.setMessagesPerPeriod(messagesUsed);
                            publish.setBytesPerPeriod(bytesUsed);
                            break;
                        case Dispatch:
                            NetworkUsage dispatch = resourceUsage.setDispatch();
                            dispatch.setMessagesPerPeriod(messagesUsed);
                            dispatch.setBytesPerPeriod(bytesUsed);
                            break;
                        case ReplicationDispatch:
                            ReplicatorUsage replicationDispatch = resourceUsage.addReplicator();
                            replicationDispatch.setLocalCluster(rgs.getPulsar().getConfiguration().getClusterName());
                            replicationDispatch.setRemoteCluster(key.getRemoteCluster());
                            NetworkUsage networkUsage = replicationDispatch.setNetworkUsage();
                            networkUsage.setMessagesPerPeriod(messagesUsed);
                            networkUsage.setBytesPerPeriod(bytesUsed);
                            break;
                    }
                    monEntity.lastReportedValues.bytes = bytesUsed;
                    monEntity.lastReportedValues.messages = messagesUsed;
                    monEntity.numSuppressedUsageReports = 0;
                    monEntity.lastResourceUsageFillTimeMSecsSinceEpoch = System.currentTimeMillis();
                } else {
                    numSuppressions = monEntity.numSuppressedUsageReports++;
                }

                final String rgName = this.ruPublisher != null ? this.ruPublisher.getID() : this.resourceGroupName;
                double sentCount = sendReport ? 1 : 0;
                rgLocalUsageReportCount.labels(rgName, key.getResourceGroupMonitoringClass().name()).inc(sentCount);
                if (sendReport) {
                    if (log.isDebugEnabled()) {
                        log.debug("fillResourceUsage for RG={}: filled a {} update; bytes={}, messages={}",
                                rgName, key.getResourceGroupMonitoringClass(), bytesUsed, messagesUsed);
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("fillResourceUsage for RG={}: report for {} suppressed "
                                        + "(suppressions={} since last sent report)",
                                rgName, key.getResourceGroupMonitoringClass(), numSuppressions);
                    }
                }
            } finally {
                monEntity.localUsageStatsLock.unlock();
            }
        });
    }


    private void updateUsageFromOtherBrokers(MonitoringKey key, Consumer<PerMonitoringClassFields> consumer) {
        PerMonitoringClassFields monEntity = monitoringClassFieldsMap.computeIfAbsent(key, (__)->{
            synchronized (this) {
                return createPerMonitoringClassFields(key.getResourceGroupMonitoringClass(), key.getRemoteCluster());
            }
        });
        consumer.accept(monEntity);
    }

    // Update fields in a particular monitoring class from a given broker in the
    // transport-manager callback for listening to usage reports.
    protected void getUsageFromMonitoredEntity(ResourceUsage resourceUsage, String broker) {
        List<MonitoringKey> monitoringKeyList = new LinkedList<>();
        if (resourceUsage.hasPublish()) {
            monitoringKeyList.add(getMonitoringKey(ResourceGroupMonitoringClass.Publish, null));
        }
        if (resourceUsage.hasDispatch()) {
            monitoringKeyList.add(getMonitoringKey(ResourceGroupMonitoringClass.Dispatch, null));
        }
        if (resourceUsage.getReplicatorsCount() != 0) {
            resourceUsage.getReplicatorsList().forEach(replicator -> {
                monitoringKeyList.add(getMonitoringKey(ResourceGroupMonitoringClass.ReplicationDispatch,
                        replicator.getRemoteCluster()));
            });
        }

        monitoringKeyList.forEach((key) -> updateUsageFromOtherBrokers(key, monEntity -> {
            monEntity.usageFromOtherBrokersLock.lock();
            PerBrokerUsageStats usageStats = monEntity.usageFromOtherBrokers.get(broker, (__) -> {
                PerBrokerUsageStats perBrokerUsageStats = new PerBrokerUsageStats();
                perBrokerUsageStats.usedValues = new BytesAndMessagesCount();
                return perBrokerUsageStats;
            });
            assert usageStats != null;
            try {
                NetworkUsage p = null;
                switch (key.getResourceGroupMonitoringClass()) {
                    case Publish:
                        p = resourceUsage.hasPublish() ? resourceUsage.getPublish() : null;
                        break;
                    case Dispatch:
                        p = resourceUsage.hasDispatch() ? resourceUsage.getDispatch() : null;
                        break;
                    case ReplicationDispatch:
                        ReplicatorUsage replicatorUsage = resourceUsage.getReplicatorsList().stream()
                                .filter(n -> Objects.equals(n.getRemoteCluster(), key.getRemoteCluster()))
                                .findFirst()
                                .orElse(null);
                        if (replicatorUsage != null) {
                            p = replicatorUsage.getNetworkUsage();
                        }
                        break;
                }
                if (p == null) {
                    return;
                }
                long newByteCount = p.getBytesPerPeriod();
                usageStats.usedValues.bytes = newByteCount;
                long newMessageCount = p.getMessagesPerPeriod();
                usageStats.usedValues.messages = newMessageCount;
                usageStats.lastResourceUsageReadTimeMSecsSinceEpoch = System.currentTimeMillis();
                String remoteCluster = key.getRemoteCluster() != null ? key.getRemoteCluster() : "";
                rgRemoteUsageReportsBytes.labels(this.ruConsumer.getID(),
                                ResourceGroupMonitoringClass.Publish.name(), broker, remoteCluster)
                        .inc(newByteCount);
                rgRemoteUsageReportsMessages.labels(this.ruConsumer.getID(),
                                ResourceGroupMonitoringClass.Publish.name(), broker, remoteCluster)
                        .inc(newMessageCount);
                rgRemoteUsageReportsBytesGauge.labels(this.ruConsumer.getID(),
                                ResourceGroupMonitoringClass.Publish.name(), broker, remoteCluster)
                        .set(newByteCount);
                rgRemoteUsageReportsMessagesGauge.labels(this.ruConsumer.getID(),
                                ResourceGroupMonitoringClass.Publish.name(), broker, remoteCluster)
                        .set(newMessageCount);
            } finally {
                monEntity.usageFromOtherBrokersLock.unlock();
            }
        }));
    }

    private long getCacheDuration() {
        ServiceConfiguration conf = rgs.getPulsar().getConfiguration();
        long resourceUsageTransportPublishIntervalInSecs = conf.getResourceUsageTransportPublishIntervalInSecs();
        int maxUsageReportSuppressRounds = Math.max(conf.getResourceUsageMaxUsageReportSuppressRounds(), 1);
        // Usage report data is cached to the memory, when the broker is restart or offline, we need an elimination
        // strategy to release the quota occupied by other broker.
        //
        // Considering that each broker starts at a different time, the cache time should be equal to the mandatory
        // reporting period * 2.
        return TimeUnit.SECONDS.toMillis(
                resourceUsageTransportPublishIntervalInSecs * maxUsageReportSuppressRounds * 2);
    }

    private void updateMonitoringClassFieldsMap(org.apache.pulsar.common.policies.data.ResourceGroup rgConfig) {
        this.rgConfig = rgConfig;
        monitoringClassFieldsMap.forEach((key, monEntity) -> {
            switch (key.getResourceGroupMonitoringClass()) {
                case Publish:
                    monEntity.configValuesPerPeriod.bytes = rgConfig.getPublishRateInBytes() == null
                            ? -1 : rgConfig.getPublishRateInBytes();
                    monEntity.configValuesPerPeriod.messages = rgConfig.getPublishRateInMsgs() == null
                            ? -1 : rgConfig.getPublishRateInMsgs();
                    break;
                case Dispatch:
                    monEntity.configValuesPerPeriod.bytes = rgConfig.getDispatchRateInBytes() == null
                            ? -1 : rgConfig.getDispatchRateInBytes();
                    monEntity.configValuesPerPeriod.messages = rgConfig.getDispatchRateInMsgs() == null
                            ? -1 : rgConfig.getDispatchRateInMsgs();
                    break;
                case ReplicationDispatch:
                    requireNonNull(key.getRemoteCluster(),
                            "remoteCluster cannot be null when monClass is ReplicationDispatch");
                    DispatchRate dispatchRate = rgConfig.getReplicatorDispatchRate()
                            .get(getReplicatorDispatchRateLimiterKey(key.getRemoteCluster()));
                    if (dispatchRate != null) {
                        monEntity.configValuesPerPeriod.bytes = dispatchRate.getDispatchThrottlingRateInByte();
                        monEntity.configValuesPerPeriod.messages =
                                dispatchRate.getDispatchThrottlingRateInMsg();
                    } else {
                        monEntity.configValuesPerPeriod.bytes = rgConfig.getReplicationDispatchRateInBytes();
                        monEntity.configValuesPerPeriod.messages = rgConfig.getReplicationDispatchRateInMsgs();
                    }
                    break;
            }
        });
    }

    private void setDefaultResourceUsageTransportHandlers() {
        this.ruPublisher = new ResourceUsagePublisher() {
            @Override
            public String getID() {
                return ResourceGroup.this.getID();
            }

            @Override
            public void fillResourceUsage(ResourceUsage resourceUsage) {
                ResourceGroup.this.rgFillResourceUsage(resourceUsage);
            }
        };

        this.ruConsumer = new ResourceUsageConsumer() {
            @Override
            public String getID() {
                return ResourceGroup.this.getID();
            }

            @Override
            public void acceptResourceUsage(String broker, ResourceUsage resourceUsage) {
                ResourceGroup.this.rgResourceUsageListener(broker, resourceUsage);
            }
        };
    }

    @VisibleForTesting
    PerMonitoringClassFields getMonitoredEntity(ResourceGroupMonitoringClass monClass, String remoteCluster) {
        return getPerMonitoringClassFields(monClass, remoteCluster);
    }

    public final String resourceGroupName;

    @ToString
    public static class MonitoringKey {
        @Getter
        private final ResourceGroupMonitoringClass resourceGroupMonitoringClass;
        @Getter
        private final String remoteCluster;

        public MonitoringKey(ResourceGroupMonitoringClass resourceGroupMonitoringClass, String remoteCluster) {
            this.resourceGroupMonitoringClass = resourceGroupMonitoringClass;
            this.remoteCluster = remoteCluster;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MonitoringKey)) {
                return false;
            }
            MonitoringKey that = (MonitoringKey) o;
            return Objects.equals(resourceGroupMonitoringClass, that.resourceGroupMonitoringClass)
                    && Objects.equals(remoteCluster, that.remoteCluster);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resourceGroupMonitoringClass, remoteCluster);
        }
    }

    @Getter
    public final Map<MonitoringKey, PerMonitoringClassFields> monitoringClassFieldsMap = new ConcurrentHashMap<>();

    private static final Logger log = LoggerFactory.getLogger(ResourceGroupService.class);

    // Tenants and namespaces that are directly referencing this resource-group.
    // These are not kept on a per-monitoring class basis, since a given NS (for example) references the same RG
    // across all of its usage classes (publish/dispatch/...).
    private Set<String> resourceGroupTenantRefs = ConcurrentHashMap.newKeySet();
    private Set<String> resourceGroupNamespaceRefs = ConcurrentHashMap.newKeySet();
    private Set<String> resourceGroupTopicRefs = ConcurrentHashMap.newKeySet();

    // Blobs required for transport manager's resource-usage register/unregister ops.
    ResourceUsageConsumer ruConsumer;
    ResourceUsagePublisher ruPublisher;

    // The creator resource-group-service [ToDo: remove later with a strict singleton ResourceGroupService]
    ResourceGroupService rgs;

    // Labels for the various counters used here.
    private static final String[] resourceGroupMontoringclassLabels = {"ResourceGroup", "MonitoringClass"};
    private static final String[] resourceGroupMontoringclassRemotebrokerLabels =
            {"ResourceGroup", "MonitoringClass", "RemoteBroker", "RemoteBrokerName"};

    private static final Counter rgRemoteUsageReportsBytes = Counter.build()
            .name("pulsar_resource_group_remote_usage_bytes_used")
            .help("Bytes used reported about this <RG, monitoring class> from a remote broker")
            .labelNames(resourceGroupMontoringclassRemotebrokerLabels)
            .register();
    private static final Gauge rgRemoteUsageReportsBytesGauge = Gauge.build()
            .name("pulsar_resource_group_remote_usage_bytes_used_gauge")
            .help("Bytes used reported about this <RG, monitoring class> from a remote broker")
            .labelNames(resourceGroupMontoringclassRemotebrokerLabels)
            .register();
    private static final Counter rgRemoteUsageReportsMessages = Counter.build()
            .name("pulsar_resource_group_remote_usage_messages_used")
            .help("Messages used reported about this <RG, monitoring class> from a remote broker")
            .labelNames(resourceGroupMontoringclassRemotebrokerLabels)
            .register();
    private static final Gauge rgRemoteUsageReportsMessagesGauge = Gauge.build()
            .name("pulsar_resource_group_remote_usage_messages_used_gauge")
            .help("Messages used reported about this <RG, monitoring class> from a remote broker")
            .labelNames(resourceGroupMontoringclassRemotebrokerLabels)
            .register();

    private static final Counter rgLocalUsageReportCount = Counter.build()
            .name("pulsar_resource_group_local_usage_reported")
            .help("Number of times local usage was reported (vs. suppressed due to negligible change)")
            .labelNames(resourceGroupMontoringclassLabels)
            .register();

    private org.apache.pulsar.common.policies.data.ResourceGroup rgConfig;

    private final Object replicatorDispatchRateLock = new Object();

    // Publish rate limiter for the resource group
    @Getter
    protected ResourceGroupPublishLimiter resourceGroupPublishLimiter;

    @Getter
    private ResourceGroupDispatchLimiter resourceGroupReplicationDispatchLimiter;

    private Map<String, ResourceGroupDispatchLimiter> replicatorDispatchRateLimiterMap =
            new ConcurrentHashMap<>();
    private Map<String, Set<Consumer<ResourceGroupDispatchLimiter>>> replicatorDispatchRateLimiterConsumerMap =
            new ConcurrentHashMap<>();

    @Getter
    protected ResourceGroupDispatchLimiter resourceGroupDispatchLimiter;

    protected static class PerMonitoringClassFields {
        // This lock covers all the "local" counts (i.e., except for the per-broker usage stats).
        Lock localUsageStatsLock;

        BytesAndMessagesCount configValuesPerPeriod;

        // Target quotas set (after reading info from other brokers) for local usage.
        BytesAndMessagesCount quotaForNextPeriod;

        // Running statistics about local usage.
        BytesAndMessagesCount usedLocallySinceLastReport;

        // Statistics about local usage that were reported in the last round.
        // We will suppress reports (as optimization) if usedLocallySinceLastReport is not
        // substantially different from lastReportedValues.
        BytesAndMessagesCount lastReportedValues;

        // Time when we last attempted to fill up in fillResourceUsage; for debugging.
        long lastResourceUsageFillTimeMSecsSinceEpoch;

        // Number of rounds of suppressed usage reports (due to not enough change) since last we reported usage.
        int numSuppressedUsageReports;

        // Accumulated stats of local usage.
        @VisibleForTesting
        BytesAndMessagesCount totalUsedLocally;

        // This lock covers all the non-local usage counts, received from other brokers.
        Lock usageFromOtherBrokersLock;
        public Cache<String, PerBrokerUsageStats> usageFromOtherBrokers;

        private PerMonitoringClassFields(){

        }

        static PerMonitoringClassFields create(long durationMs) {
            PerMonitoringClassFields perMonitoringClassFields = new PerMonitoringClassFields();
            perMonitoringClassFields.configValuesPerPeriod = new BytesAndMessagesCount();
            perMonitoringClassFields.usedLocallySinceLastReport = new BytesAndMessagesCount();
            perMonitoringClassFields.lastReportedValues = new BytesAndMessagesCount();
            perMonitoringClassFields.quotaForNextPeriod = new BytesAndMessagesCount();
            perMonitoringClassFields.totalUsedLocally = new BytesAndMessagesCount();
            perMonitoringClassFields.usageFromOtherBrokersLock = new ReentrantLock();
            // ToDo: Change the following to a ReadWrite lock if needed.
            perMonitoringClassFields.localUsageStatsLock = new ReentrantLock();

            perMonitoringClassFields.usageFromOtherBrokers = Caffeine.newBuilder()
                    .expireAfterWrite(durationMs, TimeUnit.MILLISECONDS)
                    .build();
            return perMonitoringClassFields;
        }
    }

    // Usage stats for this RG obtained from other brokers.
    protected static class PerBrokerUsageStats {
        // Time when we last read about the <RG, broker> in resourceUsageListener; for debugging.
        public long lastResourceUsageReadTimeMSecsSinceEpoch;

        // Usage stats about the <RG, broker>.
        BytesAndMessagesCount usedValues;
    }
}
