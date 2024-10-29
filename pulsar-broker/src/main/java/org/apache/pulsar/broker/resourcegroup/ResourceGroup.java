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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.ToString;
import lombok.val;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupService.ResourceGroupOpStatus;
import org.apache.pulsar.broker.service.resource.usage.NetworkUsage;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsage;
import org.apache.pulsar.client.admin.PulsarAdminException;
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
public class ResourceGroup {

    /**
     * Convenience class for bytes and messages counts, which are used together in a lot of the following code.
     */
    @ToString
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
        this.rgs = rgs;
        this.resourceGroupName = name;
        this.setResourceGroupMonitoringClassFields();
        this.setResourceGroupConfigParameters(rgConfig);
        this.setDefaultResourceUsageTransportHandlers();
        this.resourceGroupPublishLimiter = new ResourceGroupPublishLimiter(rgConfig, rgs.getPulsar().getExecutor());
        log.info("attaching publish rate limiter {} to {} get {}", this.resourceGroupPublishLimiter, name,
          this.getResourceGroupPublishLimiter());
        this.resourceGroupReplicationDispatchLimiter = ResourceGroupRateLimiterManager
                .newReplicationDispatchRateLimiter(rgConfig, rgs.getPulsar().getExecutor());
        log.info("attaching replication dispatch rate limiter {} to {}", this.resourceGroupReplicationDispatchLimiter,
                name);
        this.resourceGroupDispatchLimiter = ResourceGroupRateLimiterManager
                .newDispatchRateLimiter(rgConfig, rgs.getPulsar().getExecutor());
        log.info("attaching topic dispatch rate limiter {} to {}", this.resourceGroupDispatchLimiter, name);
    }

    // ctor for overriding the transport-manager fill/set buffer.
    // It is not expected that anything outside of this package will need to directly
    // construct a ResourceGroup (i.e., without going through ResourceGroupService).
    protected ResourceGroup(ResourceGroupService rgs, String rgName,
                            org.apache.pulsar.common.policies.data.ResourceGroup rgConfig,
                            ResourceUsagePublisher rgPublisher, ResourceUsageConsumer rgConsumer) {
        this.rgs = rgs;
        this.resourceGroupName = rgName;
        this.setResourceGroupMonitoringClassFields();
        this.setResourceGroupConfigParameters(rgConfig);
        this.resourceGroupPublishLimiter = new ResourceGroupPublishLimiter(rgConfig, rgs.getPulsar().getExecutor());
        this.resourceGroupReplicationDispatchLimiter = ResourceGroupRateLimiterManager
                .newReplicationDispatchRateLimiter(rgConfig, rgs.getPulsar().getExecutor());
        this.resourceGroupDispatchLimiter = ResourceGroupRateLimiterManager
                .newDispatchRateLimiter(rgConfig, rgs.getPulsar().getExecutor());
        this.ruPublisher = rgPublisher;
        this.ruConsumer = rgConsumer;
    }

    // copy ctor: note does shallow copy.
    // The envisioned usage is for display of an RG's state, without scope for direct update.
    public ResourceGroup(ResourceGroup other) {
        this.resourceGroupName = other.resourceGroupName;
        this.rgs = other.rgs;
        this.resourceGroupPublishLimiter = other.resourceGroupPublishLimiter;
        this.resourceGroupReplicationDispatchLimiter = other.resourceGroupReplicationDispatchLimiter;
        this.resourceGroupDispatchLimiter = other.resourceGroupDispatchLimiter;
        this.setResourceGroupMonitoringClassFields();

        // ToDo: copy the monitoring class fields, and ruPublisher/ruConsumer from other, if required.

        this.resourceGroupNamespaceRefs = other.resourceGroupNamespaceRefs;
        this.resourceGroupTenantRefs = other.resourceGroupTenantRefs;
        this.resourceGroupTopicRefs = other.resourceGroupTopicRefs;

        for (int idx = 0; idx < ResourceGroupMonitoringClass.values().length; idx++) {
            PerMonitoringClassFields thisFields = this.monitoringClassFields[idx];
            PerMonitoringClassFields otherFields = other.monitoringClassFields[idx];

            thisFields.configValuesPerPeriod.bytes = otherFields.configValuesPerPeriod.bytes;
            thisFields.configValuesPerPeriod.messages = otherFields.configValuesPerPeriod.messages;

            thisFields.quotaForNextPeriod.bytes = otherFields.quotaForNextPeriod.bytes;
            thisFields.quotaForNextPeriod.messages = otherFields.quotaForNextPeriod.messages;

            thisFields.usedLocallySinceLastReport.bytes = otherFields.usedLocallySinceLastReport.bytes;
            thisFields.usedLocallySinceLastReport.messages = otherFields.usedLocallySinceLastReport.messages;

            thisFields.lastResourceUsageFillTimeMSecsSinceEpoch = otherFields.lastResourceUsageFillTimeMSecsSinceEpoch;

            thisFields.numSuppressedUsageReports = otherFields.numSuppressedUsageReports;

            thisFields.totalUsedLocally.bytes = otherFields.totalUsedLocally.bytes;
            thisFields.totalUsedLocally.messages = otherFields.totalUsedLocally.messages;

            // ToDo: Deep copy instead?
            thisFields.usageFromOtherBrokers = otherFields.usageFromOtherBrokers;
        }
    }

    protected void updateResourceGroup(org.apache.pulsar.common.policies.data.ResourceGroup rgConfig) {
        this.setResourceGroupConfigParameters(rgConfig);
        val pubBmc = new BytesAndMessagesCount();
        pubBmc.messages = rgConfig.getPublishRateInMsgs();
        pubBmc.bytes = rgConfig.getPublishRateInBytes();
        this.resourceGroupPublishLimiter.update(pubBmc);
        ResourceGroupRateLimiterManager
                .updateReplicationDispatchRateLimiter(resourceGroupReplicationDispatchLimiter, rgConfig);
        ResourceGroupRateLimiterManager.updateDispatchRateLimiter(resourceGroupDispatchLimiter, rgConfig);
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
        NetworkUsage p;
        resourceUsage.setOwner(this.getID());

        p = resourceUsage.setPublish();
        if (!this.setUsageInMonitoredEntity(ResourceGroupMonitoringClass.Publish, p)) {
            resourceUsage.clearPublish();
        }

        p = resourceUsage.setDispatch();
        if (!this.setUsageInMonitoredEntity(ResourceGroupMonitoringClass.Dispatch, p)) {
            resourceUsage.clearDispatch();
        }

        p = resourceUsage.setReplicationDispatch();
        if (!this.setUsageInMonitoredEntity(ResourceGroupMonitoringClass.ReplicationDispatch, p)) {
            resourceUsage.clearReplicationDispatch();
        }

        // Punt storage for now.
    }

    // Transport manager mandated op.
    public void rgResourceUsageListener(String broker, ResourceUsage resourceUsage) {
        if (resourceUsage.hasPublish()) {
            this.getUsageFromMonitoredEntity(ResourceGroupMonitoringClass.Publish, resourceUsage.getPublish(), broker);
        }

        if (resourceUsage.hasDispatch()) {
            this.getUsageFromMonitoredEntity(ResourceGroupMonitoringClass.Dispatch, resourceUsage.getDispatch(),
                    broker);
        }

        if (resourceUsage.hasReplicationDispatch()) {
            this.getUsageFromMonitoredEntity(ResourceGroupMonitoringClass.ReplicationDispatch,
                    resourceUsage.getReplicationDispatch(), broker);
        }
        // Punt storage for now.
    }

    protected BytesAndMessagesCount getConfLimits(ResourceGroupMonitoringClass monClass) throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        BytesAndMessagesCount retval = new BytesAndMessagesCount();
        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.localUsageStatsLock.lock();
        try {
            retval.bytes = monEntity.configValuesPerPeriod.bytes;
            retval.messages = monEntity.configValuesPerPeriod.messages;
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }

        return retval;
    }

    protected void incrementLocalUsageStats(ResourceGroupMonitoringClass monClass, BytesAndMessagesCount stats)
                                                                                        throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.localUsageStatsLock.lock();
        try {
            monEntity.usedLocallySinceLastReport.bytes += stats.bytes;
            monEntity.usedLocallySinceLastReport.messages += stats.messages;
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }
    }

    protected BytesAndMessagesCount getLocalUsageStats(ResourceGroupMonitoringClass monClass)
                                                                                        throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        BytesAndMessagesCount retval = new BytesAndMessagesCount();
        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.localUsageStatsLock.lock();
        try {
            retval.bytes = monEntity.usedLocallySinceLastReport.bytes;
            retval.messages = monEntity.usedLocallySinceLastReport.messages;
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }

        return retval;
    }

    protected BytesAndMessagesCount getLocalUsageStatsCumulative(ResourceGroupMonitoringClass monClass)
            throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        BytesAndMessagesCount retval = new BytesAndMessagesCount();
        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.localUsageStatsLock.lock();
        try {
            // If the total wasn't accumulated yet (i.e., a report wasn't sent yet), just return the
            // partial accumulation in usedLocallySinceLastReport.
            if (monEntity.totalUsedLocally.messages == 0) {
                retval.bytes = monEntity.usedLocallySinceLastReport.bytes;
                retval.messages = monEntity.usedLocallySinceLastReport.messages;
            } else {
                retval.bytes = monEntity.totalUsedLocally.bytes;
                retval.messages = monEntity.totalUsedLocally.messages;
            }
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }

        return retval;
    }

    protected BytesAndMessagesCount getLocalUsageStatsFromBrokerReports(ResourceGroupMonitoringClass monClass)
            throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        val retval = new BytesAndMessagesCount();
        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        String myBrokerId = this.rgs.getPulsar().getBrokerServiceUrl();
        PerBrokerUsageStats pbus;

        monEntity.usageFromOtherBrokersLock.lock();
        try {
            pbus = monEntity.usageFromOtherBrokers.getIfPresent(myBrokerId);
        } finally {
            monEntity.usageFromOtherBrokersLock.unlock();
        }

        if (pbus != null) {
            retval.bytes = pbus.usedValues.bytes;
            retval.messages = pbus.usedValues.messages;
        } else {
            log.info("getLocalUsageStatsFromBrokerReports: no usage report found for broker={} and monClass={}",
                    myBrokerId, monClass);
        }

        return retval;
    }

    protected BytesAndMessagesCount getGlobalUsageStats(ResourceGroupMonitoringClass monClass)
                                                                                        throws PulsarAdminException {
        this.checkMonitoringClass(monClass);

        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.usageFromOtherBrokersLock.lock();
        BytesAndMessagesCount retStats = new BytesAndMessagesCount();
        try {
            monEntity.usageFromOtherBrokers.asMap().forEach((broker, brokerUsage) -> {
                retStats.bytes += brokerUsage.usedValues.bytes;
                retStats.messages += brokerUsage.usedValues.messages;
            });
        } finally {
            monEntity.usageFromOtherBrokersLock.unlock();
        }

        return retStats;
    }

    protected BytesAndMessagesCount updateLocalQuota(ResourceGroupMonitoringClass monClass,
                                                     BytesAndMessagesCount newQuota) throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        BytesAndMessagesCount oldBMCount;

        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.localUsageStatsLock.lock();
        oldBMCount = monEntity.quotaForNextPeriod;
        try {
            monEntity.quotaForNextPeriod = newQuota;
            switch (monClass) {
                case ReplicationDispatch:
                    ResourceGroupRateLimiterManager
                            .updateReplicationDispatchRateLimiter(resourceGroupReplicationDispatchLimiter, newQuota);
                    break;
                case Publish:
                    this.resourceGroupPublishLimiter.update(newQuota);
                    break;
                case Dispatch:
                    ResourceGroupRateLimiterManager.updateDispatchRateLimiter(resourceGroupDispatchLimiter, newQuota);
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

        return oldBMCount;
    }

    protected BytesAndMessagesCount getRgPublishRateLimiterValues() {
        BytesAndMessagesCount retVal;
        final PerMonitoringClassFields monEntity =
                                        this.monitoringClassFields[ResourceGroupMonitoringClass.Publish.ordinal()];
        monEntity.localUsageStatsLock.lock();
        try {
            retVal = this.resourceGroupPublishLimiter.getResourceGroupPublishValues();
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }

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
    protected boolean setUsageInMonitoredEntity(ResourceGroupMonitoringClass monClass, NetworkUsage p) {
        long bytesUsed, messagesUsed;
        boolean sendReport;
        int numSuppressions = 0;
        PerMonitoringClassFields monEntity;

        final int idx = monClass.ordinal();
        monEntity = this.monitoringClassFields[idx];

        monEntity.localUsageStatsLock.lock();
        try {
            sendReport = this.rgs.quotaCalculator.needToReportLocalUsage(
                    monEntity.usedLocallySinceLastReport.bytes,
                    monEntity.lastReportedValues.bytes,
                    monEntity.usedLocallySinceLastReport.messages,
                    monEntity.lastReportedValues.messages,
                    monEntity.lastResourceUsageFillTimeMSecsSinceEpoch);

            bytesUsed = monEntity.usedLocallySinceLastReport.bytes;
            messagesUsed = monEntity.usedLocallySinceLastReport.messages;
            monEntity.usedLocallySinceLastReport.bytes = monEntity.usedLocallySinceLastReport.messages = 0;
            if (sendReport) {
                p.setBytesPerPeriod(bytesUsed);
                p.setMessagesPerPeriod(messagesUsed);
                monEntity.lastReportedValues.bytes = bytesUsed;
                monEntity.lastReportedValues.messages = messagesUsed;
                monEntity.numSuppressedUsageReports = 0;
                monEntity.totalUsedLocally.bytes += bytesUsed;
                monEntity.totalUsedLocally.messages += messagesUsed;
                monEntity.lastResourceUsageFillTimeMSecsSinceEpoch = System.currentTimeMillis();
            } else {
                numSuppressions = monEntity.numSuppressedUsageReports++;
            }

        } finally {
            monEntity.localUsageStatsLock.unlock();
        }

        final String rgName = this.ruPublisher != null ? this.ruPublisher.getID() : this.resourceGroupName;
        double sentCount = sendReport ? 1 : 0;
        rgLocalUsageReportCount.labels(rgName, monClass.name()).inc(sentCount);
        if (sendReport) {
            if (log.isDebugEnabled()) {
                log.debug("fillResourceUsage for RG={}: filled a {} update; bytes={}, messages={}",
                        rgName, monClass, bytesUsed, messagesUsed);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("fillResourceUsage for RG={}: report for {} suppressed "
                    + "(suppressions={} since last sent report)",
                        rgName, monClass, numSuppressions);
            }
        }

        return sendReport;
    }

    // Update fields in a particular monitoring class from a given broker in the
    // transport-manager callback for listening to usage reports.
    private void getUsageFromMonitoredEntity(ResourceGroupMonitoringClass monClass, NetworkUsage p, String broker) {
        final int idx = monClass.ordinal();
        PerMonitoringClassFields monEntity;
        PerBrokerUsageStats usageStats, oldUsageStats;
        long oldByteCount, oldMessageCount;
        long newByteCount, newMessageCount;

        monEntity = this.monitoringClassFields[idx];
        usageStats = monEntity.usageFromOtherBrokers.getIfPresent(broker);
        if (usageStats == null) {
            usageStats = new PerBrokerUsageStats();
            usageStats.usedValues = new BytesAndMessagesCount();
        }
        monEntity.usageFromOtherBrokersLock.lock();
        try {
            newByteCount = p.getBytesPerPeriod();
            usageStats.usedValues.bytes = newByteCount;
            newMessageCount = p.getMessagesPerPeriod();
            usageStats.usedValues.messages = newMessageCount;
            usageStats.lastResourceUsageReadTimeMSecsSinceEpoch = System.currentTimeMillis();
            oldUsageStats = monEntity.usageFromOtherBrokers.getIfPresent(broker);
            monEntity.usageFromOtherBrokers.put(broker, usageStats);
        } finally {
            monEntity.usageFromOtherBrokersLock.unlock();
        }
        rgRemoteUsageReportsBytes.labels(this.ruConsumer.getID(), monClass.name(), broker).inc(newByteCount);
        rgRemoteUsageReportsMessages.labels(this.ruConsumer.getID(), monClass.name(), broker).inc(newMessageCount);
        rgRemoteUsageReportsBytesGauge.labels(this.ruConsumer.getID(), monClass.name(), broker).set(newByteCount);
        rgRemoteUsageReportsMessagesGauge.labels(this.ruConsumer.getID(), monClass.name(), broker).set(newMessageCount);
        oldByteCount = oldMessageCount = -1;
        if (oldUsageStats != null) {
            oldByteCount = oldUsageStats.usedValues.bytes;
            oldMessageCount = oldUsageStats.usedValues.messages;
        }

        if (log.isDebugEnabled()) {
            log.debug("resourceUsageListener for RG={}: updated {} stats for broker={} "
                            + "with bytes={} (old ={}), messages={} (old={})",
                    this.resourceGroupName, monClass, broker,
                    newByteCount, oldByteCount,
                    newMessageCount, oldMessageCount);
        }
    }

    private void setResourceGroupMonitoringClassFields() {
        ServiceConfiguration conf = rgs.getPulsar().getConfiguration();
        long resourceUsageTransportPublishIntervalInSecs = conf.getResourceUsageTransportPublishIntervalInSecs();
        int maxUsageReportSuppressRounds = Math.max(conf.getResourceUsageMaxUsageReportSuppressRounds(), 1);
        long cacheInMS = TimeUnit.SECONDS.toMillis(
                resourceUsageTransportPublishIntervalInSecs * maxUsageReportSuppressRounds * 2);
        // Usage report data is cached to the memory, when the broker is restart or offline, we need an elimination
        // strategy to release the quota occupied by other broker.
        //
        // Considering that each broker starts at a different time, the cache time should be equal to the mandatory
        // reporting period * 2.
        for (int idx = 0; idx < ResourceGroupMonitoringClass.values().length; idx++) {
            this.monitoringClassFields[idx] = PerMonitoringClassFields.create(cacheInMS);
        }
    }

    private void setResourceGroupConfigParameters(org.apache.pulsar.common.policies.data.ResourceGroup rgConfig) {
        int idx;

        idx = ResourceGroupMonitoringClass.Publish.ordinal();
        this.monitoringClassFields[idx].configValuesPerPeriod.bytes = rgConfig.getPublishRateInBytes() == null
                ? -1 : rgConfig.getPublishRateInBytes();
        this.monitoringClassFields[idx].configValuesPerPeriod.messages = rgConfig.getPublishRateInMsgs() == null
                ? -1 : rgConfig.getPublishRateInMsgs();

        idx = ResourceGroupMonitoringClass.Dispatch.ordinal();
        this.monitoringClassFields[idx].configValuesPerPeriod.bytes = rgConfig.getDispatchRateInBytes() == null
                ? -1 : rgConfig.getDispatchRateInBytes();
        this.monitoringClassFields[idx].configValuesPerPeriod.messages = rgConfig.getDispatchRateInMsgs() == null
                ? -1 : rgConfig.getDispatchRateInMsgs();

        idx = ResourceGroupMonitoringClass.ReplicationDispatch.ordinal();
        this.monitoringClassFields[idx]
                .configValuesPerPeriod.bytes = rgConfig.getReplicationDispatchRateInBytes() == null
                ? -1 : rgConfig.getReplicationDispatchRateInBytes();
        this.monitoringClassFields[idx]
                .configValuesPerPeriod.messages = rgConfig.getReplicationDispatchRateInMsgs() == null
                ? -1 : rgConfig.getReplicationDispatchRateInMsgs();
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
    PerMonitoringClassFields getMonitoredEntity(ResourceGroupMonitoringClass monClass) {
        return this.monitoringClassFields[monClass.ordinal()];
    }

    public final String resourceGroupName;

    public PerMonitoringClassFields[] monitoringClassFields =
            new PerMonitoringClassFields[ResourceGroupMonitoringClass.values().length];

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
                                                    {"ResourceGroup", "MonitoringClass", "RemoteBroker"};

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

    // Publish rate limiter for the resource group
    @Getter
    protected ResourceGroupPublishLimiter resourceGroupPublishLimiter;

    @Getter
    protected ResourceGroupDispatchLimiter resourceGroupReplicationDispatchLimiter;

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
