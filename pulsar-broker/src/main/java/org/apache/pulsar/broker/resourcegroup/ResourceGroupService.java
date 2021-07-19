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
package org.apache.pulsar.broker.resourcegroup;

import static com.google.common.base.Preconditions.checkNotNull;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupMonitoringClass;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupRefTypes;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>ResourceGroupService</code> contains APIs to manipulate resource groups. It is assumed that there is a
 * single instance of the ResourceGroupService on each broker.
 * The control plane operations are somewhat stringent, and throw exceptions if (for instance) the op refers to a
 * resource group which does not exist.
 * The data plane operations (e.g., increment stats) throw exceptions as a last resort; if (e.g.) increment stats
 * refers to a non-existent resource group, the stats are quietly not incremented.
 *
 * @see PulsarService
 */
public class ResourceGroupService {
    /**
     * Default constructor.
     */
    public ResourceGroupService(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.timeUnitScale = TimeUnit.SECONDS;
        this.quotaCalculator = new ResourceQuotaCalculatorImpl();
        this.resourceUsageTransportManagerMgr = pulsar.getResourceUsageTransportManager();
        this.rgConfigListener = new ResourceGroupConfigListener(this, pulsar);
        this.initialize();
    }

    // For testing only.
    public ResourceGroupService(PulsarService pulsar, TimeUnit timescale,
                                ResourceUsageTopicTransportManager transportMgr,
                                ResourceQuotaCalculator quotaCalc) {
        this.pulsar = pulsar;
        this.timeUnitScale = timescale;
        this.resourceUsageTransportManagerMgr = transportMgr;
        this.quotaCalculator = quotaCalc;
        this.rgConfigListener = new ResourceGroupConfigListener(this, pulsar);
        this.initialize();
    }

    protected enum ResourceGroupOpStatus {
        OK,
        Exists,
        DoesNotExist,
        NotSupported
    }

    /**
     * Create RG.
     *
     * @throws if RG with that name already exists.
     */
    public void resourceGroupCreate(String rgName, org.apache.pulsar.common.policies.data.ResourceGroup rgConfig)
      throws PulsarAdminException {
        this.checkRGCreateParams(rgName, rgConfig);
        ResourceGroup rg = new ResourceGroup(this, rgName, rgConfig);
        resourceGroupsMap.put(rgName, rg);
    }

    /**
     * Create RG, with non-default functions for resource-usage transport-manager.
     *
     * @throws if RG with that name already exists (even if the resource usage handlers are different).
     */
    public void resourceGroupCreate(String rgName,
                                    org.apache.pulsar.common.policies.data.ResourceGroup rgConfig,
                                    ResourceUsagePublisher rgPublisher,
                                    ResourceUsageConsumer rgConsumer) throws PulsarAdminException {
        this.checkRGCreateParams(rgName, rgConfig);
        ResourceGroup rg = new ResourceGroup(this, rgName, rgConfig, rgPublisher, rgConsumer);
        resourceGroupsMap.put(rgName, rg);
    }

    /**
     * Get a copy of the RG with the given name.
     */
    public ResourceGroup resourceGroupGet(String resourceGroupName) {
        ResourceGroup retrievedRG = this.getResourceGroupInternal(resourceGroupName);
        if (retrievedRG == null) {
            return null;
        }

        // Return a copy.
        return new ResourceGroup(retrievedRG);
    }

    /**
     * Update RG.
     *
     * @throws if RG with that name does not exist.
     */
    public void resourceGroupUpdate(String rgName, org.apache.pulsar.common.policies.data.ResourceGroup rgConfig)
      throws PulsarAdminException {
        try {
            checkNotNull(rgConfig);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("ResourceGroupUpdate: Invalid null ResourceGroup config");
        }

        ResourceGroup rg = this.getResourceGroupInternal(rgName);
        if (rg == null) {
            throw new PulsarAdminException("Resource group does not exist: " + rgName);
        }
        rg.updateResourceGroup(rgConfig);
        rgUpdates.labels(rgName).inc();
    }

    public Set<String> resourceGroupGetAll() {
        return resourceGroupsMap.keySet();
    }

    /**
     * Delete RG.
     *
     * @throws if RG with that name does not exist, or if the RG exists but is still in use.
     */
    public void resourceGroupDelete(String name) throws PulsarAdminException {
        ResourceGroup rg = this.getResourceGroupInternal(name);
        if (rg == null) {
            throw new PulsarAdminException("Resource group does not exist: " + name);
        }

        long tenantRefCount = rg.getResourceGroupNumOfTenantRefs();
        long nsRefCount = rg.getResourceGroupNumOfNSRefs();
        if ((tenantRefCount + nsRefCount) > 0) {
            String errMesg = "Resource group " + name + " still has " + tenantRefCount + " tenant refs";
            errMesg += " and " + nsRefCount + " namespace refs on it";
            throw new PulsarAdminException(errMesg);
        }

        rg.resourceGroupPublishLimiter.close();
        rg.resourceGroupPublishLimiter = null;
        resourceGroupsMap.remove(name);
    }

    /**
     * Get the current number of RGs. For testing.
     */
    protected long getNumResourceGroups() {
        return resourceGroupsMap.mappingCount();
    }

    /**
     * Registers a tenant as a user of a resource group.
     *
     * @param resourceGroupName
     * @param tenantName
     * @throws if the RG does not exist, or if the NS already references the RG.
     */
    public void registerTenant(String resourceGroupName, String tenantName) throws PulsarAdminException {
        ResourceGroup rg = checkResourceGroupExists(resourceGroupName);

        // Check that the tenant-name doesn't already have a RG association.
        // [If it does, that should be unregistered before putting a different association.]
        ResourceGroup oldRG = this.tenantToRGsMap.get(tenantName);
        if (oldRG != null) {
            String errMesg = "Tenant " + tenantName + " already references a resource group: " + oldRG.getID();
            throw new PulsarAdminException(errMesg);
        }

        ResourceGroupOpStatus status = rg.registerUsage(tenantName, ResourceGroupRefTypes.Tenants, true,
                                                        this.resourceUsageTransportManagerMgr);
        if (status == ResourceGroupOpStatus.Exists) {
            String errMesg = "Tenant " + tenantName + " already references the resource group " + resourceGroupName;
            errMesg += "; this is unexpected";
            throw new PulsarAdminException(errMesg);
        }

        // Associate this tenant name with the RG.
        this.tenantToRGsMap.put(tenantName, rg);
        rgTenantRegisters.labels(resourceGroupName).inc();
    }

    /**
     * UnRegisters a tenant from a resource group.
     *
     * @param resourceGroupName
     * @param tenantName
     * @throws if the RG does not exist, or if the tenant does not references the RG yet.
     */
    public void unRegisterTenant(String resourceGroupName, String tenantName) throws PulsarAdminException {
        ResourceGroup rg = checkResourceGroupExists(resourceGroupName);

        ResourceGroupOpStatus status = rg.registerUsage(tenantName, ResourceGroupRefTypes.Tenants, false,
                                                        this.resourceUsageTransportManagerMgr);
        if (status == ResourceGroupOpStatus.DoesNotExist) {
            String errMesg = "Tenant " + tenantName + " does not yet reference resource group " + resourceGroupName;
            throw new PulsarAdminException(errMesg);
        }

        // Dissociate this tenant name from the RG.
        this.tenantToRGsMap.remove(tenantName, rg);
        rgTenantUnRegisters.labels(resourceGroupName).inc();
    }

    /**
     * Registers a namespace as a user of a resource group.
     *
     * @param resourceGroupName
     * @param namespaceName
     * @throws if the RG does not exist, or if the NS already references the RG.
     */
    public void registerNameSpace(String resourceGroupName, String namespaceName) throws PulsarAdminException {
        ResourceGroup rg = checkResourceGroupExists(resourceGroupName);

        // Check that the NS-name doesn't already have a RG association.
        // [If it does, that should be unregistered before putting a different association.]
        ResourceGroup oldRG = this.namespaceToRGsMap.get(namespaceName);
        if (oldRG != null) {
            String errMesg = "Namespace " + namespaceName + " already references a resource group: " + oldRG.getID();
            throw new PulsarAdminException(errMesg);
        }

        ResourceGroupOpStatus status = rg.registerUsage(namespaceName, ResourceGroupRefTypes.Namespaces, true,
                                                        this.resourceUsageTransportManagerMgr);
        if (status == ResourceGroupOpStatus.Exists) {
            String errMesg = String.format("Namespace {} already references the target resource group {}",
                    namespaceName, resourceGroupName);
            throw new PulsarAdminException(errMesg);
        }

        // Associate this NS-name with the RG.
        this.namespaceToRGsMap.put(namespaceName, rg);
        rgNamespaceRegisters.labels(resourceGroupName).inc();
    }

    /**
     * Return the resource group associated with a namespace.
     *
     * @param namespaceName
     * @throws if the RG does not exist, or if the NS already references the RG.
     */
    public ResourceGroup getNamespaceResourceGroup(String namespaceName) {
        return this.namespaceToRGsMap.get(namespaceName);
    }

    /**
     * UnRegisters a namespace from a resource group.
     *
     * @param resourceGroupName
     * @param namespaceName
     * @throws if the RG does not exist, or if the NS does not references the RG yet.
     */
    public void unRegisterNameSpace(String resourceGroupName, String namespaceName) throws PulsarAdminException {
        ResourceGroup rg = checkResourceGroupExists(resourceGroupName);

        ResourceGroupOpStatus status = rg.registerUsage(namespaceName, ResourceGroupRefTypes.Namespaces, false,
                                                        this.resourceUsageTransportManagerMgr);
        if (status == ResourceGroupOpStatus.DoesNotExist) {
            String errMesg = String.format("Namespace {} does not yet reference resource group {}",
                namespaceName, resourceGroupName);
            throw new PulsarAdminException(errMesg);
        }

        // Dissociate this NS-name from the RG.
        this.namespaceToRGsMap.remove(namespaceName, rg);
        rgNamespaceUnRegisters.labels(resourceGroupName).inc();
    }

    /**
     * Increments usage stats for the resource groups associated with the given namespace and tenant.
     * Expected to be called when a message is produced or consumed on a topic, or when we calculate
     * usage periodically in the background by going through broker-service stats. [Not yet decided
     * which model we will follow.] Broker-service stats will be cumulative, while calls from the
     * topic produce/consume code will be per-produce/consume.
     *
     * If the tenant and NS are associated with different RGs, the statistics on both RGs are updated.
     * If the tenant and NS are associated with the same RG, the stats on the RG are updated only once
     * (to avoid a direct double-counting).
     * ToDo: will this distinction result in "expected semantics", or shock from users?
     * For now, the only caller is internal to this class.
     *
     * @param tenantName
     * @param nsName
     * @param monClass
     * @param incStats
     * @returns true if the stats were updated; false if nothing was updated.
     */
    public boolean incrementUsage(String tenantName, String nsName,
                                  ResourceGroupMonitoringClass monClass,
                                  BytesAndMessagesCount incStats) throws PulsarAdminException {
        final ResourceGroup nsRG = this.namespaceToRGsMap.get(nsName);
        final ResourceGroup tenantRG = this.tenantToRGsMap.get(tenantName);
        if (tenantRG == null && nsRG == null) {
            return false;
        }

        // Expect stats to increase monotonically.
        if (incStats.bytes < 0 || incStats.messages < 0) {
            String errMesg = String.format("incrementUsage on tenant=%s, NS=%s: bytes (%s) or mesgs (%s) is negative",
                    tenantName, nsName, incStats.bytes, incStats.messages);
            throw new PulsarAdminException(errMesg);
        }

        if (nsRG == tenantRG) {
            // Update only once in this case.
            // Note that we will update both tenant and namespace RGs in other cases.
            nsRG.incrementLocalUsageStats(monClass, incStats);
            rgLocalUsageMessages.labels(nsRG.resourceGroupName, monClass.name()).inc(incStats.messages);
            rgLocalUsageBytes.labels(nsRG.resourceGroupName, monClass.name()).inc(incStats.bytes);
            return true;
        }

        if (tenantRG != null) {
            tenantRG.incrementLocalUsageStats(monClass, incStats);
            rgLocalUsageMessages.labels(tenantRG.resourceGroupName, monClass.name()).inc(incStats.messages);
            rgLocalUsageBytes.labels(tenantRG.resourceGroupName, monClass.name()).inc(incStats.bytes);
        }
        if (nsRG != null) {
            nsRG.incrementLocalUsageStats(monClass, incStats);
            rgLocalUsageMessages.labels(nsRG.resourceGroupName, monClass.name()).inc(incStats.messages);
            rgLocalUsageBytes.labels(nsRG.resourceGroupName, monClass.name()).inc(incStats.bytes);
        }

        return true;
    }

    // Visibility for testing.
    protected BytesAndMessagesCount getRGUsage(String rgName, ResourceGroupMonitoringClass monClass)
                                                                                        throws PulsarAdminException {
        final ResourceGroup rg = this.getResourceGroupInternal(rgName);
        if (rg != null) {
            return rg.getLocalUsageStats(monClass);
        }

        BytesAndMessagesCount retCount = new BytesAndMessagesCount();
        retCount.bytes = -1;
        retCount.messages = -1;
        return retCount;
    }

    /**
     * Get the RG with the given name. For internal operations only.
     */
    private ResourceGroup getResourceGroupInternal(String resourceGroupName) {
        try {
            checkNotNull(resourceGroupName);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Invalid null resource group name: " + resourceGroupName);
        }

        return resourceGroupsMap.get(resourceGroupName);
    }

    private ResourceGroup checkResourceGroupExists(String rgName) throws PulsarAdminException {
        ResourceGroup rg = this.getResourceGroupInternal(rgName);
        if (rg == null) {
            throw new PulsarAdminException("Resource group does not exist: " + rgName);
        }
        return rg;
    }

    // Find the difference between the last time stats were updated for this topic, and the current
    // time. If the difference is positive, update the stats.
    private void updateStatsWithDiff(String topicName, String tenantString, String nsString,
                                     long accByteCount, long accMesgCount, ResourceGroupMonitoringClass monClass) {
        ConcurrentHashMap<String, BytesAndMessagesCount> hm;
        switch (monClass) {
            default:
                log.error("updateStatsWithDiff: Unknown monitoring class={}; ignoring", monClass);
                return;

            case Publish:
                hm = this.topicProduceStats;
                break;

            case Dispatch:
                hm = this.topicConsumeStats;
                break;
        }

        BytesAndMessagesCount bmDiff = new BytesAndMessagesCount();
        BytesAndMessagesCount bmOldCount;
        BytesAndMessagesCount bmNewCount = new BytesAndMessagesCount();

        bmNewCount.bytes = accByteCount;
        bmNewCount.messages = accMesgCount;

        bmOldCount = hm.get(topicName);
        if (bmOldCount == null) {
            bmDiff.bytes = bmNewCount.bytes;
            bmDiff.messages = bmNewCount.messages;
        } else {
            bmDiff.bytes = bmNewCount.bytes - bmOldCount.bytes;
            bmDiff.messages = bmNewCount.messages - bmOldCount.messages;
        }

        if (bmDiff.bytes <= 0 || bmDiff.messages <= 0) {
            return;
        }

        try {
            boolean statsUpdated = this.incrementUsage(tenantString, nsString, monClass, bmDiff);
            log.debug("updateStatsWithDiff: monclass={} statsUpdated={} for tenant={}, namespace={}; "
                            + "by {} bytes, {} mesgs",
                    monClass, statsUpdated, tenantString, nsString,
                    bmDiff.bytes, bmDiff.messages);
            hm.put(topicName, bmNewCount);
        } catch (Throwable t) {
            log.error("updateStatsWithDiff: got ex={} while aggregating for {} side",
                    t.getMessage(), monClass);
        }
    }

    // Visibility for testing.
    protected static double getRgQuotaByteCount (String rgName, String monClassName) {
        return rgCalculatedQuotaBytes.labels(rgName, monClassName).get();
    }

    // Visibility for testing.
    protected static double getRgQuotaMessageCount (String rgName, String monClassName) {
        return rgCalculatedQuotaMessages.labels(rgName, monClassName).get();
    }

    // Visibility for testing.
    protected static double getRgLocalUsageByteCount (String rgName, String monClassName) {
        return rgLocalUsageBytes.labels(rgName, monClassName).get();
    }

    // Visibility for testing.
    protected static double getRgLocalUsageMessageCount (String rgName, String monClassName) {
        return rgLocalUsageMessages.labels(rgName, monClassName).get();
    }

    // Visibility for testing.
    protected static double getRgUpdatesCount (String rgName) {
        return rgUpdates.labels(rgName).get();
    }

    // Visibility for testing.
    protected static double getRgTenantRegistersCount (String rgName) {
        return rgTenantRegisters.labels(rgName).get();
    }

    // Visibility for testing.
    protected static double getRgTenantUnRegistersCount (String rgName) {
        return rgTenantUnRegisters.labels(rgName).get();
    }

    // Visibility for testing.
    protected static double getRgNamespaceRegistersCount (String rgName) {
        return rgNamespaceRegisters.labels(rgName).get();
    }

    // Visibility for testing.
    protected static double getRgNamespaceUnRegistersCount (String rgName) {
        return rgNamespaceUnRegisters.labels(rgName).get();
    }

    // Visibility for testing.
    protected static Summary.Child.Value getRgUsageAggregationLatency() {
        return rgUsageAggregationLatency.get();
    }

    // Visibility for testing.
    protected static Summary.Child.Value getRgQuotaCalculationTime() {
        return rgQuotaCalculationLatency.get();
    }

    // Periodically aggregate the usage from all topics known to the BrokerService.
    // Visibility for unit testing.
    protected void aggregateResourceGroupLocalUsages() {
        final Summary.Timer aggrUsageTimer = rgUsageAggregationLatency.startTimer();
        BrokerService bs = this.pulsar.getBrokerService();
        Map<String, TopicStatsImpl> topicStatsMap = bs.getTopicStats();

        for (Map.Entry<String, TopicStatsImpl> entry : topicStatsMap.entrySet()) {
            String topicName = entry.getKey();
            TopicStatsImpl topicStats = entry.getValue();
            TopicName topic = TopicName.get(topicName);
            String tenantString = topic.getTenant();
            String nsString = topic.getNamespacePortion();

            if (!this.tenantToRGsMap.containsKey(tenantString) && !this.namespaceToRGsMap.containsKey(nsString)) {
                // This topic's NS/tenant are not registered to any RG.
                continue;
            }

            for (ResourceGroupMonitoringClass monClass : ResourceGroupMonitoringClass.values()) {
                this.updateStatsWithDiff(topicName, tenantString, nsString,
                        topicStats.bytesInCounter, topicStats.msgInCounter, monClass);
            }
        }
        double diffTimeSeconds = aggrUsageTimer.observeDuration();
        log.debug("aggregateResourceGroupLocalUsages took {} milliseconds", diffTimeSeconds * 1000);

        // Check any re-scheduling requirements for next time.
        // Use the same period as getResourceUsagePublishIntervalInSecs;
        // cancel and re-schedule this task if the period of execution has changed.
        ServiceConfiguration config = pulsar.getConfiguration();
        long newPeriodInSeconds = config.getResourceUsageTransportPublishIntervalInSecs();
        if (newPeriodInSeconds != this.aggregateLocalUsagePeriodInSeconds) {
            if (this.aggreagteLocalUsagePeriodicTask == null) {
                log.error("aggregateResourceGroupLocalUsages: Unable to find running task to cancel when "
                                + "publish period changed from {} to {} {}",
                        this.aggregateLocalUsagePeriodInSeconds, newPeriodInSeconds, timeUnitScale);
            } else {
                boolean cancelStatus = this.aggreagteLocalUsagePeriodicTask.cancel(true);
                log.info("aggregateResourceGroupLocalUsages: Got status={} in cancel of periodic "
                                + "when publish period changed from {} to {} {}",
                        cancelStatus, this.aggregateLocalUsagePeriodInSeconds, newPeriodInSeconds, timeUnitScale);
            }
            this.aggreagteLocalUsagePeriodicTask = pulsar.getExecutor().scheduleAtFixedRate(
                    this::aggregateResourceGroupLocalUsages,
                    newPeriodInSeconds,
                    newPeriodInSeconds,
                    timeUnitScale);
            this.aggregateLocalUsagePeriodInSeconds = newPeriodInSeconds;
        }
    }

    // Periodically calculate the updated quota for all RGs in the background,
    // from the reports received from other brokers.
    private void calculateQuotaForAllResourceGroups() {
        // Calculate the quota for the next window for this RG, based on the observed usage.
        final Summary.Timer quotaCalcTimer = rgQuotaCalculationLatency.startTimer();
        BytesAndMessagesCount updatedQuota = new BytesAndMessagesCount();
        this.resourceGroupsMap.forEach((rgName, resourceGroup) -> {
            BytesAndMessagesCount globalUsageStats;
            BytesAndMessagesCount localUsageStats;
            BytesAndMessagesCount confCounts;
            for (ResourceGroupMonitoringClass monClass : ResourceGroupMonitoringClass.values()) {
                try {
                    globalUsageStats = resourceGroup.getGlobalUsageStats(monClass);
                    localUsageStats = resourceGroup.getLocalUsageStats(monClass);
                    confCounts = resourceGroup.getConfLimits(monClass);

                    long[] globUsageBytesArray = new long[String.valueOf(globalUsageStats.bytes).length()];
                    updatedQuota.bytes = this.quotaCalculator.computeLocalQuota(
                            confCounts.bytes,
                            localUsageStats.bytes,
                            globUsageBytesArray);

                    long[] globUsageMessagesArray = new long[String.valueOf(globalUsageStats.messages).length()];
                    updatedQuota.messages = this.quotaCalculator.computeLocalQuota(
                            confCounts.messages,
                            localUsageStats.messages,
                            globUsageMessagesArray);

                    BytesAndMessagesCount oldBMCount = resourceGroup.updateLocalQuota(monClass, updatedQuota);
                    rgCalculatedQuotaMessages.labels(rgName, monClass.name()).inc(updatedQuota.messages);
                    rgCalculatedQuotaBytes.labels(rgName, monClass.name()).inc(updatedQuota.bytes);
                    long messagesIncrement = updatedQuota.messages - oldBMCount.messages;
                    long bytesIncrement = updatedQuota.bytes - oldBMCount.bytes;
                    log.debug("calculateQuota for RG {} [class {}]: bytes incremented by {}, messages by {}",
                            rgName, monClass, messagesIncrement, bytesIncrement);
                } catch (Throwable t) {
                    log.error("Got exception={} while calculating new quota for monitoring-class={} of RG={}",
                            t.getMessage(), monClass, rgName);
                }
            }
        });
        double diffTimeSeconds = quotaCalcTimer.observeDuration();
        log.debug("calculateQuotaForAllResourceGroups took {} milliseconds", diffTimeSeconds * 1000);

        // Check any re-scheduling requirements for next time.
        // Use the same period as getResourceUsagePublishIntervalInSecs;
        // cancel and re-schedule this task if the period of execution has changed.
        ServiceConfiguration config = pulsar.getConfiguration();
        long newPeriodInSeconds = config.getResourceUsageTransportPublishIntervalInSecs();
        if (newPeriodInSeconds != this.resourceUsagePublishPeriodInSeconds) {
            if (this.calculateQuotaPeriodicTask == null) {
                log.error("calculateQuotaForAllResourceGroups: Unable to find running task to cancel when "
                                + "publish period changed from {} to {} {}",
                        this.resourceUsagePublishPeriodInSeconds, newPeriodInSeconds, timeUnitScale);
            } else {
                boolean cancelStatus = this.calculateQuotaPeriodicTask.cancel(true);
                log.info("calculateQuotaForAllResourceGroups: Got status={} in cancel of periodic "
                        + " when publish period changed from {} to {} {}",
                        cancelStatus, this.resourceUsagePublishPeriodInSeconds, newPeriodInSeconds, timeUnitScale);
            }
            this.calculateQuotaPeriodicTask = pulsar.getExecutor().scheduleAtFixedRate(
                        this::calculateQuotaForAllResourceGroups,
                        newPeriodInSeconds,
                        newPeriodInSeconds,
                        timeUnitScale);
            this.resourceUsagePublishPeriodInSeconds = newPeriodInSeconds;
            this.maxIntervalForSuppressingReportsMSecs =
                    this.resourceUsagePublishPeriodInSeconds * this.MaxUsageReportSuppressRounds;
        }
    }

    private void initialize() {
        ServiceConfiguration config = this.pulsar.getConfiguration();
        long periodInSecs = config.getResourceUsageTransportPublishIntervalInSecs();
        this.aggregateLocalUsagePeriodInSeconds = this.resourceUsagePublishPeriodInSeconds = periodInSecs;
        this.aggreagteLocalUsagePeriodicTask = this.pulsar.getExecutor().scheduleAtFixedRate(
                    this::aggregateResourceGroupLocalUsages,
                    periodInSecs,
                    periodInSecs,
                    this.timeUnitScale);
        this.calculateQuotaPeriodicTask = this.pulsar.getExecutor().scheduleAtFixedRate(
                    this::calculateQuotaForAllResourceGroups,
                    periodInSecs,
                    periodInSecs,
                    this.timeUnitScale);
        this.maxIntervalForSuppressingReportsMSecs =
                this.resourceUsagePublishPeriodInSeconds * this.MaxUsageReportSuppressRounds;

    }

    private void checkRGCreateParams(String rgName, org.apache.pulsar.common.policies.data.ResourceGroup rgConfig)
      throws PulsarAdminException {
        try {
            checkNotNull(rgConfig);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("ResourceGroupCreate: Invalid null ResourceGroup config");
        }

        if (rgName.isEmpty()) {
            throw new IllegalArgumentException("ResourceGroupCreate: can't create resource group with an empty name");
        }

        ResourceGroup rg = getResourceGroupInternal(rgName);
        if (rg != null) {
            throw new PulsarAdminException("Resource group already exists:" + rgName);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ResourceGroupService.class);

    @Getter
    private final PulsarService pulsar;

    protected final ResourceQuotaCalculator quotaCalculator;
    private ResourceUsageTransportManager resourceUsageTransportManagerMgr;
    private final ResourceGroupConfigListener rgConfigListener;

    // Given a RG-name, get the resource-group
    private ConcurrentHashMap<String, ResourceGroup> resourceGroupsMap = new ConcurrentHashMap<>();

    // Given a tenant-name, get its associated resource-group
    private ConcurrentHashMap<String, ResourceGroup> tenantToRGsMap = new ConcurrentHashMap<>();

    // Given a NS-name, get its associated resource-group
    private ConcurrentHashMap<String, ResourceGroup> namespaceToRGsMap = new ConcurrentHashMap<>();

    // Maps to maintain the usage per topic, in produce/consume directions.
    private ConcurrentHashMap<String, BytesAndMessagesCount> topicProduceStats = new ConcurrentHashMap();
    private ConcurrentHashMap<String, BytesAndMessagesCount> topicConsumeStats = new ConcurrentHashMap();


    // The task that periodically re-calculates the quota budget for local usage.
    private ScheduledFuture<?> aggreagteLocalUsagePeriodicTask;
    private long aggregateLocalUsagePeriodInSeconds;

    // The task that periodically re-calculates the quota budget for local usage.
    private ScheduledFuture<?> calculateQuotaPeriodicTask;
    private long resourceUsagePublishPeriodInSeconds;

    // Allow a pluggable scale on time units; for testing periodic functionality.
    private TimeUnit timeUnitScale;

    // The maximum number of successive rounds that we can suppress reporting local usage, because there was no
    // substantial change from the prior round. This is to ensure the reporting does not become too chatty.
    // Set this value to one more than the cadence of sending reports; e.g., if you want to send every 3rd report,
    // set the value to 4.
    // Setting this to 0 will make us report in every round.
    // Don't set to negative values; behavior will be "undefined".
    protected static final int MaxUsageReportSuppressRounds = 5;

    // Convenient shorthand, for MaxUsageReportSuppressRounds converted to a time interval in milliseconds.
    protected static long maxIntervalForSuppressingReportsMSecs;

    // The percentage difference that is considered "within limits" to suppress usage reporting.
    // Setting this to 0 will also make us report in every round.
    // Don't set it to negative values; behavior will be "undefined".
    protected static final float UsageReportSuppressionTolerancePercentage = 5;

    // Labels for the various counters used here.
    private static final String[] resourceGroupLabel = {"ResourceGroup"};
    private static final String[] resourceGroupMonitoringclassLabels = {"ResourceGroup", "MonitoringClass"};

    private static final Counter rgCalculatedQuotaBytes = Counter.build()
            .name("pulsar_resource_group_calculated_bytes_quota")
            .help("Bytes quota calculated for resource group")
            .labelNames(resourceGroupMonitoringclassLabels)
            .register();
    private static final Counter rgCalculatedQuotaMessages = Counter.build()
            .name("pulsar_resource_group_calculated_messages_quota")
            .help("Messages quota calculated for resource group")
            .labelNames(resourceGroupMonitoringclassLabels)
            .register();

    private static final Counter rgLocalUsageBytes = Counter.build()
            .name("pulsar_resource_group_bytes_used")
            .help("Bytes locally used within this resource group during the last aggregation interval")
            .labelNames(resourceGroupMonitoringclassLabels)
            .register();
    private static final Counter rgLocalUsageMessages = Counter.build()
            .name("pulsar_resource_group_messages_used")
            .help("Messages locally used within this resource group during the last aggregation interval")
            .labelNames(resourceGroupMonitoringclassLabels)
            .register();

    private static final Counter rgUpdates = Counter.build()
            .name("pulsar_resource_group_updates")
            .help("Number of update operations on the given resource group")
            .labelNames(resourceGroupLabel)
            .register();

    private static final Counter rgTenantRegisters = Counter.build()
            .name("pulsar_resource_group_tenant_registers")
            .help("Number of registrations of tenants")
            .labelNames(resourceGroupLabel)
            .register();
    private static final Counter rgTenantUnRegisters = Counter.build()
            .name("pulsar_resource_group_tenant_unregisters")
            .help("Number of un-registrations of tenants")
            .labelNames(resourceGroupLabel)
            .register();

    private static final Counter rgNamespaceRegisters = Counter.build()
            .name("pulsar_resource_group_namespace_registers")
            .help("Number of registrations of namespaces")
            .labelNames(resourceGroupLabel)
            .register();
    private static final Counter rgNamespaceUnRegisters = Counter.build()
            .name("pulsar_resource_group_namespace_unregisters")
            .help("Number of un-registrations of namespaces")
            .labelNames(resourceGroupLabel)
            .register();

    private static final Summary rgUsageAggregationLatency = Summary.build()
            .quantile(0.5, 0.05)
            .quantile(0.9, 0.01)
            .name("pulsar_resource_group_aggregate_usage_secs")
            .help("Time required to aggregate usage of all resource groups, in seconds.")
            .register();

    private static final Summary rgQuotaCalculationLatency = Summary.build()
            .quantile(0.5, 0.05)
            .quantile(0.9, 0.01)
            .name("pulsar_resource_group_calculate_quota_secs")
            .help("Time required to calculate quota of all resource groups, in seconds.")
            .register();
}
