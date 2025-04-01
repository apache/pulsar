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
package org.apache.pulsar.broker.loadbalance.extensions.scheduler;

import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.CoolDown;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.HitCount;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBrokers;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBundles;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoLoadData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.OutDatedData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Overloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Underloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.broker.loadbalance.extensions.policies.AntiAffinityGroupPolicyHelper;
import org.apache.pulsar.broker.loadbalance.extensions.policies.IsolationPoliciesHelper;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load shedding strategy that unloads bundles from the highest loaded brokers.
 * This strategy is only configurable in the broker load balancer extensions introduced by
 * PIP-192[https://github.com/apache/pulsar/issues/16691].
 *
 * This load shedding strategy has the following goals:
 * 1. Distribute bundle load across brokers in order to make the standard deviation of the avg resource usage,
 * std(exponential-moving-avg(max(cpu, memory, network, throughput)) for each broker) below the target,
 * configurable by loadBalancerBrokerLoadTargetStd.
 * 2. Use the transfer protocol to transfer bundle load from the highest loaded to the lowest loaded brokers,
 * if configured by loadBalancerTransferEnabled=true.
 * 3. Avoid repeated bundle unloading by recomputing historical broker resource usage after unloading and also
 * skipping the bundles that are recently unloaded.
 * 4. Prioritize unloading bundles to underloaded brokers when their message throughput is zero(new brokers).
 * 5. Do not use outdated broker load data (configurable by loadBalancerBrokerLoadDataTTLInSeconds).
 * 6. Give enough time for each broker to recompute its load after unloading
 * (configurable by loadBalanceUnloadDelayInSeconds)
 * 7. Do not transfer bundles with namespace isolation policies or anti-affinity group policies.
 * 8. Limit the max number of brokers to transfer bundle load for each cycle,
 * (loadBalancerMaxNumberOfBrokerTransfersPerCycle).
 * 9. Print more logs with a debug option(loadBalancerDebugModeEnabled=true).
 */
@NoArgsConstructor
public class TransferShedder implements NamespaceUnloadStrategy {
    private static final Logger log = LoggerFactory.getLogger(TransferShedder.class);
    private static final double KB = 1024;
    private static final String CANNOT_CONTINUE_UNLOAD_MSG = "Can't continue the unload cycle.";
    private static final String CANNOT_UNLOAD_BROKER_MSG = "Can't unload broker:%s.";
    private static final String CANNOT_UNLOAD_BUNDLE_MSG = "Can't unload bundle:%s.";
    private final LoadStats stats = new LoadStats();
    private PulsarService pulsar;
    private IsolationPoliciesHelper isolationPoliciesHelper;
    private AntiAffinityGroupPolicyHelper antiAffinityGroupPolicyHelper;
    private List<BrokerFilter> brokerFilterPipeline;

    private Set<UnloadDecision> decisionCache;
    @Getter
    private UnloadCounter counter;
    private ServiceUnitStateChannel channel;
    private int unloadConditionHitCount = 0;

    @VisibleForTesting
    public TransferShedder(UnloadCounter counter){
        this.pulsar = null;
        this.decisionCache = new HashSet<>();
        this.counter = counter;
        this.isolationPoliciesHelper = null;
        this.antiAffinityGroupPolicyHelper = null;
    }

    public TransferShedder(PulsarService pulsar,
                           UnloadCounter counter,
                           List<BrokerFilter> brokerFilterPipeline,
                           IsolationPoliciesHelper isolationPoliciesHelper,
                           AntiAffinityGroupPolicyHelper antiAffinityGroupPolicyHelper){
        this.pulsar = pulsar;
        this.decisionCache = new HashSet<>();
        this.counter = counter;
        this.isolationPoliciesHelper = isolationPoliciesHelper;
        this.antiAffinityGroupPolicyHelper = antiAffinityGroupPolicyHelper;
        this.channel = ServiceUnitStateChannelImpl.get(pulsar);
        this.brokerFilterPipeline = brokerFilterPipeline;
    }

    @Override
    public void initialize(PulsarService pulsar){
        this.pulsar = pulsar;
        this.decisionCache = new HashSet<>();
        var manager = ExtensibleLoadManagerImpl.get(pulsar.getLoadManager().get());
        this.counter = manager.getUnloadCounter();
        this.isolationPoliciesHelper = manager.getIsolationPoliciesHelper();
        this.antiAffinityGroupPolicyHelper = manager.getAntiAffinityGroupPolicyHelper();
        this.channel = ServiceUnitStateChannelImpl.get(pulsar);
        this.brokerFilterPipeline = manager.getBrokerFilterPipeline();
    }


    @Getter
    @Accessors(fluent = true)
    static class LoadStats {
        private double sum;
        private double sqSum;
        private int totalBrokers;
        private double avg;
        private double std;
        private LoadDataStore<BrokerLoadData> loadDataStore;
        private List<Map.Entry<String, BrokerLoadData>> brokersSortedByLoad;
        int maxBrokerIndex;
        int minBrokerIndex;
        int numberOfBrokerSheddingPerCycle;
        int maxNumberOfBrokerSheddingPerCycle;
        LoadStats() {
            brokersSortedByLoad = new ArrayList<>();
        }

        private void update(double sum, double sqSum, int totalBrokers) {
            this.sum = sum;
            this.sqSum = sqSum;
            this.totalBrokers = totalBrokers;

            if (totalBrokers == 0) {
                this.avg = 0;
                this.std = 0;

            } else {
                this.avg = sum / totalBrokers;
                this.std = Math.sqrt(sqSum / totalBrokers - avg * avg);
            }
        }

        void offload(double max, double min, double offload) {
            sqSum -= max * max + min * min;
            double maxd = Math.max(0, max - offload);
            double mind = min + offload;
            sqSum += maxd * maxd + mind * mind;
            std = Math.sqrt(Math.abs(sqSum / totalBrokers - avg * avg));
            numberOfBrokerSheddingPerCycle++;
            minBrokerIndex++;
        }

        void clear() {
            sum = 0.0;
            sqSum = 0.0;
            totalBrokers = 0;
            avg = 0.0;
            std = 0.0;
            maxBrokerIndex = 0;
            minBrokerIndex = 0;
            numberOfBrokerSheddingPerCycle = 0;
            maxNumberOfBrokerSheddingPerCycle = 0;
            brokersSortedByLoad.clear();
            loadDataStore = null;
        }

        Optional<UnloadDecision.Reason> update(final LoadDataStore<BrokerLoadData> loadStore,
                                               final Map<String, BrokerLookupData> availableBrokers,
                                               Map<String, Long> recentlyUnloadedBrokers,
                                               final ServiceConfiguration conf) {

            maxNumberOfBrokerSheddingPerCycle = conf.getLoadBalancerMaxNumberOfBrokerSheddingPerCycle();
            var debug = ExtensibleLoadManagerImpl.debug(conf, log);
            UnloadDecision.Reason decisionReason = null;
            double sum = 0.0;
            double sqSum = 0.0;
            int totalBrokers = 0;
            long now = System.currentTimeMillis();
            var missingLoadDataBrokers = new HashSet<>(availableBrokers.keySet());
            for (Map.Entry<String, BrokerLoadData> entry : loadStore.entrySet()) {
                BrokerLoadData localBrokerData = entry.getValue();
                String broker = entry.getKey();
                missingLoadDataBrokers.remove(broker);
                // We don't want to use the outdated load data.
                if (now - localBrokerData.getUpdatedAt()
                        > conf.getLoadBalancerBrokerLoadDataTTLInSeconds() * 1000) {
                    log.warn(
                            "Ignoring broker:{} load update because the load data timestamp:{} is too old.",
                            broker, localBrokerData.getUpdatedAt());
                    decisionReason = OutDatedData;
                    continue;
                }

                // Also, we should give enough time for each broker to recompute its load after transfers.
                if (recentlyUnloadedBrokers.containsKey(broker)) {
                    var elapsed = localBrokerData.getUpdatedAt() - recentlyUnloadedBrokers.get(broker);
                    if (elapsed < conf.getLoadBalanceSheddingDelayInSeconds() * 1000) {
                        if (debug) {
                            log.warn(
                                    "Broker:{} load data is too early since "
                                            + "the last transfer. elapsed {} secs < threshold {} secs",
                                    broker,
                                    TimeUnit.MILLISECONDS.toSeconds(elapsed),
                                    conf.getLoadBalanceSheddingDelayInSeconds());
                        }
                        update(0.0, 0.0, 0);
                        return Optional.of(CoolDown);
                    } else {
                        recentlyUnloadedBrokers.remove(broker);
                    }
                }

                double load = localBrokerData.getWeightedMaxEMA();

                sum += load;
                sqSum += load * load;
                totalBrokers++;
            }

            if (totalBrokers == 0) {
                if (decisionReason == null) {
                    decisionReason = NoBrokers;
                }
                update(0.0, 0.0, 0);
                if (debug) {
                    log.info("There is no broker load data.");
                }
                return Optional.of(decisionReason);
            }

            if (!missingLoadDataBrokers.isEmpty()) {
                decisionReason = NoLoadData;
                update(0.0, 0.0, 0);
                if (debug) {
                    log.info("There is missing load data from brokers:{}", missingLoadDataBrokers);
                }
                return Optional.of(decisionReason);
            }

            update(sum, sqSum, totalBrokers);
            return Optional.empty();
        }

        void setLoadDataStore(LoadDataStore<BrokerLoadData> loadDataStore) {
            this.loadDataStore = loadDataStore;
            brokersSortedByLoad.addAll(loadDataStore.entrySet());
            brokersSortedByLoad.sort(Comparator.comparingDouble(
                    a -> a.getValue().getWeightedMaxEMA()));
            maxBrokerIndex = brokersSortedByLoad.size() - 1;
            minBrokerIndex = 0;
        }

        String peekMinBroker() {
            return brokersSortedByLoad.get(minBrokerIndex).getKey();
        }

        String peekMaxBroker() {
            return brokersSortedByLoad.get(maxBrokerIndex).getKey();
        }

        String pollMaxBroker() {
            return brokersSortedByLoad.get(maxBrokerIndex--).getKey();
        }

        @Override
        public String toString() {
            return String.format(
                    "sum:%.2f, sqSum:%.2f, avg:%.2f, std:%.2f, totalBrokers:%d, brokersSortedByLoad:%s",
                    sum, sqSum, avg, std, totalBrokers,
                    brokersSortedByLoad.stream().map(v->v.getKey()).collect(Collectors.toList()));
        }


        boolean hasTransferableBrokers() {
            return numberOfBrokerSheddingPerCycle < maxNumberOfBrokerSheddingPerCycle
                    && minBrokerIndex < maxBrokerIndex;
        }
    }


    @Override
    public Set<UnloadDecision> findBundlesForUnloading(LoadManagerContext context,
                                                  Map<String, Long> recentlyUnloadedBundles,
                                                  Map<String, Long> recentlyUnloadedBrokers) {
        final var conf = context.brokerConfiguration();
        decisionCache.clear();
        stats.clear();
        Map<String, BrokerLookupData> availableBrokers;
        try {
            availableBrokers = context.brokerRegistry().getAvailableBrokerLookupDataAsync()
                    .get(context.brokerConfiguration().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            counter.update(Failure, Unknown);
            log.warn("Failed to fetch available brokers. Stop unloading.", e);
            return decisionCache;
        }

        try {
            final var loadStore = context.brokerLoadDataStore();
            stats.setLoadDataStore(loadStore);
            boolean debugMode = ExtensibleLoadManagerImpl.debug(conf, log);

            var skipReason = stats.update(
                    context.brokerLoadDataStore(), availableBrokers, recentlyUnloadedBrokers, conf);
            if (skipReason.isPresent()) {
                if (debugMode) {
                    log.warn(CANNOT_CONTINUE_UNLOAD_MSG
                                    + " Skipped the load stat update. Reason:{}.",
                            skipReason.get());
                }
                counter.update(Skip, skipReason.get());
                return decisionCache;
            }
            counter.updateLoadData(stats.avg, stats.std);



            if (debugMode) {
                log.info("brokers' load stats:{}", stats);
            }

            // skip metrics
            int numOfBrokersWithEmptyLoadData = 0;
            int numOfBrokersWithFewBundles = 0;

            final double targetStd = conf.getLoadBalancerBrokerLoadTargetStd();
            boolean transfer = conf.isLoadBalancerTransferEnabled();
            if (stats.std() > targetStd
                    || isUnderLoaded(context, stats.peekMinBroker(), stats)
                    || isOverLoaded(context, stats.peekMaxBroker(), stats.avg)) {
                unloadConditionHitCount++;
            } else {
                unloadConditionHitCount = 0;
            }

            if (unloadConditionHitCount <= conf.getLoadBalancerSheddingConditionHitCountThreshold()) {
                if (debugMode) {
                    log.info(CANNOT_CONTINUE_UNLOAD_MSG
                                    + " Shedding condition hit count:{} is less than or equal to the threshold:{}.",
                            unloadConditionHitCount, conf.getLoadBalancerSheddingConditionHitCountThreshold());
                }
                counter.update(Skip, HitCount);
                return decisionCache;
            }

            while (true) {
                if (!stats.hasTransferableBrokers()) {
                    if (debugMode) {
                        log.info(CANNOT_CONTINUE_UNLOAD_MSG
                                + " Exhausted target transfer brokers.");
                    }
                    break;
                }
                UnloadDecision.Reason reason;
                if (stats.std() > targetStd) {
                    reason = Overloaded;
                } else if (isUnderLoaded(context, stats.peekMinBroker(), stats)) {
                    reason = Underloaded;
                    if (debugMode) {
                        log.info(String.format("broker:%s is underloaded:%s although "
                                        + "load std:%.2f <= targetStd:%.2f. "
                                        + "Continuing unload for this underloaded broker.",
                                stats.peekMinBroker(),
                                context.brokerLoadDataStore().get(stats.peekMinBroker()).get(),
                                stats.std(), targetStd));
                    }
                } else if (isOverLoaded(context, stats.peekMaxBroker(), stats.avg)) {
                    reason = Overloaded;
                    if (debugMode) {
                        log.info(String.format("broker:%s is overloaded:%s although "
                                        + "load std:%.2f <= targetStd:%.2f. "
                                        + "Continuing unload for this overloaded broker.",
                                stats.peekMaxBroker(),
                                context.brokerLoadDataStore().get(stats.peekMaxBroker()).get(),
                                stats.std(), targetStd));
                    }
                } else {
                    if (debugMode) {
                        log.info(CANNOT_CONTINUE_UNLOAD_MSG
                                        + "The overall cluster load meets the target, std:{} <= targetStd:{}."
                                        + "minBroker:{} is not underloaded. maxBroker:{} is not overloaded.",
                                stats.std(), targetStd, stats.peekMinBroker(), stats.peekMaxBroker());
                    }
                    break;
                }

                String maxBroker = stats.pollMaxBroker();
                String minBroker = stats.peekMinBroker();
                Optional<BrokerLoadData> maxBrokerLoadData = context.brokerLoadDataStore().get(maxBroker);
                Optional<BrokerLoadData> minBrokerLoadData = context.brokerLoadDataStore().get(minBroker);
                if (maxBrokerLoadData.isEmpty()) {
                    log.error(String.format(CANNOT_UNLOAD_BROKER_MSG
                            + " MaxBrokerLoadData is empty.", maxBroker));
                    numOfBrokersWithEmptyLoadData++;
                    continue;
                }
                if (minBrokerLoadData.isEmpty()) {
                    log.error("Can't transfer load to broker:{}. MinBrokerLoadData is empty.", minBroker);
                    numOfBrokersWithEmptyLoadData++;
                    continue;
                }
                double maxLoad = maxBrokerLoadData.get().getWeightedMaxEMA();
                double minLoad = minBrokerLoadData.get().getWeightedMaxEMA();
                double offload = (maxLoad - minLoad) / 2;
                BrokerLoadData brokerLoadData = maxBrokerLoadData.get();
                double maxBrokerThroughput = brokerLoadData.getMsgThroughputIn()
                        + brokerLoadData.getMsgThroughputOut();
                double minBrokerThroughput = minBrokerLoadData.get().getMsgThroughputIn()
                        + minBrokerLoadData.get().getMsgThroughputOut();
                double offloadThroughput = maxBrokerThroughput * offload / maxLoad;

                if (debugMode) {
                    log.info(String.format(
                            "Attempting to shed load from broker:%s%s, which has the max resource "
                                    + "usage:%.2f%%, targetStd:%.2f,"
                                    + " -- Trying to offload %.2f%%, %.2f KByte/s of traffic.",
                            maxBroker, transfer ? " to broker:" + minBroker : "",
                            maxLoad * 100,
                            targetStd,
                            offload * 100,
                            offloadThroughput / KB
                    ));
                }

                double trafficMarkedToOffload = 0;
                double trafficMarkedToGain = 0;

                Optional<TopBundlesLoadData> bundlesLoadData = context.topBundleLoadDataStore().get(maxBroker);
                if (bundlesLoadData.isEmpty() || bundlesLoadData.get().getTopBundlesLoadData().isEmpty()) {
                    log.error(String.format(CANNOT_UNLOAD_BROKER_MSG
                            + " TopBundlesLoadData is empty.", maxBroker));
                    numOfBrokersWithEmptyLoadData++;
                    continue;
                }

                var maxBrokerTopBundlesLoadData = bundlesLoadData.get().getTopBundlesLoadData();
                if (maxBrokerTopBundlesLoadData.size() == 1) {
                    numOfBrokersWithFewBundles++;
                    log.warn(String.format(CANNOT_UNLOAD_BROKER_MSG
                                    + " Sole namespace bundle:%s is overloading the broker. ",
                            maxBroker, maxBrokerTopBundlesLoadData.iterator().next()));
                    continue;
                }
                Optional<TopBundlesLoadData> minBundlesLoadData = context.topBundleLoadDataStore().get(minBroker);
                var minBrokerTopBundlesLoadDataIter =
                        minBundlesLoadData.isPresent() ? minBundlesLoadData.get().getTopBundlesLoadData().iterator() :
                                null;


                if (maxBrokerTopBundlesLoadData.isEmpty()) {
                    numOfBrokersWithFewBundles++;
                    log.warn(String.format(CANNOT_UNLOAD_BROKER_MSG
                            + " Broker overloaded despite having no bundles", maxBroker));
                    continue;
                }

                int remainingTopBundles = maxBrokerTopBundlesLoadData.size();
                for (var e : maxBrokerTopBundlesLoadData) {
                    String bundle = e.bundleName();
                    if (channel != null && !channel.isOwner(bundle, maxBroker)) {
                        if (debugMode) {
                            log.warn(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                    + " MaxBroker:%s is not the owner.", bundle, maxBroker));
                        }
                        continue;
                    }
                    if (recentlyUnloadedBundles.containsKey(bundle)) {
                        if (debugMode) {
                            log.info(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                            + " Bundle has been recently unloaded at ts:%d.",
                                    bundle, recentlyUnloadedBundles.get(bundle)));
                        }
                        continue;
                    }
                    if (!isTransferable(context, availableBrokers, bundle, maxBroker, Optional.of(minBroker))) {
                        if (debugMode) {
                            log.info(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                    + " This unload can't meet "
                                    + "affinity(isolation) or anti-affinity group policies.", bundle));
                        }
                        continue;
                    }
                    if (remainingTopBundles <= 1) {
                        if (debugMode) {
                            log.info(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                            + " The remaining bundles in TopBundlesLoadData from the maxBroker:%s is"
                                            + " less than or equal to 1.",
                                    bundle, maxBroker));
                        }
                        break;
                    }

                    var bundleData = e.stats();
                    double maxBrokerBundleThroughput = bundleData.msgThroughputIn + bundleData.msgThroughputOut;
                    if (maxBrokerBundleThroughput == 0) {
                        if (debugMode) {
                            log.info(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                    + " It has zero throughput.", bundle));
                        }
                        continue;
                    }
                    boolean swap = false;
                    List<Unload> minToMaxUnloads = new ArrayList<>();
                    double minBrokerBundleSwapThroughput = 0.0;
                    if (trafficMarkedToOffload - trafficMarkedToGain + maxBrokerBundleThroughput > offloadThroughput) {
                        // see if we can swap bundles from min to max broker to balance better.
                        if (transfer && minBrokerTopBundlesLoadDataIter != null) {
                            var maxBrokerNewThroughput =
                                    maxBrokerThroughput - trafficMarkedToOffload + trafficMarkedToGain
                                            - maxBrokerBundleThroughput;
                            var minBrokerNewThroughput =
                                    minBrokerThroughput + trafficMarkedToOffload - trafficMarkedToGain
                                            + maxBrokerBundleThroughput;
                            while (minBrokerTopBundlesLoadDataIter.hasNext()) {
                                var minBrokerBundleData = minBrokerTopBundlesLoadDataIter.next();
                                if (!isTransferable(context, availableBrokers,
                                        minBrokerBundleData.bundleName(), minBroker, Optional.of(maxBroker))) {
                                    continue;
                                }
                                var minBrokerBundleThroughput =
                                        minBrokerBundleData.stats().msgThroughputIn
                                                + minBrokerBundleData.stats().msgThroughputOut;
                                if (minBrokerBundleThroughput == 0) {
                                    continue;
                                }
                                var maxBrokerNewThroughputTmp = maxBrokerNewThroughput + minBrokerBundleThroughput;
                                var minBrokerNewThroughputTmp = minBrokerNewThroughput - minBrokerBundleThroughput;
                                if (maxBrokerNewThroughputTmp < maxBrokerThroughput
                                        && minBrokerNewThroughputTmp < maxBrokerThroughput) {
                                    minToMaxUnloads.add(new Unload(minBroker,
                                            minBrokerBundleData.bundleName(), Optional.of(maxBroker)));
                                    maxBrokerNewThroughput = maxBrokerNewThroughputTmp;
                                    minBrokerNewThroughput = minBrokerNewThroughputTmp;
                                    minBrokerBundleSwapThroughput += minBrokerBundleThroughput;
                                    if (minBrokerNewThroughput <= maxBrokerNewThroughput
                                            && maxBrokerNewThroughput < maxBrokerThroughput * 0.75) {
                                        swap = true;
                                        break;
                                    }
                                }
                            }
                        }
                        if (!swap) {
                            if (debugMode) {
                                log.info(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                                + " The traffic to unload:%.2f - gain:%.2f = %.2f KByte/s is "
                                                + "greater than the target :%.2f KByte/s.",
                                        bundle,
                                        (trafficMarkedToOffload + maxBrokerBundleThroughput) / KB,
                                        trafficMarkedToGain / KB,
                                        (trafficMarkedToOffload - trafficMarkedToGain + maxBrokerBundleThroughput) / KB,
                                        offloadThroughput / KB));
                            }
                            break;
                        }
                    }
                    Unload unload;
                    if (transfer) {
                        if (swap) {
                            minToMaxUnloads.forEach(minToMaxUnload -> {
                                if (debugMode) {
                                    log.info("Decided to gain bundle:{} from min broker:{}",
                                            minToMaxUnload.serviceUnit(), minToMaxUnload.sourceBroker());
                                }
                                var decision = new UnloadDecision();
                                decision.setUnload(minToMaxUnload);
                                decision.succeed(reason);
                                decisionCache.add(decision);
                            });
                            if (debugMode) {
                                log.info(String.format(
                                        "Total traffic %.2f KByte/s to transfer from min broker:%s to max broker:%s.",
                                        minBrokerBundleSwapThroughput / KB, minBroker, maxBroker));
                                trafficMarkedToGain += minBrokerBundleSwapThroughput;
                            }
                        }
                        unload = new Unload(maxBroker, bundle, Optional.of(minBroker));
                    } else {
                        unload = new Unload(maxBroker, bundle);
                    }
                    var decision = new UnloadDecision();
                    decision.setUnload(unload);
                    decision.succeed(reason);
                    decisionCache.add(decision);
                    trafficMarkedToOffload += maxBrokerBundleThroughput;
                    remainingTopBundles--;

                    if (debugMode) {
                        log.info(String.format("Decided to unload bundle:%s, throughput:%.2f KByte/s."
                                        + " The traffic marked to unload:%.2f - gain:%.2f = %.2f KByte/s."
                                        + " Target:%.2f KByte/s.",
                                bundle, maxBrokerBundleThroughput / KB,
                                trafficMarkedToOffload / KB,
                                trafficMarkedToGain / KB,
                                (trafficMarkedToOffload - trafficMarkedToGain) / KB,
                                offloadThroughput / KB));
                    }
                }
                if (trafficMarkedToOffload > 0) {
                    var adjustedOffload =
                            (trafficMarkedToOffload - trafficMarkedToGain) * maxLoad / maxBrokerThroughput;
                    stats.offload(maxLoad, minLoad, adjustedOffload);
                    if (debugMode) {
                        log.info(
                                String.format("brokers' load stats:%s, after offload{max:%.2f, min:%.2f, offload:%.2f}",
                                        stats, maxLoad, minLoad, adjustedOffload));
                    }
                } else {
                    numOfBrokersWithFewBundles++;
                    log.warn(String.format(CANNOT_UNLOAD_BROKER_MSG
                            + " There is no bundle that can be unloaded in top bundles load data. "
                            + "Consider splitting bundles owned by the broker "
                            + "to make each bundle serve less traffic "
                            + "or increasing loadBalancerMaxNumberOfBundlesInBundleLoadReport"
                            + " to report more bundles in the top bundles load data.", maxBroker));
                }

            } // while end

            if (debugMode) {
                log.info("decisionCache:{}", decisionCache);
            }

            if (decisionCache.isEmpty()) {
                UnloadDecision.Reason reason;
                if (numOfBrokersWithEmptyLoadData > 0) {
                    reason = NoLoadData;
                } else if (numOfBrokersWithFewBundles > 0) {
                    reason = NoBundles;
                } else {
                    reason = HitCount;
                }
                counter.update(Skip, reason);
            } else {
                unloadConditionHitCount = 0;
            }

        } catch (Throwable e) {
            log.error("Failed to process unloading. ", e);
            this.counter.update(Failure, Unknown);
        }
        return decisionCache;
    }


    private boolean isUnderLoaded(LoadManagerContext context, String broker, LoadStats stats) {
        var brokerLoadDataOptional = context.brokerLoadDataStore().get(broker);
        if (brokerLoadDataOptional.isEmpty()) {
            return false;
        }
        var brokerLoadData = brokerLoadDataOptional.get();

        var underLoadedMultiplier =
                Math.min(0.5, Math.max(0.0, context.brokerConfiguration().getLoadBalancerBrokerLoadTargetStd() / 2.0));

        if (brokerLoadData.getWeightedMaxEMA() < stats.avg * underLoadedMultiplier) {
            return true;
        }

        var maxBrokerLoadDataOptional = context.brokerLoadDataStore().get(stats.peekMaxBroker());
        if (maxBrokerLoadDataOptional.isEmpty()) {
            return false;
        }

        return brokerLoadData.getMsgThroughputEMA()
                < maxBrokerLoadDataOptional.get().getMsgThroughputEMA() * underLoadedMultiplier;
    }

    private boolean isOverLoaded(LoadManagerContext context, String broker, double avgLoad) {
        var brokerLoadDataOptional = context.brokerLoadDataStore().get(broker);
        if (brokerLoadDataOptional.isEmpty()) {
            return false;
        }
        var conf = context.brokerConfiguration();
        var overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        var targetStd = conf.getLoadBalancerBrokerLoadTargetStd();
        var brokerLoadData = brokerLoadDataOptional.get();
        var load = brokerLoadData.getWeightedMaxEMA();
        return load > overloadThreshold && load > avgLoad + targetStd;
    }


    private boolean isTransferable(LoadManagerContext context,
                                   Map<String, BrokerLookupData> availableBrokers,
                                   String bundle,
                                   String srcBroker,
                                   Optional<String> dstBroker) {
        if (pulsar == null) {
            return true;
        }

        String namespace = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
        final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
        NamespaceBundle namespaceBundle =
                pulsar.getNamespaceService().getNamespaceBundleFactory().getBundle(namespace, bundleRange);

        if (!isLoadBalancerSheddingBundlesWithPoliciesEnabled(context, namespaceBundle)) {
            return false;
        }

        Map<String, BrokerLookupData> candidates = new HashMap<>(availableBrokers);
        for (var filter : brokerFilterPipeline) {
            try {
                filter.filterAsync(candidates, namespaceBundle, context)
                        .get(context.brokerConfiguration().getMetadataStoreOperationTimeoutSeconds(),
                                TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error("Failed to filter brokers with filter: {}", filter.getClass().getName(), e);
                return false;
            }
        }

        if (dstBroker.isPresent()) {
            if (!candidates.containsKey(dstBroker.get())) {
                return false;
            }
        }

        // Remove the current bundle owner broker.
        candidates.remove(srcBroker);
        boolean transfer = context.brokerConfiguration().isLoadBalancerTransferEnabled();

        // Unload: Check if there are any more candidates available for selection.
        if (dstBroker.isEmpty() || !transfer) {
            return !candidates.isEmpty();
        }
        // Transfer: Check if this broker is among the candidates.
        return candidates.containsKey(dstBroker.get());
    }

    protected boolean isLoadBalancerSheddingBundlesWithPoliciesEnabled(LoadManagerContext context,
                                                                       NamespaceBundle namespaceBundle) {
        if (isolationPoliciesHelper != null
                && isolationPoliciesHelper.hasIsolationPolicy(namespaceBundle.getNamespaceObject())) {
            return context.brokerConfiguration().isLoadBalancerSheddingBundlesWithPoliciesEnabled();
        }

        if (antiAffinityGroupPolicyHelper != null
                && antiAffinityGroupPolicyHelper.hasAntiAffinityGroupPolicy(namespaceBundle.toString())) {
            return context.brokerConfiguration().isLoadBalancerSheddingBundlesWithPoliciesEnabled();
        }

        return true;
    }
}
