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
package org.apache.pulsar.broker.configuration;

import static org.apache.pulsar.broker.ServiceConfiguration.CATEGORY_LOAD_BALANCER;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

@Getter
@Setter
@ToString
public class LoadBalancerConfiguration implements PulsarConfiguration {
    private ServiceConfiguration serviceConfiguration;

    /*** --- Load balancer. --- ****/
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            doc = "Enable load balancer"
    )
    private boolean loadBalancerEnabled = true;
    @Deprecated
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            deprecated = true,
            doc = "load placement strategy[weightedRandomSelection/leastLoadedServer] (only used by "
                    + "SimpleLoadManagerImpl)"
    )
    private String loadBalancerPlacementStrategy = "leastLoadedServer"; // weighted random selection

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "load balance load shedding strategy "
                    + "(It requires broker restart if value is changed using dynamic config). "
                    + "Default is ThresholdShedder since 2.10.0"
    )
    private String loadBalancerLoadSheddingStrategy = "org.apache.pulsar.broker.loadbalance.impl.ThresholdShedder";

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            doc = "When [current usage < average usage - threshold], "
                    + "the broker with the highest load will be triggered to unload"
    )
    private boolean lowerBoundarySheddingEnabled = false;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            doc = "load balance placement strategy"
    )
    private String loadBalancerLoadPlacementStrategy =
            "org.apache.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate";

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Percentage of change to trigger load report update"
    )
    private int loadBalancerReportUpdateThresholdPercentage = 10;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "maximum interval to update load report"
    )
    private int loadBalancerReportUpdateMinIntervalMillis = 5000;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Min delay of load report to collect, in milli-seconds"
    )
    private int loadBalancerReportUpdateMaxIntervalMinutes = 15;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Frequency of report to collect, in minutes"
    )
    private int loadBalancerHostUsageCheckIntervalMinutes = 1;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Enable/disable automatic bundle unloading for load-shedding"
    )
    private boolean loadBalancerSheddingEnabled = true;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Load shedding interval. \n\nBroker periodically checks whether some traffic"
                    + " should be offload from some over-loaded broker to other under-loaded brokers"
    )
    private int loadBalancerSheddingIntervalMinutes = 1;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "enable/disable distribute bundles evenly"
    )
    private boolean loadBalancerDistributeBundlesEvenlyEnabled = true;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Prevent the same topics to be shed and moved to other broker more than"
                    + " once within this timeframe"
    )
    private long loadBalancerSheddingGracePeriodMinutes = 30;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            deprecated = true,
            doc = "Usage threshold to determine a broker as under-loaded (only used by SimpleLoadManagerImpl)"
    )
    @Deprecated
    private int loadBalancerBrokerUnderloadedThresholdPercentage = 50;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Usage threshold to allocate max number of topics to broker"
    )
    private int loadBalancerBrokerMaxTopics = 50000;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Usage threshold to determine a broker as over-loaded"
    )
    private int loadBalancerBrokerOverloadedThresholdPercentage = 85;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Usage threshold to determine a broker whether to start threshold shedder"
    )
    private int loadBalancerBrokerThresholdShedderPercentage = 10;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Average resource usage difference threshold to determine a broker whether to be a best candidate in "
                    + "LeastResourceUsageWithWeight.(eg: broker1 with 10% resource usage with weight "
                    + "and broker2 with 30% and broker3 with 80% will have 40% average resource usage. "
                    + "The placement strategy can select broker1 and broker2 as best candidates.)"
    )
    private int loadBalancerAverageResourceUsageDifferenceThresholdPercentage = 10;


    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "In FlowOrQpsEquallyDivideBundleSplitAlgorithm,"
                    + " if msgRate >= loadBalancerNamespaceBundleMaxMsgRate * "
                    + " (100 + flowOrQpsDifferenceThresholdPercentage)/100.0 "
                    + " or throughput >=  loadBalancerNamespaceBundleMaxBandwidthMbytes * "
                    + " (100 + flowOrQpsDifferenceThresholdPercentage)/100.0, "
                    + " execute split bundle"
    )
    private int flowOrQpsDifferenceThresholdPercentage = 10;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "In the UniformLoadShedder strategy, the minimum message that triggers unload."
    )
    private int minUnloadMessage = 1000;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "In the UniformLoadShedder strategy, the minimum throughput that triggers unload."
    )
    private int minUnloadMessageThroughput = 1 * 1024 * 1024;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "In the UniformLoadShedder strategy, the maximum unload ratio."
    )
    private double maxUnloadPercentage = 0.2;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Message-rate percentage threshold between highest and least loaded brokers for "
                    + "uniform load shedding. (eg: broker1 with 50K msgRate and broker2 with 30K msgRate "
                    + "will have 66% msgRate difference and load balancer can unload bundles from broker-1 "
                    + "to broker-2)"
    )
    private double loadBalancerMsgRateDifferenceShedderThreshold = 50;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Message-throughput threshold between highest and least loaded brokers for "
                    + "uniform load shedding. (eg: broker1 with 450MB msgRate and broker2 with 100MB msgRate "
                    + "will have 4.5 times msgThroughout difference and load balancer can unload bundles "
                    + "from broker-1 to broker-2)"
    )
    private double loadBalancerMsgThroughputMultiplierDifferenceShedderThreshold = 4;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "For each uniform balanced unload, the maximum number of bundles that can be unloaded."
                    + " The default value is -1, which means no limit"
    )
    private int maxUnloadBundleNumPerShedding = -1;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Resource history Usage Percentage When adding new resource usage info"
    )
    private double loadBalancerHistoryResourcePercentage = 0.9;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "BandwithIn Resource Usage Weight"
    )
    private double loadBalancerBandwithInResourceWeight = 1.0;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "BandwithOut Resource Usage Weight"
    )
    private double loadBalancerBandwithOutResourceWeight = 1.0;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "CPU Resource Usage Weight"
    )
    private double loadBalancerCPUResourceWeight = 1.0;

    @Deprecated(since = "3.0.0")
    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Memory Resource Usage Weight. Deprecated: Memory is no longer used as a load balancing item.",
            deprecated = true
    )
    private double loadBalancerMemoryResourceWeight = 1.0;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Direct Memory Resource Usage Weight"
    )
    private double loadBalancerDirectMemoryResourceWeight = 1.0;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Bundle unload minimum throughput threshold (MB)"
    )
    private double loadBalancerBundleUnloadMinThroughputThreshold = 10;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            doc = "Interval to flush dynamic resource quota to ZooKeeper"
    )
    private int loadBalancerResourceQuotaUpdateIntervalMinutes = 15;
    @Deprecated
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            deprecated = true,
            doc = "Usage threshold to determine a broker is having just right level of load"
                    + " (only used by SimpleLoadManagerImpl)"
    )
    private int loadBalancerBrokerComfortLoadLevelPercentage = 65;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "enable/disable automatic namespace bundle split"
    )
    private boolean loadBalancerAutoBundleSplitEnabled = true;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "enable/disable automatic unloading of split bundles"
    )
    private boolean loadBalancerAutoUnloadSplitBundlesEnabled = true;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "maximum topics in a bundle, otherwise bundle split will be triggered"
    )
    private int loadBalancerNamespaceBundleMaxTopics = 1000;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "maximum sessions (producers + consumers) in a bundle, otherwise bundle split will be triggered"
                    + "(disable threshold check with value -1)"
    )
    private int loadBalancerNamespaceBundleMaxSessions = 1000;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "maximum msgRate (in + out) in a bundle, otherwise bundle split will be triggered"
    )
    private int loadBalancerNamespaceBundleMaxMsgRate = 30000;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "maximum bandwidth (in + out) in a bundle, otherwise bundle split will be triggered"
    )
    private int loadBalancerNamespaceBundleMaxBandwidthMbytes = 100;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "maximum number of bundles in a namespace"
    )
    private int loadBalancerNamespaceMaximumBundles = 128;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Name of load manager to use"
    )
    private String loadManagerClassName = "org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl";
    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Supported algorithms name for namespace bundle split"
    )
    private List<String> supportedNamespaceBundleSplitAlgorithms = Lists.newArrayList("range_equally_divide",
            "topic_count_equally_divide", "specified_positions_divide", "flow_or_qps_equally_divide");
    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Default algorithm name for namespace bundle split"
    )
    private String defaultNamespaceBundleSplitAlgorithm = "range_equally_divide";
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            doc = "Option to override the auto-detected network interfaces max speed"
    )
    private Optional<Double> loadBalancerOverrideBrokerNicSpeedGbps = Optional.empty();

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Time to wait for the unloading of a namespace bundle"
    )
    private long namespaceBundleUnloadingTimeoutMs = 60000;

    /**** --- Load Balancer Extension. --- ****/
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Option to enable the debug mode for the load balancer logics. "
                    + "The debug mode prints more logs to provide more information "
                    + "such as load balance states and decisions. "
                    + "(only used in load balancer extension logics)"
    )
    private boolean loadBalancerDebugModeEnabled = false;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "The target standard deviation of the resource usage across brokers "
                    + "(100% resource usage is 1.0 load). "
                    + "The shedder logic tries to distribute bundle load across brokers to meet this target std. "
                    + "The smaller value will incur load balancing more frequently. "
                    + "(only used in load balancer extension TransferSheddeer)"
    )
    private double loadBalancerBrokerLoadTargetStd = 0.25;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Threshold to the consecutive count of fulfilled shedding(unload) conditions. "
                    + "If the unload scheduler consecutively finds bundles that meet unload conditions "
                    + "many times bigger than this threshold, the scheduler will shed the bundles. "
                    + "The bigger value will incur less bundle unloading/transfers. "
                    + "(only used in load balancer extension TransferSheddeer)"
    )
    private int loadBalancerSheddingConditionHitCountThreshold = 3;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Option to enable the bundle transfer mode when distributing bundle loads. "
                    + "On: transfer bundles from overloaded brokers to underloaded "
                    + "-- pre-assigns the destination broker upon unloading). "
                    + "Off: unload bundles from overloaded brokers "
                    + "-- post-assigns the destination broker upon lookups). "
                    + "(only used in load balancer extension TransferSheddeer)"
    )
    private boolean loadBalancerTransferEnabled = true;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Maximum number of brokers to unload bundle load for each unloading cycle. "
                    + "The bigger value will incur more unloading/transfers for each unloading cycle. "
                    + "(only used in load balancer extension TransferSheddeer)"
    )
    private int loadBalancerMaxNumberOfBrokerSheddingPerCycle = 3;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Delay (in seconds) to the next unloading cycle after unloading. "
                    + "The logic tries to give enough time for brokers to recompute load after unloading. "
                    + "The bigger value will delay the next unloading cycle longer. "
                    + "(only used in load balancer extension TransferSheddeer)"
    )
    private long loadBalanceSheddingDelayInSeconds = 180;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Broker load data time to live (TTL in seconds). "
                    + "The logic tries to avoid (possibly unavailable) brokers with out-dated load data, "
                    + "and those brokers will be ignored in the load computation. "
                    + "When tuning this value, please consider loadBalancerReportUpdateMaxIntervalMinutes. "
                    + "The current default is loadBalancerReportUpdateMaxIntervalMinutes * 2. "
                    + "(only used in load balancer extension TransferSheddeer)"
    )
    private long loadBalancerBrokerLoadDataTTLInSeconds = 1800;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_LOAD_BALANCER,
            doc = "Max number of bundles in bundle load report from each broker. "
                    + "The load balancer distributes bundles across brokers, "
                    + "based on topK bundle load data and other broker load data."
                    + "The bigger value will increase the overhead of reporting many bundles in load data. "
                    + "(only used in load balancer extension logics)"
    )
    private int loadBalancerMaxNumberOfBundlesInBundleLoadReport = 10;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            doc = "Service units'(bundles) split interval. Broker periodically checks whether "
                    + "some service units(e.g. bundles) should split if they become hot-spots. "
                    + "(only used in load balancer extension logics)"
    )
    private int loadBalancerSplitIntervalMinutes = 1;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Max number of bundles to split to per cycle. "
                    + "(only used in load balancer extension logics)"
    )
    private int loadBalancerMaxNumberOfBundlesToSplitPerCycle = 10;
    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Threshold to the consecutive count of fulfilled split conditions. "
                    + "If the split scheduler consecutively finds bundles that meet split conditions "
                    + "many times bigger than this threshold, the scheduler will trigger splits on the bundles "
                    + "(if the number of bundles is less than loadBalancerNamespaceMaximumBundles). "
                    + "(only used in load balancer extension logics)"
    )
    private int loadBalancerNamespaceBundleSplitConditionHitCountThreshold = 3;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            doc = "After this delay, the service-unit state channel tombstones any service units (e.g., bundles) "
                    + "in semi-terminal states. For example, after splits, parent bundles will be `deleted`, "
                    + "and then after this delay, the parent bundles' state will be `tombstoned` "
                    + "in the service-unit state channel. "
                    + "Pulsar does not immediately remove such semi-terminal states "
                    + "to avoid unnecessary system confusion, "
                    + "as the bundles in the `tombstoned` state might temporarily look available to reassign. "
                    + "Rarely, one could lower this delay in order to aggressively clean "
                    + "the service-unit state channel when there are a large number of bundles. "
                    + "minimum value = 30 secs"
                    + "(only used in load balancer extension logics)"
    )
    private long loadBalancerServiceUnitStateTombstoneDelayTimeInSeconds = 3600;

    @FieldContext(
            category = CATEGORY_LOAD_BALANCER,
            dynamic = true,
            doc = "Option to automatically unload namespace bundles with affinity(isolation) "
                    + "or anti-affinity group policies."
                    + "Such bundles are not ideal targets to auto-unload as destination brokers are limited."
                    + "(only used in load balancer extension logics)"
    )
    private boolean loadBalancerSheddingBundlesWithPoliciesEnabled = false;

    @ToString.Exclude
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Properties properties = new Properties();

    public Object getProperty(String key) {
        return properties.get(key);
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
