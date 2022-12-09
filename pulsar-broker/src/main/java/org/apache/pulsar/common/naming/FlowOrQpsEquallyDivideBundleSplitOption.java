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
package org.apache.pulsar.common.naming;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;

@Getter
public class FlowOrQpsEquallyDivideBundleSplitOption extends BundleSplitOption {
    private Map<String, TopicStatsImpl> topicStatsMap;
    private int loadBalancerNamespaceBundleMaxMsgRate;
    private int loadBalancerNamespaceBundleMaxBandwidthMbytes;
    private int flowOrQpsDifferenceThresholdPercentage;

    public FlowOrQpsEquallyDivideBundleSplitOption(NamespaceService namespaceService, NamespaceBundle bundle,
                                                   List<Long> boundaries,
                                                   Map<String, TopicStatsImpl> topicStatsMap,
                                                   int loadBalancerNamespaceBundleMaxMsgRate,
                                                   int loadBalancerNamespaceBundleMaxBandwidthMbytes,
                                                   int flowOrQpsDifferenceThresholdPercentage) {
        super(namespaceService, bundle, boundaries);
        this.topicStatsMap = topicStatsMap;
        this.loadBalancerNamespaceBundleMaxMsgRate = loadBalancerNamespaceBundleMaxMsgRate;
        this.loadBalancerNamespaceBundleMaxBandwidthMbytes = loadBalancerNamespaceBundleMaxBandwidthMbytes;
        this.flowOrQpsDifferenceThresholdPercentage = flowOrQpsDifferenceThresholdPercentage;
    }
}
