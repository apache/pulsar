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
package org.apache.pulsar.common.naming;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;


/**
 * Split algorithm based on flow or qps.
 */
public class FlowOrQpsEquallyDivideBundleSplitAlgorithm implements NamespaceBundleSplitAlgorithm {
    private static final long MBytes = 1024 * 1024;

    @Override
    public CompletableFuture<List<Long>> getSplitBoundary(BundleSplitOption bundleSplitOption) {
        NamespaceService service = bundleSplitOption.getService();
        NamespaceBundle bundle = bundleSplitOption.getBundle();
        Map<String, TopicStatsImpl> topicStatsMap = bundleSplitOption.getTopicStatsMap();
        int loadBalancerNamespaceBundleMaxMsgRate =
                bundleSplitOption.getLoadBalancerNamespaceBundleMaxMsgRate();
        long loadBalancerNamespaceBundleMaxBandwidthBytes =
                bundleSplitOption.getLoadBalancerNamespaceBundleMaxBandwidthMbytes() * MBytes;
        double diffThreshold = bundleSplitOption.getFlowOrQpsDifferenceThresholdPercentage() / 100.0;

        return service.getOwnedTopicListForNamespaceBundle(bundle).thenCompose(topics -> {
            if (topics == null || topics.size() <= 1) {
                return CompletableFuture.completedFuture(null);
            }

            double bundleThroughput = 0;
            double bundleMsgRate = 0;
            Map<Long, Double> hashAndMsgMap = new HashMap<>();
            Map<Long, Double> hashAndThroughput = new HashMap<>();
            Map<Long, String> hashAndTopic = new HashMap<>();
            List<Long> topicNameHashList = new ArrayList<>(topics.size());
            for (String topic : topics) {
                TopicStatsImpl topicStats = topicStatsMap.get(topic);
                if (topicStats == null) {
                    continue;
                }
                double msgRateIn = topicStats.getMsgRateIn();
                double msgRateOut = topicStats.getMsgRateOut();
                double msgThroughputIn = topicStats.getMsgThroughputIn();
                double msgThroughputOut = topicStats.getMsgThroughputOut();
                double msgRate = msgRateIn + msgRateOut;
                double throughput = msgThroughputIn + msgThroughputOut;

                if (msgRate <= 0 && throughput <= 0) {
                    // Skip empty topic
                    continue;
                }

                Long hashCode = bundle.getNamespaceBundleFactory().getLongHashCode(topic);
                topicNameHashList.add(hashCode);
                hashAndMsgMap.put(hashCode, msgRate);
                hashAndThroughput.put(hashCode, throughput);
                hashAndTopic.put(hashCode, topic);
                bundleThroughput += throughput;
                bundleMsgRate += msgRate;
            }

            if (topicNameHashList.size() < 2
                    || (bundleMsgRate < (loadBalancerNamespaceBundleMaxMsgRate * (1 + diffThreshold))
                    && bundleThroughput < (loadBalancerNamespaceBundleMaxBandwidthBytes * (1 + diffThreshold)))) {
                return CompletableFuture.completedFuture(null);
            }

            Collections.sort(topicNameHashList);
            List<Long> splitResults = new ArrayList<>();
            double bundleMsgRateTmp = 0;
            double bundleThroughputTmp = 0;
            for (int i = 0; i < topicNameHashList.size(); i++) {
                long topicHashCode = topicNameHashList.get(i);
                bundleThroughputTmp += hashAndThroughput.get(topicHashCode);
                bundleMsgRateTmp += hashAndMsgMap.get(topicHashCode);

                if (bundleMsgRateTmp > loadBalancerNamespaceBundleMaxMsgRate
                        || bundleThroughputTmp > loadBalancerNamespaceBundleMaxBandwidthBytes) {
                    long splitStart = i > 0 ? topicNameHashList.get(i - 1) : topicHashCode;
                    long splitEnd = i > 0 ? topicHashCode : topicNameHashList.get(i + 1);
                    long splitMiddle = splitStart + (splitEnd - splitStart) / 2;
                    splitResults.add(splitMiddle);
                    bundleMsgRateTmp =  hashAndMsgMap.get(topicHashCode);
                    bundleThroughputTmp = hashAndThroughput.get(topicHashCode);
                }
            }

            return CompletableFuture.completedFuture(splitResults);
        });
    }
}
