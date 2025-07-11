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

import com.google.common.hash.Hashing;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.testng.annotations.Test;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertTrue;

public class FlowOrQpsEquallyDivideBundleSplitAlgorithmTest {

    @Test
    public void testSplitBundleByFlowOrQps() {
        FlowOrQpsEquallyDivideBundleSplitAlgorithm algorithm = new FlowOrQpsEquallyDivideBundleSplitAlgorithm();
        int loadBalancerNamespaceBundleMaxMsgRate = 1010;
        int loadBalancerNamespaceBundleMaxBandwidthMbytes = 100;
        int flowOrQpsDifferenceThresholdPercentage = 10;

        Map<Long, Double> hashAndMsgMap = new HashMap<>();
        Map<Long, Double> hashAndThroughput = new HashMap<>();
        Map<String, TopicStatsImpl> topicStatsMap = new HashMap<>();
        List<String> mockTopics = new ArrayList<>();
        List<Long> hashList = new ArrayList<>();

        for (int i = 1; i < 6; i++) {
            String topicName = "persistent://test-tenant1/test-namespace1/test" + i;
            for (int j = 0; j < 20; j++) {
                String tp = topicName + "-partition-" + j;
                mockTopics.add(tp);
                TopicStatsImpl topicStats = new TopicStatsImpl();
                topicStats.msgRateIn = 24.5;
                topicStats.msgThroughputIn = 1000;
                topicStats.msgRateOut = 25;
                topicStats.msgThroughputOut = 1000;
                topicStatsMap.put(tp, topicStats);
            }
        }


        for (int i = 6; i < 13; i++) {
            String topicName = "persistent://test-tenant1/test-namespace1/test" + i;
            for (int j = 0; j < 20; j++) {
                String tp = topicName + "-partition-" + j;
                mockTopics.add(tp);
                TopicStatsImpl topicStats = new TopicStatsImpl();
                topicStats.msgRateIn = 25.5;
                topicStats.msgThroughputIn = 1000;
                topicStats.msgRateOut = 25;
                topicStats.msgThroughputOut = 1000;
                topicStatsMap.put(tp, topicStats);
            }
        }

        String tp = "persistent://test-tenant1/test-namespace1/test695-partition-0";
        mockTopics.add(tp);
        TopicStatsImpl topicStats = new TopicStatsImpl();
        topicStats.msgRateIn = 25;
        topicStats.msgThroughputIn = 1000;
        topicStats.msgRateOut = 35;
        topicStats.msgThroughputOut = 1000;
        topicStatsMap.put(tp, topicStats);

        // -- do test
        NamespaceService mockNamespaceService = mock(NamespaceService.class);
        NamespaceBundle mockNamespaceBundle = mock(NamespaceBundle.class);
        doReturn(CompletableFuture.completedFuture(mockTopics))
                .when(mockNamespaceService).getOwnedTopicListForNamespaceBundle(mockNamespaceBundle);
        NamespaceBundleFactory mockNamespaceBundleFactory = mock(NamespaceBundleFactory.class);
        doReturn(mockNamespaceBundleFactory)
                .when(mockNamespaceBundle).getNamespaceBundleFactory();
        mockTopics.forEach((topic) -> {
            long hashValue = Hashing.crc32().hashString(topic, UTF_8).padToLong();
            doReturn(hashValue)
                    .when(mockNamespaceBundleFactory).getLongHashCode(topic);
            hashList.add(hashValue);
            hashAndMsgMap.put(hashValue, topicStatsMap.get(topic).msgRateIn
                    + topicStatsMap.get(topic).msgRateOut);
            hashAndThroughput.put(hashValue, topicStatsMap.get(topic).msgThroughputIn
                    + topicStatsMap.get(topic).msgThroughputOut);
        });

        List<Long> splitPositions = algorithm.getSplitBoundary(new FlowOrQpsEquallyDivideBundleSplitOption(mockNamespaceService, mockNamespaceBundle,
                null, topicStatsMap, loadBalancerNamespaceBundleMaxMsgRate,
                loadBalancerNamespaceBundleMaxBandwidthMbytes, flowOrQpsDifferenceThresholdPercentage)).join();

        Collections.sort(hashList);
        int i = 0;
        for (Long position : splitPositions) {
            Long endPosition = position;
            double bundleMsgRateTmp = 0;
            double bundleThroughputTmp = 0;
            while (hashList.get(i) < endPosition) {
                bundleMsgRateTmp += hashAndMsgMap.get(hashList.get(i));
                bundleThroughputTmp += hashAndThroughput.get(hashList.get(i));
                i++;
            }
            assertTrue(bundleMsgRateTmp < loadBalancerNamespaceBundleMaxMsgRate);
            assertTrue(bundleThroughputTmp < loadBalancerNamespaceBundleMaxBandwidthMbytes * 1024 * 1024);
        }
    }


    @Test
    public void testFirstPositionIsOverLoad() {
        FlowOrQpsEquallyDivideBundleSplitAlgorithm algorithm = new FlowOrQpsEquallyDivideBundleSplitAlgorithm();
        int loadBalancerNamespaceBundleMaxMsgRate = 1010;
        int loadBalancerNamespaceBundleMaxBandwidthMbytes = 100;
        int flowOrQpsDifferenceThresholdPercentage = 10;
        int topicNum = 5;

        List<String> mockTopics = new ArrayList<>();
        List<Long> topicHashList = new ArrayList<>(topicNum);
        Map<Long, String> hashAndTopic = new HashMap<>();

        for (int i = 0; i < topicNum; i++) {
            String topicName = "persistent://test-tenant1/test-namespace1/test-partition-" + i;
            mockTopics.add(topicName);
            long hashValue = Hashing.crc32().hashString(topicName, UTF_8).padToLong();
            topicHashList.add(hashValue);
            hashAndTopic.put(hashValue, topicName);
        }
        Collections.sort(topicHashList);

        Map<String, TopicStatsImpl> topicStatsMap = new HashMap<>();

        long hashValue = topicHashList.get(0);
        String topicName = hashAndTopic.get(hashValue);
        TopicStatsImpl topicStats0 = new TopicStatsImpl();
        topicStats0.msgRateIn = 1000;
        topicStats0.msgThroughputIn = 1000;
        topicStats0.msgRateOut = 1000;
        topicStats0.msgThroughputOut = 1000;
        topicStatsMap.put(topicName, topicStats0);

        for (int i = 1; i < topicHashList.size(); i++) {
            hashValue = topicHashList.get(i);
            topicName = hashAndTopic.get(hashValue);
            TopicStatsImpl topicStats = new TopicStatsImpl();
            topicStats.msgRateIn = 24.5;
            topicStats.msgThroughputIn = 1000;
            topicStats.msgRateOut = 25;
            topicStats.msgThroughputOut = 1000;
            topicStatsMap.put(topicName, topicStats);
        }

        // -- do test
        NamespaceService mockNamespaceService = mock(NamespaceService.class);
        NamespaceBundle mockNamespaceBundle = mock(NamespaceBundle.class);
        doReturn(CompletableFuture.completedFuture(mockTopics))
                .when(mockNamespaceService).getOwnedTopicListForNamespaceBundle(mockNamespaceBundle);
        NamespaceBundleFactory mockNamespaceBundleFactory = mock(NamespaceBundleFactory.class);
        doReturn(mockNamespaceBundleFactory)
                .when(mockNamespaceBundle).getNamespaceBundleFactory();
        mockTopics.forEach((topic) -> {
            long hash = Hashing.crc32().hashString(topic, UTF_8).padToLong();
            doReturn(hash)
                    .when(mockNamespaceBundleFactory).getLongHashCode(topic);
        });
        List<Long> splitPositions = algorithm.getSplitBoundary(new FlowOrQpsEquallyDivideBundleSplitOption(mockNamespaceService, mockNamespaceBundle,
                null, topicStatsMap, loadBalancerNamespaceBundleMaxMsgRate,
                loadBalancerNamespaceBundleMaxBandwidthMbytes, flowOrQpsDifferenceThresholdPercentage)).join();

        long splitStart = topicHashList.get(0);
        long splitEnd = topicHashList.get(1);
        long splitMiddle = splitStart + (splitEnd - splitStart) / 2 + 1;
        assertTrue(splitPositions.get(0) == splitMiddle);
    }
}
