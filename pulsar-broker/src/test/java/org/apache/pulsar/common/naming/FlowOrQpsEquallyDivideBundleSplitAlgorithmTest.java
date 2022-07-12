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

import com.google.common.base.Charsets;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertTrue;

public class FlowOrQpsEquallyDivideBundleSplitAlgorithmTest {

    @Test
    public void testSplitBundleByFlowOrQps() {
        FlowOrQpsEquallyDivideBundleSplitAlgorithm algorithm = new FlowOrQpsEquallyDivideBundleSplitAlgorithm();
        Map<Long, Double> hashAndMsgMap = new HashMap<>();
        Map<Long, Double> hashAndThroughput = new HashMap<>();
        Map<String, TopicStatsImpl> topicStatsMap = new HashMap<>();
        List<String> mockTopics = new ArrayList<>();

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

        NamespaceService namespaceServiceForMockResult = mock(NamespaceService.class);
        NamespaceBundle namespaceBundleForMockResult = mock(NamespaceBundle.class);
        doReturn(CompletableFuture.completedFuture(mockTopics))
                .when(namespaceServiceForMockResult).getOwnedTopicListForNamespaceBundle(namespaceBundleForMockResult);
        List<Long> hashList = new ArrayList<>();
        NamespaceBundleFactory namespaceBundleFactoryForMockResult = mock(NamespaceBundleFactory.class);
        mockTopics.forEach((topic) -> {
            long hashValue = Hashing.crc32().hashString(topic, Charsets.UTF_8).padToLong();
            doReturn(namespaceBundleFactoryForMockResult)
                    .when(namespaceBundleForMockResult).getNamespaceBundleFactory();
            doReturn(hashValue)
                    .when(namespaceBundleFactoryForMockResult).getLongHashCode(topic);
            hashList.add(hashValue);
            hashAndMsgMap.put(hashValue, topicStatsMap.get(topic).msgRateIn
                    + topicStatsMap.get(topic).msgRateOut);
            hashAndThroughput.put(hashValue, topicStatsMap.get(topic).msgThroughputIn
                    + topicStatsMap.get(topic).msgThroughputOut);
        });
        Collections.sort(hashList);
        // -- do test
        NamespaceService mockNamespaceService = mock(NamespaceService.class);
        NamespaceBundle mockNamespaceBundle = mock(NamespaceBundle.class);
        doReturn(CompletableFuture.completedFuture(mockTopics))
                .when(mockNamespaceService).getOwnedTopicListForNamespaceBundle(mockNamespaceBundle);
        NamespaceBundleFactory mockNamespaceBundleFactory = mock(NamespaceBundleFactory.class);
        mockTopics.forEach((topic) -> {
            doReturn(mockNamespaceBundleFactory)
                    .when(mockNamespaceBundle).getNamespaceBundleFactory();
            long hashValue = Hashing.crc32().hashString(topic, Charsets.UTF_8).padToLong();
            doReturn(hashValue)
                    .when(mockNamespaceBundleFactory).getLongHashCode(topic);
        });
        List<Long> splitPositions = algorithm.getSplitBoundary(new BundleSplitOption(mockNamespaceService, mockNamespaceBundle,
                null, topicStatsMap, 1010,
                100, 10)).join();

        int i = 0;
        for (Long position : splitPositions) {
            Long endPosition = position;
            double bundleMsgRateTmp = 0;
            double bundleThroughputTmp = 0;
            while (hashList.get(i++) < endPosition) {
                bundleMsgRateTmp += hashAndMsgMap.get(hashList.get(i));
                bundleThroughputTmp += hashAndThroughput.get(hashList.get(i));
            }
            assertTrue(bundleMsgRateTmp < 1010);
            assertTrue(bundleThroughputTmp < 100 * 1024 * 1024);
        }
    }
}
