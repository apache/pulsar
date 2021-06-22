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
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.junit.Test;
import org.mockito.Mockito;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class TopicCountEquallyDivideBundleSplitAlgorithmTest {

    @Test
    public void testWrongArg() {
        TopicCountEquallyDivideBundleSplitAlgorithm algorithm = new TopicCountEquallyDivideBundleSplitAlgorithm();
        Assert.assertThrows(NullPointerException.class, () -> algorithm.getSplitBoundary(null, null));
    }

    @Test
    public void testTopicsSizeLessThan1() {
        TopicCountEquallyDivideBundleSplitAlgorithm algorithm = new TopicCountEquallyDivideBundleSplitAlgorithm();
        NamespaceService mockNamespaceService = Mockito.mock(NamespaceService.class);
        NamespaceBundle mockNamespaceBundle = Mockito.mock(NamespaceBundle.class);
        Mockito.doReturn(CompletableFuture.completedFuture(Lists.newArrayList("a")))
                .when(mockNamespaceService).getOwnedTopicListForNamespaceBundle(mockNamespaceBundle);
        Assert.assertNull(algorithm.getSplitBoundary(mockNamespaceService, mockNamespaceBundle).join());
    }
    @Test
    public void testAlgorithmReturnCorrectResult() {
        TopicCountEquallyDivideBundleSplitAlgorithm algorithm = new TopicCountEquallyDivideBundleSplitAlgorithm();
        List<String> mockTopics = Lists.newArrayList("a", "b", "c");
        long correctResult = getCorrectResult(mockTopics);
        NamespaceService mockNamespaceService1 = Mockito.mock(NamespaceService.class);
        NamespaceBundle mockNamespaceBundle1 = Mockito.mock(NamespaceBundle.class);
        Mockito.doReturn(CompletableFuture.completedFuture(mockTopics))
                .when(mockNamespaceService1).getOwnedTopicListForNamespaceBundle(mockNamespaceBundle1);
        NamespaceBundleFactory mockNamespaceBundleFactory = Mockito.mock(NamespaceBundleFactory.class);
        mockTopics.forEach((topic)->{
            Mockito.doReturn(mockNamespaceBundleFactory)
                    .when(mockNamespaceBundle1).getNamespaceBundleFactory();
            long hashValue = Hashing.crc32().hashString(topic, Charsets.UTF_8).padToLong();
            Mockito.doReturn(hashValue)
                    .when(mockNamespaceBundleFactory).getLongHashCode(topic);
        });
        Assert.assertEquals((long)algorithm.getSplitBoundary(mockNamespaceService1, mockNamespaceBundle1).join(),correctResult);
    }

    private long getCorrectResult(List<String> mockTopics){
        NamespaceService mockNamespaceService = Mockito.mock(NamespaceService.class);
        NamespaceBundle mockNamespaceBundle = Mockito.mock(NamespaceBundle.class);
        Mockito.doReturn(CompletableFuture.completedFuture(mockTopics))
                .when(mockNamespaceService).getOwnedTopicListForNamespaceBundle(mockNamespaceBundle);
        List<Long> hashList = new ArrayList<>();
        mockTopics.forEach((topic)->{
            long hashValue = Hashing.crc32().hashString(topic, Charsets.UTF_8).padToLong();
            NamespaceBundleFactory mockNamespaceBundleFactory = Mockito.mock(NamespaceBundleFactory.class);
            Mockito.doReturn(mockNamespaceBundleFactory)
                    .when(mockNamespaceBundle).getNamespaceBundleFactory();
            Mockito.doReturn(hashValue)
                    .when(mockNamespaceBundleFactory).getLongHashCode(topic);
            hashList.add(hashValue);
        });
        Collections.sort(hashList);
        long splitStart = hashList.get(Math.max((hashList.size() / 2) - 1, 0));
        long splitEnd = hashList.get(hashList.size() / 2);
        return splitStart + (splitEnd - splitStart) / 2;
    }
}