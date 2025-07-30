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
package org.apache.pulsar.zookeeper;

import static org.apache.pulsar.zookeeper.MaxValueMetadataNodePayloadLenEstimator.DEFAULT_LEN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.Stat;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MaxValueMetadataNodePayloadLenEstimatorTest {

    private MaxValueMetadataNodePayloadLenEstimator estimator;

    @BeforeMethod
    public void setUp() {
        estimator = new MaxValueMetadataNodePayloadLenEstimator();
    }

    @Test
    public void testRecordPutAndInternalEstimateGetResPayloadLen() {
        // Test with cluster path
        String clusterPath = "/admin/clusters/test-cluster";
        byte[] smallData = "small".getBytes(StandardCharsets.UTF_8);
        byte[] largeData = "this is a much larger data payload".getBytes(StandardCharsets.UTF_8);

        // Record small data first
        estimator.recordPut(clusterPath, smallData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(clusterPath), smallData.length);

        // Record larger data - should update the max
        estimator.recordPut(clusterPath, largeData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(clusterPath), largeData.length);

        // Record smaller data again - should not change the max
        estimator.recordPut(clusterPath, smallData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(clusterPath), largeData.length);
    }

    @Test
    public void testRecordGetResAndInternalEstimateGetResPayloadLen() {
        String tenantPath = "/admin/policies/test-tenant";
        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "much longer data payload".getBytes(StandardCharsets.UTF_8);

        // Create mock GetResult objects
        Stat mockStat = Mockito.mock(Stat.class);
        GetResult getResult1 = new GetResult(data1, mockStat);
        GetResult getResult2 = new GetResult(data2, mockStat);

        // Record first result
        estimator.recordGetRes(tenantPath, getResult1);
        assertEquals(estimator.internalEstimateGetResPayloadLen(tenantPath), data1.length);

        // Record larger result - should update the max
        estimator.recordGetRes(tenantPath, getResult2);
        assertEquals(estimator.internalEstimateGetResPayloadLen(tenantPath), data2.length);

        // Record smaller result again - should not change the max
        estimator.recordGetRes(tenantPath, getResult1);
        assertEquals(estimator.internalEstimateGetResPayloadLen(tenantPath), data2.length);
    }

    @Test
    public void testRecordGetChildrenResAndInternalEstimateGetChildrenResPayloadLen() {
        String namespacePath = "/admin/policies/test-tenant/test-namespace";
        List<String> smallList = Arrays.asList("a", "b", "c");
        List<String> largeList = Arrays.asList("longer-name-1", "longer-name-2", "longer-name-3", "longer-name-4");

        // Calculate expected lengths
        int smallListLength = smallList.stream().mapToInt(String::length).sum() + smallList.size() * 4;
        int largeListLength = largeList.stream().mapToInt(String::length).sum() + largeList.size() * 4;

        // Record small list first
        estimator.recordGetChildrenRes(namespacePath, smallList);
        assertEquals(estimator.internalEstimateGetChildrenResPayloadLen(namespacePath), smallListLength);

        // Record larger list - should update the max
        estimator.recordGetChildrenRes(namespacePath, largeList);
        assertEquals(estimator.internalEstimateGetChildrenResPayloadLen(namespacePath), largeListLength);

        // Record smaller list again - should not change the max
        estimator.recordGetChildrenRes(namespacePath, smallList);
        assertEquals(estimator.internalEstimateGetChildrenResPayloadLen(namespacePath), largeListLength);
    }

    @Test
    public void testDefaultLengthForUnknownPaths() {
        // Test that unknown paths return the default length
        String unknownPath = "/some/unknown/path";
        assertEquals(estimator.internalEstimateGetResPayloadLen(unknownPath), DEFAULT_LEN);
        assertEquals(estimator.internalEstimateGetChildrenResPayloadLen(unknownPath), DEFAULT_LEN);
    }

    @Test
    public void testPathTypeClassification() {
        // Test different path types to ensure they are classified correctly
        
        // Cluster paths
        String clusterPath = "/admin/clusters/test-cluster";
        byte[] clusterData = "cluster-data".getBytes(StandardCharsets.UTF_8);
        estimator.recordPut(clusterPath, clusterData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(clusterPath), clusterData.length);

        // Tenant paths
        String tenantPath = "/admin/policies/test-tenant";
        byte[] tenantData = "tenant-data".getBytes(StandardCharsets.UTF_8);
        estimator.recordPut(tenantPath, tenantData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(tenantPath), tenantData.length);

        // Namespace policy paths
        String namespacePath = "/admin/policies/test-tenant/test-namespace";
        byte[] namespaceData = "namespace-data".getBytes(StandardCharsets.UTF_8);
        estimator.recordPut(namespacePath, namespaceData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(namespacePath), namespaceData.length);

        // Partitioned topic paths
        String partitionedTopicPath = "/admin/partitioned-topics/persistent/test-tenant/test-namespace/test-topic";
        byte[] partitionedTopicData = "partitioned-topic-data".getBytes(StandardCharsets.UTF_8);
        estimator.recordPut(partitionedTopicPath, partitionedTopicData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(partitionedTopicPath), partitionedTopicData.length);

        // Managed ledger paths
        String mlTopicPath = "/managed-ledgers/test-tenant/test-namespace/persistent/test-topic";
        byte[] mlTopicData = "ml-topic-data".getBytes(StandardCharsets.UTF_8);
        estimator.recordPut(mlTopicPath, mlTopicData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(mlTopicPath), mlTopicData.length);

        // Subscription paths
        String subscriptionPath = "/managed-ledgers/test-tenant/test-namespace/persistent/test-topic/test-subscription";
        byte[] subscriptionData = "subscription-data".getBytes(StandardCharsets.UTF_8);
        estimator.recordPut(subscriptionPath, subscriptionData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(subscriptionPath), subscriptionData.length);

        // Verify that different path types maintain separate max values
        assertTrue(estimator.internalEstimateGetResPayloadLen(clusterPath) != estimator.internalEstimateGetResPayloadLen(tenantPath));
    }

    @Test
    public void testEmptyAndNullData() {
        String testPath = "/admin/clusters/test";
        
        // Test with empty data
        byte[] emptyData = new byte[0];
        estimator.recordPut(testPath, emptyData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(testPath), 0);

        // Test with empty list
        List<String> emptyList = Collections.emptyList();
        estimator.recordGetChildrenRes(testPath, emptyList);
        assertEquals(estimator.internalEstimateGetChildrenResPayloadLen(testPath), 0);
    }

    @Test
    public void testMixedOperationsOnSamePath() {
        String testPath = "/admin/policies/test-tenant";
        
        // Record via put
        byte[] putData = "put-data".getBytes(StandardCharsets.UTF_8);
        estimator.recordPut(testPath, putData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(testPath), putData.length);

        // Record via get with larger data - should update max
        byte[] largerData = "much larger get result data".getBytes(StandardCharsets.UTF_8);
        Stat mockStat = Mockito.mock(Stat.class);
        GetResult getResult = new GetResult(largerData, mockStat);
        estimator.recordGetRes(testPath, getResult);
        assertEquals(estimator.internalEstimateGetResPayloadLen(testPath), largerData.length);

        // Record via put with smaller data - should not change max
        byte[] smallerData = "small".getBytes(StandardCharsets.UTF_8);
        estimator.recordPut(testPath, smallerData);
        assertEquals(estimator.internalEstimateGetResPayloadLen(testPath), largerData.length);
    }

    @Test
    public void testShortPaths() {
        // Test paths that are too short to be classified
        String shortPath1 = "/";
        String shortPath2 = "/admin";
        
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        
        estimator.recordPut(shortPath1, data);
        estimator.recordPut(shortPath2, data);
        
        // Both should use the OTHERS path type and return the recorded length
        assertEquals(estimator.internalEstimateGetResPayloadLen(shortPath1), data.length);
        assertEquals(estimator.internalEstimateGetResPayloadLen(shortPath2), data.length);
        
        // They should share the same path type, so the max should be the same
        assertEquals(estimator.internalEstimateGetResPayloadLen(shortPath1),
                     estimator.internalEstimateGetResPayloadLen(shortPath2));
    }

    @Test
    public void testUnknownAdminPaths() {
        // Test admin paths that don't match known patterns
        String unknownAdminPath = "/admin/unknown/path";
        byte[] data = "unknown-admin-data".getBytes(StandardCharsets.UTF_8);
        
        estimator.recordPut(unknownAdminPath, data);
        assertEquals(estimator.internalEstimateGetResPayloadLen(unknownAdminPath), data.length);
    }

    @Test
    public void testManagedLedgerPathVariations() {
        // Test different managed ledger path lengths
        String mlNamespacePath = "/managed-ledgers/tenant/namespace/persistent";
        String mlTopicPath = "/managed-ledgers/tenant/namespace/persistent/topic";
        String mlSubscriptionPath = "/managed-ledgers/tenant/namespace/persistent/topic/subscription";
        
        byte[] namespaceData = "namespace-data".getBytes(StandardCharsets.UTF_8);
        byte[] topicData = "topic-data".getBytes(StandardCharsets.UTF_8);
        byte[] subscriptionData = "subscription-data".getBytes(StandardCharsets.UTF_8);
        
        estimator.recordPut(mlNamespacePath, namespaceData);
        estimator.recordPut(mlTopicPath, topicData);
        estimator.recordPut(mlSubscriptionPath, subscriptionData);
        
        // Each should maintain its own max value based on path type
        assertEquals(estimator.internalEstimateGetResPayloadLen(mlNamespacePath), namespaceData.length);
        assertEquals(estimator.internalEstimateGetResPayloadLen(mlTopicPath), topicData.length);
        assertEquals(estimator.internalEstimateGetResPayloadLen(mlSubscriptionPath), subscriptionData.length);
    }
}