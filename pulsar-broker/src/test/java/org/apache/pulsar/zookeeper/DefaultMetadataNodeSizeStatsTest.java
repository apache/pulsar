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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.Stat;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Comprehensive test suite for DefaultMetadataNodeSizeStats class.
 *
 * This test covers all the functionality of the DefaultMetadataNodeSizeStats class including:
 * - Path type classification for different Pulsar metadata paths
 * - Size tracking for put and get operations
 * - Children count tracking for list operations
 * - Proper handling of edge cases and null values
 * - Path splitting functionality
 */
public class DefaultMetadataNodeSizeStatsTest {

    private DefaultMetadataNodeSizeStats stats;

    @BeforeMethod
    public void setUp() {
        stats = new DefaultMetadataNodeSizeStats();
    }

    @Test
    public void testRecordPutAndGetMaxSize() {
        // Test with cluster path
        String clusterPath = "/admin/clusters/test-cluster";
        byte[] smallData = "small".getBytes(StandardCharsets.UTF_8);
        byte[] largeData = "this is a much larger data payload".getBytes(StandardCharsets.UTF_8);

        // Initially should return UNSET (-1)
        assertEquals(stats.getMaxSizeOfSameResourceType(clusterPath), DefaultMetadataNodeSizeStats.UNSET);

        // Record small data first
        stats.recordPut(clusterPath, smallData);
        assertEquals(stats.getMaxSizeOfSameResourceType(clusterPath), smallData.length);

        // Record larger data - should update the max
        stats.recordPut(clusterPath, largeData);
        assertEquals(stats.getMaxSizeOfSameResourceType(clusterPath), largeData.length);

        // Record smaller data again - should not change the max
        stats.recordPut(clusterPath, smallData);
        assertEquals(stats.getMaxSizeOfSameResourceType(clusterPath), largeData.length);
    }

    @Test
    public void testRecordGetResAndGetMaxSize() {
        String tenantPath = "/admin/policies/test-tenant";
        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "much longer data payload".getBytes(StandardCharsets.UTF_8);

        // Create mock GetResult objects
        Stat mockStat = Mockito.mock(Stat.class);
        GetResult getResult1 = new GetResult(data1, mockStat);
        GetResult getResult2 = new GetResult(data2, mockStat);

        // Initially should return UNSET (-1)
        assertEquals(stats.getMaxSizeOfSameResourceType(tenantPath), DefaultMetadataNodeSizeStats.UNSET);

        // Record first result
        stats.recordGetRes(tenantPath, getResult1);
        assertEquals(stats.getMaxSizeOfSameResourceType(tenantPath), data1.length);

        // Record larger result - should update the max
        stats.recordGetRes(tenantPath, getResult2);
        assertEquals(stats.getMaxSizeOfSameResourceType(tenantPath), data2.length);

        // Record smaller result again - should not change the max
        stats.recordGetRes(tenantPath, getResult1);
        assertEquals(stats.getMaxSizeOfSameResourceType(tenantPath), data2.length);
    }

    @Test
    public void testRecordGetChildrenResAndGetMaxChildrenCount() {
        String namespacePath = "/admin/policies/test-tenant/test-namespace";
        List<String> smallList = Arrays.asList("a", "b", "c");
        List<String> largeList = Arrays.asList("longer-name-1", "longer-name-2", "longer-name-3", "longer-name-4");

        // Initially should return UNSET (-1)
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(namespacePath), DefaultMetadataNodeSizeStats.UNSET);

        // Record small list first - should track the count, not the total length
        stats.recordGetChildrenRes(namespacePath, smallList);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(namespacePath), smallList.size());

        // Record larger list - should update the max count
        stats.recordGetChildrenRes(namespacePath, largeList);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(namespacePath), largeList.size());

        // Record smaller list again - should not change the max
        stats.recordGetChildrenRes(namespacePath, smallList);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(namespacePath), largeList.size());
    }

    @Test
    public void testUnknownPathsReturnMinusOne() {
        // Test that unknown paths return -1 (not UNSET from internal storage)
        String unknownPath = "/some/unknown/path";
        assertEquals(stats.getMaxSizeOfSameResourceType(unknownPath), -1);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(unknownPath), -1);

        // Test that recording data for unknown paths is ignored
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(unknownPath, data);
        assertEquals(stats.getMaxSizeOfSameResourceType(unknownPath), -1);

        // Test that recording children for unknown paths is ignored
        List<String> children = Arrays.asList("child1", "child2");
        stats.recordGetChildrenRes(unknownPath, children);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(unknownPath), -1);
    }

    @Test
    public void testNullGetResult() {
        String testPath = "/admin/clusters/test";

        // Recording null GetResult should not affect the stats
        stats.recordGetRes(testPath, null);
        assertEquals(stats.getMaxSizeOfSameResourceType(testPath), DefaultMetadataNodeSizeStats.UNSET);

        // Record valid data first
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(testPath, data);
        assertEquals(stats.getMaxSizeOfSameResourceType(testPath), data.length);

        // Recording null GetResult should not change the existing max
        stats.recordGetRes(testPath, null);
        assertEquals(stats.getMaxSizeOfSameResourceType(testPath), data.length);
    }

    @Test
    public void testAdminPathTypeClassification() {
        // Test cluster paths
        String clusterPath = "/admin/clusters/test-cluster";
        byte[] clusterData = "cluster-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(clusterPath, clusterData);
        assertEquals(stats.getMaxSizeOfSameResourceType(clusterPath), clusterData.length);

        // Test tenant paths (policies with 2 parts)
        String tenantPath = "/admin/policies/test-tenant";
        byte[] tenantData = "tenant-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(tenantPath, tenantData);
        assertEquals(stats.getMaxSizeOfSameResourceType(tenantPath), tenantData.length);

        // Test namespace policy paths (policies with 3 parts)
        String namespacePath = "/admin/policies/test-tenant/test-namespace";
        byte[] namespaceData = "namespace-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(namespacePath, namespaceData);
        assertEquals(stats.getMaxSizeOfSameResourceType(namespacePath), namespaceData.length);

        // Test local policies paths
        String localPoliciesPath = "/admin/local-policies/test-tenant/test-namespace";
        byte[] localPoliciesData = "local-policies-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(localPoliciesPath, localPoliciesData);
        assertEquals(stats.getMaxSizeOfSameResourceType(localPoliciesPath), localPoliciesData.length);

        // Test partitioned namespace paths (5 parts)
        String partitionedNamespacePath = "/admin/partitioned-topics/persistent/test-tenant/test-namespace";
        byte[] partitionedNamespaceData = "partitioned-namespace-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(partitionedNamespacePath, partitionedNamespaceData);
        assertEquals(stats.getMaxSizeOfSameResourceType(partitionedNamespacePath), partitionedNamespaceData.length);

        // Test partitioned topic paths (6 parts)
        String partitionedTopicPath = "/admin/partitioned-topics/persistent/test-tenant/test-namespace/test-topic";
        byte[] partitionedTopicData = "partitioned-topic-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(partitionedTopicPath, partitionedTopicData);
        assertEquals(stats.getMaxSizeOfSameResourceType(partitionedTopicPath), partitionedTopicData.length);

        // Verify that different path types maintain separate max values
        assertNotEquals(stats.getMaxSizeOfSameResourceType(clusterPath),
                       stats.getMaxSizeOfSameResourceType(tenantPath));
        assertNotEquals(stats.getMaxSizeOfSameResourceType(tenantPath),
                       stats.getMaxSizeOfSameResourceType(namespacePath));
    }

    @Test
    public void testManagedLedgerPathTypes() {
        // Test ML namespace paths (4 parts)
        String mlNamespacePath = "/managed-ledgers/tenant/namespace/persistent";
        byte[] mlNamespaceData = "ml-namespace-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(mlNamespacePath, mlNamespaceData);
        assertEquals(stats.getMaxSizeOfSameResourceType(mlNamespacePath), mlNamespaceData.length);

        // Test topic paths (5 parts)
        String topicPath = "/managed-ledgers/tenant/namespace/persistent/topic";
        byte[] topicData = "topic-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(topicPath, topicData);
        assertEquals(stats.getMaxSizeOfSameResourceType(topicPath), topicData.length);

        // Test v2 subscription paths (6 parts)
        String v2SubscriptionPath = "/managed-ledgers/tenant/namespace/persistent/topic/subscription";
        byte[] v2SubscriptionData = "v2-subscription-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(v2SubscriptionPath, v2SubscriptionData);
        assertEquals(stats.getMaxSizeOfSameResourceType(v2SubscriptionPath), v2SubscriptionData.length);

        // Test v1 subscription paths (7 parts)
        String v1SubscriptionPath = "/managed-ledgers/tenant/cluster/namespace/persistent/topic/subscription";
        byte[] v1SubscriptionData = "v1-subscription-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(v1SubscriptionPath, v1SubscriptionData);
        assertEquals(stats.getMaxSizeOfSameResourceType(v1SubscriptionPath), v1SubscriptionData.length);

        // Verify that v2 and v1 subscriptions use the same path type (SUBSCRIPTION)
        assertEquals(stats.getMaxSizeOfSameResourceType(v2SubscriptionPath),
                    Math.max(v2SubscriptionData.length, v1SubscriptionData.length));
        assertEquals(stats.getMaxSizeOfSameResourceType(v1SubscriptionPath),
                    Math.max(v2SubscriptionData.length, v1SubscriptionData.length));

        // Verify different path types maintain separate max values
        assertNotEquals(stats.getMaxSizeOfSameResourceType(mlNamespacePath),
                       stats.getMaxSizeOfSameResourceType(topicPath));
        assertNotEquals(stats.getMaxSizeOfSameResourceType(topicPath),
                       stats.getMaxSizeOfSameResourceType(v2SubscriptionPath));
    }

    @Test
    public void testLoadBalancePathTypes() {
        // Test brokers path (2 parts)
        String brokersPath = "/loadbalance/brokers";
        byte[] brokersData = "brokers-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(brokersPath, brokersData);
        assertEquals(stats.getMaxSizeOfSameResourceType(brokersPath), brokersData.length);

        // Test broker path (3 parts)
        String brokerPath = "/loadbalance/brokers/broker1";
        byte[] brokerData = "broker-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(brokerPath, brokerData);
        assertEquals(stats.getMaxSizeOfSameResourceType(brokerPath), brokerData.length);

        // Test bundle namespace path (4 parts)
        String bundleNamespacePath = "/loadbalance/bundle-data/tenant/namespace";
        byte[] bundleNamespaceData = "bundle-namespace-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(bundleNamespacePath, bundleNamespaceData);
        assertEquals(stats.getMaxSizeOfSameResourceType(bundleNamespacePath), bundleNamespaceData.length);

        // Test bundle data path (5 parts)
        String bundleDataPath = "/loadbalance/bundle-data/tenant/namespace/bundle";
        byte[] bundleData = "bundle-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(bundleDataPath, bundleData);
        assertEquals(stats.getMaxSizeOfSameResourceType(bundleDataPath), bundleData.length);

        // Test broker time average path (3 parts)
        String brokerTimeAvgPath = "/loadbalance/broker-time-average/broker1";
        byte[] brokerTimeAvgData = "broker-time-avg-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(brokerTimeAvgPath, brokerTimeAvgData);
        assertEquals(stats.getMaxSizeOfSameResourceType(brokerTimeAvgPath), brokerTimeAvgData.length);

        // Test leader path
        String leaderPath = "/loadbalance/leader";
        byte[] leaderData = "leader-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(leaderPath, leaderData);
        assertEquals(stats.getMaxSizeOfSameResourceType(leaderPath), leaderData.length);

        // Verify different path types maintain separate max values
        assertNotEquals(stats.getMaxSizeOfSameResourceType(brokersPath),
                       stats.getMaxSizeOfSameResourceType(brokerPath));
    }

    @Test
    public void testBundleOwnerPathTypes() {
        // Test bundle owner namespace path (3 parts)
        String bundleOwnerNamespacePath = "/namespace/tenant/namespace";
        byte[] bundleOwnerNamespaceData = "bundle-owner-namespace-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(bundleOwnerNamespacePath, bundleOwnerNamespaceData);
        assertEquals(stats.getMaxSizeOfSameResourceType(bundleOwnerNamespacePath), bundleOwnerNamespaceData.length);

        // Test bundle owner path (4 parts)
        String bundleOwnerPath = "/namespace/tenant/namespace/bundle";
        byte[] bundleOwnerData = "bundle-owner-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(bundleOwnerPath, bundleOwnerData);
        assertEquals(stats.getMaxSizeOfSameResourceType(bundleOwnerPath), bundleOwnerData.length);

        // Verify different path types maintain separate max values
        assertNotEquals(stats.getMaxSizeOfSameResourceType(bundleOwnerNamespacePath),
                       stats.getMaxSizeOfSameResourceType(bundleOwnerPath));
    }

    @Test
    public void testSchemaPathTypes() {
        // Test topic schema path (4 parts)
        String schemaPath = "/schemas/tenant/namespace/topic";
        byte[] schemaData = "schema-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(schemaPath, schemaData);
        assertEquals(stats.getMaxSizeOfSameResourceType(schemaPath), schemaData.length);

        // Test invalid schema path (3 parts) - should be UNKNOWN
        String invalidSchemaPath = "/schemas/tenant/namespace";
        byte[] invalidSchemaData = "invalid-schema-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(invalidSchemaPath, invalidSchemaData);
        assertEquals(stats.getMaxSizeOfSameResourceType(invalidSchemaPath), invalidSchemaData.length);

        // Verify they use different path types
        assertNotEquals(stats.getMaxSizeOfSameResourceType(schemaPath),
                       stats.getMaxSizeOfSameResourceType(invalidSchemaPath));
    }

    @Test
    public void testShortAndUnknownPaths() {
        // Test paths that are too short to be classified
        String shortPath1 = "/";
        String shortPath2 = "/admin";

        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);

        // Recording data for unknown paths should be ignored
        stats.recordPut(shortPath1, data);
        stats.recordPut(shortPath2, data);

        // Both should return -1 since they are UNKNOWN path types
        assertEquals(stats.getMaxSizeOfSameResourceType(shortPath1), -1);
        assertEquals(stats.getMaxSizeOfSameResourceType(shortPath2), -1);

        // Test unknown admin paths
        String unknownAdminPath = "/admin/unknown/path";
        byte[] unknownData = "unknown-admin-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(unknownAdminPath, unknownData);

        // Should return -1 since it's an UNKNOWN path type
        assertEquals(stats.getMaxSizeOfSameResourceType(unknownAdminPath), -1);

        // Test children recording for unknown paths
        List<String> children = Arrays.asList("child1", "child2");
        stats.recordGetChildrenRes(shortPath1, children);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(shortPath1), -1);
    }

    @Test
    public void testEmptyAndZeroSizeData() {
        String testPath = "/admin/clusters/test";

        // Test with empty data
        byte[] emptyData = new byte[0];
        stats.recordPut(testPath, emptyData);
        assertEquals(stats.getMaxSizeOfSameResourceType(testPath), 0);

        // Test with empty list
        List<String> emptyList = Collections.emptyList();
        stats.recordGetChildrenRes(testPath, emptyList);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(testPath), 0);

        // Record larger data - should update the max
        byte[] largerData = "larger-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(testPath, largerData);
        assertEquals(stats.getMaxSizeOfSameResourceType(testPath), largerData.length);

        // Record larger list - should update the max
        List<String> largerList = Arrays.asList("item1", "item2", "item3");
        stats.recordGetChildrenRes(testPath, largerList);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(testPath), largerList.size());
    }

    @Test
    public void testMixedOperationsOnSamePath() {
        String testPath = "/admin/policies/test-tenant";

        // Record via put
        byte[] putData = "put-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(testPath, putData);
        assertEquals(stats.getMaxSizeOfSameResourceType(testPath), putData.length);

        // Record via get with larger data - should update max
        byte[] largerData = "much larger get result data".getBytes(StandardCharsets.UTF_8);
        Stat mockStat = Mockito.mock(Stat.class);
        GetResult getResult = new GetResult(largerData, mockStat);
        stats.recordGetRes(testPath, getResult);
        assertEquals(stats.getMaxSizeOfSameResourceType(testPath), largerData.length);

        // Record via put with smaller data - should not change max
        byte[] smallerData = "small".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(testPath, smallerData);
        assertEquals(stats.getMaxSizeOfSameResourceType(testPath), largerData.length);

        // Test children count operations
        List<String> smallList = Arrays.asList("a", "b");
        stats.recordGetChildrenRes(testPath, smallList);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(testPath), smallList.size());

        List<String> largerList = Arrays.asList("a", "b", "c", "d", "e");
        stats.recordGetChildrenRes(testPath, largerList);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(testPath), largerList.size());
    }

    @Test
    public void testSplitPathFunctionality() {
        // Test the static splitPath method indirectly through path type classification

        // Test path with trailing slashes
        String pathWithTrailingSlash = "/admin/clusters/test-cluster/";
        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        stats.recordPut(pathWithTrailingSlash, data);
        assertEquals(stats.getMaxSizeOfSameResourceType(pathWithTrailingSlash), data.length);

        // Should be classified the same as path without trailing slash
        String pathWithoutTrailingSlash = "/admin/clusters/test-cluster";
        assertEquals(stats.getMaxSizeOfSameResourceType(pathWithoutTrailingSlash), data.length);

        // Test path with multiple consecutive slashes
        String pathWithMultipleSlashes = "/admin//clusters//test-cluster";
        assertEquals(stats.getMaxSizeOfSameResourceType(pathWithMultipleSlashes), data.length);
    }

    @Test
    public void testUnknownPathTypesAreIgnored() {
        // Test that UNKNOWN path types don't affect any statistics
        String[] unknownPaths = {
            "/",
            "/admin",
            "/unknown/path",
            "/admin/unknown/path/type",
            "/managed-ledgers", // too short
            "/loadbalance", // too short
            "/schemas/tenant", // too short (needs 4 parts)
            "/admin/policies/tenant/namespace/extra", // too long for policies
            "/admin/partitioned-topics/persistent", // too short for partitioned topics
            "/admin/partitioned-topics/persistent/tenant/namespace/topic/extra" // too long
        };

        byte[] data = "test-data".getBytes(StandardCharsets.UTF_8);
        List<String> children = Arrays.asList("child1", "child2", "child3");

        for (String unknownPath : unknownPaths) {
            // Record operations should be ignored
            stats.recordPut(unknownPath, data);
            stats.recordGetChildrenRes(unknownPath, children);

            // Should always return -1
            assertEquals(stats.getMaxSizeOfSameResourceType(unknownPath), -1,
                        "Path should return -1: " + unknownPath);
            assertEquals(stats.getMaxChildrenCountOfSameResourceType(unknownPath), -1,
                        "Path should return -1: " + unknownPath);
        }
    }

    @Test
    public void testEmptyAndNullListHandling() {
        String testPath = "/admin/clusters/test";

        // Test with null list (should be handled gracefully)
        stats.recordGetChildrenRes(testPath, null);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(testPath), 0);

        // Test with empty list
        List<String> emptyList = Collections.emptyList();
        stats.recordGetChildrenRes(testPath, emptyList);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(testPath), 0);

        // Test with non-empty list - should update the max
        List<String> nonEmptyList = Arrays.asList("item1", "item2", "item3");
        stats.recordGetChildrenRes(testPath, nonEmptyList);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(testPath), nonEmptyList.size());

        // Test with empty list again - should not change the max
        stats.recordGetChildrenRes(testPath, emptyList);
        assertEquals(stats.getMaxChildrenCountOfSameResourceType(testPath), nonEmptyList.size());
    }
}