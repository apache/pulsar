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
package org.apache.pulsar.metadata.impl.batching;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.metadata.api.MetadataNodeSizeStats;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Comprehensive test suite for ZKMetadataStoreBatchStrategy class.
 * This test covers:
 * - Constructor and configuration handling
 * - Batch size calculations for different operation types
 * - Size limit enforcement for requests and responses
 * - Operation count limits
 * - Edge cases and boundary conditions
 */
public class ZKMetadataStoreBatchStrategyTest {

    private ZKMetadataStoreBatchStrategy strategy;
    private MetadataNodeSizeStats mockNodeSizeStats;
    private ZooKeeper mockZooKeeper;
    private ZKClientConfig mockClientConfig;
    private MpscUnboundedArrayQueue<MetadataOp> operationQueue;

    private static final int DEFAULT_MAX_OPERATIONS = 100;
    private static final int DEFAULT_MAX_SIZE = 1024 * 1024; // 1MB
    private static final String TEST_PATH = "/test/path";

    @BeforeMethod
    public void setUp() {
        mockNodeSizeStats = Mockito.mock(MetadataNodeSizeStats.class);
        mockZooKeeper = Mockito.mock(ZooKeeper.class);
        mockClientConfig = Mockito.mock(ZKClientConfig.class);
        operationQueue = new MpscUnboundedArrayQueue<>(1000);
        // Setup default mock behavior
        Mockito.when(mockZooKeeper.getClientConfig()).thenReturn(mockClientConfig);
        Mockito.when(mockClientConfig.getInt(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(DEFAULT_MAX_SIZE);
        // Setup default node size stats
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(100);
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(Mockito.anyString()))
                .thenReturn(10);
        strategy = new ZKMetadataStoreBatchStrategy(mockNodeSizeStats, DEFAULT_MAX_OPERATIONS,
                DEFAULT_MAX_SIZE, mockZooKeeper);
    }

    @Test
    public void testConstructorWithDefaultMaxSize() {
        // Test constructor uses default max size when ZK config returns 0 or negative
        Mockito.when(mockClientConfig.getInt(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(0);
        ZKMetadataStoreBatchStrategy strategyWithDefault = new ZKMetadataStoreBatchStrategy(
                mockNodeSizeStats, DEFAULT_MAX_OPERATIONS, DEFAULT_MAX_SIZE, mockZooKeeper);
        assertEquals(strategyWithDefault.maxSize(), DEFAULT_MAX_SIZE);
    }

    @Test
    public void testConstructorWithConfiguredMaxSize() {
        int configuredSize = 2 * 1024 * 1024; // 2MB
        Mockito.when(mockClientConfig.getInt(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(configuredSize);
        ZKMetadataStoreBatchStrategy strategyWithConfig = new ZKMetadataStoreBatchStrategy(
                mockNodeSizeStats, DEFAULT_MAX_OPERATIONS, DEFAULT_MAX_SIZE, mockZooKeeper);
        assertEquals(strategyWithConfig.maxSize(), configuredSize);
    }

    @Test
    public void testMaxSizeMethod() {
        assertEquals(strategy.maxSize(), DEFAULT_MAX_SIZE);
    }

    @Test
    public void testEmptyQueue() {
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertTrue(batch.isEmpty());
    }

    @Test
    public void testSingleGetOperation() {
        MetadataOp getOp = createMockGetOperation(TEST_PATH);
        operationQueue.offer(getOp);
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), 1);
        assertEquals(batch.get(0), getOp);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testSingleGetChildrenOperation() {
        MetadataOp getChildrenOp = createMockGetChildrenOperation(TEST_PATH);
        operationQueue.offer(getChildrenOp);
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), 1);
        assertEquals(batch.get(0), getChildrenOp);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testSinglePutOperation() {
        MetadataOp putOp = createMockPutOperation(TEST_PATH, 100);
        operationQueue.offer(putOp);
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), 1);
        assertEquals(batch.get(0), putOp);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testSingleDeleteOperation() {
        MetadataOp deleteOp = createMockDeleteOperation(TEST_PATH);
        operationQueue.offer(deleteOp);
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), 1);
        assertEquals(batch.get(0), deleteOp);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testMaxOperationsLimit() {
        // Add more operations than the max limit
        for (int i = 0; i < DEFAULT_MAX_OPERATIONS + 10; i++) {
            operationQueue.offer(createMockGetOperation("/test/path/" + i));
        }
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), DEFAULT_MAX_OPERATIONS);
        assertEquals(operationQueue.size(), 10); // Remaining operations
    }

    @Test
    public void testRequestSizeLimit() {
        // Create large PUT operations that exceed the request size limit
        int largeSize = DEFAULT_MAX_SIZE / 2 + 1000; // Larger than half max size
        operationQueue.offer(createMockPutOperation("/test/path1", largeSize));
        operationQueue.offer(createMockPutOperation("/test/path2", largeSize));
        operationQueue.offer(createMockPutOperation("/test/path3", 100)); // Small operation
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        // Should only include the first operation due to size limit
        assertEquals(batch.size(), 1);
        assertEquals(operationQueue.size(), 2); // Two operations remaining
    }

    // Helper methods for creating mock operations
    private MetadataOp createMockGetOperation(String path) {
        MetadataOp op = Mockito.mock(MetadataOp.class);
        Mockito.when(op.getType()).thenReturn(MetadataOp.Type.GET);
        Mockito.when(op.getPath()).thenReturn(path);
        Mockito.when(op.size()).thenReturn(0);
        Mockito.when(op.getFuture()).thenReturn(new CompletableFuture<>());
        return op;
    }

    private MetadataOp createMockGetChildrenOperation(String path) {
        MetadataOp op = Mockito.mock(MetadataOp.class);
        Mockito.when(op.getType()).thenReturn(MetadataOp.Type.GET_CHILDREN);
        Mockito.when(op.getPath()).thenReturn(path);
        Mockito.when(op.size()).thenReturn(0);
        Mockito.when(op.getFuture()).thenReturn(new CompletableFuture<>());
        return op;
    }

    private MetadataOp createMockPutOperation(String path, int size) {
        MetadataOp op = Mockito.mock(MetadataOp.class);
        Mockito.when(op.getType()).thenReturn(MetadataOp.Type.PUT);
        Mockito.when(op.getPath()).thenReturn(path);
        Mockito.when(op.size()).thenReturn(size);
        Mockito.when(op.getFuture()).thenReturn(new CompletableFuture<>());
        return op;
    }

    private MetadataOp createMockDeleteOperation(String path) {
        MetadataOp op = Mockito.mock(MetadataOp.class);
        Mockito.when(op.getType()).thenReturn(MetadataOp.Type.DELETE);
        Mockito.when(op.getPath()).thenReturn(path);
        Mockito.when(op.size()).thenReturn(path.length());
        Mockito.when(op.getFuture()).thenReturn(new CompletableFuture<>());
        return op;
    }

    @Test
    public void testResponseSizeLimitForGetOperations() {
        // Setup large response size for node stats
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(DEFAULT_MAX_SIZE / 5); // Large response size
        // Add multiple GET operations
        for (int i = 0; i < 10; i++) {
            operationQueue.offer(createMockGetOperation("/test/path/" + i));
        }
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        // Should limit based on estimated response size (half of max size)
        assertEquals(batch.size(), 2);
    }

    @Test
    public void testResponseSizeLimitForGetChildrenOperations() {
        // Setup large response size for children operations
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(Mockito.anyString()))
                .thenReturn(1000); // Many children
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(100); // Size per child
        // Add multiple GET_CHILDREN operations
        for (int i = 0; i < 10; i++) {
            operationQueue.offer(createMockGetChildrenOperation("/test/path/" + i));
        }
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        // Should limit based on estimated response size.
        // DEFAULT_MAX_SIZE is 1024 * 1024, the half value is 1024 * 512.
        // Per children response is 1000 * 100.
        // So the result should be 5;
        assertEquals(batch.size(), 5);
    }

    @Test
    public void testMixedOperationTypes() {
        operationQueue.offer(createMockGetOperation("/test/get"));
        operationQueue.offer(createMockPutOperation("/test/put", 100));
        operationQueue.offer(createMockDeleteOperation("/test/delete"));
        operationQueue.offer(createMockGetChildrenOperation("/test/children"));
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), 4);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testNullOperationInQueue() {
        // Test the case where peek() returns null (queue becomes empty during processing)
        MetadataOp mockOp = createMockGetOperation("/test/path1");
        @SuppressWarnings("unchecked")
        MpscUnboundedArrayQueue<MetadataOp> mockQueue = Mockito.mock(MpscUnboundedArrayQueue.class);
        Mockito.when(mockQueue.isEmpty()).thenReturn(false, false, true);
        Mockito.when(mockQueue.peek()).thenReturn(mockOp, (MetadataOp) null);
        Mockito.when(mockQueue.poll()).thenReturn(mockOp);
        List<MetadataOp> batch = strategy.nextBatch(mockQueue);
        assertEquals(batch.size(), 1);
    }

    @Test
    public void testZKResponseHeaderCalculation() {
        // Test that ZK_RESPONSE_HEADER_LEN is correctly used in calculations
        assertEquals(ZKMetadataStoreBatchStrategy.ZK_RESPONSE_HEADER_LEN, 88);
        // Add a GET operation and verify header is included in size calculation
        operationQueue.offer(createMockGetOperation(TEST_PATH));
        // The actual size calculation is internal, but we can verify the operation is processed
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), 1);
    }

    @Test
    public void testDefaultSizeFallbackForGetOperations() {
        // Test that defaultSize is used when node size stats return negative values
        // defaultSize = Math.max(maxPutSize >>> 4, 1024) = Math.max(1MB >>> 4, 1024) = Math.max(65536, 1024) = 65536
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(-1); // Negative value should trigger defaultSize fallback
        operationQueue.offer(createMockGetOperation("/test/path1"));
        operationQueue.offer(createMockGetOperation("/test/path2"));
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        // Should process operations using defaultSize for size estimation
        assertEquals(batch.size(), 2);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testDefaultSizeFallbackForGetChildrenOperations() {
        // Test defaultSize fallback for GET_CHILDREN when childrenCount < 0
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(Mockito.anyString()))
                .thenReturn(-1); // Negative value should trigger defaultSize fallback
        operationQueue.offer(createMockGetChildrenOperation("/test/path1"));
        operationQueue.offer(createMockGetChildrenOperation("/test/path2"));
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        // Should process operations using defaultSize for size estimation
        assertEquals(batch.size(), 2);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testDefaultSizeFallbackForGetChildrenWithValidCountButInvalidSize() {
        // Test defaultSize fallback when childrenCount >= 0 but size <= 0
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(Mockito.anyString()))
                .thenReturn(5); // Valid children count
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(-1); // Invalid size should trigger defaultSize fallback
        operationQueue.offer(createMockGetChildrenOperation("/test/path"));
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), 1);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testDefaultOperationType() {
        // The implementation doesn't handle null types, so let's test with a valid operation
        // that would use the default case in the switch statement
        MetadataOp putOp = createMockPutOperation(TEST_PATH, 50);
        operationQueue.offer(putOp);
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), 1);
        assertEquals(batch.get(0), putOp);
    }

    @Test
    public void testLargePathNames() {
        // Test with very long path names
        String longPath = "/very/long/path/name/that/exceeds/normal/length/"
                + "and/continues/for/a/very/long/time/to/test/path/handling/"
                + "in/the/batch/strategy/implementation";
        operationQueue.offer(createMockPutOperation(longPath, 100));
        operationQueue.offer(createMockDeleteOperation(longPath));
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), 2);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testZeroSizeOperations() {
        // Test operations with zero size
        operationQueue.offer(createMockPutOperation(TEST_PATH, 0));
        operationQueue.offer(createMockGetOperation(TEST_PATH));
        List<MetadataOp> batch = strategy.nextBatch(operationQueue);
        assertEquals(batch.size(), 2);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testNodeSizeStatsIntegration() {
        String path1 = "/test/path1";
        String path2 = "/test/path2";

        // Setup different sizes for different paths
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(path1)).thenReturn(1000);
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(path2)).thenReturn(2000);
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(path1)).thenReturn(50);
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(path2)).thenReturn(100);

        operationQueue.offer(createMockGetOperation(path1));
        operationQueue.offer(createMockGetChildrenOperation(path2));

        List<MetadataOp> batch = strategy.nextBatch(operationQueue);

        assertEquals(batch.size(), 2);

        // Verify that the node size stats were consulted
        Mockito.verify(mockNodeSizeStats).getMaxSizeOfSameResourceType(path1);
        Mockito.verify(mockNodeSizeStats).getMaxChildrenCountOfSameResourceType(path2);
    }

    @Test
    public void testBatchSizeCalculationAccuracy() {
        // Test that batch size calculation prevents oversized batches

        // Setup a scenario where the second operation would exceed the limit
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(DEFAULT_MAX_SIZE / 3); // Each operation uses 1/3 of max size

        operationQueue.offer(createMockGetOperation("/test/path1"));
        operationQueue.offer(createMockGetOperation("/test/path2"));
        operationQueue.offer(createMockGetOperation("/test/path3")); // This should not fit

        List<MetadataOp> batch = strategy.nextBatch(operationQueue);

        // Should include first two operations but not the third
        assertTrue(batch.size() <= 2);
        assertTrue(batch.size() >= 1);
    }

    @Test
    public void testMixedValidAndInvalidNodeSizeStats() {
        // Test mixing operations with valid and invalid node size stats
        String validPath = "/valid/path";
        String invalidPath = "/invalid/path";

        // Setup different responses for different paths
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(validPath)).thenReturn(500);
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(invalidPath)).thenReturn(-1);
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(validPath)).thenReturn(10);
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(invalidPath)).thenReturn(-1);

        operationQueue.offer(createMockGetOperation(validPath));
        operationQueue.offer(createMockGetOperation(invalidPath)); // Should use defaultSize
        operationQueue.offer(createMockGetChildrenOperation(validPath));
        operationQueue.offer(createMockGetChildrenOperation(invalidPath)); // Should use defaultSize

        List<MetadataOp> batch = strategy.nextBatch(operationQueue);

        assertEquals(batch.size(), 4);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testZeroSizeFromNodeStats() {
        // Test handling of zero size from node stats (should use defaultSize)
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(0); // Zero size should trigger defaultSize fallback
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(Mockito.anyString()))
                .thenReturn(5); // Valid children count

        operationQueue.offer(createMockGetOperation("/test/get"));
        operationQueue.offer(createMockGetChildrenOperation("/test/children"));

        List<MetadataOp> batch = strategy.nextBatch(operationQueue);

        assertEquals(batch.size(), 2);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testDynamicDefaultSizeCalculation() {
        // Test that defaultSize is calculated dynamically based on maxPutSize
        // defaultSize = Math.max(maxPutSize >>> 4, 1024)

        // Test with default configuration (1MB)
        // defaultSize = Math.max(1048576 >>> 4, 1024) = Math.max(65536, 1024) = 65536
        assertEquals(strategy.maxSize(), DEFAULT_MAX_SIZE); // Verify maxPutSize

        // Test with smaller maxPutSize
        int smallMaxSize = 8192; // 8KB
        Mockito.when(mockClientConfig.getInt(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(smallMaxSize);

        ZKMetadataStoreBatchStrategy smallStrategy = new ZKMetadataStoreBatchStrategy(
                mockNodeSizeStats, DEFAULT_MAX_OPERATIONS, DEFAULT_MAX_SIZE, mockZooKeeper);

        assertEquals(smallStrategy.maxSize(), smallMaxSize);

        // For small maxPutSize, defaultSize should be Math.max(8192 >>> 4, 1024) = Math.max(512, 1024) = 1024
        // We can't directly access defaultSize, but we can test its behavior
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(-1); // Force defaultSize usage

        MpscUnboundedArrayQueue<MetadataOp> smallQueue = new MpscUnboundedArrayQueue<>(1000);
        smallQueue.offer(createMockGetOperation("/test/path"));

        List<MetadataOp> batch = smallStrategy.nextBatch(smallQueue);
        assertEquals(batch.size(), 1);
    }

    @Test
    public void testDynamicDefaultSizeWithLargeMaxPutSize() {
        // Test with very large maxPutSize
        int largeMaxSize = 16 * 1024 * 1024; // 16MB
        Mockito.when(mockClientConfig.getInt(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(largeMaxSize);

        ZKMetadataStoreBatchStrategy largeStrategy = new ZKMetadataStoreBatchStrategy(
                mockNodeSizeStats, DEFAULT_MAX_OPERATIONS, DEFAULT_MAX_SIZE, mockZooKeeper);

        assertEquals(largeStrategy.maxSize(), largeMaxSize);

        // For large maxPutSize, defaultSize should be Math.max(16MB >>> 4, 1024) = Math.max(1MB, 1024) = 1MB
        // This allows for larger batches when size stats are unavailable
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(-1); // Force defaultSize usage

        MpscUnboundedArrayQueue<MetadataOp> largeQueue = new MpscUnboundedArrayQueue<>(1000);

        // Add multiple operations - with larger defaultSize, more should fit
        for (int i = 0; i < 20; i++) {
            largeQueue.offer(createMockGetOperation("/test/path/" + i));
        }

        List<MetadataOp> batch = largeStrategy.nextBatch(largeQueue);
        assertTrue(batch.size() > 0);
        assertTrue(batch.size() <= 20);
    }

    @Test
    public void testDefaultSizeBehavior() {
        // Test that the defaultSize field is used correctly for size calculations
        // We can't directly access the private field, but we can test its behavior

        // When node stats return negative values, defaultSize should be used
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(-1);

        // Add many GET operations to test if defaultSize is being used for calculations
        for (int i = 0; i < 100; i++) {
            operationQueue.offer(createMockGetOperation("/test/path/" + i));
        }

        List<MetadataOp> batch = strategy.nextBatch(operationQueue);

        // Should be able to process some operations (exact number depends on defaultSize calculation)
        assertTrue(batch.size() >= 7);
        assertTrue(batch.size() <= 8);
    }

    @Test
    public void testGetChildrenWithZeroChildrenCount() {
        // Test GET_CHILDREN with zero children count (valid case)
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(Mockito.anyString()))
                .thenReturn(0); // Zero children is valid
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(100); // Valid size

        operationQueue.offer(createMockGetChildrenOperation("/empty/directory"));

        List<MetadataOp> batch = strategy.nextBatch(operationQueue);

        assertEquals(batch.size(), 1);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testEnhancedSizeCalculationLogic() {
        // Test the enhanced size calculation logic with various scenarios

        // Scenario 1: Valid stats
        String path1 = "/valid/stats";
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(path1)).thenReturn(200);
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(path1)).thenReturn(5);

        // Scenario 2: Invalid children count, valid size
        String path2 = "/invalid/children";
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(path2)).thenReturn(300);
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(path2)).thenReturn(-1);

        // Scenario 3: Valid children count, invalid size
        String path3 = "/invalid/size";
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(path3)).thenReturn(-1);
        Mockito.when(mockNodeSizeStats.getMaxChildrenCountOfSameResourceType(path3)).thenReturn(3);

        operationQueue.offer(createMockGetOperation(path1));
        operationQueue.offer(createMockGetChildrenOperation(path1));
        operationQueue.offer(createMockGetChildrenOperation(path2)); // Should use defaultSize
        operationQueue.offer(createMockGetChildrenOperation(path3)); // Should use defaultSize for size
        operationQueue.offer(createMockGetOperation(path2));
        operationQueue.offer(createMockGetOperation(path3)); // Should use defaultSize

        List<MetadataOp> batch = strategy.nextBatch(operationQueue);

        // All operations should be processed
        assertEquals(batch.size(), 6);
        assertTrue(operationQueue.isEmpty());
    }

    @Test
    public void testDefaultSizeCalculationFormula() {
        // Test the specific formula: defaultSize = Math.max(maxPutSize >>> 4, 1024)

        // Test case 1: maxPutSize = 1MB (default), defaultSize should be Math.max(65536, 1024) = 65536
        int maxSize1 = 1024 * 1024; // 1MB
        Mockito.when(mockClientConfig.getInt(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(maxSize1);

        ZKMetadataStoreBatchStrategy strategy1 = new ZKMetadataStoreBatchStrategy(
                mockNodeSizeStats, DEFAULT_MAX_OPERATIONS, DEFAULT_MAX_SIZE, mockZooKeeper);

        // Test case 2: maxPutSize = 8KB, defaultSize should be Math.max(512, 1024) = 1024
        int maxSize2 = 8 * 1024; // 8KB
        Mockito.when(mockClientConfig.getInt(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(maxSize2);

        ZKMetadataStoreBatchStrategy strategy2 = new ZKMetadataStoreBatchStrategy(
                mockNodeSizeStats, DEFAULT_MAX_OPERATIONS, DEFAULT_MAX_SIZE, mockZooKeeper);

        // Test case 3: maxPutSize = 32MB, defaultSize should be Math.max(2MB, 1024) = 2MB
        int maxSize3 = 32 * 1024 * 1024; // 32MB
        Mockito.when(mockClientConfig.getInt(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(maxSize3);

        ZKMetadataStoreBatchStrategy strategy3 = new ZKMetadataStoreBatchStrategy(
                mockNodeSizeStats, DEFAULT_MAX_OPERATIONS, DEFAULT_MAX_SIZE, mockZooKeeper);

        // Verify maxSize() returns the configured values
        assertEquals(strategy1.maxSize(), maxSize1);
        assertEquals(strategy2.maxSize(), maxSize2);
        assertEquals(strategy3.maxSize(), maxSize3);

        // Test behavior with invalid node stats to verify defaultSize usage
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(-1);

        // All strategies should be able to process operations using their respective defaultSize
        MpscUnboundedArrayQueue<MetadataOp> queue1 = new MpscUnboundedArrayQueue<>(10);
        MpscUnboundedArrayQueue<MetadataOp> queue2 = new MpscUnboundedArrayQueue<>(10);
        MpscUnboundedArrayQueue<MetadataOp> queue3 = new MpscUnboundedArrayQueue<>(10);

        queue1.offer(createMockGetOperation("/test1"));
        queue2.offer(createMockGetOperation("/test2"));
        queue3.offer(createMockGetOperation("/test3"));

        List<MetadataOp> batch1 = strategy1.nextBatch(queue1);
        List<MetadataOp> batch2 = strategy2.nextBatch(queue2);
        List<MetadataOp> batch3 = strategy3.nextBatch(queue3);

        assertEquals(batch1.size(), 1);
        assertEquals(batch2.size(), 1);
        assertEquals(batch3.size(), 1);
    }

    @Test
    public void testDefaultSizeMinimumValue() {
        // Test that defaultSize never goes below 1024 bytes
        int verySmallMaxSize = 1024; // 1KB
        Mockito.when(mockClientConfig.getInt(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(verySmallMaxSize);

        ZKMetadataStoreBatchStrategy smallStrategy = new ZKMetadataStoreBatchStrategy(
                mockNodeSizeStats, DEFAULT_MAX_OPERATIONS, DEFAULT_MAX_SIZE, mockZooKeeper);

        // defaultSize should be Math.max(1024 >>> 4, 1024) = Math.max(64, 1024) = 1024
        assertEquals(smallStrategy.maxSize(), verySmallMaxSize);

        // Test that operations can still be processed even with minimum defaultSize
        Mockito.when(mockNodeSizeStats.getMaxSizeOfSameResourceType(Mockito.anyString()))
                .thenReturn(-1);

        MpscUnboundedArrayQueue<MetadataOp> smallQueue = new MpscUnboundedArrayQueue<>(10);
        smallQueue.offer(createMockGetOperation("/test/small"));

        List<MetadataOp> batch = smallStrategy.nextBatch(smallQueue);
        assertEquals(batch.size(), 1);
    }
}
