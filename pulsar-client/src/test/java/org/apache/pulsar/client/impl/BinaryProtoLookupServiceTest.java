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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException.LookupException;
import org.apache.pulsar.client.impl.BinaryProtoLookupService.LookupDataResult;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.BaseCommand.Type;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BinaryProtoLookupServiceTest {
    private BinaryProtoLookupService lookup;
    private TopicName topicName;
    private ExecutorService internalExecutor;

    @AfterMethod
    public void cleanup() throws Exception {
        internalExecutor.shutdown();
        lookup.close();
    }

    @BeforeMethod
    public void setup() throws Exception {
        LookupDataResult lookupResult1 = createLookupDataResult("pulsar://broker1.pulsar.apache.org:6650", true);
        LookupDataResult lookupResult2 = createLookupDataResult("pulsar://broker2.pulsar.apache.org:6650", false);

        CompletableFuture<LookupDataResult> lookupFuture1 = CompletableFuture.completedFuture(lookupResult1);
        CompletableFuture<LookupDataResult> lookupFuture2 = CompletableFuture.completedFuture(lookupResult2);

        ClientCnx clientCnx = mock(ClientCnx.class);
        AtomicInteger lookupInvocationCounter = new AtomicInteger();
        doAnswer(invocation -> {
            ByteBuf byteBuf = invocation.getArgument(0);
            byteBuf.release();
            int lookupInvocationCount = lookupInvocationCounter.incrementAndGet();
            if (lookupInvocationCount < 3) {
                return lookupFuture1;
            } else {
                return lookupFuture2;
            }
        }).when(clientCnx).newLookup(any(ByteBuf.class), anyLong());

        CompletableFuture<ClientCnx> connectionFuture = CompletableFuture.completedFuture(clientCnx);

        ConnectionPool cnxPool = mock(ConnectionPool.class);
        when(cnxPool.getConnection(any(InetSocketAddress.class))).thenReturn(connectionFuture);

        ClientConfigurationData clientConfig = mock(ClientConfigurationData.class);
        doReturn(0).when(clientConfig).getMaxLookupRedirects();

        PulsarClientImpl client = mock(PulsarClientImpl.class);
        doReturn(cnxPool).when(client).getCnxPool();
        doReturn(clientConfig).when(client).getConfiguration();
        doReturn(1L).when(client).newRequestId();
        ClientConfigurationData data = new ClientConfigurationData();
        doReturn(data).when(client).getConfiguration();
        internalExecutor =
                Executors.newSingleThreadExecutor(new DefaultThreadFactory("pulsar-client-test-internal-executor"));
        doReturn(internalExecutor).when(client).getInternalExecutorService();

        lookup = spy(new BinaryProtoLookupService(client, "pulsar://localhost:6650", null, false,
                mock(ExecutorService.class), internalExecutor));

        topicName = TopicName.get("persistent://tenant1/ns1/t1");
    }

    @Test(invocationTimeOut = 3000)
    public void maxLookupRedirectsTest1() throws Exception {
        Pair<InetSocketAddress, InetSocketAddress> addressPair = lookup.getBroker(topicName).get();
        assertEquals(addressPair.getLeft(), InetSocketAddress
                .createUnresolved("broker2.pulsar.apache.org" ,6650));
        assertEquals(addressPair.getRight(), InetSocketAddress
                .createUnresolved("broker2.pulsar.apache.org" ,6650));
    }

    @Test(invocationTimeOut = 3000)
    public void maxLookupRedirectsTest2() throws Exception {
        Field field = BinaryProtoLookupService.class.getDeclaredField("maxLookupRedirects");
        field.setAccessible(true);
        field.set(lookup, 2);

        Pair<InetSocketAddress, InetSocketAddress> addressPair = lookup.getBroker(topicName).get();
        assertEquals(addressPair.getLeft(), InetSocketAddress
                .createUnresolved("broker2.pulsar.apache.org" ,6650));
        assertEquals(addressPair.getRight(), InetSocketAddress
                .createUnresolved("broker2.pulsar.apache.org" ,6650));
    }

    @Test(invocationTimeOut = 3000)
    public void maxLookupRedirectsTest3() throws Exception {
        Field field = BinaryProtoLookupService.class.getDeclaredField("maxLookupRedirects");
        field.setAccessible(true);
        field.set(lookup, 1);

        try {
            lookup.getBroker(topicName).get();
            fail("should have thrown ExecutionException");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof LookupException);
            assertEquals(cause.getMessage(), "Too many redirects: 1");
        }
    }

    @Test
    public void testCommandUnChangedInDifferentThread() throws Exception {
        BaseCommand successCommand = Commands.newSuccessCommand(10000);
        lookup.getBroker(topicName).get();
        assertEquals(successCommand.getType(), Type.SUCCESS);
        lookup.getPartitionedTopicMetadata(topicName, true, true).get();
        assertEquals(successCommand.getType(), Type.SUCCESS);
    }

    @Test
    public void testCommandChangedInSameThread() throws Exception {
        AtomicReference<BaseCommand> successCommand = new AtomicReference<>();
        internalExecutor.execute(() -> successCommand.set(Commands.newSuccessCommand(10000)));
        Awaitility.await().untilAsserted(() -> {
            BaseCommand baseCommand = successCommand.get();
            assertNotNull(baseCommand);
            assertEquals(baseCommand.getType(), Type.SUCCESS);
        });
        lookup.getBroker(topicName).get();
        assertEquals(successCommand.get().getType(), Type.LOOKUP);

        internalExecutor.execute(() -> successCommand.set(Commands.newSuccessCommand(10000)));
        Awaitility.await().untilAsserted(() -> {
            BaseCommand baseCommand = successCommand.get();
            assertNotNull(baseCommand);
            assertEquals(baseCommand.getType(), Type.SUCCESS);
        });
        lookup.getPartitionedTopicMetadata(topicName, true, true).get();
        assertEquals(successCommand.get().getType(), Type.PARTITIONED_METADATA);
    }

    private static LookupDataResult createLookupDataResult(String brokerUrl, boolean redirect) throws Exception {
        LookupDataResult lookupResult = new LookupDataResult(-1);

        Field brokerUrlField = LookupDataResult.class.getDeclaredField("brokerUrl");
        brokerUrlField.setAccessible(true);
        brokerUrlField.set(lookupResult, brokerUrl);

        Field redirectField = LookupDataResult.class.getDeclaredField("redirect");
        redirectField.setAccessible(true);
        redirectField.set(lookupResult, redirect);

        return lookupResult;
    }

    /**
     * Verifies that getPartitionedTopicMetadata() deduplicates concurrent requests and cleans up after completion.
     *
     * First, two concurrent calls with identical parameters (topicName, metadataAutoCreationEnabled,
     * useFallbackForNonPIP344Brokers) should return the same CompletableFuture and trigger only one connection pool
     * request (deduplication).
     *
     * Second, after the future completes, the map entry should be removed so a subsequent call
     * with the same parameters creates a new future (cleanup).
     *
     * This test uses a never-completing connection future to isolate the deduplication logic
     * without executing the network request path.
     */
    @Test(timeOut = 60000)
    public void testPartitionedMetadataDeduplicationAndCleanup() throws Exception {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool cnxPool = mock(ConnectionPool.class);

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(30000);
        when(client.getConfiguration()).thenReturn(conf);
        when(client.getCnxPool()).thenReturn(cnxPool);

        // Never-completing connection prevents the thenAcceptAsync callback in getPartitionedTopicMetadata
        // from executing, isolating only the deduplication logic without network calls.
        CompletableFuture<ClientCnx> neverCompletes = new CompletableFuture<>();
        when(cnxPool.getConnection(any(InetSocketAddress.class))).thenReturn(neverCompletes);

        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("lookup-test-sched"));

        try (BinaryProtoLookupService lookup = new BinaryProtoLookupService(
                client, "pulsar://broker:6650", null, false, scheduler, null)) {

            TopicName topic = TopicName.get("persistent://public/default/t1");
            boolean metadataAutoCreationEnabled = true;
            boolean useFallbackForNonPIP344Brokers = false;

            CompletableFuture<PartitionedTopicMetadata> f1 = lookup.getPartitionedTopicMetadata(topic,
                    metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);
            CompletableFuture<PartitionedTopicMetadata> f1b = lookup.getPartitionedTopicMetadata(topic,
                    metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);

            // Dedup: same parameters share the same future and one getConnection call.
            assertSame(f1b, f1,
                    "Concurrent requests with identical parameters should return the same future");
            verify(cnxPool, times(1)).getConnection(any(InetSocketAddress.class));

            // Complete the future. This triggers the whenComplete callback that removes the map entry.
            f1.complete(new PartitionedTopicMetadata(0));
            assertTrue(f1.isDone());

            // Verify cleanup: after completion, same parameters create a new future and
            // trigger another connection request.
            CompletableFuture<PartitionedTopicMetadata> f2 = lookup.getPartitionedTopicMetadata(topic,
                            metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);
            org.testng.Assert.assertNotSame(f2, f1,
                    "After completion, the deduplication map entry should be removed and a new future created");
            verify(cnxPool, times(2)).getConnection(any(InetSocketAddress.class));
        } finally {
            scheduler.shutdownNow();
        }
    }

    /**
     * Verifies that different parameter combinations are treated as distinct keys.
     *
     * Calls that differ in metadataAutoCreationEnabled or useFallbackForNonPIP344Brokers must return different futures
     * and trigger separate connection requests.
     *
     * Cleanup is per key. Completing one does not affect another in-flight entry.
     */
    @Test(timeOut = 60000)
    public void testPartitionedMetadataDeduplicationDifferentParameterCombinations() throws Exception {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ConnectionPool cnxPool = mock(ConnectionPool.class);

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(30000);
        when(client.getConfiguration()).thenReturn(conf);
        when(client.getCnxPool()).thenReturn(cnxPool);

        // Never-completing connection prevents the thenAcceptAsync callback in getPartitionedTopicMetadata
        // from executing, so the test exercises only the deduplication map without network calls
        CompletableFuture<ClientCnx> neverCompletes = new CompletableFuture<>();
        when(cnxPool.getConnection(any(InetSocketAddress.class))).thenReturn(neverCompletes);

        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("lookup-test-sched"));

        try (BinaryProtoLookupService lookup = new BinaryProtoLookupService(
                client, "pulsar://broker:6650", null, false, scheduler, null)) {

            TopicName topic = TopicName.get("persistent://public/default/t1");

            // A: metadataAutoCreationEnabled=true, useFallbackForNonPIP344Brokers=false
            CompletableFuture<PartitionedTopicMetadata> fa =
                    lookup.getPartitionedTopicMetadata(topic, true, false);
            // B: metadataAutoCreationEnabled=false, useFallbackForNonPIP344Brokers=false
            CompletableFuture<PartitionedTopicMetadata> fb =
                    lookup.getPartitionedTopicMetadata(topic, false, false);

            assertNotSame(fa, fb,
                    "Requests with different metadataAutoCreationEnabled must not share the same future");
            verify(cnxPool, times(2)).getConnection(any(InetSocketAddress.class));

            // Complete the future. This triggers the whenComplete callback that removes the map entry.
            fa.complete(new PartitionedTopicMetadata(0));

            CompletableFuture<PartitionedTopicMetadata> fa2 =
                    lookup.getPartitionedTopicMetadata(topic, true, false);
            assertNotSame(fa2, fa,
                    "After completion, a call with the same parameters must create a new future");
            verify(cnxPool, times(3)).getConnection(any(InetSocketAddress.class));

            // The call with (metadataAutoCreationEnabled=false, useFallbackForNonPIP344Brokers=false) is still
            // in flight. A new call with the same parameters must return the same future (fb).
            CompletableFuture<PartitionedTopicMetadata> fb2 =
                    lookup.getPartitionedTopicMetadata(topic, false, false);
            assertSame(fb2, fb,
                    "An in-flight request with the same parameters must return the original future");
            verify(cnxPool, times(3)).getConnection(any(InetSocketAddress.class));

            // Also verify distinct fallback flag is treated as a separate key
            CompletableFuture<PartitionedTopicMetadata> fc =
                    lookup.getPartitionedTopicMetadata(topic, false, true);
            assertNotSame(fc, fb,
                    "Requests that differ in useFallbackForNonPIP344Brokers must not share the same future");
            verify(cnxPool, times(4)).getConnection(any(InetSocketAddress.class));
        } finally {
            scheduler.shutdownNow();
        }
    }
}
