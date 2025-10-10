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
package org.apache.pulsar.client.api;

import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.stats.JvmMetrics;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class PatternConsumerBackPressureMultipleConsumersTest extends MockedPulsarServiceBaseTest {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        isTcpLookup = true;
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60 * 1000)
    public void testGetTopicsWithLargeAmountOfConcurrentClientConnections()
            throws PulsarAdminException, InterruptedException, IOException {
        // number of requests to send to the broker
        final int requests = 500;
        // use multiple clients so that each client has a separate connection to the broker
        final int numberOfClients = 200;
        // create a long topic name to consume more memory per topic
        final String topicName = StringUtils.repeat('a', 512) + UUID.randomUUID();
        // number of topics to create
        final int topicCount = 8192;
        // maximum number of requests in flight at any given time
        final int maxRequestsInFlight = 200;

        // create a single topic with multiple partitions
        admin.topics().createPartitionedTopic(topicName, topicCount);

        // reduce available direct memory to reproduce issues with less concurrency
        long directMemoryRequired = getDirectMemoryRequiredMB() * 1024 * 1024;
        List<ByteBuf> buffers = allocateDirectMemory(directMemoryRequired);
        @Cleanup
        Closeable releaseBuffers = () -> {
            for (ByteBuf byteBuf : buffers) {
                byteBuf.release();
            }
        };

        @Cleanup("shutdownNow")
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime()
                .availableProcessors());

        @Cleanup
        PulsarClientSharedResources sharedResources =
                PulsarClientSharedResources.builder().build();
        List<PulsarClientImpl> clients = new ArrayList<>(requests);
        @Cleanup
        Closeable closeClients = () -> {
            for (PulsarClient client : clients) {
                try {
                    client.close();
                } catch (PulsarClientException e) {
                    log.error("Failed to close client {}", client, e);
                }
            }
        };
        for (int i = 0; i < numberOfClients; i++) {
            PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                    .serviceUrl(getClientServiceUrl())
                    .sharedResources(sharedResources)
                    .build();
            clients.add(client);
        }

        final AtomicInteger success = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(requests);
        final Semaphore semaphore = new Semaphore(maxRequestsInFlight);
        for (int i = 0; i < requests; i++) {
            PulsarClientImpl pulsarClientImpl = clients.get(i % numberOfClients);
            executorService.execute(() -> {
                semaphore.acquireUninterruptibly();
                try {
                    pulsarClientImpl.getLookup()
                            .getTopicsUnderNamespace(NamespaceName.get("public", "default"),
                                    CommandGetTopicsOfNamespace.Mode.PERSISTENT, ".*", "")
                            .whenComplete((result, ex) -> {
                                semaphore.release();
                                if (ex == null) {
                                    success.incrementAndGet();
                                } else {
                                    log.error("Failed to get topic list.", ex);
                                }
                                log.info("latch-count: {}, succeed: {}, available direct mem: {} MB", latch.getCount(),
                                        success.get(),
                                        (DirectMemoryUtils.jvmMaxDirectMemory() - JvmMetrics.getJvmDirectMemoryUsed())
                                                / (1024 * 1024));
                                latch.countDown();
                            });
                } catch (Exception e) {
                    semaphore.release();
                }
            });
        }
        latch.await();
        Assert.assertEquals(success.get(), requests);
    }

    protected int getDirectMemoryRequiredMB() {
        return 175;
    }

    protected String getClientServiceUrl() {
        return pulsar.getBrokerServiceUrl();
    }

    /**
     * Allocate direct memory to reduce available direct memory to the given amount of required memory.
     * @param directMemoryRequired required direct memory in bytes
     * @return list of ByteBufs allocated to reduce available direct memory
     */
    private static List<ByteBuf> allocateDirectMemory(long directMemoryRequired) {
        long usedMemory = JvmMetrics.getJvmDirectMemoryUsed();
        long maxMemory = DirectMemoryUtils.jvmMaxDirectMemory();
        long availableMemory = maxMemory - usedMemory;
        List<ByteBuf> buffers = new ArrayList<>();
        if (availableMemory > directMemoryRequired) {
            long allocateRemaining = availableMemory - directMemoryRequired;
            log.info("Making allocations for {} MB to reduce available direct memory",
                    allocateRemaining / (1024 * 1024));
            int blockSize = 5 * 1024 * 1024;
            while (allocateRemaining > 0) {
                ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.directBuffer(blockSize);
                buffers.add(byteBuf);
                allocateRemaining -= blockSize;
            }
        }
        return buffers;
    }
}
