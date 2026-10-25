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
package org.apache.pulsar.broker.service;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerServiceThrottlingTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder builder) {
        super.customizeMainPulsarTestContextBuilder(builder);
        builder.enableOpenTelemetry(true);
    }

    @Test
    public void testDynamicInProgressRequestFrameMemoryLimit() throws Exception {
        InProgressRequestFrameMemoryLimiter previousLimiter = pulsar.getBrokerService()
                .getInProgressRequestFrameMemoryLimiter();
        ManagedLedgerFactoryImpl factory = (ManagedLedgerFactoryImpl) pulsar.getDefaultManagedLedgerFactory();
        boolean inProgressRequestFrameMemoryLimit = pulsar.getConfiguration()
                .isMaxMessagePublishBufferSizeInMBUseInputBuffer();
        boolean managedLedgerCacheCopyEntries = pulsar.getConfiguration().isManagedLedgerCacheCopyEntries();
        int maxMessagePublishBufferSizeInMB = pulsar.getConfiguration().getMaxMessagePublishBufferSizeInMB();
        InProgressRequestFrameMemoryLimiter limiter = null;
        boolean acquired = false;
        try {
            assertThat(previousLimiter.isEnabled()).isFalse();
            previousLimiter.acquire(1);
            assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryUsedBytes()).isEqualTo(1);
            previousLimiter.release(1);
            assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryUsedBytes()).isZero();

            admin.brokers().updateDynamicConfiguration("maxMessagePublishBufferSizeInMBUseInputBuffer", "true");
            waitAtMost(1, TimeUnit.SECONDS).until(() -> pulsar.getConfiguration()
                    .isMaxMessagePublishBufferSizeInMBUseInputBuffer());
            assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryLimiter())
                    .isSameAs(previousLimiter);
            assertThat(factory.getConfig().isCopyEntriesInCache()).isEqualTo(managedLedgerCacheCopyEntries);

            admin.brokers().updateDynamicConfiguration("managedLedgerCacheCopyEntries", "true");
            waitAtMost(1, TimeUnit.SECONDS).until(() -> pulsar.getConfiguration().isManagedLedgerCacheCopyEntries()
                    && factory.getConfig().isCopyEntriesInCache());
            assertThat(pulsar.getConfiguration().isMaxMessagePublishBufferSizeInMBUseInputBuffer()).isTrue();

            admin.brokers().deleteDynamicConfiguration("managedLedgerCacheCopyEntries");
            waitAtMost(1, TimeUnit.SECONDS).until(() -> pulsar.getConfiguration().isManagedLedgerCacheCopyEntries()
                    == managedLedgerCacheCopyEntries
                    && factory.getConfig().isCopyEntriesInCache() == managedLedgerCacheCopyEntries);
            assertThat(pulsar.getConfiguration().isMaxMessagePublishBufferSizeInMBUseInputBuffer()).isTrue();

            admin.brokers().updateDynamicConfiguration("maxMessagePublishBufferSizeInMB", "1");
            waitAtMost(1, TimeUnit.SECONDS).until(() -> pulsar.getConfiguration()
                    .getMaxMessagePublishBufferSizeInMB() == 1);
            limiter = pulsar.getBrokerService().getInProgressRequestFrameMemoryLimiter();
            assertThat(limiter).isNotSameAs(previousLimiter);

            limiter.acquire(1024L * 1024L + 1);
            acquired = true;
            assertThat(limiter.isThrottled()).isTrue();
            assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryUsedBytes())
                    .isEqualTo(1024L * 1024L + 1);

            admin.brokers().deleteDynamicConfiguration("maxMessagePublishBufferSizeInMBUseInputBuffer");
            waitAtMost(1, TimeUnit.SECONDS).until(() -> pulsar.getConfiguration()
                    .isMaxMessagePublishBufferSizeInMBUseInputBuffer() == inProgressRequestFrameMemoryLimit);
            assertThat(limiter.isThrottled()).isEqualTo(inProgressRequestFrameMemoryLimit);
        } finally {
            if (acquired) {
                limiter.release(1024L * 1024L + 1);
                assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryUsedBytes()).isZero();
            }
            admin.brokers().deleteDynamicConfiguration("maxMessagePublishBufferSizeInMBUseInputBuffer");
            admin.brokers().deleteDynamicConfiguration("managedLedgerCacheCopyEntries");
            admin.brokers().deleteDynamicConfiguration("maxMessagePublishBufferSizeInMB");
            waitAtMost(1, TimeUnit.SECONDS).until(() -> pulsar.getConfiguration()
                    .isMaxMessagePublishBufferSizeInMBUseInputBuffer() == inProgressRequestFrameMemoryLimit
                    && pulsar.getConfiguration().isManagedLedgerCacheCopyEntries() == managedLedgerCacheCopyEntries
                    && pulsar.getConfiguration().getMaxMessagePublishBufferSizeInMB()
                            == maxMessagePublishBufferSizeInMB
                    && factory.getConfig().isCopyEntriesInCache() == managedLedgerCacheCopyEntries);
        }
    }

    @Test(timeOut = 30_000)
    public void testInProgressRequestFrameMemoryTracksPublishUntilBookKeeperCompletion() throws Exception {
        boolean inProgressRequestFrameMemoryLimit = pulsar.getConfiguration()
                .isMaxMessagePublishBufferSizeInMBUseInputBuffer();
        admin.brokers().updateDynamicConfiguration("maxMessagePublishBufferSizeInMBUseInputBuffer", "true");
        waitAtMost(10, TimeUnit.SECONDS).until(() -> pulsar.getConfiguration()
                .isMaxMessagePublishBufferSizeInMBUseInputBuffer());
        InProgressRequestFrameMemoryLimiter limiter = pulsar.getBrokerService().getInProgressRequestFrameMemoryLimiter();
        pulsarTestContext.getMockBookKeeper().setDefaultAddEntryDelayMillis(500);
        try {
            try (PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build()) {
                try (Producer<byte[]> producer = client.newProducer(Schema.BYTES).topic(newTopicName())
                        .enableBatching(false).create()) {
                    long usedBytesBeforePublish = limiter.usedBytes();
                    byte[] payload = new byte[128 * 1024];
                    List<CompletableFuture<?>> sends = new ArrayList<>();
                    for (int i = 0; i < 16; i++) {
                        sends.add(producer.sendAsync(payload));
                    }

                    waitAtMost(10, TimeUnit.SECONDS).until(() -> limiter.usedBytes() > usedBytesBeforePublish);
                    assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryUsedBytes())
                            .isGreaterThan(usedBytesBeforePublish);
                    assertThat(sends).anyMatch(send -> !send.isDone());

                    CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(20, TimeUnit.SECONDS);
                }
            }
        } finally {
            pulsarTestContext.getMockBookKeeper().setDefaultAddEntryDelayMillis(1);
            admin.brokers().deleteDynamicConfiguration("maxMessagePublishBufferSizeInMBUseInputBuffer");
            waitAtMost(10, TimeUnit.SECONDS).until(() -> pulsar.getConfiguration()
                    .isMaxMessagePublishBufferSizeInMBUseInputBuffer() == inProgressRequestFrameMemoryLimit);
        }
    }

    /**
     * Verifies: updating zk-throttling node reflects broker-maxConcurrentLookupRequest and updates semaphore, as well
     * as the related limit metric value.
     */
    @Test
    public void testThrottlingLookupRequestSemaphore() throws Exception {
        var lookupRequestSemaphore = pulsar.getBrokerService().lookupRequestSemaphore;
        var configName = "maxConcurrentLookupRequest";
        var metricName = BrokerService.TOPIC_LOOKUP_LIMIT_METRIC_NAME;
        // Validate that the configuration has not been overridden.
        assertThat(admin.brokers().getAllDynamicConfigurations()).doesNotContainKey(configName);
        assertOtelMetricLongSumValue(metricName, 50_000);
        assertThat(lookupRequestSemaphore.get().availablePermits()).isNotEqualTo(0);
        admin.brokers().updateDynamicConfiguration(configName, Integer.toString(0));
        waitAtMost(1, TimeUnit.SECONDS).until(() -> lookupRequestSemaphore.get().availablePermits() == 0);
        assertOtelMetricLongSumValue(metricName, 0);
    }

    /**
     * Verifies: updating zk-throttling node reflects broker-maxConcurrentTopicLoadRequest and updates semaphore, as
     * well as the related limit metric value.
     */
    @Test
    public void testThrottlingTopicLoadRequestSemaphore() throws Exception {
        var topicLoadRequestSemaphore = pulsar.getBrokerService().topicLoadRequestSemaphore;
        var configName = "maxConcurrentTopicLoadRequest";
        var metricName = BrokerService.TOPIC_LOAD_LIMIT_METRIC_NAME;
        // Validate that the configuration has not been overridden.
        assertThat(admin.brokers().getAllDynamicConfigurations()).doesNotContainKey(configName);
        assertOtelMetricLongSumValue(metricName, 5_000);
        assertThat(topicLoadRequestSemaphore.get().availablePermits()).isNotEqualTo(0);
        admin.brokers().updateDynamicConfiguration(configName, Integer.toString(0));
        waitAtMost(1, TimeUnit.SECONDS).until(() -> topicLoadRequestSemaphore.get().availablePermits() == 0);
        assertOtelMetricLongSumValue(metricName, 0);
    }

    /**
     * Broker has maxConcurrentLookupRequest = 0 so, it rejects incoming lookup request and it cause consumer creation
     * failure.
     *
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testLookupThrottlingForClientByBroker0Permit() throws Exception {

        final String topicName = "persistent://prop/ns-abc/newTopic";

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub").subscribe();
        consumer.close();

        int newPermits = 0;
        admin.brokers().updateDynamicConfiguration("maxConcurrentLookupRequest", Integer.toString(newPermits));
        // wait config to be updated
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getMaxConcurrentLookupRequest() != newPermits) {
                Thread.sleep(100 + (i * 10));
            } else {
                break;
            }
        }

        try {
            consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub").subscribe();
            consumer.close();
            fail("It should fail as throttling should not receive any request");
        } catch (org.apache.pulsar.client.api.PulsarClientException.TooManyRequestsException e) {
            // ok as throttling set to 0
        }
    }

    /**
     * Verifies: Broker side throttling:
     *
     * <pre>
     * 1. concurrent_consumer_creation > maxConcurrentLookupRequest at broker
     * 2. few of the consumer creation must fail with TooManyLookupRequestException.
     * </pre>
     *
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testLookupThrottlingForClientByBroker() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic";

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .ioThreads(20).connectionsPerBroker(20).build();

        int newPermits = 1;
        admin.brokers().updateDynamicConfiguration("maxConcurrentLookupRequest", Integer.toString(newPermits));
        // wait config to be updated
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getMaxConcurrentLookupRequest() != newPermits) {
                Thread.sleep(100 + (i * 10));
            } else {
                break;
            }
        }

        PulsarServiceNameResolver resolver = new PulsarServiceNameResolver();
        resolver.updateServiceUrl(pulsar.getBrokerServiceUrl());
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setConnectionsPerBroker(20);

        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(20, false,
                new DefaultThreadFactory("test-pool", Thread.currentThread().isDaemon()));
        ExecutorService executor = Executors.newFixedThreadPool(10);
        try (ConnectionPool pool = new ConnectionPool(InstrumentProvider.NOOP, conf, eventLoop, null)) {
            final int totalConsumers = 20;
            List<Future<?>> futures = new ArrayList<>();

            // test for partitionMetadataRequest
            for (int i = 0; i < totalConsumers; i++) {
                long reqId = 0xdeadbeef + i;
                Future<?> f = executor.submit(() -> {
                        ByteBuf request = Commands.newPartitionMetadataRequest(topicName, reqId, true);
                        pool.getConnection(resolver.resolveHost())
                            .thenCompose(clientCnx -> clientCnx.newLookup(request, reqId))
                            .get();
                        return null;
                    });
                futures.add(f);
            }

            int rejects = 0;
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    Throwable rootCause = e;
                    while (rootCause instanceof ExecutionException) {
                        rootCause = rootCause.getCause();
                    }
                    if (rootCause
                            instanceof org.apache.pulsar.client.api.PulsarClientException.TooManyRequestsException) {
                        rejects++;
                    } else {
                        throw e;
                    }
                }
            }
            assertTrue(rejects > 0);
            futures.clear();

            // test for lookup
            for (int i = 0; i < totalConsumers; i++) {
                long reqId = 0xdeadfeef + i;
                Future<?> f = executor.submit(() -> {
                        ByteBuf request = Commands.newLookup(topicName, true, reqId);
                        pool.getConnection(resolver.resolveHost())
                            .thenCompose(clientCnx -> clientCnx.newLookup(request, reqId))
                            .get();
                        return null;
                    });
                futures.add(f);
            }

            rejects = 0;
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    Throwable rootCause = e;
                    while (rootCause instanceof ExecutionException) {
                        rootCause = rootCause.getCause();
                    }
                    if (rootCause
                            instanceof org.apache.pulsar.client.api.PulsarClientException.TooManyRequestsException) {
                        rejects++;
                    } else {
                        throw e;
                    }
                }
            }
            assertTrue(rejects > 0);
        } finally {
            executor.shutdownNow();
            eventLoop.shutdownNow();
        }
    }


    /**
     * This testcase make sure that once consumer lost connection with broker, it always reconnects with broker by
     * retrying on throttling-error exception also.
     *
     * <pre>
     * 1. all consumers get connected
     * 2. broker restarts with maxConcurrentLookupRequest = 1
     * 3. consumers reconnect and some get TooManyRequestException and again retries
     * 4. eventually all consumers will successfully connect to broker
     * </pre>
     *
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testLookupThrottlingForClientByBrokerInternalRetry() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic-" + UUID.randomUUID().toString();

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .ioThreads(20).connectionsPerBroker(20).build();
        upsertLookupPermits(100);
        List<Consumer<byte[]>> consumers = Collections.synchronizedList(new ArrayList<>());
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(10);
        final int totalConsumers = 8;
        CountDownLatch latch = new CountDownLatch(totalConsumers);
        for (int i = 0; i < totalConsumers; i++) {
            executor.execute(() -> {
                try {
                    consumers.add(pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub")
                            .subscriptionType(SubscriptionType.Shared).subscribe());
                } catch (PulsarClientException.TooManyRequestsException e) {
                    // ok
                } catch (Exception e) {
                    fail("it shouldn't failed");
                }
                latch.countDown();
            });
        }
        latch.await();

        admin.brokers().updateDynamicConfiguration("maxConcurrentLookupRequest", "1");
        admin.topics().unload(topicName);

        // wait strategically for all consumers to reconnect
        retryStrategically((test) -> areAllConsumersConnected(consumers), 5, 500);

        int totalConnectedConsumers = 0;
        for (Consumer<byte[]> consumer : consumers) {
            if (consumer.isConnected()) {
                totalConnectedConsumers++;
            }
            consumer.close();

        }
        assertEquals(totalConnectedConsumers, totalConsumers);
    }

    private boolean areAllConsumersConnected(List<Consumer<byte[]>> consumers) {
        for (Consumer<byte[]> consumer : consumers) {
            if (!consumer.isConnected()) {
                return false;
            }
        }
        return true;
    }

    private void upsertLookupPermits(int permits) throws Exception {
        pulsar.getPulsarResources().getDynamicConfigResources().setDynamicConfigurationWithCreate(optMap -> {
            Map<String, String> map = optMap.orElse(new TreeMap<>());
            map.put("maxConcurrentLookupRequest", Integer.toString(permits));
            return map;
        });
    }
}
