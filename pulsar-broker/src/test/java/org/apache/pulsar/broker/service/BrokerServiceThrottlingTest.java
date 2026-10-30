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
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongGaugeValue;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.waitAtMost;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.common.Attributes;
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
import org.apache.pulsar.broker.InProgressRequestFrameMemoryLimitMode;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerServiceThrottlingTest extends BrokerTestBase {

    @BeforeMethod
    protected void setupRequestFrameMemoryMode(Object[] parameters) throws Exception {
        conf.setInProgressRequestFrameMemoryLimitEnabled(parameters.length == 1
                && parameters[0] instanceof InProgressRequestFrameMemoryLimitMode mode
                ? mode : InProgressRequestFrameMemoryLimitMode.AUTO);
        setup();
    }

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

    @DataProvider
    public Object[][] enabledRequestFrameMemoryMode() {
        return new Object[][] {{InProgressRequestFrameMemoryLimitMode.ENABLED}};
    }

    @Test(dataProvider = "enabledRequestFrameMemoryMode")
    public void testDynamicInProgressRequestFrameMemoryLimit(InProgressRequestFrameMemoryLimitMode mode)
            throws Exception {
        InProgressRequestFrameMemoryLimiter limiter = pulsar.getBrokerService()
                .getInProgressRequestFrameMemoryLimiter();
        InProgressRequestFrameMemoryController controller = limiter.getController();
        int maxMessagePublishBufferSizeInMB = pulsar.getConfiguration().getMaxMessagePublishBufferSizeInMB();
        boolean acquired = false;
        try {
            limiter.registerThrottleStateListener("test-connection", () -> { });
            assertThat(controller.isEnabled()).isTrue();
            assertRequestFrameMemoryThrottledMetric(0);
            controller.acquire(1);
            assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryUsedBytes()).isEqualTo(1);
            controller.release(1);
            assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryUsedBytes()).isZero();

            admin.brokers().updateDynamicConfiguration("maxMessagePublishBufferSizeInMB", "1");
            waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(pulsar.getConfiguration().getMaxMessagePublishBufferSizeInMB()).isEqualTo(1);
                assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryLimiter()).isSameAs(limiter);
                assertThat(limiter.getLimitBytes()).isEqualTo(1024L * 1024L);
            });

            controller.acquire(1024L * 1024L + 1);
            acquired = true;
            controller.onInboundRead();
            assertThat(limiter.isThrottled()).isTrue();
            assertRequestFrameMemoryThrottledMetric(1);
            assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryUsedBytes())
                    .isEqualTo(1024L * 1024L + 1);

            // Existing connections must observe numeric updates and deletion without any new inbound read.
            for (int limitMB : new int[] {4, 1, 0, -1, 1}) {
                admin.brokers().updateDynamicConfiguration(
                        "maxMessagePublishBufferSizeInMB", Integer.toString(limitMB));
                waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertThat(limiter.getLimitBytes()).isEqualTo(limitMB * 1024L * 1024L);
                    assertThat(limiter.isThrottled()).isEqualTo(limitMB == 1);
                    assertRequestFrameMemoryThrottledMetric(limitMB == 1 ? 1 : 0);
                    assertThat(controller.getRetainedFrameBytes()).isEqualTo(1024L * 1024L + 1);
                });
            }
            admin.brokers().deleteDynamicConfiguration("maxMessagePublishBufferSizeInMB");
            waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(limiter.getLimitBytes()).isEqualTo(maxMessagePublishBufferSizeInMB * 1024L * 1024L);
                assertThat(limiter.isThrottled()).isFalse();
                assertRequestFrameMemoryThrottledMetric(0);
            });
            admin.brokers().updateDynamicConfiguration("maxMessagePublishBufferSizeInMB", "1");
            waitAtMost(10, TimeUnit.SECONDS).until(limiter::isThrottled);
        } finally {
            if (acquired) {
                controller.release(1024L * 1024L + 1);
                assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryUsedBytes()).isZero();
            }
            limiter.unregisterThrottleStateListener("test-connection");
            admin.brokers().deleteDynamicConfiguration("maxMessagePublishBufferSizeInMB");
            waitAtMost(1, TimeUnit.SECONDS).until(() -> pulsar.getConfiguration()
                    .getMaxMessagePublishBufferSizeInMB() == maxMessagePublishBufferSizeInMB
                    && pulsar.getBrokerService().getInProgressRequestFrameMemoryLimiter().getLimitBytes()
                            == maxMessagePublishBufferSizeInMB * 1024L * 1024L);
        }
    }

    @Test(dataProvider = "enabledRequestFrameMemoryMode")
    public void testRequestFrameMemoryThrottledMetricFollowsHysteresis(InProgressRequestFrameMemoryLimitMode mode) {
        InProgressRequestFrameMemoryController controller = pulsar.getBrokerService()
                .getInProgressRequestFrameMemoryLimiter().getController();
        long initialLimitBytes = controller.getLimiter().getLimitBytes();
        long retainedBytes = 0;
        try {
            controller.updateLimitBytes(100);
            assertRequestFrameMemoryThrottledMetric(0);
            controller.acquire(101);
            retainedBytes = 101;
            controller.onInboundRead();
            assertRequestFrameMemoryThrottledMetric(1);
            controller.release(1);
            retainedBytes -= 1;
            // Usage is no longer above the limit, but throttling persists until the half-limit resume threshold.
            assertRequestFrameMemoryThrottledMetric(1);
            controller.release(49);
            retainedBytes -= 49;
            assertRequestFrameMemoryThrottledMetric(1);
            controller.release(1);
            retainedBytes -= 1;
            assertRequestFrameMemoryThrottledMetric(0);
            assertThat(controller.isEnabled()).isTrue();
            assertThat(controller.getRetainedFrameBytes()).isEqualTo(50);
        } finally {
            controller.release(retainedBytes);
            controller.updateLimitBytes(initialLimitBytes);
        }
    }

    private void assertRequestFrameMemoryThrottledMetric(long expectedValue) {
        assertMetricLongGaugeValue(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics(),
                BrokerService.IN_PROGRESS_REQUEST_FRAME_MEMORY_THROTTLED_METRIC_NAME,
                Attributes.empty(), expectedValue);
    }

    @DataProvider
    public Object[][] requestFrameMemoryModes() {
        return new Object[][] { { InProgressRequestFrameMemoryLimitMode.DISABLED },
            { InProgressRequestFrameMemoryLimitMode.TRACKING }, { InProgressRequestFrameMemoryLimitMode.AUTO },
            { InProgressRequestFrameMemoryLimitMode.ENABLED } };
    }

    @Test(dataProvider = "requestFrameMemoryModes")
    public void testRequestFrameMemoryModeIsStartupOnly(InProgressRequestFrameMemoryLimitMode mode) throws Exception {
        String configName = "inProgressRequestFrameMemoryLimitEnabled";
        InProgressRequestFrameMemoryLimiter limiter = pulsar.getBrokerService()
                .getInProgressRequestFrameMemoryLimiter();
        assertThat(limiter == null).isEqualTo(mode == InProgressRequestFrameMemoryLimitMode.DISABLED);
        assertThat(admin.brokers().getDynamicConfigurationNames()).doesNotContain(configName);
        for (String value : new String[] {"DISABLED", "TRACKING", "AUTO", "ENABLED", "true", "false", "invalid"}) {
            assertThatThrownBy(() -> admin.brokers().updateDynamicConfiguration(configName, value))
                    .isInstanceOf(PulsarAdminException.PreconditionFailedException.class);
        }
        assertThatThrownBy(() -> admin.brokers().deleteDynamicConfiguration(configName))
                .isInstanceOf(PulsarAdminException.PreconditionFailedException.class);
        assertThat(admin.brokers().getAllDynamicConfigurations()).doesNotContainKey(configName);
        assertThat(pulsar.getConfiguration().getInProgressRequestFrameMemoryLimitEnabled()).isEqualTo(mode);

        if (limiter != null) {
            InProgressRequestFrameMemoryController controller = limiter.getController();
            assertThat(controller.isEnabled()).isEqualTo(mode == InProgressRequestFrameMemoryLimitMode.ENABLED);
            controller.acquire(256);
            try {
                controller.onInboundRead();
                assertOtelMetricLongSumValue("pulsar.broker.publish.in.progress.buffer.memory.usage", 256);
                assertRequestFrameMemoryThrottledMetric(0);
            } finally {
                controller.release(256);
            }
            assertOtelMetricLongSumValue("pulsar.broker.publish.in.progress.buffer.memory.usage", 0);
        } else {
            assertRequestFrameMemoryMetricsAbsent();
        }

        // A stale metadata value written by an older broker must also be ignored. The following numeric
        // update is processed after it (TreeMap order), so observing that update verifies notification handling.
        int initialLimitMB = pulsar.getConfiguration().getMaxMessagePublishBufferSizeInMB();
        var resources = pulsar.getPulsarResources().getDynamicConfigResources();
        try {
            resources.setDynamicConfigurationWithCreate(ignored -> {
                Map<String, String> values = new TreeMap<>();
                values.put(configName, mode == InProgressRequestFrameMemoryLimitMode.ENABLED ? "DISABLED" : "ENABLED");
                values.put("maxMessagePublishBufferSizeInMB", "1");
                return values;
            });
            waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(pulsar.getConfiguration().getMaxMessagePublishBufferSizeInMB()).isEqualTo(1);
                assertThat(pulsar.getConfiguration().getInProgressRequestFrameMemoryLimitEnabled()).isEqualTo(mode);
                assertThat(pulsar.getBrokerService().getInProgressRequestFrameMemoryLimiter()).isSameAs(limiter);
                if (limiter != null) {
                    assertThat(limiter.getLimitBytes()).isEqualTo(1024L * 1024L);
                    assertThat(limiter.getController().isEnabled())
                            .isEqualTo(mode == InProgressRequestFrameMemoryLimitMode.ENABLED);
                } else {
                    assertRequestFrameMemoryMetricsAbsent();
                }
            });
        } finally {
            resources.setDynamicConfiguration(values -> {
                values.remove(configName);
                values.remove("maxMessagePublishBufferSizeInMB");
                return values;
            });
            waitAtMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(pulsar.getConfiguration().getMaxMessagePublishBufferSizeInMB()).isEqualTo(initialLimitMB);
                assertThat(pulsar.getConfiguration().getInProgressRequestFrameMemoryLimitEnabled()).isEqualTo(mode);
                if (limiter != null) {
                    assertThat(limiter.getLimitBytes()).isEqualTo(initialLimitMB * 1024L * 1024L);
                }
            });
        }
    }

    @Test(timeOut = 30_000, dataProvider = "requestFrameMemoryModes")
    public void testRequestFrameMemoryModeDuringPublish(InProgressRequestFrameMemoryLimitMode mode) throws Exception {
        BrokerService broker = pulsar.getBrokerService();
        InProgressRequestFrameMemoryLimiter limiter = broker.getInProgressRequestFrameMemoryLimiter();
        try {
            assertThat(pulsar.getConfiguration().getInProgressRequestFrameMemoryLimitEnabled()).isEqualTo(mode);
            assertThat(limiter == null).isEqualTo(mode == InProgressRequestFrameMemoryLimitMode.DISABLED);
            pulsarTestContext.getMockBookKeeper().setDefaultAddEntryDelayMillis(500);
            try (PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
                 Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                         .topic("persistent://" + newTopicName()).enableBatching(false).create()) {
                ServerCnx cnx = (ServerCnx) broker.getTopicReference(producer.getTopic()).orElseThrow()
                        .getProducers().get(producer.getProducerName()).getCnx();
                assertThat(cnx.ctx().pipeline().get(FrameDecoderUtil.FRAME_DECODER_HANDLER))
                        .isExactlyInstanceOf(mode == InProgressRequestFrameMemoryLimitMode.DISABLED
                                ? LengthFieldBasedFrameDecoder.class : InProgressRequestFrameTrackingDecoder.class);
                long usedBytesBeforePublish = broker.getInProgressRequestFrameMemoryUsedBytes();
                byte[] payload = new byte[128 * 1024];
                List<CompletableFuture<?>> sends = new ArrayList<>();
                for (int i = 0; i < 16; i++) {
                    sends.add(producer.sendAsync(payload));
                }

                if (limiter != null) {
                    waitAtMost(10, TimeUnit.SECONDS).until(() ->
                            broker.getInProgressRequestFrameMemoryUsedBytes() > usedBytesBeforePublish);
                    assertThat(sends).anyMatch(send -> !send.isDone());
                } else {
                    assertThat(broker.getInProgressRequestFrameMemoryUsedBytes()).isZero();
                    assertRequestFrameMemoryMetricsAbsent();
                }
                CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(20, TimeUnit.SECONDS);
            }
        } finally {
            pulsarTestContext.getMockBookKeeper().setDefaultAddEntryDelayMillis(1);
        }
        if (mode == InProgressRequestFrameMemoryLimitMode.DISABLED) {
            assertRequestFrameMemoryMetricsAbsent();
        }
    }

    private void assertRequestFrameMemoryMetricsAbsent() {
        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        // Verify collection is active, so a disabled telemetry SDK cannot make the absence check pass.
        assertThat(metrics).anySatisfy(metric -> assertThat(metric.getName())
                .isEqualTo(BrokerService.TOPIC_LOOKUP_LIMIT_METRIC_NAME));
        assertThat(metrics).noneSatisfy(metric -> assertThat(metric.getName())
                .startsWith("pulsar.broker.publish.in.progress.buffer.memory."));
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
     * Verifies: Broker side throttling.
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
