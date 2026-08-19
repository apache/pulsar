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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ProducerQueueSizeTest extends ProducerConsumerBase {

    /**
     * The bounds {@code PulsarClientImpl} falls back to when the client memory limit is disabled.
     * Duplicated here on purpose: these are a documented client default, so a change to them should
     * break a test rather than pass silently.
     */
    private static final int NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES = 1000;
    private static final int NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS = 50000;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private static ProducerConfigurationData confOf(Producer<?> producer) {
        return ((ProducerBase<?>) producer).getConfiguration();
    }

    @DataProvider(name = "partitioned")
    public Object[][] partitioned() {
        return new Object[][]{{Boolean.FALSE}, {Boolean.TRUE}};
    }

    @DataProvider(name = "matrix")
    public Object[][] matrix() {
        return new Object[][]{
                {Boolean.FALSE, Boolean.FALSE},
                {Boolean.FALSE, Boolean.TRUE},
                {Boolean.TRUE, Boolean.FALSE},
                {Boolean.TRUE, Boolean.TRUE},
        };
    }

    @Test(dataProvider = "matrix")
    public void testRemoveMaxQueueLimit(boolean blockIfQueueFull, boolean partitioned) throws Exception {
        String topic = newTopicName();

        if (partitioned) {
            admin.topics().createPartitionedTopic(topic, 10);
        }

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(10, SizeUnit.KILO_BYTES)
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .blockIfQueueFull(blockIfQueueFull)
                .maxPendingMessages(0)
                .maxPendingMessagesAcrossPartitions(0)
                .create();

        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(producer.sendAsync("hello"));
        }

        producer.flush();

        for (CompletableFuture<?>f : futures) {
            f.get();
        }
    }

    /**
     * A client with the memory limit disabled has no byte-based backpressure, so producers must fall
     * back to a bounded pending-message queue. This has to hold for the no-argument
     * {@code newProducer()} overload as well, not just {@code newProducer(Schema)}.
     */
    @Test
    public void testNoArgNewProducerIsBoundedWhenMemoryLimitDisabled() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(newTopicName()).create();

        assertThat(confOf(producer).getMaxPendingMessages())
                .isEqualTo(NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES);
        assertThat(confOf(producer).getMaxPendingMessagesAcrossPartitions())
                .isEqualTo(NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS);
    }

    /**
     * The fallback is a default, not a floor. An application that asks for no message-count limit at
     * all still gets it, by passing 0 explicitly. This is what keeps 0 a usable value rather than an
     * alias for "unset".
     *
     * <p>A single {@code maxPendingMessages(0)} has to be enough whatever the topic's shape: filling
     * in the across-partitions budget would put a per-partition limit back on a partitioned topic.
     */
    @Test(dataProvider = "partitioned")
    public void testExplicitZeroDisablesTheBoundWhenMemoryLimitDisabled(boolean partitioned) throws Exception {
        String topic = newTopicName();
        if (partitioned) {
            admin.topics().createPartitionedTopic(topic, 10);
        }

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(topic)
                .maxPendingMessages(0)
                .create();

        assertThat(confOf(producer).getMaxPendingMessages()).isZero();
        assertThat(confOf(producer).getMaxPendingMessagesAcrossPartitions()).isZero();
    }

    /**
     * Mirror of the above: disabling only the across-partitions budget must not take the per-producer
     * default down with it. Filling in that default would otherwise be capped by a budget of 0.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testExplicitZeroAcrossPartitionsKeepsThePerProducerDefault() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(newTopicName())
                .maxPendingMessagesAcrossPartitions(0)
                .create();

        assertThat(confOf(producer).getMaxPendingMessages())
                .isEqualTo(NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES);
        assertThat(confOf(producer).getMaxPendingMessagesAcrossPartitions()).isZero();
    }

    /**
     * {@code loadConf} is the other way an application configures a limit. A limit present in the map
     * counts as configured, including a 0, even though {@code loadConf} rebuilds the configuration
     * object and so cannot carry any marker on it.
     */
    @Test
    public void testLoadConfZeroDisablesTheBoundWhenMemoryLimitDisabled() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(newTopicName())
                .loadConf(Map.of("maxPendingMessages", 0))
                .create();

        assertThat(confOf(producer).getMaxPendingMessages()).isZero();
        assertThat(confOf(producer).getMaxPendingMessagesAcrossPartitions()).isZero();
    }

    /**
     * A {@code loadConf} that does not mention the limits leaves them unconfigured, so the defaults
     * still apply. Pins that rebuilding the configuration is not mistaken for configuring it.
     */
    @Test
    public void testLoadConfWithoutTheLimitsKeepsTheDefaults() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(newTopicName())
                .loadConf(Map.of("producerName", "loadConfWithoutLimits"))
                .create();

        assertThat(confOf(producer).getMaxPendingMessages())
                .isEqualTo(NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES);
        assertThat(confOf(producer).getMaxPendingMessagesAcrossPartitions())
                .isEqualTo(NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS);
    }

    /**
     * A cloned builder has to keep knowing which limits were configured, or the clone would silently
     * get the defaults back.
     */
    @Test
    public void testCloneKeepsAnExplicitlyDisabledBound() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        ProducerBuilder<byte[]> builder = client.newProducer().maxPendingMessages(0);

        @Cleanup
        Producer<byte[]> producer = builder.clone().topic(newTopicName()).create();

        assertThat(confOf(producer).getMaxPendingMessages()).isZero();
    }

    /**
     * {@code maxPendingMessagesAcrossPartitions} must be {@code >= maxPendingMessages}. Filling in
     * the across-partitions fallback must therefore never lower it below an explicitly configured
     * per-partition limit, which would fail producer creation.
     */
    @Test
    public void testExplicitMaxPendingMessagesAboveTheFallbackDoesNotFailCreation() throws Exception {
        int maxPendingMessages = NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS + 10_000;

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(newTopicName())
                .maxPendingMessages(maxPendingMessages)
                .create();

        assertThat(confOf(producer).getMaxPendingMessages()).isEqualTo(maxPendingMessages);
        assertThat(confOf(producer).getMaxPendingMessagesAcrossPartitions())
                .isGreaterThanOrEqualTo(maxPendingMessages);
    }

    /**
     * Filling in the fallback must not write it back into the builder's own configuration. The
     * builder stays reusable, and a limit set on it afterwards is still validated against what the
     * caller configured rather than against a filled-in default.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testFallbackLeavesTheBuilderReusable() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        ProducerBuilder<byte[]> builder = client.newProducer();

        @Cleanup
        Producer<byte[]> first = builder.topic(newTopicName()).create();
        assertThat(confOf(first).getMaxPendingMessages())
                .isEqualTo(NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES);

        // Rejected if creating the first producer had left the fallback in the builder, since the
        // across-partitions limit has to be >= maxPendingMessages.
        @Cleanup
        Producer<byte[]> second = builder.topic(newTopicName())
                .maxPendingMessagesAcrossPartitions(500)
                .create();
        assertThat(confOf(second).getMaxPendingMessages()).isEqualTo(500);
    }

    /**
     * The fallback has to reach partitioned producers too, where the per-partition queue is derived
     * from the across-partitions budget.
     */
    @Test
    public void testPartitionedProducerIsBoundedWhenMemoryLimitDisabled() throws Exception {
        String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(topic).create();

        // The budget spread over 10 partitions is well above the per-producer default, so each
        // partition keeps the full default.
        assertThat(confOf(producer).getMaxPendingMessages())
                .isEqualTo(NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES);
        assertThat(confOf(producer).getMaxPendingMessagesAcrossPartitions())
                .isEqualTo(NO_MEMORY_LIMIT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS);
    }

    /**
     * The across-partitions limit is a budget shared by every partition, so the per-producer
     * fallback must be capped by it. Otherwise the fallback would exceed an explicitly configured
     * budget, which producer creation rejects.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testExplicitAcrossPartitionsLimitCapsTheFallback() throws Exception {
        int maxPendingMessagesAcrossPartitions = 500;

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(newTopicName())
                .maxPendingMessagesAcrossPartitions(maxPendingMessagesAcrossPartitions)
                .create();

        assertThat(confOf(producer).getMaxPendingMessages())
                .isEqualTo(maxPendingMessagesAcrossPartitions);
        assertThat(confOf(producer).getMaxPendingMessagesAcrossPartitions())
                .isEqualTo(maxPendingMessagesAcrossPartitions);
    }

    /**
     * The fallback only exists to replace the missing byte-based backpressure. When a memory limit
     * is configured, an unset pending-message limit keeps meaning "no message-count limit".
     */
    @Test
    public void testMemoryLimitedClientKeepsUnboundedPendingMessages() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(64, SizeUnit.MEGA_BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(newTopicName()).create();

        assertThat(confOf(producer).getMaxPendingMessages()).isZero();
        assertThat(confOf(producer).getMaxPendingMessagesAcrossPartitions()).isZero();
    }

    /**
     * A budget the application asked for is what gets divided between the partitions. Pins that the
     * per-producer default filled in alongside it does not win the division and cap every partition at
     * that default instead of at its share of the budget.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testExplicitAcrossPartitionsBudgetIsDividedBetweenThePartitions() throws Exception {
        String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .maxPendingMessagesAcrossPartitions(60_000)
                .create();

        assertThat(confOf(producer).getMaxPendingMessages()).isEqualTo(6_000);
    }

    /**
     * The mirror image: a budget that was only filled in as a default must not be divided, because
     * dividing it would lower a per-producer limit the application did ask for.
     */
    @Test
    public void testFilledInBudgetDoesNotLowerAnExplicitPerProducerLimit() throws Exception {
        String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .maxPendingMessages(60_000)
                .create();

        assertThat(confOf(producer).getMaxPendingMessages()).isEqualTo(60_000);
    }

    /**
     * A budget smaller than the partition count divides to zero, which used to remove the queue bound
     * altogether — asking for a tighter budget made the producer unbounded. Each partition keeps the
     * smallest possible queue instead.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testBudgetSmallerThanThePartitionCountStillBoundsEachPartition() throws Exception {
        String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .maxPendingMessagesAcrossPartitions(5)
                .create();

        assertThat(confOf(producer).getMaxPendingMessages()).isEqualTo(1);
    }

    /**
     * An explicit {@code 0} means "no message-count limit" and stays that way on a partitioned topic,
     * even next to an across-partitions budget. Reading the unset value instead of the marker used to
     * overwrite it with the budget's per-partition share.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testExplicitZeroIsKeptAlongsideAnAcrossPartitionsBudget() throws Exception {
        String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .memoryLimit(0, SizeUnit.BYTES)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .maxPendingMessages(0)
                .maxPendingMessagesAcrossPartitions(60_000)
                .create();

        assertThat(confOf(producer).getMaxPendingMessages()).isZero();
    }
}
