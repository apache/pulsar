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

import static org.apache.pulsar.client.impl.ClientTestFixtures.createPulsarClientMockWithMockedClientCnx;
import static org.apache.pulsar.client.impl.ClientTestFixtures.mockClientCnx;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PatternConsumerSchemaTest {
    private static final String TOPIC = "persistent://tenant/namespace/schema-lifetime";
    private static final List<String> PARTITIONS = List.of(TOPIC + "-partition-0", TOPIC + "-partition-1");
    private ExecutorProvider executorProvider;
    private ExecutorProvider internalExecutorProvider;
    private PulsarClientImpl client;
    private ClientCnx cnx;
    private Timer timer;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        executorProvider = new ExecutorProvider(1, "PatternSchemaListener");
        internalExecutorProvider = new ExecutorProvider(1, "PatternSchemaInternal");
        cnx = mockClientCnx();
        client = createPulsarClientMockWithMockedClientCnx(
                executorProvider, internalExecutorProvider.getExecutor(), cnx);
        timer = new HashedWheelTimer();
        when(client.timer()).thenReturn(timer);
        when(client.getPartitionedTopicMetadata(any(), anyBoolean(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata(PARTITIONS.size())));
        when(client.getLookup().getTopicsUnderNamespace(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new GetTopicsResult(PARTITIONS, null, true, true)));
        when(client.getSchemaProviderLoadingCache().get(any())).thenReturn(mock(SchemaInfoProvider.class));
        when(client.preProcessSchemaBeforeSubscribe(any(), any(), any())).thenCallRealMethod();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanUp() {
        executorProvider.shutdownNow();
        internalExecutorProvider.shutdownNow();
        timer.stop();
    }

    @DataProvider
    public Object[][] schemaVersioning() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "schemaVersioning")
    public void testFailedPartitionKeepsHealthySiblingSchemaOpen(boolean versioned) throws Exception {
        ResourceSchema template = new ResourceSchema(versioned);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createConsumer(template);
        CompletableFuture<ProducerResponse> failedSubscribe = new CompletableFuture<>();
        setSubscribeResponses(List.of(
                CompletableFuture.completedFuture(mock(ProducerResponse.class)), failedSubscribe));
        try {
            CompletableFuture<Void> adding = consumer.topicsChangeListener.onTopicsAdded(PARTITIONS);
            Awaitility.await().untilAsserted(() -> assertThat(consumer.getConsumers()).hasSize(2));
            ConsumerImpl<byte[]> healthy = consumer.consumers.get(PARTITIONS.get(0));
            ResourceSchema shared = (ResourceSchema) healthy.schema;
            assertThat(consumer.consumers.get(PARTITIONS.get(1)).schema).isSameAs(shared);
            failSubscribe(failedSubscribe);

            assertThat(adding).failsWithin(Duration.ofSeconds(5));
            assertThat(consumer.getConsumers()).containsExactly(healthy);
            assertThat(shared.closeCount.get()).as("failed sibling must not close a shared schema").isZero();
            assertDecodes(healthy);

            client.getConfiguration().setOperationTimeoutMs(30000);
            consumer.topicsChangeListener.onTopicsAdded(List.of(PARTITIONS.get(1))).get(5, TimeUnit.SECONDS);
            assertThat(consumer.consumers.get(PARTITIONS.get(0))).isSameAs(healthy);
            assertDecodes(healthy);
            assertDecodes(consumer.consumers.get(PARTITIONS.get(1)));
        } finally {
            failedSubscribe.complete(mock(ProducerResponse.class));
            consumer.closeAsync().get(5, TimeUnit.SECONDS);
        }
        assertThat(template.closeCount.get()).as("parent also releases its template schema").isPositive();
    }

    @Test
    public void testAllFailedPartitionsReleaseTheirDistinctSchemaOnce() throws Exception {
        ResourceSchema template = new ResourceSchema(true);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createConsumer(template);
        CompletableFuture<ProducerResponse> firstSubscribe = new CompletableFuture<>();
        CompletableFuture<ProducerResponse> secondSubscribe = new CompletableFuture<>();
        setSubscribeResponses(List.of(firstSubscribe, secondSubscribe));
        try {
            CompletableFuture<Void> adding = consumer.topicsChangeListener.onTopicsAdded(PARTITIONS);
            Awaitility.await().untilAsserted(() -> assertThat(consumer.getConsumers()).hasSize(2));
            ResourceSchema attemptSchema = (ResourceSchema) consumer.consumers.get(PARTITIONS.get(0)).schema;
            assertThat(attemptSchema).isNotSameAs(template);
            failSubscribe(firstSubscribe);
            failSubscribe(secondSubscribe);

            assertThat(adding).failsWithin(Duration.ofSeconds(5));
            assertThat(consumer.getConsumers()).isEmpty();
            assertThat(attemptSchema.closeCount.get()).as("last failed child releases the topic schema once")
                    .isEqualTo(1);

            client.getConfiguration().setOperationTimeoutMs(30000);
            consumer.topicsChangeListener.onTopicsAdded(PARTITIONS).get(5, TimeUnit.SECONDS);
            consumer.getConsumers().forEach(PatternConsumerSchemaTest::assertDecodes);
        } finally {
            firstSubscribe.complete(mock(ProducerResponse.class));
            secondSubscribe.complete(mock(ProducerResponse.class));
            consumer.closeAsync().get(5, TimeUnit.SECONDS);
        }
        assertThat(template.closeCount.get()).isEqualTo(1);
    }

    @Test
    public void testAllFailedPartitionsKeepTemplateUsableForRetryAndParentClose() throws Exception {
        ResourceSchema template = new ResourceSchema(false);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createConsumer(template);
        CompletableFuture<ProducerResponse> firstSubscribe = new CompletableFuture<>();
        CompletableFuture<ProducerResponse> secondSubscribe = new CompletableFuture<>();
        setSubscribeResponses(List.of(firstSubscribe, secondSubscribe));
        try {
            CompletableFuture<Void> adding = consumer.topicsChangeListener.onTopicsAdded(PARTITIONS);
            Awaitility.await().untilAsserted(() -> assertThat(consumer.getConsumers()).hasSize(2));
            failSubscribe(firstSubscribe);
            failSubscribe(secondSubscribe);
            assertThat(adding).failsWithin(Duration.ofSeconds(5));
            assertThat(consumer.getConsumers()).isEmpty();
            assertThat(template.closeCount.get()).as("template remains available to the next subscription").isZero();

            client.getConfiguration().setOperationTimeoutMs(30000);
            consumer.topicsChangeListener.onTopicsAdded(PARTITIONS).get(5, TimeUnit.SECONDS);
            consumer.getConsumers().forEach(PatternConsumerSchemaTest::assertDecodes);
        } finally {
            firstSubscribe.complete(mock(ProducerResponse.class));
            secondSubscribe.complete(mock(ProducerResponse.class));
            consumer.closeAsync().get(5, TimeUnit.SECONDS);
        }
        assertThat(template.closeCount.get()).isPositive();
    }

    @Test
    public void testCloseWaitsForUnownedTemplateSchemaAndRepeatedCloseSharesFuture() throws Exception {
        ResourceSchema template = new ResourceSchema(false);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createConsumer(template);
        CompletableFuture<ProducerResponse> firstSubscribe = new CompletableFuture<>();
        CompletableFuture<ProducerResponse> secondSubscribe = new CompletableFuture<>();
        CompletableFuture<Void> schemaClose = new CompletableFuture<>();
        setSubscribeResponses(List.of(firstSubscribe, secondSubscribe));
        try {
            CompletableFuture<Void> adding = consumer.topicsChangeListener.onTopicsAdded(PARTITIONS);
            Awaitility.await().untilAsserted(() -> assertThat(consumer.getConsumers()).hasSize(2));
            failSubscribe(firstSubscribe);
            failSubscribe(secondSubscribe);
            assertThat(adding).failsWithin(Duration.ofSeconds(5));
            assertThat(consumer.getConsumers()).isEmpty();
            assertThat(template.closeCount.get()).isZero();

            template.closeResult = schemaClose;
            CompletableFuture<Void> closing = consumer.closeAsync();
            assertThat(consumer.closeAsync()).as("repeated close must await the same schema shutdown")
                    .isSameAs(closing);
            Awaitility.await().untilAsserted(() -> assertThat(template.closeCount.get()).isEqualTo(1));
            assertThat(closing).isNotDone();
            schemaClose.complete(null);
            closing.get(5, TimeUnit.SECONDS);
            assertThat(template.closeCount.get()).isEqualTo(1);
        } finally {
            firstSubscribe.complete(mock(ProducerResponse.class));
            secondSubscribe.complete(mock(ProducerResponse.class));
            schemaClose.complete(null);
            consumer.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    private PatternMultiTopicsConsumerImpl<byte[]> createConsumer(ResourceSchema schema) {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("schema-lifetime");
        conf.setAutoUpdatePartitions(false);
        conf.setPatternAutoDiscoveryPeriod(3600);
        TopicsPattern pattern = TopicsPatternFactory.create("persistent://tenant/namespace/.*",
                TopicsPattern.RegexImplementation.JDK);
        return new PatternMultiTopicsConsumerImpl<>(pattern, client, conf, executorProvider,
                new CompletableFuture<>(), schema, CommandGetTopicsOfNamespace.Mode.PERSISTENT, null);
    }

    private void setSubscribeResponses(List<CompletableFuture<ProducerResponse>> responses) {
        client.getConfiguration().setOperationTimeoutMs(0);
        AtomicInteger requests = new AtomicInteger();
        doAnswer(invocation -> {
            invocation.<ByteBuf>getArgument(0).release();
            int request = requests.getAndIncrement();
            return request < responses.size() ? responses.get(request)
                    : CompletableFuture.completedFuture(mock(ProducerResponse.class));
        }).when(cnx).sendRequestWithId(any(), anyLong());
    }

    private static void failSubscribe(CompletableFuture<ProducerResponse> subscribe) {
        subscribe.completeExceptionally(new PulsarClientException.TimeoutException("Initial subscribe failed"));
    }

    private static void assertDecodes(ConsumerImpl<byte[]> consumer) {
        byte[] payload = new byte[] {1, 2, 3};
        assertThat(consumer.schema.decode(payload)).isEqualTo(payload);
    }

    private static final class ResourceSchema implements Schema<byte[]> {
        private final boolean versioned;
        private final AtomicInteger closeCount = new AtomicInteger();
        private CompletableFuture<Void> closeResult = CompletableFuture.completedFuture(null);

        private ResourceSchema(boolean versioned) {
            this.versioned = versioned;
        }

        @Override
        public byte[] encode(byte[] message) {
            return message;
        }

        @Override
        public byte[] decode(byte[] bytes) {
            if (closeCount.get() != 0) {
                throw new IllegalStateException("Schema resource is closed");
            }
            return bytes;
        }

        @Override
        public boolean supportSchemaVersioning() {
            return versioned;
        }

        @Override
        public SchemaInfo getSchemaInfo() {
            return Schema.BYTES.getSchemaInfo();
        }

        @Override
        public Schema<byte[]> clone() {
            return new ResourceSchema(versioned);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            closeCount.incrementAndGet();
            return closeResult;
        }
    }
}
