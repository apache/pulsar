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
package org.apache.pulsar.functions.instance.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.Map;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionType;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.common.functions.BatchingConfig;
import org.apache.pulsar.common.functions.CryptoConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class V5ProducerFactoryTest {

    @SuppressWarnings("unchecked")
    private static ProducerBuilder<String> mockProducerBuilder(PulsarClient client) throws Exception {
        ProducerBuilder<String> builder = mock(ProducerBuilder.class);
        when(client.newProducer(any())).thenAnswer(invocation -> builder);
        when(builder.topic(any())).thenReturn(builder);
        when(builder.blockIfQueueFull(org.mockito.ArgumentMatchers.anyBoolean())).thenReturn(builder);
        when(builder.sendTimeout(any())).thenReturn(builder);
        when(builder.compressionPolicy(any())).thenReturn(builder);
        when(builder.batchingPolicy(any())).thenReturn(builder);
        when(builder.producerName(any())).thenReturn(builder);
        when(builder.properties(any())).thenReturn(builder);
        when(builder.create()).thenReturn(mock(org.apache.pulsar.client.api.v5.Producer.class));
        return builder;
    }

    @Test
    public void testDefaults() throws Exception {
        PulsarClient client = mock(PulsarClient.class);
        ProducerBuilder<String> builder = mockProducerBuilder(client);

        Producer<String> producer = new V5ProducerFactory(() -> client, null, CompressionType.LZ4)
                .createProducer("topic://public/default/out", Schema.STRING, "p1", Map.of("a", "1"));

        assertThat(producer).isInstanceOf(V5ProducerAdapter.class);
        verify(builder).topic("topic://public/default/out");
        verify(builder).blockIfQueueFull(true);
        verify(builder).sendTimeout(Duration.ZERO);
        verify(builder).producerName("p1");
        verify(builder).properties(Map.of("a", "1"));
        verify(builder).compressionPolicy(CompressionPolicy.of(CompressionType.LZ4));
        ArgumentCaptor<BatchingPolicy> batching = ArgumentCaptor.forClass(BatchingPolicy.class);
        verify(builder).batchingPolicy(batching.capture());
        assertThat(batching.getValue().enabled()).isTrue();
        assertThat(batching.getValue().maxPublishDelay()).isEqualTo(Duration.ofMillis(10));
    }

    @Test
    public void testProducerConfig() throws Exception {
        PulsarClient client = mock(PulsarClient.class);
        ProducerBuilder<String> builder = mockProducerBuilder(client);
        ProducerConfig producerConfig = ProducerConfig.builder()
                .compressionType(org.apache.pulsar.client.api.CompressionType.ZSTD)
                .batchingConfig(BatchingConfig.builder()
                        .enabled(true)
                        .batchingMaxPublishDelayMs(25)
                        .batchingMaxMessages(500)
                        .batchingMaxBytes(4096)
                        .build())
                .build();

        new V5ProducerFactory(() -> client, producerConfig, CompressionType.LZ4)
                .createProducer("topic://public/default/out", Schema.STRING, null, null);

        verify(builder).compressionPolicy(CompressionPolicy.of(CompressionType.ZSTD));
        ArgumentCaptor<BatchingPolicy> batching = ArgumentCaptor.forClass(BatchingPolicy.class);
        verify(builder).batchingPolicy(batching.capture());
        assertThat(batching.getValue().maxPublishDelay()).isEqualTo(Duration.ofMillis(25));
        assertThat(batching.getValue().maxMessages()).isEqualTo(500);
        assertThat(batching.getValue().maxSize()).isEqualTo(MemorySize.ofBytes(4096));
    }

    @Test
    public void testBatchingDisabled() throws Exception {
        PulsarClient client = mock(PulsarClient.class);
        ProducerBuilder<String> builder = mockProducerBuilder(client);
        ProducerConfig producerConfig = ProducerConfig.builder()
                .batchingConfig(BatchingConfig.builder().enabled(false).build())
                .build();

        new V5ProducerFactory(() -> client, producerConfig, CompressionType.NONE)
                .createProducer("topic://public/default/out", Schema.STRING, null, null);

        verify(builder).batchingPolicy(BatchingPolicy.ofDisabled());
        verify(builder).compressionPolicy(CompressionPolicy.of(CompressionType.NONE));
    }

    @Test
    public void testRejectsProducerEncryption() {
        ProducerConfig producerConfig = ProducerConfig.builder()
                .cryptoConfig(CryptoConfig.builder().cryptoKeyReaderClassName("Reader").build())
                .build();
        assertThatThrownBy(() -> new V5ProducerFactory(() -> null, producerConfig, CompressionType.LZ4))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Producer encryption");
    }

    @Test
    public void testClientIsCreatedOnFirstProducer() throws Exception {
        PulsarClient client = mock(PulsarClient.class);
        mockProducerBuilder(client);
        int[] created = {0};
        V5ProducerFactory factory = new V5ProducerFactory(() -> {
            created[0]++;
            return client;
        }, null, CompressionType.LZ4);
        assertThat(created[0]).isZero();
        factory.createProducer("topic://public/default/out", Schema.STRING, null, null);
        assertThat(created[0]).isEqualTo(1);
    }
}
