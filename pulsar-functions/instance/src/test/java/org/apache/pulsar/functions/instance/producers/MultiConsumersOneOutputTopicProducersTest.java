/**
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
package org.apache.pulsar.functions.instance.producers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.ProducerInterceptor;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link MultiConsumersOneOuputTopicProducers}.
 */
public class MultiConsumersOneOutputTopicProducersTest {

    private static final String TEST_OUTPUT_TOPIC = "test-output-topic";

    private PulsarClient mockClient;
    private final Map<String, Producer<byte[]>> mockProducers = new HashMap<>();
    private MultiConsumersOneOuputTopicProducers<byte[]> producers;

    private class MockProducerBuilder implements ProducerBuilder<byte[]> {

        String producerName = "";

        @Override
        public Producer<byte[]> create() throws PulsarClientException {
            Producer<byte[]> producer;
            synchronized (mockProducers) {
                producer = mockProducers.get(producerName);
                if (null == producer) {
                    producer = createMockProducer(producerName);
                    mockProducers.put(producerName, producer);
                }
            }
            return producer;
        }

        @Override
        public CompletableFuture<Producer<byte[]>> createAsync() {
            try {
                return CompletableFuture.completedFuture(create());
            } catch (PulsarClientException e) {
                CompletableFuture<Producer<byte[]>> future = new CompletableFuture<>();
                future.completeExceptionally(e);
                return future;
            }
        }

        @Override
        public ProducerBuilder<byte[]> loadConf(Map<String, Object> config) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> clone() {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> topic(String topicName) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> producerName(String producerName) {
            this.producerName = producerName;
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> sendTimeout(int sendTimeout, TimeUnit unit) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> maxPendingMessages(int maxPendingMessages) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> maxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> blockIfQueueFull(boolean blockIfQueueFull) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> messageRoutingMode(MessageRoutingMode messageRoutingMode) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> hashingScheme(HashingScheme hashingScheme) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> compressionType(CompressionType compressionType) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> messageRouter(MessageRouter messageRouter) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> enableBatching(boolean enableBatching) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> addEncryptionKey(String key) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> cryptoFailureAction(ProducerCryptoFailureAction action) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> batchingMaxPublishDelay(long batchDelay, TimeUnit timeUnit) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> batchingMaxMessages(int batchMessagesMaxMessagesPerBatch) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> initialSequenceId(long initialSequenceId) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> property(String key, String value) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> properties(Map<String, String> properties) {
            return this;
        }

        @Override
        public ProducerBuilder<byte[]> intercept(ProducerInterceptor<byte[]>... interceptors) {
            return null;
        }
    }

    @BeforeMethod
    public void setup() throws Exception {
        this.mockClient = mock(PulsarClient.class);

        when(mockClient.newProducer(any(Schema.class)))
                .thenReturn(new MockProducerBuilder());

        producers = new MultiConsumersOneOuputTopicProducers<byte[]>(mockClient, TEST_OUTPUT_TOPIC, Schema.BYTES);
        producers.initialize();
    }

    private Producer<byte[]> createMockProducer(String topic) {
        Producer<byte[]> producer = mock(Producer.class);
        when(producer.closeAsync())
                .thenAnswer(invocationOnMock -> {
                    synchronized (mockProducers) {
                        mockProducers.remove(topic);
                    }
                    return FutureUtils.Void();
                });
        return producer;
    }

    @Test
    public void testGetCloseProducer() throws Exception {
        String srcTopic = "test-src-topic";
        String ptnIdx = "1234";
        String producerName = String.format("%s-%s", srcTopic, ptnIdx);
        Producer<byte[]> producer = producers.getProducer(producerName);

        assertSame(mockProducers.get(producerName), producer);
        verify(mockClient, times(1))
                .newProducer(Schema.BYTES);
        assertTrue(producers.getProducers().containsKey(producerName));

        // second get will not create a new producer
        assertSame(mockProducers.get(producerName), producer);
        verify(mockClient, times(1))
                .newProducer(Schema.BYTES);
        assertTrue(producers.getProducers().containsKey(producerName));

        // close
        producers.closeProducer(producerName);
        verify(producer, times(1)).closeAsync();
        assertFalse(producers.getProducers().containsKey(srcTopic));
    }

}
