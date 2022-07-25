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
package org.apache.pulsar.io.kafka.connect;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.util.Callback;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test the implementation of {@link PulsarOffsetBackingStore}.
 */
@Slf4j
public class PulsarOffsetBackingStoreTest extends ProducerConsumerBase {

    private Map<String, String> defaultProps = new HashMap<>();
    private PulsarKafkaWorkerConfig distributedConfig;
    private String topicName;
    private PulsarOffsetBackingStore offsetBackingStore;
    private PulsarClient client;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        this.topicName = "persistent://my-property/my-ns/offset-topic";
        this.defaultProps.put(PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG, topicName);
        this.client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .build();
        this.offsetBackingStore = new PulsarOffsetBackingStore(client);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (null != offsetBackingStore) {
            offsetBackingStore.stop();
            offsetBackingStore = null;
        }

        super.internalCleanup();
    }

    private void testOffsetBackingStore(boolean testWithReaderConfig) throws Exception {
        if (testWithReaderConfig) {
            this.defaultProps.put(PulsarKafkaWorkerConfig.OFFSET_STORAGE_READER_CONFIG,
                    "{\"subscriptionName\":\"my-subscription\"}");
        }
        this.distributedConfig = new PulsarKafkaWorkerConfig(this.defaultProps);
        this.offsetBackingStore.configure(distributedConfig);
        this.offsetBackingStore.start();
    }

    @Test
    public void testGetFromEmpty() throws Exception {
        testOffsetBackingStore(false);
        assertTrue(offsetBackingStore.get(
            Arrays.asList(ByteBuffer.wrap("empty-key".getBytes(UTF_8)))
        ).get().isEmpty());
    }

    @Test
    public void testGetSet() throws Exception {
        testOffsetBackingStore(false);
        testGetSet(false);
    }

    @Test
    public void testGetSetCallback() throws Exception {
        testOffsetBackingStore(false);
        testGetSet(true);
    }

    private void testGetSet(boolean testCallback) throws Exception {
        final int numKeys = 10;
        final List<ByteBuffer> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            Map<ByteBuffer, ByteBuffer> kvs = new HashMap<>();
            ByteBuffer key = ByteBuffer.wrap(("test-key-" + i).getBytes(UTF_8));
            keys.add(key);
            kvs.put(
                key,
                ByteBuffer.wrap(("test-val-" + i).getBytes(UTF_8)));
            CompletableFuture<Void> setCallback = new CompletableFuture<>();
            offsetBackingStore.set(
                kvs,
                testCallback ? (Callback<Void>) (error, result) -> {
                    if (null != error) {
                        setCallback.completeExceptionally(error);
                    } else {
                        setCallback.complete(result);
                    }
                } : null
            ).get();
            if (testCallback) {
                setCallback.join();
            }
        }

        Map<ByteBuffer, ByteBuffer> result =
            offsetBackingStore.get(keys).get();
        assertEquals(numKeys, result.size());
        AtomicInteger count = new AtomicInteger();
        new TreeMap<>(result).forEach((key, value) -> {
            int idx = count.getAndIncrement();
            byte[] keyData = ByteBufUtil.getBytes(Unpooled.wrappedBuffer(key));
            assertEquals(new String(keyData, UTF_8), "test-key-" + idx);
            byte[] valData = ByteBufUtil.getBytes(Unpooled.wrappedBuffer(value));
            assertEquals(new String(valData, UTF_8), "test-val-" + idx);
        });
    }

    @Test
    public void testWithReaderConfig() throws Exception {
        testOffsetBackingStore(true);
        testGetSet(false);
        List<String> subscriptions = admin.topics().getSubscriptions(topicName);
        assertTrue(subscriptions.contains("my-subscription"));
    }
}
