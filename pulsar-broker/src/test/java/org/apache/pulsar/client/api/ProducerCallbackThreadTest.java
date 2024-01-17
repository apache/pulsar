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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class ProducerCallbackThreadTest extends ProducerConsumerBase {

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

    @Test
    public void testSyncCloseInCreateAsyncThread() {
        String topic = "public/default/testSyncCloseInCreateCallbackThread";

        CompletableFuture<Void> createFuture = pulsarClient.newProducer()
                .topic(topic)
                .createAsync().thenAccept((producer) -> {
                    try {
                        producer.close();
                    } catch (PulsarClientException e) {
                        throw new RuntimeException(e);
                    }
                });

        Awaitility.await().untilAsserted(() -> {
            assertTrue(createFuture.isDone());
            assertFalse(createFuture.isCompletedExceptionally());
        });
    }

    @Test
    public void testSyncCloseInSendAsyncThread() throws PulsarClientException {
        String topic = "public/default/testSyncCloseInSendAsyncCallbackThread";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        CompletableFuture<Void> producerCloseFuture = producer.sendAsync("hello".getBytes(StandardCharsets.UTF_8))
                .thenAccept((messageId) -> {
                    assertNotNull(messageId);
                    try {
                        producer.close();
                    } catch (PulsarClientException e) {
                        throw new RuntimeException(e);
                    }
                });

        Awaitility.await().untilAsserted(() -> {
            assertFalse(producer.isConnected());
            assertTrue(producerCloseFuture.isDone());
            assertFalse(producerCloseFuture.isCompletedExceptionally());
        });
    }

    @Test
    public void testSyncCreateInCloseSyncThread() throws PulsarClientException {
        String topic = "public/default/testSyncCreateInCloseSyncThread";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        CompletableFuture<Producer<byte[]>> newProducerFuture = new CompletableFuture<>();
        producer.closeAsync().thenAccept((__) -> {
            try {
                newProducerFuture.complete(pulsarClient.newProducer().topic(topic).create());
            } catch (PulsarClientException e) {
                newProducerFuture.completeExceptionally(e);
            }
        });

        Awaitility.await().untilAsserted(() -> {
            assertFalse(producer.isConnected());

            assertTrue(newProducerFuture.isDone());
            assertFalse(newProducerFuture.isCompletedExceptionally());
            assertTrue(newProducerFuture.get().isConnected());
        });
    }

    @Test
    public void testMultipleThreadSyncCreateAndCloseProducer() {
        String topic = "public/default/testMultipleThreadSyncCreateProducer";

        Queue<CompletableFuture<Producer<byte[]>>> producers = new ConcurrentLinkedQueue<>();
        int numProducers = 36;
        for (int i = 0; i < numProducers; i++) {
            new Thread(() -> {
                try {
                    Producer<byte[]> producer = pulsarClient.newProducer()
                            .topic(topic)
                            .create();
                    producers.add(CompletableFuture.completedFuture(producer));
                } catch (PulsarClientException e) {
                    producers.add(CompletableFuture.failedFuture(e));
                }
            }).start();
        }

        Awaitility.await().untilAsserted(() -> {
            assertEquals(producers.size(), numProducers);
            assertTrue(producers.stream().allMatch(n -> n.isDone() & !n.isCompletedExceptionally()));
        });

        producers.forEach(n -> new Thread(() -> {
            try {
                n.get().close();
            } catch (PulsarClientException | InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).start());

        Awaitility.await().untilAsserted(() -> {
            assertTrue(producers.stream().allMatch(n -> {
                try {
                    return !n.get().isConnected();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }));
        });
    }

}
