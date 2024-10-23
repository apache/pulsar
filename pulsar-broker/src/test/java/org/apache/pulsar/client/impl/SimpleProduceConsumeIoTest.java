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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class SimpleProduceConsumeIoTest extends ProducerConsumerBase {

    private PulsarClientImpl singleConnectionPerBrokerClient;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        singleConnectionPerBrokerClient = (PulsarClientImpl) PulsarClient.builder().connectionsPerBroker(1)
                .serviceUrl(lookupUrl.toString()).build();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (singleConnectionPerBrokerClient != null) {
            singleConnectionPerBrokerClient.close();
        }
        super.internalCleanup();
    }

    /**
     * 1. Create a producer with a pooled connection.
     * 2. When executing "producer.connectionOpened", the pooled connection has been closed due to a network issue.
     * 3. Verify: the producer can be created successfully.
     */
    @Test
    public void testUnstableNetWorkWhenCreatingProducer() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topic);
        // Trigger a pooled connection creation.
        ProducerImpl p = (ProducerImpl) singleConnectionPerBrokerClient.newProducer().topic(topic).create();
        ClientCnx cnx = p.getClientCnx();
        p.close();

        // 1. Create a new producer with the pooled connection(since there is a pooled connection, the new producer
        // will reuse it).
        // 2. Trigger a network issue.
        CountDownLatch countDownLatch = new CountDownLatch(1);
        // A task for trigger network issue.
        new Thread(() -> {
            try {
                countDownLatch.await();
                cnx.ctx().close();
            } catch (Exception ex) {
            }
        }).start();
        // Create a new producer with the pooled connection.
        AtomicReference<CompletableFuture<Producer<byte[]>>> p2FutureWrap = new AtomicReference<>();
        new Thread(() -> {
            ProducerBuilder producerBuilder = singleConnectionPerBrokerClient.newProducer().topic(topic);
            ProducerConfigurationData producerConf = WhiteboxImpl.getInternalState(producerBuilder, "conf");
            CompletableFuture<Producer<byte[]>> p2Future = new CompletableFuture();
            p2FutureWrap.set(p2Future);
            new ProducerImpl<>(singleConnectionPerBrokerClient, "public/default/tp1", producerConf, p2Future,
                    -1, Schema.BYTES, null, Optional.empty()) {
                @Override
                public CompletableFuture<Void> connectionOpened(final ClientCnx cnx) {
                    // Mock a network issue, and wait for the issue occurred.
                    countDownLatch.countDown();
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException e) {
                    }
                    // Call the real implementation.
                    return super.connectionOpened(cnx);
                }
            };
        }).start();

        // Verify: the producer can be created successfully.
        Awaitility.await().untilAsserted(() -> {
            assertNotNull(p2FutureWrap.get());
            assertTrue(p2FutureWrap.get().isDone());
        });
        // Print log.
        p2FutureWrap.get().exceptionally(ex -> {
            log.error("Failed to create producer", ex);
            return null;
        });
        Awaitility.await().untilAsserted(() -> {
            assertFalse(p2FutureWrap.get().isCompletedExceptionally());
            assertTrue("Ready".equals(
                    WhiteboxImpl.getInternalState(p2FutureWrap.get().join(), "state").toString()));
        });

        // Cleanup.
        p2FutureWrap.get().join().close();
        admin.topics().delete(topic);
    }
}
