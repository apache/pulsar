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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class ProducerReconnectionTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConcurrencyReconnectAndClose() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        admin.topics().createNonPartitionedTopic(topicName);
        PulsarClientImpl client = (PulsarClientImpl) pulsarClient;

        // Create producer which will run with special steps.
        ProducerBuilderImpl<byte[]> producerBuilder = (ProducerBuilderImpl<byte[]>) client.newProducer()
                .blockIfQueueFull(false).maxPendingMessages(1).producerName("p1")
                .enableBatching(true).topic(topicName);
        CompletableFuture<Producer<byte[]>> producerFuture = new CompletableFuture<>();
        AtomicBoolean reconnectionStartTrigger = new AtomicBoolean();
        CountDownLatch reconnectingSignal = new CountDownLatch(1);
        CountDownLatch closedSignal = new CountDownLatch(1);
        ProducerImpl<byte[]> producer = new ProducerImpl<>(client, topicName, producerBuilder.getConf(), producerFuture,
                -1, Schema.BYTES, null, Optional.empty()) {
            @Override
            ConnectionHandler initConnectionHandler() {
                ConnectionHandler connectionHandler = super.initConnectionHandler();
                ConnectionHandler spyConnectionHandler = spy(connectionHandler);
                doAnswer(invocation -> {
                    boolean result = (boolean) invocation.callRealMethod();
                    if (reconnectionStartTrigger.get()) {
                        log.info("[testConcurrencyReconnectAndClose] verified state for reconnection");
                        reconnectingSignal.countDown();
                        closedSignal.await();
                        log.info("[testConcurrencyReconnectAndClose] reconnected");
                    }
                    return result;
                }).when(spyConnectionHandler).isValidStateForReconnection();
                return spyConnectionHandler;
            }
        };
        log.info("[testConcurrencyReconnectAndClose] producer created");
        producerFuture.get(5, TimeUnit.SECONDS);

        // Reconnect.
        log.info("[testConcurrencyReconnectAndClose] trigger a reconnection");
        ServerCnx serverCnx = (ServerCnx) pulsar.getBrokerService().getTopic(topicName, false).join()
                .get().getProducers().values().iterator().next().getCnx();
        reconnectionStartTrigger.set(true);
        serverCnx.ctx().close();
        producer.sendAsync("1".getBytes(StandardCharsets.UTF_8));
        Awaitility.await().untilAsserted(() -> {
            assertNotEquals(producer.getPendingQueueSize(), 0);
        });

        // Close producer when reconnecting.
        reconnectingSignal.await();
        log.info("[testConcurrencyReconnectAndClose] producer close");
        producer.closeAsync();
        Awaitility.await().untilAsserted(() -> {
            HandlerState.State state1 = producer.getState();
            assertTrue(state1 == HandlerState.State.Closed || state1 == HandlerState.State.Closing);
        });
        // give another thread time to call "signalToChangeStateToConnecting.await()".
        closedSignal.countDown();

        // Wait for reconnection.
        Thread.sleep(3000);

        HandlerState.State state2 = producer.getState();
        log.info("producer state: {}", state2);
        assertTrue(state2 == HandlerState.State.Closed || state2 == HandlerState.State.Closing);
        assertEquals(producer.getPendingQueueSize(), 0);

        // Verify: ref is expected.
        producer.close();
        admin.topics().delete(topicName);
    }
}
