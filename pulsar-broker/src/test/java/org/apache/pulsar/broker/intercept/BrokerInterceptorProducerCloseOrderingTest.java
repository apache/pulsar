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
package org.apache.pulsar.broker.intercept;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerInterceptorProducerCloseOrderingTest extends ProducerConsumerBase {

    private ProducerCloseOrderingBrokerInterceptor orderingInterceptor;

    @BeforeClass(alwaysRun = true)
    @Override
    public void setup() throws Exception {
        conf.setSystemTopicEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        enableBrokerInterceptor = true;
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    public void resetInterceptor() {
        ProducerCloseOrderingBrokerInterceptor.reset();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        NarClassLoader narClassLoader = org.mockito.Mockito.mock(NarClassLoader.class,
                delegatesTo(new URLClassLoader(new URL[0], getClass().getClassLoader())));
        orderingInterceptor = new ProducerCloseOrderingBrokerInterceptor();
        pulsarTestContextBuilder.brokerInterceptor(
                new BrokerInterceptorWithClassLoader(orderingInterceptor, narClassLoader));
    }

    @Test
    public void testProducerClosedAfterMessageProducedOnGracefulCloseWithPendingAck() throws Exception {
        String topic = "persistent://public/default/producer-close-ordering-" + UUID.randomUUID();
        var clientProducer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        ProducerCloseOrderingBrokerInterceptor.prepareBlockOnFirstMessageProduced();

        CompletableFuture<?> inFlight = clientProducer.sendAsync("payload");
        assertTrue(ProducerCloseOrderingBrokerInterceptor.messageProducedEntered
                .await(20, TimeUnit.SECONDS));

        CompletableFuture<Void> closeFuture = clientProducer.closeAsync();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> !closeFuture.isDone());

        ProducerCloseOrderingBrokerInterceptor.unblockMessageProduced.countDown();
        closeFuture.get(30, TimeUnit.SECONDS);
        inFlight.get(30, TimeUnit.SECONDS);

        List<String> events = new ArrayList<>(ProducerCloseOrderingBrokerInterceptor.LIFECYCLE_EVENTS);
        assertEquals(events.indexOf(ProducerCloseOrderingBrokerInterceptor.MESSAGE_PRODUCED),
                0);
        assertEquals(events.indexOf(ProducerCloseOrderingBrokerInterceptor.PRODUCER_CLOSED),
                1);
        assertEquals(events.size(), 2);
    }

    /**
     * Records {@code messageProduced} / {@code producerClosed} order. Can pause the first {@code messageProduced}
     * while a graceful client close is pending so the publish-ack path runs with {@code isClosed} set.
     */
    static final class ProducerCloseOrderingBrokerInterceptor extends MockBrokerInterceptor {

        static final String MESSAGE_PRODUCED = "messageProduced";
        static final String PRODUCER_CLOSED = "producerClosed";

        static final List<String> LIFECYCLE_EVENTS = new CopyOnWriteArrayList<>();
        static CountDownLatch messageProducedEntered = new CountDownLatch(1);
        static CountDownLatch unblockMessageProduced = new CountDownLatch(1);

        static void reset() {
            LIFECYCLE_EVENTS.clear();
            messageProducedEntered = new CountDownLatch(1);
            unblockMessageProduced = new CountDownLatch(1);
        }

        static void prepareBlockOnFirstMessageProduced() {
            reset();
        }

        @Override
        public void producerClosed(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
            LIFECYCLE_EVENTS.add(PRODUCER_CLOSED);
        }

        @Override
        public void messageProduced(ServerCnx cnx, Producer producer, long startTimeNs, long ledgerId, long entryId,
                                    Topic.PublishContext publishContext) {
            messageProducedEntered.countDown();
            try {
                if (!unblockMessageProduced.await(30, TimeUnit.SECONDS)) {
                    throw new RuntimeException("timed out waiting to unblock messageProduced");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            LIFECYCLE_EVENTS.add(MESSAGE_PRODUCED);
        }

        @Override
        public void producerCreated(ServerCnx cnx, Producer producer, Map<String, String> metadata) {
            producer.markBrokerInterceptorCreatedNotified(0);
        }
    }
}