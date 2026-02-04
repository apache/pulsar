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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.testinterceptor.BrokerTestInterceptor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumersReadLimitsTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // start at max batch size to reproduce the issue more easily
        conf.setDispatcherMinReadBatchSize(conf.getDispatcherMaxReadBatchSize());
        BrokerTestInterceptor.INSTANCE.configure(conf);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    protected void resetInterceptors() throws Exception {
        BrokerTestInterceptor.INSTANCE.reset();
    }

    @Test(timeOut = 30 * 1000)
    public void testDispatcherMaxReadSizeBytes() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName(
                "persistent://public/default/testDispatcherMaxReadSizeBytes");
        final String subscription = "sub";

        AtomicInteger entriesReadMax = new AtomicInteger(0);
        BrokerTestInterceptor.INSTANCE.applyDispatcherSpyDecorator(PersistentDispatcherMultipleConsumers.class,
                spy -> {
                    doAnswer(invocation -> {
                        List<Entry> entries = invocation.getArgument(0);
                        PersistentDispatcherMultipleConsumers.ReadType readType = invocation.getArgument(1);
                        int numberOfEntries = entries.size();
                        log.info("intercepted readEntriesComplete with {} entries, read type {}", numberOfEntries,
                                readType);
                        entriesReadMax.updateAndGet(current -> Math.max(current, numberOfEntries));
                        return invocation.callRealMethod();
                    }).when(spy).readEntriesComplete(any(), any());
                }
        );

        // Create two consumers on a shared subscription
        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .consumerName("c1")
                .topic(topicName)
                .subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(10000)
                .subscribe();

        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .consumerName("c2")
                .topic(topicName)
                .subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared)
                .startPaused(true)
                .receiverQueueSize(10000)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer =
                pulsarClient.newProducer().enableBatching(false).topic(topicName).create();
        int numberOfMessages = 200;
        int payLoadSizeBytes = 1025 * 1024; // 1025kB
        byte[] payload = RandomUtils.nextBytes(payLoadSizeBytes);
        for (int i = 0; i < numberOfMessages; i++) {
            producer.send(payload);
        }

        // Consume messages with consumer1 but don't ack
        for (int i = 0; i < numberOfMessages; i++) {
            consumer1.receive();
        }

        // Close consumer1 and resume consumer2 to replay the messages
        consumer1.close();
        consumer2.resume();

        // Verify that consumer2 can receive the messages
        for (int i = 0; i < numberOfMessages; i++) {
            Message<byte[]> msg = consumer2.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg, "Consumer2 should receive the message");
            consumer2.acknowledge(msg);
        }

        int expectedMaxEntriesInRead = conf.getDispatcherMaxReadSizeBytes() / payLoadSizeBytes;
        assertThat(entriesReadMax.get()).isLessThanOrEqualTo(expectedMaxEntriesInRead);
    }
}
