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

import static org.testng.Assert.assertTrue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class ConsumerCloseTest extends ProducerConsumerBase {

    @BeforeClass
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
    public void testInterruptedWhenCreateConsumer() throws InterruptedException {

        String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        String subName = "test-sub";
        String mlCursorPath = BrokerService.MANAGED_LEDGER_PATH_ZNODE + "/"
                + TopicName.get(tpName).getPersistenceNamingEncoding() + "/" + subName;

        // Make create cursor delay 1s
        CountDownLatch topicLoadLatch = new CountDownLatch(1);
        for (int i = 0; i < 5; i++) {
            mockZooKeeper.delay(1000, (op, path) -> {
                if (mlCursorPath.equals(path)) {
                    topicLoadLatch.countDown();
                    return true;
                }
                return false;
            });
        }

        Thread startConsumer = new Thread(() -> {
            try {
                pulsarClient.newConsumer()
                        .topic(tpName)
                        .subscriptionName(subName)
                        .subscribe();
                Assert.fail("Should have thrown an exception");
            } catch (PulsarClientException e) {
                assertTrue(e.getCause() instanceof InterruptedException);
            }
        });
        startConsumer.start();
        topicLoadLatch.await();
        startConsumer.interrupt();

        PulsarClientImpl clientImpl = (PulsarClientImpl) pulsarClient;
        Awaitility.await().ignoreExceptions().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertEquals(clientImpl.consumersCount(), 0);
        });
    }

    @Test
    public void testReceiveWillDoneAfterClosedConsumer() throws Exception {
        String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        String subName = "test-sub";
        admin.topics().createNonPartitionedTopic(tpName);
        admin.topics().createSubscription(tpName, subName, MessageId.earliest);
        ConsumerImpl<byte[]> consumer =
                (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(tpName).subscriptionName(subName).subscribe();
        CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
        consumer.close();
        Awaitility.await().untilAsserted(() -> {
            assertTrue(future.isDone());
        });
    }

    @Test
    public void testReceiveWillDoneAfterTopicDeleted() throws Exception {
        String namespace = "public/default";
        admin.namespaces().setAutoTopicCreation(namespace, AutoTopicCreationOverride.builder()
                .allowAutoTopicCreation(false).build());
        String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        String subName = "test-sub";
        admin.topics().createNonPartitionedTopic(tpName);
        admin.topics().createSubscription(tpName, subName, MessageId.earliest);
        ConsumerImpl<byte[]> consumer =
                (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(tpName).subscriptionName(subName).subscribe();
        CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
        admin.topics().delete(tpName, true);
        Awaitility.await().untilAsserted(() -> {
            assertTrue(future.isDone());
        });
        // cleanup.
        admin.namespaces().removeAutoTopicCreation(namespace);
    }
}
