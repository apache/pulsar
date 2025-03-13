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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@Slf4j
public class PersistentTopicTerminateTest extends ProducerConsumerBase {


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
    public void testRecoverAfterTerminate() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscriptionName = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);

        // Trigger 2 ledgers creation.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        producer.send("1");
        admin.topics().unload(topicName);
        producer.send("2");

        // Terminate topic.
        producer.close();
        admin.topics().terminateTopic(topicName);
        admin.topics().unload(topicName);

        // Verify: consume 2 msgs.
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionName(subscriptionName).subscribe();

        Message<String> msg1 = consumer.receive(2, TimeUnit.SECONDS);
        assertNotNull(msg1);
        assertEquals(msg1.getValue(), "1");
        Message<String> msg2 = consumer.receive(2, TimeUnit.SECONDS);
        assertNotNull(msg2);
        assertEquals(msg2.getValue(), "2");

        // Verify: the ledgers acked will be cleaned up.
        admin.topics().skipAllMessages(topicName, subscriptionName);
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
            ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            CompletableFuture<Void> trimLedgersFuture = new CompletableFuture<>();
            ml.trimConsumedLedgersInBackground(trimLedgersFuture);
            trimLedgersFuture.join();
            assertTrue(ml.getLedgersInfo().size() <= 1);
        });

        // Cleanup.
        consumer.close();
        admin.topics().delete(topicName, false);
    }
}
