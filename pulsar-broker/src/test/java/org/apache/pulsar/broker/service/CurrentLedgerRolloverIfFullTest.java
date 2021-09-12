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
package org.apache.pulsar.broker.service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class CurrentLedgerRolloverIfFullTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void testCurrentLedgerRolloverIfFull() throws Exception {
        final String topicName = "persistent://prop/ns-abc/CurrentLedgerRolloverIfFullTest";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .producerName("CurrentLedgerRolloverIfFullTest-producer-name")
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("CurrentLedgerRolloverIfFullTest-subscriber-name")
                .subscribe();

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topicName).get();
        Assert.assertNotNull(topicRef);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();

        ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
        managedLedgerConfig.setRetentionTime(1, TimeUnit.SECONDS);
        managedLedgerConfig.setMaxEntriesPerLedger(2);
        managedLedgerConfig.setMinimumRolloverTime(1, TimeUnit.MILLISECONDS);
        managedLedgerConfig.setMaximumRolloverTime(1, TimeUnit.SECONDS);

        int msgNum = 10;
        for (int i = 0; i < msgNum; i++) {
            producer.send(new byte[1024 * 1024]);
        }

        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), msgNum / 2);

        for (int i = 0; i < msgNum; i++) {
            Message<byte[]> msg = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            consumer.acknowledge(msg);
        }

        // all the messages have been acknowledged
        // and all the ledgers have been removed except the the last ledger
        Awaitility.await()
                .pollInterval(Duration.ofMillis(500L))
                .untilAsserted(() -> {
                            Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), 1);
                            Assert.assertNotEquals(managedLedger.getCurrentLedgerSize(), 0);
                        });

        // trigger a ledger rollover
        managedLedger.rollCurrentLedgerIfFull();

        // the last ledger will be closed and removed and we have one ledger for empty
        Awaitility.await()
                .pollInterval(Duration.ofMillis(1000L))
                .untilAsserted(() -> {
                    Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), 1);
                    Assert.assertEquals(managedLedger.getTotalSize(), 0);
                });
    }
}
