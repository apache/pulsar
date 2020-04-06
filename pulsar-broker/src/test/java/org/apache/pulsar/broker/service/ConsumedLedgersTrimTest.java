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


import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.junit.Test;
import org.testng.Assert;

import java.util.concurrent.TimeUnit;

public class ConsumedLedgersTrimTest extends BrokerTestBase {
    @Override
    protected void setup() throws Exception {
        //No-op
    }

    @Override
    protected void cleanup() throws Exception {
        //No-op
    }

    @Test
    public void TestConsumedLedgersTrim() throws Exception {
        conf.setConsumedLedgersCheckIntervalInSeconds(2);
        super.baseSetup();
        Thread.sleep(4);
        final String topicName = "persistent://prop/ns-abc/TestConsumedLedgersTrim";
        final String subscriptionName = "my-subscriber-name";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .producerName("producer-name")
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        Topic topicRef = pulsar.getBrokerService().getTopicReference(topicName).get();
        Assert.assertNotNull(topicRef);

        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();

        ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
        managedLedgerConfig.setRetentionSizeInMB(1L);
        managedLedgerConfig.setRetentionTime(1, TimeUnit.SECONDS);
        managedLedgerConfig.setMaxEntriesPerLedger(2);
        managedLedgerConfig.setMinimumRolloverTime(1, TimeUnit.MILLISECONDS);

        int msgNum = 10;
        for (int i = 0; i < msgNum; i++) {
            producer.sendAsync(new byte[1024 * 1024]).get(2, TimeUnit.SECONDS);
        }

        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), msgNum / 2);

        //no traffic, unconsumed ledger will be retained
        Thread.sleep(5000);
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), msgNum / 2);

        for (int i = 0; i < msgNum; i++) {
            Message<byte[]> msg = consumer.receive(2, TimeUnit.SECONDS);
            Assert.assertTrue(msg != null);
            consumer.acknowledge(msg);
        }
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), msgNum / 2);

        //no traffic, but consumed ledger will be cleaned
        Thread.sleep(5000);
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), 1);
    }
}
