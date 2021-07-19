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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ConsumedLedgersTrimTest extends BrokerTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumedLedgersTrimTest.class);

    @Override
    protected void setup() throws Exception {
        //No-op
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void TestConsumedLedgersTrim() throws Exception {
        conf.setRetentionCheckIntervalInSeconds(1);
        super.baseSetup();
        final String topicName = "persistent://prop/ns-abc/TestConsumedLedgersTrim";
        final String subscriptionName = "my-subscriber-name";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .producerName("producer-name")
                .create();
        @Cleanup
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
            producer.send(new byte[1024 * 1024]);
        }

        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), msgNum / 2);

        //no traffic, unconsumed ledger will be retained
        Thread.sleep(1200);
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), msgNum / 2);

        for (int i = 0; i < msgNum; i++) {
            Message<byte[]> msg = consumer.receive(2, TimeUnit.SECONDS);
            assertNotNull(msg);
            consumer.acknowledge(msg);
        }
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), msgNum / 2);

        //no traffic, but consumed ledger will be cleaned
        Thread.sleep(1500);
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), 1);
    }


    @Test
    public void testConsumedLedgersTrimNoSubscriptions() throws Exception {
        conf.setRetentionCheckIntervalInSeconds(1);
        conf.setBrokerDeleteInactiveTopicsEnabled(false);
        super.baseSetup();
        final String topicName = "persistent://prop/ns-abc/TestConsumedLedgersTrimNoSubscriptions";

        // write some messages
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .producerName("producer-name")
                .create();

        // set retention parameters, the ledgers are to be deleted as soon as possible
        // but the topic is not to be automatically deleted
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
        managedLedgerConfig.setRetentionSizeInMB(-1);
        managedLedgerConfig.setRetentionTime(1, TimeUnit.SECONDS);
        managedLedgerConfig.setMaxEntriesPerLedger(1000);
        managedLedgerConfig.setMinimumRolloverTime(1, TimeUnit.MILLISECONDS);
        MessageId initialMessageId = persistentTopic.getLastMessageId().get();
        LOG.info("lastmessageid " + initialMessageId);

        int msgNum = 7;
        for (int i = 0; i < msgNum; i++) {
            producer.send(new byte[1024 * 1024]);
        }

        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), 1);
        MessageId messageIdBeforeRestart = pulsar.getAdminClient().topics().getLastMessageId(topicName);
        LOG.info("messageIdBeforeRestart " + messageIdBeforeRestart);
        assertNotEquals(messageIdBeforeRestart, initialMessageId);

        // restart the broker we have to start a new ledger
        // the lastMessageId is still on the previous ledger
        restartBroker();
        // force load topic
        pulsar.getAdminClient().topics().getStats(topicName);
        MessageId messageIdAfterRestart = pulsar.getAdminClient().topics().getLastMessageId(topicName);
        LOG.info("lastmessageid " + messageIdAfterRestart);
        assertEquals(messageIdAfterRestart, messageIdBeforeRestart);

        persistentTopic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
        managedLedgerConfig.setRetentionSizeInMB(-1);
        managedLedgerConfig.setRetentionTime(1, TimeUnit.SECONDS);
        managedLedgerConfig.setMaxEntriesPerLedger(1);
        managedLedgerConfig.setMinimumRolloverTime(1, TimeUnit.MILLISECONDS);
        managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        // now we have two ledgers, the first is expired but is contains the lastMessageId
        // the second is empty and should be kept as it is the current tail
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), 2);

        // force trimConsumedLedgers
        Thread.sleep(3000);
        CompletableFuture f = new CompletableFuture();
        managedLedger.trimConsumedLedgersInBackground(f);
        f.join();

        // lastMessageId should be available even in this case, but is must
        // refer to -1
        MessageId messageIdAfterTrim = pulsar.getAdminClient().topics().getLastMessageId(topicName);
        LOG.info("lastmessageid " + messageIdAfterTrim);
        assertEquals(messageIdAfterTrim, MessageId.earliest);

    }
}
