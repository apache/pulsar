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
import static org.testng.Assert.assertNotEquals;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.functions.api.Function;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class RetentionPolicesSettingTest extends ProducerConsumerBase {

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

    @DataProvider(name = "changeRetentionPolicyOps")
    public Object[][] changeRetentionPolicyOps() {
        Function<String, Void> setNamespaceLevelPolicy60Min = (topicName, context) -> {
            String ns = TopicName.get(topicName).getNamespace();
            RetentionPolicies retentionPolicies = new RetentionPolicies(60, -1);
            admin.namespaces().setRetentionAsync(ns, retentionPolicies).join();
            return null;
        };

        Function<String, Void> setTopicLevelPolicy60Min = (topicName, context) -> {
            RetentionPolicies retentionPolicies = new RetentionPolicies(60, -1);
            admin.topics().setRetentionAsync(topicName, retentionPolicies).join();
            return null;
        };

        Function<String, Void> setNamespaceLevelPolicy60M = (topicName, context) -> {
            String ns = TopicName.get(topicName).getNamespace();
            RetentionPolicies retentionPolicies = new RetentionPolicies(-1, 60);
            admin.namespaces().setRetentionAsync(ns, retentionPolicies).join();
            return null;
        };

        Function<String, Void> setTopicLevelPolicy60M = (topicName, context) -> {
            RetentionPolicies retentionPolicies = new RetentionPolicies(-1, 60);
            admin.topics().setRetentionAsync(topicName, retentionPolicies).join();
            return null;
        };

        return new Object[][]{
            {setNamespaceLevelPolicy60Min},
            {setNamespaceLevelPolicy60M},
            {setTopicLevelPolicy60Min},
            {setTopicLevelPolicy60M}
        };
    }

    @Test(dataProvider = "changeRetentionPolicyOps")
    public void testSetRetentionPolicies(Function<String, Void> setRetentionPolicy) throws Exception {
        final String ns = "public/default";
        final String tpName = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp");
        admin.topics().createNonPartitionedTopic(tpName);

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).enableBatching(false).topic(tpName).create();
        MessageIdAdv messageId = (MessageIdAdv) producer.send("1");
        long ledger1 = messageId.getLedgerId();

        // Switch ledger, and trigger one trim ledgers.
        // Verify: Since the retention policy is empty, the old ledger should be deleted.
        triggerLedgerSwitchAndSendOneMsg(producer);
        ManagedLedgerImpl ml1 = triggerTrimLedgers(tpName);
        assertEquals(ml1.getLedgersInfo().size(), 1);
        long ledger2 = ml1.getLedgersInfo().keySet().iterator().next();
        assertNotEquals(ledger2, ledger1);

        // Change retention.
        setRetentionPolicy.process(tpName, null);

        // Switch ledger, and trigger one trim ledgers.
        // Verify: Since the retention policy is set, the old ledger should not be deleted.
        triggerLedgerSwitchAndSendOneMsg(producer);
        ManagedLedgerImpl ml2 = triggerTrimLedgers(tpName);
        assertEquals(ml2.getLedgersInfo().size(), 2);
        Iterator<Long> ledgers2 = ml2.getLedgersInfo().keySet().iterator();
        assertEquals(ledgers2.next(), ledger2);
        long ledger3 = ledgers2.next();
        assertNotEquals(ledger3, ledger2);

        // cleanup.
        producer.close();
        admin.namespaces().removeRetention(ns);
        admin.topics().delete(tpName);
    }

    @Test(dataProvider = "changeRetentionPolicyOps")
    public void testRemoveRetentionPolicies(Function<String, Void> initializeRetentionPolicy) throws Exception {
        final String ns = "public/default";
        final String tpName = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp");
        admin.topics().createNonPartitionedTopic(tpName);

        // Init retention.
        initializeRetentionPolicy.process(tpName, null);

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).enableBatching(false).topic(tpName).create();
        MessageIdAdv messageId = (MessageIdAdv) producer.send("1");
        long ledger1 = messageId.getLedgerId();

        // Switch ledger, and trigger one trim ledgers.
        // Verify: Since the retention policy is set, the old ledger should not be deleted.
        triggerLedgerSwitchAndSendOneMsg(producer);
        ManagedLedgerImpl ml1 = triggerTrimLedgers(tpName);
        assertEquals(ml1.getLedgersInfo().size(), 2);
        Iterator<Long> ledgers1 = ml1.getLedgersInfo().keySet().iterator();
        assertEquals(ledgers1.next(), ledger1);
        long ledger2 = ledgers1.next();
        assertNotEquals(ledger2, ledger1);

        // Remove retention.
        admin.namespaces().removeRetention(ns);
        admin.topicPolicies(false).removeRetention(tpName);

        // Switch ledger, and trigger one trim ledgers.
        // Verify: Since the retention policy is empty, the old ledger should be deleted.
        triggerLedgerSwitchAndSendOneMsg(producer);
        ManagedLedgerImpl ml2 = triggerTrimLedgers(tpName);
        assertEquals(ml2.getLedgersInfo().size(), 1);
        long ledger3 = ml2.getLedgersInfo().keySet().iterator().next();
        assertNotEquals(ledger3, ledger1);
        assertNotEquals(ledger3, ledger2);

        // cleanup.
        producer.close();
        admin.topics().delete(tpName);
    }

    private ManagedLedgerImpl triggerTrimLedgers(String tpName) {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        CompletableFuture<Void> future = new CompletableFuture<>();
        ml.trimConsumedLedgersInBackground(future);
        future.join();
        return ml;
    }

    private PersistentTopic triggerLedgerSwitchAndSendOneMsg(Producer<String> producer)
            throws Exception {
        String tpName = producer.getTopic();
        PersistentTopic persistentTopic1 =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        LedgerHandle currentLedger1 =
                WhiteboxImpl.getInternalState(persistentTopic1.getManagedLedger(), "currentLedger");
        admin.topics().unload(tpName);

        producer.send("2");
        PersistentTopic persistentTopic2 =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        LedgerHandle currentLedger2 =
                WhiteboxImpl.getInternalState(persistentTopic2.getManagedLedger(), "currentLedger");
        assertNotEquals(currentLedger1.getId(), currentLedger2.getId());
        return persistentTopic2;
    }
}
