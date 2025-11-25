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
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class CustomizedManagedLedgerTest extends ProducerConsumerBase {

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

    @EqualsAndHashCode.Include
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setManagedLedgerStorageClassName(
            CustomizedManagedLedgerStorageForTest.class.getName());
        conf.setManagedLedgerCacheSizeMB(10);
        conf.setManagedLedgerCacheEvictionFrequency(60_000);
        conf.setManagedLedgerCacheEvictionTimeThresholdMillis(60_000);
    }

    private enum SubscribeTopicType {
        MULTI_PARTITIONED_TOPIC,
        REGEX_TOPIC;
    }

    @DataProvider(name = "subscribeTopicTypes")
    public Object[][] subTopicTypes() {
        return new Object[][]{
                {SubscribeTopicType.MULTI_PARTITIONED_TOPIC},
                {SubscribeTopicType.REGEX_TOPIC}
        };
    }

    @Test
    public void testNoMemoryLeakWhenExpireMessages() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";

        // Create topic with "CustomizedManagedLedger", which will
        // call "PersistentTopic.checkMessageExpiryWithoutSharedPosition" when expiring messages.
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        int messageTTLInSecond = 1;
        ManagedLedger ml = pulsar.getDefaultManagedLedgerFactory()
            .open(TopicName.get(topic).getPersistenceNamingEncoding(), config);
        CustomizedManagedLedgerStorageForTest.CustomizedManagedLedger customizedManagedLedger =
                (CustomizedManagedLedgerStorageForTest.CustomizedManagedLedger) ml;
        assertTrue(ml instanceof CustomizedManagedLedgerStorageForTest.CustomizedManagedLedger);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName(subscription).subscribe();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        admin.topicPolicies().setMessageTTL(topic, messageTTLInSecond);
        for (int i = 0; i < 50; i++) {
            producer.send("msg-" + i);
        }

        // Trigger a messages expiring.
        ManagedCursor managedCursor = persistentTopic.getSubscription(subscription).getCursor();
        assertTrue(managedCursor instanceof CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator);
        CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator cursorDecorator =
                (CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator) managedCursor;
        Thread.sleep(messageTTLInSecond * 3);
        Awaitility.await().untilAsserted(() -> {
            persistentTopic.checkMessageExpiry();
            assertEquals(cursorDecorator.getNumberOfEntriesInBacklog(true), 0);
        });

        // Verify: no memory leak.
        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            assertFalse(cursorDecorator.entryReleasedStatusMap.isEmpty());
            for (Boolean released : cursorDecorator.entryReleasedStatusMap.values()) {
                assertTrue(released);
            }
        });

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topic);
    }
}
