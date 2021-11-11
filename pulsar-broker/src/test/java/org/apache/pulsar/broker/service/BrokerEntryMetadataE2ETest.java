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

import static org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;

import java.time.Duration;
import java.util.List;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.assertj.core.util.Sets;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test for the broker entry metadata.
 */
@Test(groups = "broker")
public class BrokerEntryMetadataE2ETest extends BrokerTestBase {


    @DataProvider(name = "subscriptionTypes")
    public static Object[] subscriptionTypes() {
        return new Object[] {
                SubscriptionType.Exclusive,
                SubscriptionType.Failover,
                SubscriptionType.Shared,
                SubscriptionType.Key_Shared
        };
    }

    @BeforeClass
    protected void setup() throws Exception {
        conf.setBrokerEntryMetadataInterceptors(Sets.newTreeSet(
                "org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor",
                "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor"
                ));
        baseSetup();
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testProduceAndConsume(SubscriptionType subType) throws Exception {
        final String topic = newTopicName();
        final int messages = 10;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionType(subType)
                .subscriptionName("my-sub")
                .subscribe();

        for (int i = 0; i < messages; i++) {
            producer.send(String.valueOf(i).getBytes());
        }

        int receives = 0;
        for (int i = 0; i < messages; i++) {
            Message<byte[]> received = consumer.receive();
            ++ receives;
            Assert.assertEquals(i, Integer.valueOf(new String(received.getValue())).intValue());
        }

        Assert.assertEquals(messages, receives);
    }

    @Test(timeOut = 20000)
    public void testPeekMessage() throws Exception {
        final String topic = newTopicName();
        final String subscription = "my-sub";
        final long eventTime= 200;
        final long deliverAtTime = 300;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        long sendTime = System.currentTimeMillis();
        producer.newMessage()
                .eventTime(eventTime)
                .deliverAt(deliverAtTime)
                .value("hello".getBytes())
                .send();

        admin.topics().createSubscription(topic, subscription, MessageId.earliest);
        final List<Message<byte[]>> messages = admin.topics().peekMessages(topic, subscription, 1);
        Assert.assertEquals(messages.size(), 1);
        MessageImpl message = (MessageImpl) messages.get(0);
        Assert.assertEquals(message.getData(), "hello".getBytes());
        Assert.assertEquals(message.getEventTime(), eventTime);
        Assert.assertEquals(message.getDeliverAtTime(), deliverAtTime);
        Assert.assertTrue(message.getPublishTime() >= sendTime);

        BrokerEntryMetadata entryMetadata = message.getBrokerEntryMetadata();
        Assert.assertEquals(entryMetadata.getIndex(), 0);
        Assert.assertTrue(entryMetadata.getBrokerTimestamp() >= sendTime);
    }

    @Test(timeOut = 20000)
    public void testGetMessageById() throws Exception {
        final String topic = newTopicName();
        final String subscription = "my-sub";
        final long eventTime= 200;
        final long deliverAtTime = 300;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        long sendTime = System.currentTimeMillis();
        MessageIdImpl messageId = (MessageIdImpl) producer.newMessage()
                .eventTime(eventTime)
                .deliverAt(deliverAtTime)
                .value("hello".getBytes())
                .send();

        admin.topics().createSubscription(topic, subscription, MessageId.earliest);
        MessageImpl message = (MessageImpl) admin.topics()
                .getMessageById(topic, messageId.getLedgerId(), messageId.getEntryId());
        Assert.assertEquals(message.getData(), "hello".getBytes());
        Assert.assertEquals(message.getEventTime(), eventTime);
        Assert.assertEquals(message.getDeliverAtTime(), deliverAtTime);
        Assert.assertTrue(message.getPublishTime() >= sendTime);

        BrokerEntryMetadata entryMetadata = message.getBrokerEntryMetadata();
        Assert.assertEquals(entryMetadata.getIndex(), 0);
        Assert.assertTrue(entryMetadata.getBrokerTimestamp() >= sendTime);
    }


    @Test(timeOut = 20000)
    public void testExamineMessage() throws Exception {
        final String topic = newTopicName();
        final String subscription = "my-sub";
        final long eventTime= 200;
        final long deliverAtTime = 300;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        long sendTime = System.currentTimeMillis();
        producer.newMessage()
                .eventTime(eventTime)
                .deliverAt(deliverAtTime)
                .value("hello".getBytes())
                .send();

        admin.topics().createSubscription(topic, subscription, MessageId.earliest);
        MessageImpl message =
                (MessageImpl) admin.topics().examineMessage(topic, "earliest", 1);
        Assert.assertEquals(message.getData(), "hello".getBytes());
        Assert.assertEquals(message.getEventTime(), eventTime);
        Assert.assertEquals(message.getDeliverAtTime(), deliverAtTime);
        Assert.assertTrue(message.getPublishTime() >= sendTime);

        BrokerEntryMetadata entryMetadata = message.getBrokerEntryMetadata();
        Assert.assertEquals(entryMetadata.getIndex(), 0);
        Assert.assertTrue(entryMetadata.getBrokerTimestamp() >= sendTime);
    }

    @Test(timeOut = 20000)
    public void testGetLastMessageId() throws Exception {
        final String topic = "persistent://prop/ns-abc/topic-test";
        final String subscription = "my-sub";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
        producer.newMessage().value("hello".getBytes()).send();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName(subscription)
                .subscribe();
        consumer.getLastMessageId();
    }

    @Test
    public void testManagedLedgerTotalSize() throws Exception {
        final String topic = newTopicName();
        final int messages = 10;

        admin.topics().createNonPartitionedTopic(topic);
        admin.lookups().lookupTopic(topic);
        final ManagedLedgerImpl managedLedger = pulsar.getBrokerService().getTopicIfExists(topic).get()
                .map(topicObject -> (ManagedLedgerImpl) ((PersistentTopic) topicObject).getManagedLedger())
                .orElse(null);
        Assert.assertNotNull(managedLedger);
        final ManagedCursor cursor = managedLedger.openCursor("cursor"); // prevent ledgers being removed

        @Cleanup
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();
        for (int i = 0; i < messages; i++) {
            producer.send("msg-" + i);
        }

        Assert.assertTrue(managedLedger.getTotalSize() > 0);

        managedLedger.getConfig().setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        managedLedger.getConfig().setMaxEntriesPerLedger(1);
        managedLedger.rollCurrentLedgerIfFull();

        Awaitility.await().atMost(Duration.ofSeconds(3))
                .until(() -> managedLedger.getLedgersInfo().size() > 1);

        final List<LedgerInfo> ledgerInfoList = managedLedger.getLedgersInfoAsList();
        Assert.assertEquals(ledgerInfoList.size(), 2);
        Assert.assertEquals(ledgerInfoList.get(0).getSize(), managedLedger.getTotalSize());

        cursor.close();
    }
}
