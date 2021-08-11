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

package org.apache.pulsar.client.impl;


import com.google.common.collect.Sets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CursorClient;
import org.apache.pulsar.client.api.CursorData;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.cursor.CursorDataImpl;
import org.apache.pulsar.common.api.proto.CursorPosition;
import org.apache.pulsar.common.api.proto.MessageRange;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class CursorClientTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/cursor-client-test");
    }

    @Test
    public void testGetCursor() throws ExecutionException, InterruptedException, PulsarClientException {
        String topic = "persistent://my-property/cursor-client-test/testGetCursor";
        String subscription = "sub-testGetCursor";
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionName(subscription)
                .topic(topic)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(0)
                .subscribeAsync()
                .get();


        log.info("testGetCursor start");
        Producer<byte[]> producer = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(topic)
                .createAsync()
                .get();
        MessageId[] msgIds = new MessageId[3];
        for (int i = 0; i < 3; i++) {
            MessageId msgId = producer.send(("data" + i).getBytes());
            msgIds[i] = msgId;
            log.info("{}, msgId={}", i, msgId);
        }


        Message<byte[]> msg = consumer.receive();
        Assert.assertEquals(msg.getMessageId(), msgIds[0]);
        consumer.acknowledge(msg.getMessageId());

        log.info("getCursorAsync start");
        CursorClient cursorClient = pulsarClient.newCursorClient().topic(topic).create();
        CursorData cursorData = cursorClient.getCursorAsync(subscription).get();
        Assert.assertTrue(cursorData instanceof CursorDataImpl);
        Assert.assertTrue(msg.getMessageId() instanceof MessageIdImpl);
        MessageIdImpl messageId = (MessageIdImpl) msg.getMessageId();
        CursorPosition position = ((CursorDataImpl) cursorData).getPosition();
        Assert.assertEquals(position.getLedgerId(), messageId.getLedgerId());
        Assert.assertEquals(position.getEntryId(), messageId.getEntryId());
        log.info("testGetCursor end");

        producer.close();
        consumer.close();
        cursorClient.close();
    }

    @Test
    public void testCreateCursor() throws Exception {
        String topic = "persistent://my-property/cursor-client-test/testCreateCursor";
        String subscription = "sub-testCreateCursor";

        Producer<byte[]> producer = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(topic)
                .createAsync()
                .get();
        MessageIdImpl msgId1 = (MessageIdImpl) producer.send("data1".getBytes());
        MessageIdImpl msgId2 = (MessageIdImpl) producer.send("data2".getBytes());
        MessageIdImpl msgId3 = (MessageIdImpl) producer.send("data3".getBytes());


        CursorClient cursorClient = pulsarClient.newCursorClient().topic(topic).create();

        CursorPosition position = new CursorPosition();
        position.setLedgerId(PositionImpl.latest.getLedgerId());
        position.setEntryId(PositionImpl.latest.getLedgerId());
        position.addProperty().setName("P1").setValue(1159);
        CursorData cursorData = new CursorDataImpl(position);
        CursorData newData = cursorClient.createCursorAsync(subscription, cursorData).get();
        log.info("newCursorData:{}", newData);

        PersistentTopicInternalStats topicStats =
                admin.topics().getInternalStats(topic);

        ManagedLedgerInternalStats.CursorStats cursorStat =
                topicStats.cursors.get(subscription);
        Assert.assertNotNull(cursorStat);
        log.info("sub stat:{}", cursorStat);

        Assert.assertEquals(cursorStat.markDeletePosition, msgId3.getLedgerId() + ":" + msgId3.getEntryId());
        Assert.assertEquals((long) cursorStat.properties.get("P1"), 1159);

        cursorClient.close();
        producer.close();
    }

    @Test
    public void testDeleteCursor() throws Exception {
        String topic = "persistent://my-property/cursor-client-test/testDeleteCursor";
        String subscription = "sub-testDeleteCursor";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionName(subscription)
                .topic(topic)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(0)
                .subscribeAsync()
                .get();

        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(topic)
                .createAsync()
                .get();

        MessageIdImpl msgId1 = (MessageIdImpl) producer.send("data1".getBytes());
        log.info("msg id = {}", msgId1);

        Assert.assertNotNull(admin.topics().getInternalStats(topic).cursors.get(subscription));
        CursorClient cursorClient = pulsarClient.newCursorClient().topic(topic).create();

        cursorClient.deleteCursorAsync(subscription).get();

        Assert.assertNull(admin.topics().getInternalStats(topic).cursors.get(subscription));

        cursorClient.close();
        producer.close();
    }

    @Test
    public void testUpdateCursor() throws Exception {
        String topic = "persistent://my-property/cursor-client-test/testUpdateCursor";
        String subscription = "sub-testUpdateCursor";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionName(subscription)
                .topic(topic)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(0)
                .subscribeAsync()
                .get();


        Producer<byte[]> producer = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(topic)
                .createAsync()
                .get();

        MessageIdImpl msgId1 = (MessageIdImpl) producer.send("data1".getBytes());
        MessageIdImpl msgId2 = (MessageIdImpl) producer.send("data2".getBytes());
        MessageIdImpl msgId3 = (MessageIdImpl) producer.send("data3".getBytes());
        MessageIdImpl msgId4 = (MessageIdImpl) producer.send("data4".getBytes());
        MessageIdImpl msgId5 = (MessageIdImpl) producer.send("data5".getBytes());
        log.info("msg id = {}", msgId1);

        Message<byte[]> msg = consumer.receive();
        Assert.assertEquals(msg.getMessageId(), msgId1);
        consumer.acknowledge(msg.getMessageId());
        consumer.close();

        //current md pos is msgId1
        Assert.assertEquals(admin.topics().getInternalStats(topic).cursors.get(subscription).markDeletePosition,
                msgId1.getLedgerId() + ":" + msgId1.getEntryId());


        CursorClient cursorClient = pulsarClient.newCursorClient().topic(topic).create();

        //markdelete at msgId2, individual delete msgId4
        CursorPosition position = new CursorPosition();
        position.setLedgerId(msgId2.getLedgerId());
        position.setEntryId(msgId2.getEntryId());
        position.addProperty().setName("testUpdateCursor").setValue(1208);
        MessageRange individualDeletedMessage = position.addIndividualDeletedMessage();
        individualDeletedMessage.setLowerEndpoint()
                .setLedgerId(msgId3.getLedgerId())
                .setEntryId(msgId3.getEntryId());
        individualDeletedMessage.setUpperEndpoint()
                .setLedgerId(msgId4.getLedgerId())
                .setEntryId(msgId4.getEntryId());
        CursorData cursorData = new CursorDataImpl(position);
        cursorClient.updateCursorAsync(subscription, cursorData).get();


        ManagedLedgerInternalStats.CursorStats cursorStats =
                admin.topics().getInternalStats(topic).cursors.get(subscription);
        Assert.assertNotNull(cursorStats);
        Assert.assertEquals(cursorStats.markDeletePosition, msgId2.getLedgerId() + ":" + msgId2.getEntryId());
        Assert.assertEquals(cursorStats.individuallyDeletedMessages,
                String.format("[(%d:%d..%d:%d]]", msgId3.ledgerId, msgId3.entryId, msgId4.ledgerId, msgId4.entryId));

        Assert.assertEquals((long) cursorStats.properties.get("testUpdateCursor"), 1208);

        consumer = pulsarClient.newConsumer()
                .subscriptionName(subscription)
                .topic(topic)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(0)
                .subscribeAsync()
                .get();

        //we should receive msgId3 and msgId5
        Message<byte[]> msg1 = consumer.receive();
        log.info("Get Msg 1:{}", msg1.getMessageId());
        MessageIdImpl id = (MessageIdImpl) msg1.getMessageId();
        Assert.assertEquals(id.ledgerId, msgId3.ledgerId);
        Assert.assertEquals(id.entryId, msgId3.entryId);

        Message<byte[]> msg2 = consumer.receive();
        log.info("Get Msg 2:{}", msg2.getMessageId());
        id = (MessageIdImpl) msg2.getMessageId();
        Assert.assertEquals(id.ledgerId, msgId5.ledgerId);
        Assert.assertEquals(id.entryId, msgId5.entryId);

        cursorClient.close();
        producer.close();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }
}
