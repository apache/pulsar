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


import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ShadowManagedLedgerImpl;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ShadowTopicTest extends BrokerTestBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    private String newShadowSourceTopicName() {
        return "persistent://" + newTopicName();
    }

    @Test
    public void testNonPartitionedShadowTopicSetup() throws Exception {
        String sourceTopic = newShadowSourceTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        //1. test shadow topic setting in topic creation.
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        PersistentTopic brokerShadowTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopic).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopic);
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);

        //2. test shadow topic could be properly loaded after unload.
        admin.namespaces().unload("prop/ns-abc");
        Assert.assertTrue(pulsar.getBrokerService().getTopicReference(shadowTopic).isEmpty());
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);
        brokerShadowTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopic).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopic);
    }

    @Test
    public void testPartitionedShadowTopicSetup() throws Exception {
        String sourceTopic = newShadowSourceTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        String sourceTopicPartition = TopicName.get(sourceTopic).getPartition(0).toString();
        String shadowTopicPartition = TopicName.get(shadowTopic).getPartition(0).toString();

        //1. test shadow topic setting in topic creation.
        admin.topics().createPartitionedTopic(sourceTopic, 2);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        pulsarClient.newProducer().topic(shadowTopic).create().close();//trigger loading partitions.

        PersistentTopic brokerShadowTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(shadowTopicPartition).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopicPartition);
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);

        //2. test shadow topic could be properly loaded after unload.
        admin.namespaces().unload("prop/ns-abc");
        Assert.assertTrue(pulsar.getBrokerService().getTopicReference(shadowTopic).isEmpty());

        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);
        brokerShadowTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopicPartition).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopicPartition);
    }

    @Test
    public void testShadowTopicNotWritable() throws Exception {
        String sourceTopic = newShadowSourceTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(shadowTopic).create();
        Assert.expectThrows(PulsarClientException.NotAllowedException.class, ()-> producer.send(new byte[]{1,2,3}));
    }

    private void awaitUntilShadowReplicatorReady(String sourceTopic, String shadowTopic) {
        Awaitility.await().untilAsserted(()->{
            PersistentTopic sourcePersistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(sourceTopic).get().get();
            ShadowReplicator
                    replicator = (ShadowReplicator) sourcePersistentTopic.getShadowReplicators().get(shadowTopic);
            Assert.assertNotNull(replicator);
            Assert.assertEquals(String.valueOf(replicator.getState()), "Started");
        });
    }
    @Test
    public void testShadowTopicConsuming() throws Exception {
        String sourceTopic = newShadowSourceTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().setShadowTopics(sourceTopic, Lists.newArrayList(shadowTopic));
        awaitUntilShadowReplicatorReady(sourceTopic, shadowTopic);

        @Cleanup Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopic).create();
        @Cleanup Consumer<byte[]> consumer =
                pulsarClient.newConsumer().topic(shadowTopic).subscriptionName("sub").subscribe();
        byte[] content = "Hello Shadow Topic".getBytes(StandardCharsets.UTF_8);
        MessageId id = producer.send(content);
        log.info("msg send to source topic, id={}", id);
        Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertEquals(msg.getMessageId(), id);
        Assert.assertEquals(msg.getValue(), content);
    }


    @Test
    public void testShadowTopicConsumingWithStringSchema() throws Exception {
        String sourceTopic = newShadowSourceTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().setShadowTopics(sourceTopic, Lists.newArrayList(shadowTopic));
        awaitUntilShadowReplicatorReady(sourceTopic, shadowTopic);

        @Cleanup Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        @Cleanup Consumer<String> consumer =
                pulsarClient.newConsumer(Schema.STRING).topic(shadowTopic).subscriptionName("sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        String content = "Hello Shadow Topic";
        MessageId id = producer.send(content);
        Message<String> msg = consumer.receive();
        Assert.assertEquals(msg.getMessageId(), id);
        Assert.assertEquals(msg.getValue(), content);

        for (int i = 0; i < 10; i++) {
            producer.send(content + i);
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(consumer.receive().getValue(), content + i);
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    private static class Point {
        int x;
        int y;
    }
    @Test
    public void testShadowTopicConsumingWithJsonSchema() throws Exception {
        String sourceTopic = newShadowSourceTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().setShadowTopics(sourceTopic, Lists.newArrayList(shadowTopic));
        awaitUntilShadowReplicatorReady(sourceTopic, shadowTopic);


        @Cleanup Producer<Point> producer =
                pulsarClient.newProducer(Schema.JSON(Point.class)).topic(sourceTopic).create();
        @Cleanup Consumer<Point> consumer =
                pulsarClient.newConsumer(Schema.JSON(Point.class)).topic(shadowTopic).subscriptionName("sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        Point content = new Point(1, 2);
        MessageId id = producer.send(content);
        Message<Point> msg = consumer.receive();
        Assert.assertEquals(msg.getMessageId(), id);
        Assert.assertEquals(msg.getValue(), content);
    }

    @Test
    public void testConsumeShadowMessageWithoutCache() throws Exception {
        String sourceTopic = newShadowSourceTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        @Cleanup Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        String content = "Hello Shadow Topic";
        MessageId id = producer.send(content);
        for (int i = 0; i < 10; i++) {
            producer.send(content + i);
        }

        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        // disable shadow replicator
        // admin.topics().setShadowTopics(sourceTopic, Lists.newArrayList(shadowTopic));
        @Cleanup Consumer<String> consumer =
                pulsarClient.newConsumer(Schema.STRING).topic(shadowTopic).subscriptionName("sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe();

        Message<String> msg = consumer.receive();
        Assert.assertEquals(msg.getMessageId(), id);
        Assert.assertEquals(msg.getValue(), content);

        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(consumer.receive().getValue(), content + i);
        }
    }
}
