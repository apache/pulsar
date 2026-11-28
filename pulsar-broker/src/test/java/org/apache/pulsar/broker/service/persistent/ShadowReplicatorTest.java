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

import static org.apache.pulsar.broker.service.persistent.BrokerServicePersistInternalMethodInvoker.ensureNoBacklogByInflightTask;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.schema.Schemas;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-replication")
public class ShadowReplicatorTest extends BrokerTestBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setEnableShadowTopics(true);
        super.baseSetup();
        admin.tenants().createTenant("prop1",
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("prop1/ns-source");
        admin.namespaces().createNamespace("prop1/ns-shadow");
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testShadowReplication() throws Exception {
        String sourceTopicName = "persistent://prop1/ns-source/source-topic";
        String shadowTopicName = "persistent://prop1/ns-shadow/shadow-topic";
        String shadowTopicName2 = "persistent://prop1/ns-shadow/shadow-topic-2";

        admin.topics().createNonPartitionedTopic(sourceTopicName);
        admin.topics().createShadowTopic(shadowTopicName, sourceTopicName);
        admin.topics().createShadowTopic(shadowTopicName2, sourceTopicName);
        admin.topics().setShadowTopics(sourceTopicName, Lists.newArrayList(shadowTopicName, shadowTopicName2));

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopicName).create();

        @Cleanup
        Consumer<byte[]> shadowConsumer =
                pulsarClient.newConsumer().topic(shadowTopicName).subscriptionName("shadow-sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        @Cleanup
        Consumer<byte[]> shadowConsumer2 =
                pulsarClient.newConsumer().topic(shadowTopicName2).subscriptionName("shadow-sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        PersistentTopic sourceTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(sourceTopicName).get().get();

        Awaitility.await().untilAsserted(()->Assert.assertEquals(sourceTopic.getShadowReplicators().size(), 2));

        ShadowReplicator
                replicator = (ShadowReplicator) sourceTopic.getShadowReplicators().get(shadowTopicName);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(String.valueOf(replicator.getState()), "Started"));

        @Cleanup
        Consumer<byte[]> sourceConsumer =
                pulsarClient.newConsumer().topic(sourceTopicName).subscriptionName("source-sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        byte[] data = "test-shadow-topic".getBytes(StandardCharsets.UTF_8);
        MessageId sourceMessageId = producer.newMessage()
                .sequenceId(1)
                .key("K")
                .property("PK", "PV")
                .eventTime(123)
                .value(data)
                .send();

        Message<byte[]> sourceMessage = sourceConsumer.receive();
        Assert.assertEquals(sourceMessage.getMessageId(), sourceMessageId);

        //Wait until msg is replicated to shadow topic.
        Awaitility.await().until(() -> {
            replicator.msgOut.calculateRate();
            return replicator.msgOut.getCount() >= 1;
        });
        ensureNoBacklogByInflightTask(replicator);

        PersistentTopic shadowTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopicName).get().get();
        Assert.assertNotNull(shadowTopic);

        Message<byte[]> shadowMessage = shadowConsumer.receive(5, TimeUnit.SECONDS);

        Assert.assertEquals(shadowMessage.getData(), sourceMessage.getData());
        Assert.assertEquals(shadowMessage.getSequenceId(), sourceMessage.getSequenceId());
        Assert.assertEquals(shadowMessage.getEventTime(), sourceMessage.getEventTime());
        Assert.assertEquals(shadowMessage.getProperties(), sourceMessage.getProperties());
        Assert.assertEquals(shadowMessage.getKey(), sourceMessage.getKey());
        Assert.assertEquals(shadowMessage.getOrderingKey(), sourceMessage.getOrderingKey());
        Assert.assertEquals(shadowMessage.getSchemaVersion(), sourceMessage.getSchemaVersion());
        Assert.assertEquals(shadowMessage.getPublishTime(), sourceMessage.getPublishTime());
        Assert.assertEquals(shadowMessage.getBrokerPublishTime(), sourceMessage.getBrokerPublishTime());
        Assert.assertEquals(shadowMessage.getIndex(), sourceMessage.getIndex());

        Assert.assertEquals(replicator.stats.getBytesOutCount(), 0);

        Assert.assertEquals(shadowMessage.getMessageId(), sourceMessage.getMessageId());
    }

    @Test
    public void testShadowReplicatorReleasesSourceEntryBuffer() throws Exception {
        String sourceTopicName = BrokerTestUtil.newUniqueName("persistent://prop1/ns-source/source-topic");
        String shadowTopicName = BrokerTestUtil.newUniqueName("persistent://prop1/ns-shadow/shadow-topic");

        admin.topics().createNonPartitionedTopic(sourceTopicName);
        admin.topics().createShadowTopic(shadowTopicName, sourceTopicName);
        admin.topics().setShadowTopics(sourceTopicName, Lists.newArrayList(shadowTopicName));

        PersistentTopic sourceTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(sourceTopicName).get().get();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(sourceTopic.getShadowReplicators().size(), 1));
        ShadowReplicator replicator = (ShadowReplicator) sourceTopic.getShadowReplicators().get(shadowTopicName);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(String.valueOf(replicator.getState()), "Started"));

        Entry entry = createEntry(1, 0, "ref-count-check", 1);
        ByteBuf entryBuffer = entry.getDataBuffer();
        Assert.assertEquals(entryBuffer.refCnt(), 1);

        List<Entry> entries = Lists.newArrayList(entry);
        PersistentReplicator.InFlightTask inFlightTask =
                new PersistentReplicator.InFlightTask(
                        entry.getPosition(), entries.size(), replicator.getReplicatorId());
        inFlightTask.setEntries(entries);
        Assert.assertTrue(replicator.replicateEntries(entries, inFlightTask));
        inFlightTask.setSubmissionComplete(true);

        Awaitility.await().untilAsserted(() -> {
            Assert.assertTrue(inFlightTask.isDone());
            Assert.assertEquals(entryBuffer.refCnt(), 0);
        });
    }

    private static PersistentReplicator getAnyShadowReplicator(TopicName topicName, PulsarService pulsar) {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName.toString(), false).join().get();
        Awaitility.await().until(() -> !persistentTopic.getShadowReplicators().isEmpty());
        return (PersistentReplicator) persistentTopic.getShadowReplicators().values().iterator().next();
    }

    private static void waitReplicateFinish(TopicName topicName, PulsarAdmin admin){
        Awaitility.await().untilAsserted(() -> {
            for (Map.Entry<String, ? extends ReplicatorStats> subStats :
                    admin.topics().getStats(topicName.toString(), true, false, false).getReplication().entrySet()){
                assertTrue(subStats.getValue().getReplicationBacklog() == 0, "replication task finished");
            }
        });
    }

    @Test
    public void testCounterOfPendingMessagesCorrect() throws Exception {
        TopicName sourceTopicName = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://prop1/ns-source/source-topic"));
        TopicName shadowTopicName = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://prop1/ns-shadow/shadow-topic"));

        admin.topics().createNonPartitionedTopic(sourceTopicName.toString());
        admin.topics().createShadowTopic(shadowTopicName.toString(), sourceTopicName.toString());
        admin.topics().setShadowTopics(sourceTopicName.toString(), Lists.newArrayList(shadowTopicName.toString()));

        // Init replicator and send many messages.
        final String subName = "my-sub";
        @Cleanup
        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(sourceTopicName.toString())
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscribe();
        @Cleanup
        Producer<Schemas.PersonOne> producer = pulsarClient.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(sourceTopicName.toString())
                .enableBatching(false)
                .create();
        for (int i = 0; i < 20; i++) {
            producer.send(new Schemas.PersonOne(i));
        }

        // Verify "inflight replication tasks" are correct.
        PersistentReplicator replicator = getAnyShadowReplicator(sourceTopicName, pulsar);
        waitReplicateFinish(sourceTopicName, admin);
        ensureNoBacklogByInflightTask(replicator);
    }

    /**
     * Verify the message TTL check of a topic expires the messages of a shadow replicator that is behind every
     * subscription. The cursor of a shadow replicator is non-durable.
     */
    @Test
    public void testTopicExpiryCheckExpiresShadowReplicatorBehindSubscriptions() throws Exception {
        String sourceTopicName = BrokerTestUtil.newUniqueName("persistent://prop1/ns-source/source-topic");
        String shadowTopicName = BrokerTestUtil.newUniqueName("persistent://prop1/ns-shadow/shadow-topic");
        String subscriptionName = "source-sub";

        admin.topics().createNonPartitionedTopic(sourceTopicName);
        admin.topics().createSubscription(sourceTopicName, subscriptionName, MessageId.earliest);
        admin.topics().createShadowTopic(shadowTopicName, sourceTopicName);
        admin.topics().setShadowTopics(sourceTopicName, Lists.newArrayList(shadowTopicName));
        PersistentTopic sourceTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(sourceTopicName).get().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) sourceTopic.getManagedLedger();
        PersistentReplicator replicator = getAnyShadowReplicator(TopicName.get(sourceTopicName), pulsar);
        Awaitility.await().untilAsserted(() -> assertThat(String.valueOf(replicator.getState()))
                .as("state of the shadow replicator").isEqualTo("Started"));
        ManagedCursor shadowCursor = replicator.getCursor();
        assertThat(shadowCursor.isDurable()).as("the cursor of the shadow replicator is durable").isFalse();

        // The shadow replicator replicates 1 message and then waits for its dispatch rate, so it stays behind
        admin.topicPolicies().setReplicatorDispatchRate(sourceTopicName, DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1).ratePeriodInSecond(3600).build());
        Awaitility.await().untilAsserted(() -> {
            assertThat(replicator.getRateLimiter()).as("rate limiter of the shadow replicator").isPresent();
            assertThat(replicator.getRateLimiter().orElseThrow().getDispatchRateOnMsg())
                    .as("dispatch rate of the shadow replicator").isEqualTo(1);
        });
        admin.topicPolicies().setMessageTTL(sourceTopicName, 1);
        Awaitility.await().untilAsserted(() ->
                assertThat(sourceTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get())
                        .as("message TTL").isEqualTo(1));
        // The TTL update also scheduled an expiry check, which must not run after the messages are published
        pulsar.getBrokerService().getMessageExpiryMonitor().submit(() -> { }).get();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopicName)
                .enableBatching(false).create();
        MessageIdImpl firstMessageId = (MessageIdImpl) producer.send("0");
        Position firstPosition = PositionFactory.create(firstMessageId.getLedgerId(), firstMessageId.getEntryId());
        Awaitility.await().untilAsserted(() -> assertThat(shadowCursor.getMarkDeletedPosition())
                .as("mark-delete position of the shadow replicator after the first message")
                .isEqualTo(firstPosition));
        for (int i = 1; i < 5; i++) {
            producer.send("" + i);
        }
        long lastPublishedAt = System.currentTimeMillis();
        Position lastPosition = ml.getLastConfirmedEntry();
        Awaitility.await().until(() -> System.currentTimeMillis() - lastPublishedAt > TimeUnit.SECONDS.toMillis(1));

        admin.topics().skipAllMessages(sourceTopicName, subscriptionName);
        Awaitility.await().untilAsserted(() ->
                assertThat(ml.getCursors().getCursorWithOldestPosition().getPosition())
                        .as("oldest mark-delete position that a durable cursor has persisted")
                        .isEqualTo(lastPosition));
        assertThat(shadowCursor.getMarkDeletedPosition())
                .as("mark-delete position of the shadow replicator before the check").isLessThan(lastPosition);

        sourceTopic.checkMessageExpiry();

        Awaitility.await().untilAsserted(() -> assertThat(shadowCursor.getMarkDeletedPosition())
                .as("mark-delete position of the shadow replicator after the check").isEqualTo(lastPosition));
    }

    private Entry createEntry(long ledgerId, long entryId, String message, long sequenceId) {
        ByteBuf headersAndPayload = createMessage(message, sequenceId);
        Entry entry = EntryImpl.create(ledgerId, entryId, headersAndPayload);
        headersAndPayload.release();
        return entry;
    }

    private ByteBuf createMessage(String message, long sequenceId) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPublishTime(System.currentTimeMillis());
        ByteBuf payload = Unpooled.copiedBuffer(message.getBytes(StandardCharsets.UTF_8));
        ByteBuf headersAndPayload = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, payload);
        payload.release();
        return headersAndPayload;
    }

}
