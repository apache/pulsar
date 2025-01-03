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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.vertx.core.impl.ConcurrentHashSet;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.MessageDeduplication;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandGetOrCreateSchemaResponse;
import org.apache.pulsar.common.api.proto.CommandGetSchemaResponse;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class OneWayReplicatorDeduplicationTest extends OneWayReplicatorTestBase {

    static final ObjectMapper JACKSON = new ObjectMapper();

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
        waitInternalClientCreated();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Override
    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
        // For check whether deduplication snapshot has done.
        config.setBrokerDeduplicationEntriesInterval(10);
        config.setReplicationStartAt("earliest");
        // To cover more cases, write more than one ledger.
        config.setManagedLedgerMaxEntriesPerLedger(100);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        config.setManagedLedgerMaxLedgerRolloverTimeMinutes(1);
    }

    protected void waitReplicatorStopped(String topicName) {
        Awaitility.await().untilAsserted(() -> {
            Optional<Topic> topicOptional2 = pulsar2.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional2.isPresent());
            PersistentTopic persistentTopic2 = (PersistentTopic) topicOptional2.get();
            assertTrue(persistentTopic2.getProducers().isEmpty());
            Optional<Topic> topicOptional1 = pulsar2.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional1.isPresent());
            PersistentTopic persistentTopic1 = (PersistentTopic) topicOptional2.get();
            assertTrue(persistentTopic1.getReplicators().isEmpty()
                    || !persistentTopic1.getReplicators().get(cluster2).isConnected());
        });
    }

    protected void waitInternalClientCreated() throws Exception {
        // Wait for the internal client created.
        final String topicNameTriggerInternalClientCreate =
                BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicNameTriggerInternalClientCreate);
        waitReplicatorStarted(topicNameTriggerInternalClientCreate);
        cleanupTopics(() -> {
            admin1.topics().delete(topicNameTriggerInternalClientCreate);
            admin2.topics().delete(topicNameTriggerInternalClientCreate);
        });
    }

    protected Runnable injectReplicatorClientCnx(
            InjectedClientCnxClientBuilder.ClientCnxFactory clientCnxFactory) throws Exception {
        String cluster2 = pulsar2.getConfig().getClusterName();
        BrokerService brokerService = pulsar1.getBrokerService();
        ClientBuilderImpl clientBuilder2 = (ClientBuilderImpl) PulsarClient.builder().serviceUrl(url2.toString());

        // Inject spy client.
        final var replicationClients = brokerService.getReplicationClients();
        PulsarClientImpl internalClient = (PulsarClientImpl) replicationClients.get(cluster2);
        PulsarClientImpl injectedClient = InjectedClientCnxClientBuilder.create(clientBuilder2, clientCnxFactory);
        assertTrue(replicationClients.remove(cluster2, internalClient));
        assertNull(replicationClients.putIfAbsent(cluster2, injectedClient));

        // Return a cleanup injection task;
        return () -> {
            assertTrue(replicationClients.remove(cluster2, injectedClient));
            assertNull(replicationClients.putIfAbsent(cluster2, internalClient));
            injectedClient.closeAsync();
        };
    }

    @DataProvider(name = "deduplicationArgs")
    public Object[][] deduplicationArgs() {
        return new Object[][] {
            {true/* inject repeated publishing*/, 1/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {true/* inject repeated publishing*/, 2/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {true/* inject repeated publishing*/, 3/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {true/* inject repeated publishing*/, 4/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {true/* inject repeated publishing*/, 5/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {true/* inject repeated publishing*/, 10/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            // ===== multi schema
            {true/* inject repeated publishing*/, 1/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 2/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 3/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 4/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 5/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 10/* repeated messages window */,
                    true /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            // ===== Compatability "source-cluster: old, target-cluster: new".
            {false/* inject repeated publishing*/, 0/* repeated messages window */,
                    false /* supportsReplDedupByLidAndEid */, false/* multi schemas */},
            {false/* inject repeated publishing*/, 0/* repeated messages window */,
                    false /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
            {true/* inject repeated publishing*/, 3/* repeated messages window */,
                    false /* supportsReplDedupByLidAndEid */, true/* multi schemas */},
        };
    }

    // TODO
    //  - Review the code to confirm that multi source-brokers can work when the source topic switch.
    @Test(timeOut = 360 * 1000, dataProvider = "deduplicationArgs")
    public void testDeduplication(final boolean injectRepeatedPublish, final int repeatedMessagesWindow,
                                  final boolean supportsReplDedupByLidAndEid, boolean multiSchemas) throws Exception {
        // 0. Inject a mechanism that duplicate all Send-Command for the replicator.
        final List<ByteBufPair> duplicatedMsgs = new ArrayList<>();
        Runnable taskToClearInjection = injectReplicatorClientCnx(
            (conf, eventLoopGroup) -> new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup) {

                @Override
                protected ByteBuf newConnectCommand() throws Exception {
                    if (supportsReplDedupByLidAndEid) {
                        return super.newConnectCommand();
                    }
                    authenticationDataProvider = authentication.getAuthData(remoteHostName);
                    AuthData authData = authenticationDataProvider.authenticate(AuthData.INIT_AUTH_DATA);
                    BaseCommand cmd = Commands.newConnectWithoutSerialize(authentication.getAuthMethodName(), authData,
                            this.protocolVersion, clientVersion, proxyToTargetBrokerAddress, null, null, null, null);
                    cmd.getConnect().getFeatureFlags().setSupportsReplDedupByLidAndEid(false);
                    return Commands.serializeWithSize(cmd);
                }

                @Override
                public boolean isBrokerSupportsReplDedupByLidAndEid() {
                    return supportsReplDedupByLidAndEid;
                }

                @Override
                public ChannelHandlerContext ctx() {
                    if (!injectRepeatedPublish) {
                        return super.ctx();
                    }
                    final ChannelHandlerContext originalCtx = super.ctx;
                    ChannelHandlerContext spyContext = spy(originalCtx);
                    Answer injectedAnswer = invocation -> {
                        // Do not repeat the messages re-sending, and clear the previous cached messages when
                        // calling re-sending, to avoid publishing outs of order.
                        for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
                            if (stackTraceElement.toString().contains("recoverProcessOpSendMsgFrom")
                                    || stackTraceElement.toString().contains("resendMessages")) {
                                duplicatedMsgs.clear();
                                return invocation.callRealMethod();
                            }
                        }

                        Object data = invocation.getArguments()[0];
                        if (true && !(data instanceof ByteBufPair)) {
                            return invocation.callRealMethod();
                        }
                        // Repeatedly send every message.
                        ByteBufPair byteBufPair = (ByteBufPair) data;
                        ByteBuf buf1 = byteBufPair.getFirst();
                        ByteBuf buf2 = byteBufPair.getSecond();
                        int bufferIndex1 = buf1.readerIndex();
                        int bufferIndex2 = buf2.readerIndex();
                        // Skip totalSize.
                        buf1.readInt();
                        int cmdSize = buf1.readInt();
                        BaseCommand cmd = new BaseCommand();
                        cmd.parseFrom(buf1, cmdSize);
                        buf1.readerIndex(bufferIndex1);
                        if (cmd.getType().equals(BaseCommand.Type.SEND)) {
                            synchronized (duplicatedMsgs) {
                                if (duplicatedMsgs.size() >= repeatedMessagesWindow) {
                                    for (ByteBufPair bufferPair : duplicatedMsgs) {
                                        originalCtx.channel().write(bufferPair, originalCtx.voidPromise());
                                        originalCtx.channel().flush();
                                    }
                                    duplicatedMsgs.clear();
                                }
                            }
                            ByteBuf newBuffer1 = UnpooledByteBufAllocator.DEFAULT.heapBuffer(
                                    buf1.readableBytes());
                            buf1.readBytes(newBuffer1);
                            buf1.readerIndex(bufferIndex1);
                            ByteBuf newBuffer2 = UnpooledByteBufAllocator.DEFAULT.heapBuffer(
                                    buf2.readableBytes());
                            buf2.readBytes(newBuffer2);
                            buf2.readerIndex(bufferIndex2);
                            synchronized (duplicatedMsgs) {
                                if (newBuffer2.readableBytes() > 0) {
                                    duplicatedMsgs.add(ByteBufPair.get(newBuffer1, newBuffer2));
                                }
                            }
                            return invocation.callRealMethod();
                        } else {
                            return invocation.callRealMethod();
                        }
                    };
                    doAnswer(injectedAnswer).when(spyContext).write(any());
                    doAnswer(injectedAnswer).when(spyContext).write(any(), any(ChannelPromise.class));
                    doAnswer(injectedAnswer).when(spyContext).writeAndFlush(any());
                    doAnswer(injectedAnswer).when(spyContext).writeAndFlush(any(), any(ChannelPromise.class));
                    return spyContext;
                }
            });

        // 1. Create topics and enable deduplication.
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            PersistentTopic persistentTopic1 =
                    (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
            PersistentTopic persistentTopic2 =
                    (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
            admin1.topicPolicies().setDeduplicationStatus(topicName, true);
            admin1.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            admin2.topicPolicies().setDeduplicationStatus(topicName, true);
            admin2.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            MessageDeduplication messageDeduplication1 = persistentTopic1.getMessageDeduplication();
            if (messageDeduplication1 != null) {
                int snapshotInterval1 = WhiteboxImpl.getInternalState(messageDeduplication1, "snapshotInterval");
                assertEquals(snapshotInterval1, 10);
            }
            MessageDeduplication messageDeduplication2 = persistentTopic2.getMessageDeduplication();
            if (messageDeduplication2 != null) {
                int snapshotInterval2 = WhiteboxImpl.getInternalState(messageDeduplication2, "snapshotInterval");
                assertEquals(snapshotInterval2, 10);
            }
            assertEquals(persistentTopic1.getHierarchyTopicPolicies().getDeduplicationEnabled().get(), Boolean.TRUE);
            assertEquals(persistentTopic1.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            assertEquals(persistentTopic2.getHierarchyTopicPolicies().getDeduplicationEnabled().get(), Boolean.TRUE);
            assertEquals(persistentTopic2.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        });
        PersistentTopic tp1 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentTopic tp2 =
                (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();

        // 2, Publish messages.
        List<String> msgSent = new ArrayList<>();
        Producer<Integer> p1 = client1.newProducer(Schema.INT32).topic(topicName).create();
        Producer<Integer> p2 = client1.newProducer(Schema.INT32).topic(topicName).create();
        Producer<String> p3 = client1.newProducer(Schema.STRING).topic(topicName).create();
        Producer<Boolean> p4 = client1.newProducer(Schema.BOOL).topic(topicName).create();
        for (int i = 0; i < 10; i++) {
            p1.send(i);
            msgSent.add(String.valueOf(i));
        }
        for (int i = 10; i < 200; i++) {
            int msg1 = i;
            int msg2 = 1000 + i;
            String msg3 = (2000 + i) + "";
            boolean msg4 = i % 2 == 0;
            p1.send(msg1);
            p2.send(msg2);
            msgSent.add(String.valueOf(msg1));
            msgSent.add(String.valueOf(msg2));
            if (multiSchemas) {
                p3.send(msg3);
                p4.send(msg4);
                msgSent.add(String.valueOf(msg3));
                msgSent.add(String.valueOf(msg4));
            }
        }
        p1.close();
        p2.close();
        p3.close();
        p4.close();

        // 3. Enable replication and wait the task to be finished.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
        waitReplicatorStarted(topicName);
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            for (ManagedCursor cursor : tp1.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.r2")) {
                    long replBacklog = cursor.getNumberOfEntriesInBacklog(true);
                    log.info("repl backlog: {}", replBacklog);
                    assertEquals(replBacklog, 0);
                }
            }
        });

        // Verify: all messages were copied correctly.
        List<String> msgReceived = new ArrayList<>();
        Consumer<GenericRecord> consumer = client2.newConsumer(Schema.AUTO_CONSUME()).topic(topicName)
                .subscriptionName("s1").subscribe();
        while (true) {
            Message<GenericRecord> msg = consumer.receive(10, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            MessageIdAdv messageIdAdv = (MessageIdAdv) msg.getMessageId();
            log.info("received msg. source {}, target {}:{}", StringUtils.join(msg.getProperties().values(), ":"),
                    messageIdAdv.getLedgerId(), messageIdAdv.getEntryId());
            msgReceived.add(String.valueOf(msg.getValue()));
            consumer.acknowledgeAsync(msg);
        }
        log.info("c1 topic stats-internal: "
                + JACKSON.writeValueAsString(admin1.topics().getInternalStats(topicName)));
        log.info("c2 topic stats-internal: "
                + JACKSON.writeValueAsString(admin2.topics().getInternalStats(topicName)));
        log.info("c1 topic stats-internal: "
                + JACKSON.writeValueAsString(admin1.topics().getStats(topicName)));
        log.info("c2 topic stats-internal: "
                + JACKSON.writeValueAsString(admin2.topics().getStats(topicName)));
        assertEquals(msgReceived, msgSent);
        consumer.close();

        // Verify: the deduplication cursor has been acked.
        // "topic-policy.DeduplicationSnapshotInterval" is "10".
        Awaitility.await().untilAsserted(() -> {
            for (ManagedCursor cursor : tp1.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.dedup")) {
                    assertTrue(cursor.getNumberOfEntriesInBacklog(true) < 10);
                }
            }
            for (ManagedCursor cursor : tp2.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.dedup")) {
                    assertTrue(cursor.getNumberOfEntriesInBacklog(true) < 10);
                }
            }
        });
        // Remove the injection.
        taskToClearInjection.run();

        log.info("======  Verify: all messages will be replicated after reopening replication  ======");

        // Verify: all messages will be replicated after reopening replication.
        // Reopen replication: stop replication.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
        waitReplicatorStopped(topicName);
        admin2.topics().unload(topicName);
        admin2.topics().delete(topicName);
        // Reopen replication: enable replication.
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            PersistentTopic persistentTopic2 =
                    (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
            admin2.topicPolicies().setDeduplicationStatus(topicName, true);
            admin2.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            MessageDeduplication messageDeduplication2 = persistentTopic2.getMessageDeduplication();
            if (messageDeduplication2 != null) {
                int snapshotInterval2 = WhiteboxImpl.getInternalState(messageDeduplication2, "snapshotInterval");
                assertEquals(snapshotInterval2, 10);
            }
            assertEquals(persistentTopic2.getHierarchyTopicPolicies().getDeduplicationEnabled().get(), Boolean.TRUE);
            assertEquals(persistentTopic2.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        });
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            for (ManagedCursor cursor : tp2.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.c2")) {
                    assertEquals(cursor.getNumberOfEntriesInBacklog(true), 0);
                }
            }
        });
        // Reopen replication: consumption.
        List<String> msgReceived2 = new ArrayList<>();
        Consumer<GenericRecord> consumer2 = client2.newConsumer(Schema.AUTO_CONSUME()).topic(topicName)
                .subscriptionName("s1").subscribe();
        while (true) {
            Message<GenericRecord> msg = consumer2.receive(10, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            MessageIdAdv messageIdAdv = (MessageIdAdv) msg.getMessageId();
            log.info("received msg. source {}, target {}:{}", StringUtils.join(msg.getProperties().values(), ":"),
                    messageIdAdv.getLedgerId(), messageIdAdv.getEntryId());
            msgReceived2.add(String.valueOf(msg.getValue()));
            consumer2.acknowledgeAsync(msg);
        }
        // Verify: all messages were copied correctly.
        log.info("c1 topic stats-internal: "
                + JACKSON.writeValueAsString(admin1.topics().getInternalStats(topicName)));
        log.info("c2 topic stats-internal: "
                + JACKSON.writeValueAsString(admin2.topics().getInternalStats(topicName)));
        log.info("c1 topic stats-internal: "
                + JACKSON.writeValueAsString(admin1.topics().getStats(topicName)));
        log.info("c2 topic stats-internal: "
                + JACKSON.writeValueAsString(admin2.topics().getStats(topicName)));
        assertEquals(msgReceived2, msgSent);
        consumer2.close();

        // cleanup.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
        waitReplicatorStopped(topicName);
        Awaitility.await().until(() -> {
            for (ManagedCursor cursor : tp1.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.r2")) {
                    return false;
                }
            }
            return true;
        });
        admin1.topics().delete(topicName);
        admin2.topics().delete(topicName);
    }

    @DataProvider(name = "enabledDeduplication")
    public Object[][] enabledDeduplication() {
        return new Object[][] {
                {true},
                {false}
        };
    }

    /***
     * To reproduce the issue that replication loss message if enabled deduplication
     * 1. Publishing in the source cluster
     *   1-1. Producer-1 send 2 messages: M1, M2
     *   1-2. Producer-2 send 2 messages: M3, M4
     * 2. Replicate messages to the remote cluster
     *   2-1. Copies M1 and M2
     *   2-2. Repeatedly copies M1 and M2. and copies M3 and M4.
     *     2-2-1. After repeatedly copies M1 and M2, the network broke.
     * 3. After a topic unloading.
     *   3-1. The replicator will start after the topic is loaded up.
     *   3-2. The client will create a new connection.
     * 4. Verify: All 4 messages are copied to the remote cluster.
     */
    @Test(timeOut = 360 * 1000, dataProvider = "enabledDeduplication")
    public void testDeduplicationNotLostMessage(boolean enabledDeduplication) throws Exception {
        waitInternalClientCreated();

        /**
         * step-2: Inject a mechanism that makes the client connect broke after repeatedly copied M1 and M2.
         */
        final List<ByteBufPair> duplicatedMsgs = new ArrayList<>();
        final int repeatMsgIndex = 2;
        AtomicInteger msgSent = new AtomicInteger(0);
        ConcurrentHashSet<Channel> injectedChannel = new ConcurrentHashSet<>();
        Runnable taskToClearInjection = injectReplicatorClientCnx(
                (conf, eventLoopGroup) -> new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup) {

                    @Override
                    public ChannelHandlerContext ctx() {
                        final ChannelHandlerContext originalCtx = super.ctx;
                        ChannelHandlerContext spyContext = spy(originalCtx);
                        Answer injectedAnswer = invocation -> {
                            // Do not repeat the messages re-sending, and clear the previous cached messages when
                            // calling re-sending, to avoid publishing outs of order.
                            for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
                                if (stackTraceElement.toString().contains("recoverProcessOpSendMsgFrom")
                                        || stackTraceElement.toString().contains("resendMessages")) {
                                    duplicatedMsgs.clear();
                                    return invocation.callRealMethod();
                                }
                            }

                            Object data = invocation.getArguments()[0];
                            if (true && !(data instanceof ByteBufPair)) {
                                return invocation.callRealMethod();
                            }
                            // Repeatedly send every message.
                            ByteBufPair byteBufPair = (ByteBufPair) data;
                            ByteBuf buf1 = byteBufPair.getFirst();
                            ByteBuf buf2 = byteBufPair.getSecond();
                            int bufferIndex1 = buf1.readerIndex();
                            int bufferIndex2 = buf2.readerIndex();
                            // Skip totalSize.
                            buf1.readInt();
                            int cmdSize = buf1.readInt();
                            BaseCommand cmd = new BaseCommand();
                            cmd.parseFrom(buf1, cmdSize);
                            buf1.readerIndex(bufferIndex1);
                            if (cmd.getType().equals(BaseCommand.Type.SEND)) {
                                synchronized (duplicatedMsgs) {
                                    if (duplicatedMsgs.isEmpty() && msgSent.get() == repeatMsgIndex) {
                                        return null;
                                    }
                                    if (msgSent.get() == repeatMsgIndex) {
                                        for (ByteBufPair bufferPair : duplicatedMsgs) {
                                            originalCtx.channel().write(bufferPair, originalCtx.voidPromise());
                                            originalCtx.channel().flush();
                                        }
                                        duplicatedMsgs.clear();
                                        return null;
                                    }
                                }
                                ByteBuf newBuffer1 = UnpooledByteBufAllocator.DEFAULT.heapBuffer(
                                        buf1.readableBytes());
                                buf1.readBytes(newBuffer1);
                                buf1.readerIndex(bufferIndex1);
                                ByteBuf newBuffer2 = UnpooledByteBufAllocator.DEFAULT.heapBuffer(
                                        buf2.readableBytes());
                                buf2.readBytes(newBuffer2);
                                buf2.readerIndex(bufferIndex2);
                                synchronized (duplicatedMsgs) {
                                    if (newBuffer2.readableBytes() > 0 && msgSent.incrementAndGet() <= repeatMsgIndex) {
                                        duplicatedMsgs.add(ByteBufPair.get(newBuffer1, newBuffer2));
                                    }
                                }
                                return invocation.callRealMethod();
                            } else {
                                return invocation.callRealMethod();
                            }
                        };
                        doAnswer(injectedAnswer).when(spyContext).write(any());
                        doAnswer(injectedAnswer).when(spyContext).write(any(), any(ChannelPromise.class));
                        doAnswer(injectedAnswer).when(spyContext).writeAndFlush(any());
                        doAnswer(injectedAnswer).when(spyContext).writeAndFlush(any(), any(ChannelPromise.class));
                        injectedChannel.add(originalCtx.channel());
                        return spyContext;
                    }
                });

        // Create topics and enable deduplication.
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
        PersistentTopic tp1 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentTopic tp2 =
                (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        ManagedLedgerImpl ml2 = (ManagedLedgerImpl) tp2.getManagedLedger();
        if (enabledDeduplication) {
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                PersistentTopic persistentTopic1 =
                        (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
                PersistentTopic persistentTopic2 =
                        (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
                admin1.topicPolicies().setDeduplicationStatus(topicName, true);
                admin2.topicPolicies().setDeduplicationStatus(topicName, true);
                assertEquals(persistentTopic1.getHierarchyTopicPolicies().getDeduplicationEnabled().get(),
                        Boolean.TRUE);
                assertEquals(persistentTopic2.getHierarchyTopicPolicies().getDeduplicationEnabled().get(),
                        Boolean.TRUE);
            });
        }
        // Let broker persist messages one by one, in other words, it starts to persist the next message after the
        // previous has been written into BKs.
        PersistentTopic spyTp2 = spy(tp2);
        doAnswer(invocation -> {
            try {
                Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                    assertEquals(ml2.getPendingAddEntriesCount(), 0);
                });
            } catch (Throwable throwable) {
                // Ignore this timeout error.
            }
            return invocation.callRealMethod();
        }).when(spyTp2).publishMessage(any(ByteBuf.class), any(Topic.PublishContext.class));
        CompletableFuture<Optional<Topic>> originalTp2 = pulsar2.getBrokerService().getTopics().put(tp2.getName(),
                CompletableFuture.completedFuture(Optional.of(spyTp2)));

        /**
         * Step-1: Publishes messages in the source cluster and start replication,
         */
        ProducerImpl p1 = (ProducerImpl) client1.newProducer().topic(topicName).producerName("p1").create();
        ProducerImpl p2 = (ProducerImpl) client1.newProducer().topic(topicName).producerName("p2").create();
        p1.send("1".toString().getBytes(StandardCharsets.UTF_8));
        p1.send("2".toString().getBytes(StandardCharsets.UTF_8));
        p2.send("3".toString().getBytes(StandardCharsets.UTF_8));
        p2.send("4".toString().getBytes(StandardCharsets.UTF_8));

        // Enable replication and wait the task to be finished, it should not finish if no bug.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
        waitReplicatorStarted(topicName);
        try {
            Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                for (ManagedCursor cursor : tp1.getManagedLedger().getCursors()) {
                    if (cursor.getName().equals("pulsar.repl.r2")) {
                        long replBacklog = cursor.getNumberOfEntriesInBacklog(true);
                        log.info("repl backlog: {}", replBacklog);
                        assertEquals(replBacklog, 0);
                    }
                }
            });
        } catch (Throwable t) {
            // Ignore the error.
        }

        /**
         * Step-3: remove the injections, unload topics and rebuild connections of the replicator.
         */
        taskToClearInjection.run();
        pulsar2.getBrokerService().getTopics().put(tp2.getName(), originalTp2);
        admin1.topics().unload(topicName);
        admin2.topics().unload(topicName);
        for (Channel channel : injectedChannel) {
            channel.close();
        }
        waitReplicatorStarted(topicName);
        PersistentTopic tp12 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentTopic tp22 =
                (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            for (ManagedCursor cursor : tp12.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.r2")) {
                    long replBacklog = cursor.getNumberOfEntriesInBacklog(true);
                    log.info("repl backlog: {}", replBacklog);
                    assertEquals(replBacklog, 0);
                }
            }
        });

        /**
         * Verify: All 4 messages are copied to the remote cluster.
         */
        List<String> msgReceived = new ArrayList<>();
        Consumer consumer = client2.newConsumer().topic(topicName)
                .subscriptionName("s1").subscribe();
        while (true) {
            Message msg = consumer.receive(10, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            MessageIdAdv messageIdAdv = (MessageIdAdv) msg.getMessageId();
            log.info("received msg. source {}, target {}:{}", StringUtils.join(msg.getProperties().values(), ":"),
                    messageIdAdv.getLedgerId(), messageIdAdv.getEntryId());
            msgReceived.add(new String(msg.getData(), StandardCharsets.UTF_8));
            consumer.acknowledgeAsync(msg);
        }

        log.info("received msgs: {}", msgReceived);
        assertTrue(msgReceived.contains("1"));
        assertTrue(msgReceived.contains("2"));
        assertTrue(msgReceived.contains("3"));
        assertTrue(msgReceived.contains("4"));
        if (enabledDeduplication) {
            assertEquals(msgReceived, Arrays.asList("1", "2", "3", "4"));
        }

        // cleanup.
        consumer.close();
        p1.close();
        p2.close();
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
        waitReplicatorStopped(topicName);
        Awaitility.await().until(() -> {
            for (ManagedCursor cursor : tp12.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.r2")) {
                    return false;
                }
            }
            return true;
        });
        admin1.topics().delete(topicName);
        admin2.topics().delete(topicName);
    }

    @Test(timeOut = 360 * 1000, dataProvider = "enabledDeduplication")
    public void testReplicationLoadSchemaTimeout(boolean enabledDeduplication) throws Exception {
        waitInternalClientCreated();

        /**
         * Inject a timeout error for Get Schema.
         */
        Field filedSchemaRegistryService = PulsarService.class.getDeclaredField("schemaRegistryService");
        filedSchemaRegistryService.setAccessible(true);
        SchemaRegistryService originalSchemaRegistryService =
                (SchemaRegistryService) filedSchemaRegistryService.get(pulsar2);
        SchemaRegistryService spySchemaRegistryService = spy(originalSchemaRegistryService);
        AtomicBoolean getSchemaSuccess = new AtomicBoolean(false);
        doAnswer(invocation -> {
            if (getSchemaSuccess.get()) {
                getSchemaSuccess.set(false);
                return invocation.callRealMethod();
            } else {
                getSchemaSuccess.set(true);
            }
            Thread.sleep(60 * 1000);
            return invocation.callRealMethod();
        }).when(spySchemaRegistryService).findSchemaVersion(any(String.class), any(SchemaData.class));
        filedSchemaRegistryService.set(pulsar2, spySchemaRegistryService);
        Runnable taskToClearInjection = injectReplicatorClientCnx(
            (conf, eventLoopGroup) -> new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup) {
                @Override
                protected void handleGetSchemaResponse(CommandGetSchemaResponse commandGetSchemaResponse) {
                    if (getSchemaSuccess.get()) {
                        getSchemaSuccess.set(false);
                        super.handleGetSchemaResponse(commandGetSchemaResponse);
                        return;
                    } else {
                        getSchemaSuccess.set(true);
                    }
                    checkArgument(state == State.Ready);
                    long requestId = commandGetSchemaResponse.getRequestId();
                    CompletableFuture<CommandGetSchemaResponse> future =
                            (CompletableFuture<CommandGetSchemaResponse>) pendingRequests.remove(requestId);
                    if (future == null) {
                        duplicatedResponseCounter.incrementAndGet();
                        log.warn("{} Received unknown request id from server: {}", ctx.channel(), requestId);
                        return;
                    }
                    future.completeExceptionally(new PulsarClientException.TimeoutException("Mocked timeout"));
                }

                @Override
                protected void handleGetOrCreateSchemaResponse(CommandGetOrCreateSchemaResponse
                                                                       commandGetOrCreateSchemaResponse) {

                    if (getSchemaSuccess.get()) {
                        getSchemaSuccess.set(false);
                        super.handleGetOrCreateSchemaResponse(commandGetOrCreateSchemaResponse);
                        return;
                    } else {
                        getSchemaSuccess.set(true);
                    }

                    checkArgument(state == State.Ready);
                    long requestId = commandGetOrCreateSchemaResponse.getRequestId();
                    CompletableFuture<CommandGetOrCreateSchemaResponse> future =
                            (CompletableFuture<CommandGetOrCreateSchemaResponse>) pendingRequests.remove(requestId);
                    if (future == null) {
                        duplicatedResponseCounter.incrementAndGet();
                        log.warn("{} Received unknown request id from server: {}", ctx.channel(), requestId);
                        return;
                    }
                    future.completeExceptionally(new PulsarClientException.TimeoutException("Mocked timeout"));
                }
            });

        // Create topics and enable deduplication.
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createSubscription(topicName, "s1", MessageId.earliest);
        PersistentTopic tp1 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
        PersistentTopic tp2 =
                (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        if (enabledDeduplication) {
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                PersistentTopic persistentTopic1 =
                        (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
                PersistentTopic persistentTopic2 =
                        (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
                admin1.topicPolicies().setDeduplicationStatus(topicName, true);
                admin2.topicPolicies().setDeduplicationStatus(topicName, true);
                assertEquals(persistentTopic1.getHierarchyTopicPolicies().getDeduplicationEnabled().get(),
                        Boolean.TRUE);
                assertEquals(persistentTopic2.getHierarchyTopicPolicies().getDeduplicationEnabled().get(),
                        Boolean.TRUE);
            });
        }
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            PersistentTopic persistentTopic1 =
                    (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
            PersistentTopic persistentTopic2 =
                    (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
            admin1.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            admin2.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            assertEquals(persistentTopic1.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
            assertEquals(persistentTopic2.getHierarchyTopicPolicies().getSchemaCompatibilityStrategy().get(),
                    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        });

        // Publishes messages in the source cluster.
        Producer p1 = client1.newProducer().topic(topicName).producerName("p1").create();
        Producer p2 = client1.newProducer().topic(topicName).producerName("p2").create();
        Producer p3 = client1.newProducer(Schema.STRING).topic(topicName).producerName("p3").create();
        p1.send("1".toString().getBytes(StandardCharsets.UTF_8));
        p1.send("2".toString().getBytes(StandardCharsets.UTF_8));
        p3.send("2-1");
        p3.send("2-2");
        p2.send("3".toString().getBytes(StandardCharsets.UTF_8));
        p2.send("4".toString().getBytes(StandardCharsets.UTF_8));

        // Enable replication and wait the task to be finished, it should not finish if no bug.
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
        waitReplicatorStarted(topicName);
        Awaitility.await().atMost(Duration.ofSeconds(180)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            for (ManagedCursor cursor : tp1.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.r2")) {
                    long replBacklog = cursor.getNumberOfEntriesInBacklog(true);
                    log.info("repl backlog: {}", replBacklog);
                    assertEquals(replBacklog, 0);
                }
            }
        });

        // Verify: All messages are copied to the remote cluster.
        List<String> msgReceived = new ArrayList<>();
        Consumer consumer = client2.newConsumer().topic(topicName)
                .subscriptionName("s1").subscribe();
        while (true) {
            Message msg = consumer.receive(10, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            MessageIdAdv messageIdAdv = (MessageIdAdv) msg.getMessageId();
            log.info("received msg. source {}, target {}:{}", StringUtils.join(msg.getProperties().values(), ":"),
                    messageIdAdv.getLedgerId(), messageIdAdv.getEntryId());
            msgReceived.add(new String(msg.getData(), StandardCharsets.UTF_8));
            consumer.acknowledgeAsync(msg);
        }
        log.info("received msgs: {}", msgReceived);
        assertTrue(msgReceived.contains("1"));
        assertTrue(msgReceived.contains("2"));
        assertTrue(msgReceived.contains("2-1"));
        assertTrue(msgReceived.contains("2-2"));
        assertTrue(msgReceived.contains("3"));
        assertTrue(msgReceived.contains("4"));
        if (enabledDeduplication) {
            assertEquals(msgReceived, Arrays.asList("1", "2", "2-1", "2-2", "3", "4"));
        }

        // cleanup.
        taskToClearInjection.run();
        filedSchemaRegistryService.set(pulsar2, originalSchemaRegistryService);
        consumer.close();
        p1.close();
        p2.close();
        p3.close();
        admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1));
        waitReplicatorStopped(topicName);
        Awaitility.await().until(() -> {
            for (ManagedCursor cursor : tp1.getManagedLedger().getCursors()) {
                if (cursor.getName().equals("pulsar.repl.r2")) {
                    return false;
                }
            }
            return true;
        });
        admin1.topics().delete(topicName);
        admin2.topics().delete(topicName);
    }
}
