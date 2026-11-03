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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalIoHandler;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentTopicPublishTxnTest extends SharedPulsarBaseTest {

    @Test(timeOut = 60_000)
    public void testPendingWritesDrainWhenConnectionExecutorRejectsReceipt() throws Exception {
        PersistentTopic topic = (PersistentTopic) getTopic(newTopicName(), true)
                .get(10, TimeUnit.SECONDS).orElseThrow();
        assertThat(topic.getTransactionBuffer()).isInstanceOf(TopicTransactionBuffer.class);
        topic.getTransactionBuffer().checkIfTBRecoverCompletely().get(10, TimeUnit.SECONDS);

        EventLoopGroup eventLoops = new MultiThreadIoEventLoopGroup(1, LocalIoHandler.newFactory());
        try {
            LocalChannel channel = new LocalChannel();
            eventLoops.register(channel).sync();
            // Only bridge the connection to its channel; the executor, transaction buffer and ledger are real.
            ChannelHandlerContext context = mock(ChannelHandlerContext.class);
            when(context.channel()).thenReturn(channel);
            CompletableFuture<RejectedExecutionException> rejection = new CompletableFuture<>();
            AtomicInteger completionAttempts = new AtomicInteger();
            ServerCnx cnx = new ServerCnx(getPulsar()) {
                @Override
                public ChannelHandlerContext ctx() {
                    return context;
                }

                @Override
                public String clientSourceAddressAndPort() {
                    return "127.0.0.1:1234";
                }

                @Override
                public void execute(Runnable task) {
                    completionAttempts.incrementAndGet();
                    try {
                        super.execute(task);
                    } catch (RejectedExecutionException e) {
                        rejection.complete(e);
                        throw e;
                    }
                }
            };
            Producer producer = new Producer(topic, cnx, 1, "txn-producer", "role", false, null,
                    SchemaVersion.Latest, 0, false, ProducerAccessMode.Shared, Optional.empty(), false);

            TxnID txnId = new TxnID(1, 2);
            MessageMetadata metadata = new MessageMetadata().setProducerName(producer.getProducerName())
                    .setSequenceId(0).setPublishTime(System.currentTimeMillis())
                    .setTxnidMostBits(txnId.getMostSigBits()).setTxnidLeastBits(txnId.getLeastSigBits());
            ByteBuf content = Unpooled.copiedBuffer("content", UTF_8);
            ByteBuf payload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, content);
            CompletableFuture<Void> writerBlocked = new CompletableFuture<>();
            CompletableFuture<Void> releaseWrites = new CompletableFuture<>();
            // Hold the real writer until the publish is queued and its connection's event loop has stopped.
            CompletableFuture<Void> writerGate = CompletableFuture.runAsync(() -> {
                writerBlocked.complete(null);
                releaseWrites.orTimeout(30, TimeUnit.SECONDS).join();
            }, ((ManagedLedgerImpl) topic.getManagedLedger()).getExecutor());
            try {
                try {
                    writerBlocked.get(10, TimeUnit.SECONDS);
                    producer.publishTxnMessage(txnId, 1, 0, 0, payload, 1, false, false);
                    assertThat(topic.getPendingWriteOps().get()).isEqualTo(1);
                    eventLoops.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
                } finally {
                    releaseWrites.complete(null);
                }
                writerGate.get(10, TimeUnit.SECONDS);
                assertThat(rejection.get(10, TimeUnit.SECONDS)).isNotNull();
                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(topic.getPendingWriteOps().get()).isZero());
                assertThat(topic.getLastPublishedSequenceId(producer.getProducerName())).isZero();
                assertThat(completionAttempts.get()).isEqualTo(1);
            } finally {
                payload.release();
                content.release();
            }
        } finally {
            eventLoops.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
        }
    }
}
