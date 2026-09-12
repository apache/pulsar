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
package org.apache.pulsar.client.cli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.v5.MessageBuilder;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncMessageBuilder;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.Assert;

/**
 * An implementation of {@link PulsarClientTool} for test, which publishes messages only once there
 * are enough messages in a batch. The producer is forced to batch exactly {@code batchNum} messages
 * with an effectively-infinite publish delay, and the synchronous {@code send()} is turned into an
 * asynchronous send so the messages actually accumulate into a batch (a blocking send would flush
 * each message individually).
 */
public class PulsarClientToolForceBatchNum extends PulsarClientTool {
    private final String topic;
    private final int batchNum;

    /**
     * @param properties properties
     * @param topic topic
     * @param batchNum iff there are batchNum messages in the batch, the producer will flush and send.
     */
    public PulsarClientToolForceBatchNum(Properties properties, String topic, int batchNum) {
        super(properties);
        this.topic = topic;
        this.batchNum = batchNum;
        produceCommand = new CmdProduce() {
            @Override
            public void updateConfig(PulsarClientBuilder newBuilder, Authentication authentication,
                                     String serviceURL) {
                super.updateConfig(mockClientBuilder(newBuilder), authentication, serviceURL);
            }
        };
        replaceProducerCommand(produceCommand);
    }

    private PulsarClientBuilder mockClientBuilder(PulsarClientBuilder newBuilder) {
        try {
            PulsarClient realClient = newBuilder.build();
            PulsarClient spyClient = spy(realClient);

            doAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                Schema<byte[]> schema = (Schema<byte[]>) invocation.getArgument(0);
                // Build a producer that batches exactly batchNum messages and (practically) never
                // flushes on the timer, so batching is deterministic. CmdProduce will still call
                // topic()/batchingPolicy() on the returned spy; for the batched case it leaves the
                // default batching alone, so this forced policy stands.
                ProducerBuilder<byte[]> realBuilder = realClient.newProducer(schema)
                        .topic(topic)
                        .batchingPolicy(BatchingPolicy.builder()
                                .enabled(true)
                                .maxMessages(batchNum)
                                .maxPublishDelay(Duration.ofDays(1))
                                .maxSize(MemorySize.ofBytes(Integer.MAX_VALUE))
                                .build());
                ProducerBuilder<byte[]> spyBuilder = spy(realBuilder);
                doAnswer(c -> forceAsyncSend(realBuilder.create())).when(spyBuilder).create();
                return spyBuilder;
            }).when(spyClient).newProducer(any(Schema.class));

            PulsarClientBuilder spyBuilder = spy(newBuilder);
            doReturn(spyClient).when(spyBuilder).build();
            return spyBuilder;
        } catch (Exception e) {
            Assert.fail("update config fail " + e.getMessage());
            return newBuilder;
        }
    }

    /**
     * Wrap a producer so that the synchronous {@code newMessage().send()} the CLI uses is dispatched
     * asynchronously, letting messages accumulate into a batch instead of flushing one-by-one. The
     * send futures are collected and awaited on {@code close()} so no batched message is lost (the
     * CLI ignores the per-send result, and the V5 producer does not implicitly await fire-and-forget
     * sends issued through a different builder instance).
     */
    private static Producer<byte[]> forceAsyncSend(Producer<byte[]> realProducer) throws Exception {
        List<CompletableFuture<MessageId>> pending = Collections.synchronizedList(new ArrayList<>());
        Producer<byte[]> spyProducer = spy(realProducer);
        doAnswer(inv -> new AsyncForwardingMessageBuilder(realProducer.async().newMessage(), pending))
                .when(spyProducer).newMessage();
        doAnswer(inv -> {
            CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
            realProducer.close();
            return null;
        }).when(spyProducer).close();
        return spyProducer;
    }

    /**
     * A {@link MessageBuilder} that accumulates metadata onto an {@link AsyncMessageBuilder} and
     * fires the send asynchronously, returning {@code null} (the CLI ignores the send result). The
     * pending send future is recorded so the producer can await it on close.
     */
    private static final class AsyncForwardingMessageBuilder implements MessageBuilder<byte[]> {
        private final AsyncMessageBuilder<byte[]> delegate;
        private final List<CompletableFuture<MessageId>> pending;

        AsyncForwardingMessageBuilder(AsyncMessageBuilder<byte[]> delegate,
                                      List<CompletableFuture<MessageId>> pending) {
            this.delegate = delegate;
            this.pending = pending;
        }

        @Override
        public MessageId send() {
            pending.add(delegate.send());
            return null;
        }

        @Override
        public MessageBuilder<byte[]> value(byte[] value) {
            delegate.value(value);
            return this;
        }

        @Override
        public MessageBuilder<byte[]> key(String key) {
            delegate.key(key);
            return this;
        }

        @Override
        public MessageBuilder<byte[]> transaction(Transaction txn) {
            delegate.transaction(txn);
            return this;
        }

        @Override
        public MessageBuilder<byte[]> property(String name, String value) {
            delegate.property(name, value);
            return this;
        }

        @Override
        public MessageBuilder<byte[]> properties(Map<String, String> properties) {
            delegate.properties(properties);
            return this;
        }

        @Override
        public MessageBuilder<byte[]> eventTime(Instant eventTime) {
            delegate.eventTime(eventTime);
            return this;
        }

        @Override
        public MessageBuilder<byte[]> sequenceId(long sequenceId) {
            delegate.sequenceId(sequenceId);
            return this;
        }

        @Override
        public MessageBuilder<byte[]> deliverAfter(Duration delay) {
            delegate.deliverAfter(delay);
            return this;
        }

        @Override
        public MessageBuilder<byte[]> deliverAt(Instant timestamp) {
            delegate.deliverAt(timestamp);
            return this;
        }

        @Override
        public MessageBuilder<byte[]> replicationClusters(List<String> clusters) {
            delegate.replicationClusters(clusters);
            return this;
        }
    }
}
