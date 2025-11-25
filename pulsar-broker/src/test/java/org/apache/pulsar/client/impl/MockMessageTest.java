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
package org.apache.pulsar.client.impl;

import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class MockMessageTest extends ProducerConsumerBase {

    private final Map<Thread, List<Throwable>> threadFailures = new ConcurrentHashMap<>();

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMessageWithWrongEpoch() throws Exception {
        threadFailures.clear();
        final var conf = new ClientConfigurationData();
        conf.setServiceUrl(pulsar.getBrokerServiceUrl());
        @Cleanup final var client = PulsarClientImpl.builder().conf(conf)
                .internalExecutorProvider(new ExecutorProvider(1, "internal", false,
                        this::newThreadFactory))
                .externalExecutorProvider(new ExecutorProvider(1, "external", false))
                .build();

        final var topic = "test-message-with-wrong-epoch";
        @Cleanup final var consumer = (ConsumerImpl<byte[]>) client.newConsumer()
                .topic(topic).subscriptionName("sub").poolMessages(true).subscribe();

        final var cnx = consumer.cnx();
        consumer.redeliverUnacknowledgedMessages(); // increase the consumer epoch
        Assert.assertEquals(consumer.consumerEpoch, 1L);
        final BiConsumer<Long, String> sendMessage = (epoch, value) -> {
            cnx.ctx().executor().execute(() -> {
                final var cmd = new BaseCommand();
                cmd.copyFrom(Commands.newMessageCommand(consumer.consumerId, 0L, 0L, 0, 0, null, epoch));
                final var metadata = new MessageMetadata().setPublishTime(System.currentTimeMillis())
                        .setProducerName("producer").setSequenceId(0).clearNumMessagesInBatch();
                final var buffer = Commands.serializeMetadataAndPayload(Commands.ChecksumType.None, metadata,
                        Unpooled.wrappedBuffer(value.getBytes()));
                cnx.handleMessage(cmd.getMessage(), buffer);
            });
        };
        sendMessage.accept(0L, "msg-0"); // 0 is an old epoch that will be rejected
        sendMessage.accept(1L, "msg-1");

        final var msg = consumer.receive(3, TimeUnit.SECONDS);
        Assert.assertNotNull(msg);
        Assert.assertEquals(msg.getValue(), "msg-1".getBytes(StandardCharsets.UTF_8));
        Assert.assertTrue(threadFailures.isEmpty());
    }

    private ExecutorProvider.ExtendedThreadFactory newThreadFactory(String poolName, boolean daemon) {
        return new ExecutorProvider.ExtendedThreadFactory(poolName, daemon) {

            @Override
            public Thread newThread(Runnable r) {
                final var thread = super.newThread(r);
                thread.setUncaughtExceptionHandler((t, e) -> {
                    log.error("Unexpected exception in {}", t.getName(), e);
                    threadFailures.computeIfAbsent(t, __ -> new CopyOnWriteArrayList<>()).add(e);
                });
                return thread;
            }
        };
    }
}
