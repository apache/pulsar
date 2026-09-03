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
package org.apache.pulsar.common.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test
public class ProducerBatchSendTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] flushSend() {
        return new Object[][] {
                {Collections.emptyList(), CompressionType.NONE},
                {Arrays.asList(1), CompressionType.NONE},
                {Arrays.asList(2), CompressionType.NONE},
                {Arrays.asList(3), CompressionType.NONE},
                {Arrays.asList(1, 2), CompressionType.NONE},
                {Arrays.asList(2, 3), CompressionType.NONE},
                {Arrays.asList(1, 2, 3), CompressionType.NONE},
                {Collections.emptyList(), CompressionType.ZLIB},
                {Arrays.asList(1), CompressionType.ZLIB},
                {Arrays.asList(2), CompressionType.ZLIB},
                {Arrays.asList(3), CompressionType.ZLIB},
                {Arrays.asList(1, 2), CompressionType.ZLIB},
                {Arrays.asList(2, 3), CompressionType.ZLIB},
                {Arrays.asList(1, 2, 3), CompressionType.ZLIB},
        };
    }

    /**
     * {@link org.apache.pulsar.client.impl.BatchMessageContainerImpl#createOpSendMsg} may fail after the batch
     * payload was already built, e.g. when the command buffer allocation fails in
     * {@link Commands#serializeCommandSendWithSize}. With compression enabled, the batch buffer has already been
     * released at that point, so the recovery must not reuse it when the batch is retried.
     */
    @Test(dataProvider = "flushSend")
    public void testNoEnoughMemSend(List<Integer> flushSend, CompressionType compressionType) throws Exception {
        final String topic = newTopicName();
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, subscription, MessageId.earliest);
        ProducerBuilder<String> builder = pulsarClient.newProducer(Schema.STRING).topic(topic).enableBatching(true)
                .batchingMaxMessages(Integer.MAX_VALUE).batchingMaxPublishDelay(1, TimeUnit.HOURS);
        if (compressionType != CompressionType.NONE) {
            builder.compressionType(compressionType);
        }
        Producer<String> producer = builder.create();

        AtomicBoolean failure = new AtomicBoolean(true);
        BaseCommand threadLocalBaseCommand = Commands.LOCAL_BASE_COMMAND.get();
        BaseCommand spyBaseCommand = spy(threadLocalBaseCommand);
        doAnswer(invocation -> {
            if (failure.get()) {
                throw new RuntimeException("mocked exception");
            } else {
                return invocation.callRealMethod();
            }
        }).when(spyBaseCommand).setSend();
        Commands.LOCAL_BASE_COMMAND.set(spyBaseCommand);

        // 6 KB payloads stay above the 4 KB compressMinMsgBodySize threshold, so the ZLIB cases
        // really take the compression branch even when a single message is flushed.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 750; i++) {
            sb.append("abcdefgh");
        }

        try {
            // Failed sending 3 times.
            producer.sendAsync(sb + "-1");
            if (flushSend.contains(1)) {
                producer.flushAsync();
            }
            producer.sendAsync(sb + "-2");
            if (flushSend.contains(2)) {
                producer.flushAsync();
            }
            producer.sendAsync(sb + "-3");
            if (flushSend.contains(3)) {
                producer.flushAsync();
            }
            // Publishing is finished eventually.
            failure.set(false);
            assertThat(producer.flushAsync()).succeedsWithin(10, TimeUnit.SECONDS);
            Awaitility.await().untilAsserted(() -> {
                assertTrue(admin.topics().getStats(topic).getSubscriptions().get(subscription).getMsgBacklog() > 0);
            });

            // Verify: all messages can be consumed.
            ConsumerImpl<String> consumer = (ConsumerImpl<String>) pulsarClient.newConsumer(Schema.STRING)
                    .topic(topic).subscriptionName(subscription).subscribe();
            for (int i = 1; i <= 3; i++) {
                Message<String> msg = consumer.receive(2, TimeUnit.SECONDS);
                assertNotNull(msg, "message " + i + " lost");
                assertEquals(msg.getValue(), sb + "-" + i);
            }
            consumer.close();
        } finally {
            Commands.LOCAL_BASE_COMMAND.set(threadLocalBaseCommand);
            producer.close();
        }
    }
}
