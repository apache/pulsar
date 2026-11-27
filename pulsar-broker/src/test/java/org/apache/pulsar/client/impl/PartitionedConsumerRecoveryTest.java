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

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.util.Timeout;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.utils.TestLogAppender;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class PartitionedConsumerRecoveryTest extends SharedPulsarBaseTest {

    @DataProvider(name = "expandBeforeRecovery")
    public Object[][] expandBeforeRecovery() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "expandBeforeRecovery", timeOut = 60000)
    public void testRetryFailedNewPartition(boolean expandBeforeRecovery) throws Exception {
        String topic = newTopicName();
        String subscription = "subscription";
        String partition2 = topic + "-partition-2";
        String partition3 = topic + "-partition-3";
        String partition4 = topic + "-partition-4";
        admin.topics().createPartitionedTopic(topic, 2);
        CloseTrackingSchema schema = new CloseTrackingSchema();

        try (MultiTopicsConsumerImpl<byte[]> consumer = (MultiTopicsConsumerImpl<byte[]>) pulsarClient
                .newConsumer(schema)
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .autoUpdatePartitionsInterval(60, TimeUnit.SECONDS)
                .subscribe();
             TestLogAppender appender = TestLogAppender.create(PersistentTopic.class)) {
            admin.topics().updatePartitionedTopic(topic, 4);
            try (Consumer<byte[]> blocker = pulsarClient.newConsumer()
                    .topic(partition2)
                    .subscriptionName(subscription)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe()) {
                runPartitionUpdate(consumer);
                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertThat(consumer.partitionsAutoUpdateFuture).isCompleted();
                    assertThat(appender.getEvents()).anySatisfy(event -> {
                        assertThat(event.getMessage().getFormattedMessage()).contains("Consumer already connected");
                        assertThat(event.getContextData().<String>getValue("topic")).isEqualTo(partition2);
                    });
                });
                assertThat(schema.closeCount).as("schema stays open after a partition subscription fails").hasValue(0);

                if (expandBeforeRecovery) {
                    // The other new partition must begin receiving even though partition 2 failed.
                    sendAndReceive(consumer, partition3, (byte) 3);
                    admin.topics().updatePartitionedTopic(topic, 5);
                    runPartitionUpdate(consumer);
                    Awaitility.await().atMost(10, TimeUnit.SECONDS)
                            .untilAsserted(() -> assertThat(consumer.partitionsAutoUpdateFuture).isCompleted());
                    sendAndReceive(consumer, partition4, (byte) 4);
                }
            }

            // The partition count can be unchanged at this point. A later check must still retry partition 2.
            runPartitionUpdate(consumer);
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(consumer.partitionsAutoUpdateFuture).isCompleted());
            assertThat(schema.closeCount).as("retry must not close the schema shared with other partitions")
                    .hasValue(0);
            sendAndReceive(consumer, partition2, (byte) 2);
            sendAndReceive(consumer, partition3, (byte) 3);

            if (!expandBeforeRecovery) {
                admin.topics().updatePartitionedTopic(topic, 5);
                runPartitionUpdate(consumer);
                Awaitility.await().atMost(10, TimeUnit.SECONDS)
                        .untilAsserted(() -> assertThat(consumer.partitionsAutoUpdateFuture).isCompleted());
                sendAndReceive(consumer, partition4, (byte) 4);
            }
        }
        assertThat(schema.closeCount.get()).as("closing the parent consumer closes its schema").isPositive();
    }

    private void sendAndReceive(Consumer<byte[]> consumer, String topic, byte value) throws Exception {
        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create()) {
            producer.send(new byte[] {value});
            Message<byte[]> message = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(message).as("message from %s", topic).isNotNull();
            assertThat(message.getValue()).containsExactly(value);
            consumer.acknowledge(message);
        }
    }

    private static void runPartitionUpdate(MultiTopicsConsumerImpl<?> consumer) throws Exception {
        Timeout timeout = consumer.getPartitionsAutoUpdateTimeout();
        try {
            timeout.task().run(timeout);
        } finally {
            timeout.cancel();
        }
    }

    private static class CloseTrackingSchema implements Schema<byte[]> {
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public byte[] encode(byte[] value) {
            return value;
        }

        @Override
        public byte[] decode(byte[] bytes) {
            if (closeCount.get() > 0) {
                throw new IllegalStateException("Schema is closed");
            }
            return bytes;
        }

        @Override
        public SchemaInfo getSchemaInfo() {
            return Schema.BYTES.getSchemaInfo();
        }

        @Override
        public Schema<byte[]> clone() {
            return new CloseTrackingSchema();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            closeCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }
}
