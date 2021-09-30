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

import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Null value message produce and consume test.
 */
@Slf4j
@Test(groups = "broker")
public class NullValueTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "topics")
    public static Object[][] topics() {
        return new Object[][]{
                {"persistent://prop/ns-abc/null-value-test-0", 1},
                {"persistent://prop/ns-abc/null-value-test-1", 3},
        };
    }

    @Test(dataProvider = "topics")
    public void nullValueBytesSchemaTest(String topic, int partitions)
            throws PulsarClientException, PulsarAdminException {
        admin.topics().createPartitionedTopic(topic, partitions);

        @Cleanup
        Producer producer = pulsarClient.newProducer()
                .topic(topic)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        @Cleanup
        Consumer consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            if (i % 2 == 0) {
                producer.newMessage().value("not null".getBytes()).send();
            } else {
                producer.newMessage().value(null).send();
            }
        }

        for (int i = 0; i < numMessage; i++) {
            Message message = consumer.receive();
            if (i % 2 == 0) {
                Assert.assertNotNull(message.getData());
                Assert.assertNotNull(message.getValue());
                Assert.assertEquals(new String(message.getData()), "not null");
            } else {
                Assert.assertNull(message.getData());
                Assert.assertNull(message.getValue());
            }
            consumer.acknowledge(message);
        }

        for (int i = 0; i < numMessage; i++) {
            if (i % 2 == 0) {
                producer.newMessage().value("not null".getBytes()).sendAsync();
            } else {
                producer.newMessage().value(null).sendAsync();
            }
        }

        for (int i = 0; i < numMessage; i++) {
            CompletableFuture<Message> completableFuture = consumer.receiveAsync();
            final int index = i;
            completableFuture.whenComplete((message, throwable) -> {
                Assert.assertNull(throwable);
                if (index % 2 == 0) {
                    Assert.assertNotNull(message.getData());
                    Assert.assertNotNull(message.getValue());
                    Assert.assertEquals(new String(message.getData()), "not null");
                } else {
                    Assert.assertNull(message.getData());
                    Assert.assertNull(message.getValue());
                }
                try {
                    consumer.acknowledge(message);
                } catch (PulsarClientException e) {
                    Assert.assertNull(e);
                }
            });
        }

    }

    @Test(dataProvider = "topics")
    public void nullValueBooleanSchemaTest(String topic, int partitions)
            throws PulsarClientException, PulsarAdminException {
        admin.topics().createPartitionedTopic(topic, partitions);

        @Cleanup
        Producer<Boolean> producer = pulsarClient.newProducer(Schema.BOOL)
                .topic(topic)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        @Cleanup
        Consumer<Boolean> consumer = pulsarClient.newConsumer(Schema.BOOL)
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            producer.newMessage().value(null).sendAsync();
        }

        for (int i = 0; i < numMessage; i++) {
            Message<Boolean> message = consumer.receive();
            Assert.assertNull(message.getValue());
            Assert.assertNull(message.getData());
        }

    }

    @Test(dataProvider = "topics")
    public void keyValueNullInlineTest(String topic, int partitions)
            throws PulsarClientException, PulsarAdminException {
        admin.topics().createPartitionedTopic(topic, partitions);

        @Cleanup
        Producer<KeyValue<String, String>> producer = pulsarClient
                .newProducer(KeyValueSchemaImpl.of(Schema.STRING, Schema.STRING))
                .topic(topic)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        @Cleanup
        Consumer<KeyValue<String, String>> consumer = pulsarClient
                .newConsumer(KeyValueSchemaImpl.of(Schema.STRING, Schema.STRING))
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            producer.newMessage().value(new KeyValue<>(null, "test")).send();
            producer.newMessage().value(new KeyValue<>("test", null)).send();
            producer.newMessage().value(new KeyValue<>(null, null)).send();
        }

        Message<KeyValue<String, String>> message;
        KeyValue<String, String> keyValue;
        for (int i = 0; i < numMessage; i++) {
            message = consumer.receive();
            keyValue = message.getValue();
            Assert.assertNull(keyValue.getKey());
            Assert.assertEquals("test", keyValue.getValue());

            message = consumer.receive();
            keyValue = message.getValue();
            Assert.assertEquals("test", keyValue.getKey());
            Assert.assertNull(keyValue.getValue());

            message = consumer.receive();
            keyValue = message.getValue();
            Assert.assertNull(keyValue.getKey());
            Assert.assertNull(keyValue.getValue());
        }

    }

    @Test(dataProvider = "topics")
    public void keyValueNullSeparatedTest(String topic, int partitions)
            throws PulsarClientException, PulsarAdminException {
        admin.topics().createPartitionedTopic(topic, partitions);

        @Cleanup
        Producer<KeyValue<String, String>> producer = pulsarClient
                .newProducer(KeyValueSchemaImpl.of(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED))
                .topic(topic)
                // The default SinglePartition routing mode will be affected by the key when the KeyValueEncodingType is
                // SEPARATED so we need to define a message router to guarantee the message order.
                .messageRouter(new MessageRouter() {
                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return 0;
                    }
                })
                .create();

        @Cleanup
        Consumer<KeyValue<String, String>> consumer = pulsarClient
                .newConsumer(KeyValueSchemaImpl.of(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED))
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        int numMessage = 10;
        for (int i = 0; i < numMessage; i++) {
            producer.newMessage().value(new KeyValue<>(null, "test")).send();
            producer.newMessage().value(new KeyValue<>("test", null)).send();
            producer.newMessage().value(new KeyValue<>(null, null)).send();
        }

        Message<KeyValue<String, String>> message;
        KeyValue<String, String> keyValue;
        for (int i = 0; i < numMessage; i++) {
            message = consumer.receive();
            keyValue = message.getValue();
            Assert.assertNull(keyValue.getKey());
            Assert.assertEquals("test", keyValue.getValue());

            message = consumer.receive();
            keyValue = message.getValue();
            Assert.assertEquals("test", keyValue.getKey());
            Assert.assertNull(keyValue.getValue());

            message = consumer.receive();
            keyValue = message.getValue();
            Assert.assertNull(keyValue.getKey());
            Assert.assertNull(keyValue.getValue());
        }

    }

}
