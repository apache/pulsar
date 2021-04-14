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

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import lombok.Data;

import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class NonPersistentTopicE2ETest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Data
    private static class Foo {
        private String field1;
        private String field2;
        private int field3;
    }

    private Optional<Topic> getTopic(String topicName) {
        return pulsar.getBrokerService().getTopicReference(topicName);
    }

    private boolean topicHasSchema(String topicName) {
        String base = TopicName.get(topicName).getPartitionedTopicName();
        String schemaName = TopicName.get(base).getSchemaName();
        SchemaRegistry.SchemaAndMetadata result = pulsar.getSchemaRegistryService().getSchema(schemaName).join();
        return result != null && !result.schema.isDeleted();
    }

    @Test(groups = "broker")
    public void testGCWillDeleteSchema() throws Exception {
        // 1. Simple successful GC
        String topicName = "non-persistent://prop/ns-abc/topic-1";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();

        Optional<Topic> topic = getTopic(topicName);
        assertTrue(topic.isPresent());

        byte[] data = JSONSchema.of(SchemaDefinition.builder()
                .withPojo(Foo.class).build()).getSchemaInfo().getSchema();
        SchemaData schemaData = SchemaData.builder()
                .data(data)
                .type(SchemaType.BYTES)
                .user("foo").build();
        topic.get().addSchema(schemaData).join();
        assertTrue(topicHasSchema(topicName));
        runGC();

        topic = getTopic(topicName);
        assertFalse(topic.isPresent());
        assertFalse(topicHasSchema(topicName));

        // 1a. Topic that add/removes subscription can be GC'd
        topicName = "non-persistent://prop/ns-abc/topic-1a";
        String subName = "sub1";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        topic = getTopic(topicName);
        assertTrue(topic.isPresent());
        topic.get().addSchema(schemaData).join();
        assertTrue(topicHasSchema(topicName));

        admin.topics().deleteSubscription(topicName, subName);
        consumer.close();

        runGC();
        topic = getTopic(topicName);
        assertFalse(topic.isPresent());
        assertFalse(topicHasSchema(topicName));

        // 2. Topic is not GCed with live connection
        topicName = "non-persistent://prop/ns-abc/topic-2";
        subName = "sub1";
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        topic = getTopic(topicName);
        assertTrue(topic.isPresent());
        topic.get().addSchema(schemaData).join();
        assertTrue(topicHasSchema(topicName));

        runGC();
        topic = getTopic(topicName);
        assertTrue(topic.isPresent());
        assertTrue(topicHasSchema(topicName));

        // 3. Topic with subscription is not GCed even with no connections
        consumer.close();

        runGC();
        topic = getTopic(topicName);
        assertTrue(topic.isPresent());
        assertTrue(topicHasSchema(topicName));

        // 4. Topic can be GCed after unsubscribe
        admin.topics().deleteSubscription(topicName, subName);

        runGC();
        topic = getTopic(topicName);
        assertFalse(topic.isPresent());
        assertFalse(topicHasSchema(topicName));
    }

    @Test(groups = "broker")
    public void testPatternTopic() throws PulsarClientException, InterruptedException {
        final String topic1 = "non-persistent://prop/ns-abc/testPatternTopic1-" + UUID.randomUUID().toString();
        final String topic2 = "non-persistent://prop/ns-abc/testPatternTopic2-" + UUID.randomUUID().toString();
        Pattern pattern = Pattern.compile("prop/ns-abc/test.*");
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topicsPattern(pattern)
                .subscriptionName("my-sub")
                .patternAutoDiscoveryPeriod(1, TimeUnit.SECONDS)
                .subscriptionTopicsMode(RegexSubscriptionMode.AllTopics)
                .subscribe();

        Producer<String> producer1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic1)
                .create();

        Producer<String> producer2 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic2)
                .create();

        Thread.sleep(2000);
        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer1.send("Message sent by producer-1 -> " + i);
            producer2.send("Message sent by producer-2 -> " + i);
        }

        for (int i = 0; i < messages * 2; i++) {
            Message<String> received = consumer.receive(3, TimeUnit.SECONDS);
            Assert.assertNotNull(received);
        }

        consumer.close();
        producer1.close();
        producer2.close();
    }

    @Test(groups = "broker")
    public void testGC() throws Exception {
        // 1. Simple successful GC
        String topicName = "non-persistent://prop/ns-abc/topic-10";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();

        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
        runGC();
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // 2. Topic is not GCed with live connection
        String subName = "sub1";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();

        runGC();
        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // 3. Topic with subscription is not GCed even with no connections
        consumer.close();

        runGC();
        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // 4. Topic can be GCed after unsubscribe
        admin.topics().deleteSubscription(topicName, subName);

        runGC();
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
        // 5. Get the topic and make sure it doesn't come back
        admin.lookups().lookupTopic(topicName);
        Optional<Topic> topic = pulsar.getBrokerService().getTopicIfExists(topicName).join();
        assertFalse(topic.isPresent());
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // write again, the topic will be available
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName).create();
        producer2.close();

        assertTrue(pulsar.getBrokerService().getTopicReference(topicName).isPresent());
    }
}
