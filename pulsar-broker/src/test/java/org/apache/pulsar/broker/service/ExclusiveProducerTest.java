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

import static org.testng.Assert.fail;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import lombok.Cleanup;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerFencedException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ExclusiveProducerTest extends BrokerTestBase {

    @BeforeClass
    protected void setup() throws Exception {
        baseSetup();
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @DataProvider(name = "topics")
    public static Object[][] topics() {
        return new Object[][] {
                { "persistent", Boolean.TRUE },
                { "persistent", Boolean.FALSE },
                { "non-persistent", Boolean.TRUE },
                { "non-persistent", Boolean.FALSE },
        };
    }

    @DataProvider(name = "partitioned")
    public static Object[][] partitioned() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Test(dataProvider = "topics")
    public void simpleTest(String type, boolean partitioned) throws Exception {
        String topic = newTopic(type, partitioned);

        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.Exclusive)
                .create();

        try {
            pulsarClient.newProducer(Schema.STRING)
                    .topic(topic)
                    .accessMode(ProducerAccessMode.Exclusive)
                    .create();
            fail("Should have failed");
        } catch (ProducerFencedException e) {
            // Expected
        }

        try {
            pulsarClient.newProducer(Schema.STRING)
                    .topic(topic)
                    .accessMode(ProducerAccessMode.Shared)
                    .create();
            fail("Should have failed");
        } catch (ProducerBusyException e) {
            // Expected
        }

        p1.close();

        // Now producer should be allowed to get in
        Producer<String> p2 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.Exclusive)
                .create();
        p2.close();
    }

    @Test(dataProvider = "topics")
    public void existingSharedProducer(String type, boolean partitioned) throws Exception {
        String topic = newTopic(type, partitioned);

        @Cleanup
        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.Shared)
                .create();

        try {
            pulsarClient.newProducer(Schema.STRING)
                    .topic(topic)
                    .accessMode(ProducerAccessMode.Exclusive)
                    .create();
            fail("Should have failed");
        } catch (ProducerFencedException e) {
            // Expected
        }
    }

    @Test(dataProvider = "topics")
    public void producerReconnection(String type, boolean partitioned) throws Exception {
        String topic = newTopic(type, partitioned);

        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.Exclusive)
                .create();

        p1.send("msg-1");

        admin.topics().unload(topic);

        p1.send("msg-2");
    }

    @Test(dataProvider = "partitioned")
    public void producerFenced(boolean partitioned) throws Exception {
        String topic = newTopic("persistent", partitioned);

        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .accessMode(ProducerAccessMode.Exclusive)
                .create();

        p1.send("msg-1");

        // Simulate a producer that takes over and fences p1 through the topic epoch
        if (!partitioned) {
            Topic t = pulsar.getBrokerService().getTopic(topic, false).get().get();
            CompletableFuture<?> f = (CompletableFuture<?>) Whitebox
                    .getMethod(AbstractTopic.class, "incrementTopicEpoch", Optional.class)
                    .invoke(t, Optional.of(0L));
            f.get();
        } else {
            for (int i = 0; i < 3; i++) {
                String name = TopicName.get(topic).getPartition(i).toString();
                Topic t = pulsar.getBrokerService().getTopic(name, false).get().get();
                CompletableFuture<?> f = (CompletableFuture<?>) Whitebox
                        .getMethod(AbstractTopic.class, "incrementTopicEpoch", Optional.class)
                        .invoke(t, Optional.of(0L));
                f.get();
            }
        }

        admin.topics().unload(topic);

        try {
            p1.send("msg-2");
            fail("Should have failed");
        } catch (ProducerFencedException e) {
            // Expected
        }
    }

    private String newTopic(String type, boolean isPartitioned) throws Exception {
        String topic = type + "://" + newTopicName();
        if (isPartitioned) {
            admin.topics().createPartitionedTopic(topic, 3);
        }

        return topic;
    }
}
