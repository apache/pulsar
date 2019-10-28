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
package org.apache.pulsar.spark;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.spark.storage.StorageLevel;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class SparkStreamingPulsarReceiverTest extends PulsarTestSuite {
    private static final String TOPIC = "persistent://p1/c1/ns1/topic1";
    private static final String SUBS = "sub1";
    private static final String EXPECTED_MESSAGE = "pulsar-spark test message";

    @Test(dataProvider = "ServiceUrls")
    public void testReceivedMessage(String serviceUrl) throws Exception {
        ConsumerConfigurationData<byte[]> consConf = new ConsumerConfigurationData<>();

        Set<String> set = new HashSet<>();
        set.add(TOPIC);
        consConf.setTopicNames(set);
        consConf.setSubscriptionName(SUBS);

        MessageListener msgListener = spy(new MessageListener() {
            @Override
            public void received(Consumer consumer, Message msg) {
                return;
            }
        });
        final ArgumentCaptor<Consumer> consCaptor = ArgumentCaptor.forClass(Consumer.class);
        final ArgumentCaptor<Message> msgCaptor = ArgumentCaptor.forClass(Message.class);
        doNothing().when(msgListener).received(consCaptor.capture(), msgCaptor.capture());
        consConf.setMessageListener(msgListener);

        SparkStreamingPulsarReceiver receiver = new SparkStreamingPulsarReceiver(
            serviceUrl,
            consConf,
            new AuthenticationDisabled());

        receiver.onStart();
        waitForTransmission();

        PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
        Producer<byte[]> producer = client.newProducer().topic(TOPIC).create();
        producer.send(EXPECTED_MESSAGE.getBytes());

        waitForTransmission();
        receiver.onStop();
        assertEquals(new String(msgCaptor.getValue().getData()), EXPECTED_MESSAGE);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testDefaultSettingsOfReceiver(String serviceUrl) {
        ConsumerConfigurationData<byte[]> consConf = new ConsumerConfigurationData<>();

        Set<String> set = new HashSet<>();
        set.add(TOPIC);
        consConf.setTopicNames(set);
        consConf.setSubscriptionName(SUBS);

        SparkStreamingPulsarReceiver receiver = new SparkStreamingPulsarReceiver(
            serviceUrl,
            consConf,
            new AuthenticationDisabled());

        assertEquals(receiver.storageLevel(), StorageLevel.MEMORY_AND_DISK_2());
        assertNotNull(consConf.getMessageListener());
    }

    @Test(dataProvider = "ServiceUrls")
    public void testSharedSubscription(String serviceUrl) throws Exception {
        ConsumerConfigurationData<byte[]> consConf = new ConsumerConfigurationData<>();

        Set<String> set = new HashSet<>();
        set.add(TOPIC);
        consConf.setTopicNames(set);
        consConf.setSubscriptionName(SUBS);
        consConf.setSubscriptionType(SubscriptionType.Shared);
        consConf.setReceiverQueueSize(1);

        Map<String, MutableInt> receveidCounts = new HashMap<>();

        consConf.setMessageListener((consumer, msg) -> {
            receveidCounts.computeIfAbsent(consumer.getConsumerName(), x -> new MutableInt(0)).increment();
        });

        SparkStreamingPulsarReceiver receiver1 = new SparkStreamingPulsarReceiver(
            serviceUrl,
            consConf,
            new AuthenticationDisabled());

        SparkStreamingPulsarReceiver receiver2 = new SparkStreamingPulsarReceiver(
                serviceUrl,
                consConf,
                new AuthenticationDisabled());

        receiver1.onStart();
        receiver2.onStart();
        waitForTransmission();

        PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
        Producer<byte[]> producer = client.newProducer().topic(TOPIC).create();
        for (int i = 0; i < 10; i++) {
            producer.send(EXPECTED_MESSAGE.getBytes());
        }

        waitForTransmission();
        receiver1.onStop();
        receiver2.onStop();

        assertEquals(receveidCounts.size(), 2);
    }

    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "ConsumerConfigurationData must not be null",
            dataProvider = "ServiceUrls")
    public void testReceiverWhenClientConfigurationIsNull(String serviceUrl) {
        new SparkStreamingPulsarReceiver(serviceUrl, null, new AuthenticationDisabled());
    }

    private static void waitForTransmission() {
        try {
            Thread.sleep(1_000);
        } catch (InterruptedException e) {
        }
    }
}
