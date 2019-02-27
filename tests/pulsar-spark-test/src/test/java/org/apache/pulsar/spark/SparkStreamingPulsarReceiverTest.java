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

import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
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
        ClientConfiguration clientConf = new ClientConfiguration();
        ConsumerConfiguration consConf = new ConsumerConfiguration();

        SparkStreamingPulsarReceiver receiver = spy(
                new SparkStreamingPulsarReceiver(clientConf, consConf, serviceUrl, TOPIC, SUBS));
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

        receiver.onStart();
        waitForTransmission();
        PulsarClient pulsarClient = PulsarClient.create(serviceUrl, clientConf);
        Producer producer = pulsarClient.createProducer(TOPIC, new ProducerConfiguration());
        producer.send(EXPECTED_MESSAGE.getBytes());
        waitForTransmission();
        receiver.onStop();
        assertEquals(new String(msgCaptor.getValue().getData()), EXPECTED_MESSAGE);
    }


    @Test(dataProvider = "ServiceUrls")
    public void testDefaultSettingsOfReceiver(String serviceUrl) throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration();
        ConsumerConfiguration consConf = new ConsumerConfiguration();
        SparkStreamingPulsarReceiver receiver =
                new SparkStreamingPulsarReceiver(clientConf, consConf, serviceUrl, TOPIC, SUBS);
        assertEquals(receiver.storageLevel(), StorageLevel.MEMORY_AND_DISK_2());
        assertEquals(consConf.getAckTimeoutMillis(), 60_000);
        assertNotNull(consConf.getMessageListener());
    }

    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "ClientConfiguration must not be null",
            dataProvider = "ServiceUrls")
    public void testReceiverWhenClientConfigurationIsNull(String serviceUrl) {
        new SparkStreamingPulsarReceiver(null, new ConsumerConfiguration(), serviceUrl, TOPIC, SUBS);
    }

    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "ConsumerConfiguration must not be null",
            dataProvider = "ServiceUrls")
    public void testReceiverWhenConsumerConfigurationIsNull(String serviceUrl) {
        new SparkStreamingPulsarReceiver(new ClientConfiguration(), null, serviceUrl, TOPIC, SUBS);
    }

    private static void waitForTransmission() {
        try {
            Thread.sleep(1_000);
        } catch (InterruptedException e) {
        }
    }
}
