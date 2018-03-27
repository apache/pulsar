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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doNothing;

import org.apache.pulsar.client.api.*;
import org.apache.spark.storage.StorageLevel;
import org.mockito.ArgumentCaptor;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;

public class SparkStreamingPulsarReceiverTest extends MockedPulsarServiceBaseTest {

    private final String URL = "pulsar://127.0.0.1:" + BROKER_PORT + "/";
    private static final String TOPIC = "persistent://p1/c1/ns1/topic1";
    private static final String SUBS = "sub1";
    private static final String EXPECTED_MESSAGE = "pulsar-spark test message";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void testReceivedMessage() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration();
        ConsumerConfiguration consConf = new ConsumerConfiguration();

        SparkStreamingPulsarReceiver receiver = spy(
                new SparkStreamingPulsarReceiver(clientConf, consConf, URL, TOPIC, SUBS));
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
        PulsarClient pulsarClient = PulsarClient.create(URL, clientConf);
        Producer producer = pulsarClient.createProducer(TOPIC, new ProducerConfiguration());
        producer.send(EXPECTED_MESSAGE.getBytes());
        waitForTransmission();
        receiver.onStop();
        assertEquals(new String(msgCaptor.getValue().getData()), EXPECTED_MESSAGE);
    }

    @Test
    public void testDefaultSettingsOfReceiver() {
        ClientConfiguration clientConf = new ClientConfiguration();
        ConsumerConfiguration consConf = new ConsumerConfiguration();
        SparkStreamingPulsarReceiver receiver =
                new SparkStreamingPulsarReceiver(clientConf, consConf, URL, TOPIC, SUBS);
        assertEquals(receiver.storageLevel(), StorageLevel.MEMORY_AND_DISK_2());
        assertEquals(consConf.getAckTimeoutMillis(), 60_000);
        assertNotNull(consConf.getMessageListener());
    }

    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "ClientConfiguration must not be null")
    public void testReceiverWhenClientConfigurationIsNull() {
        new SparkStreamingPulsarReceiver(null, new ConsumerConfiguration(), URL, TOPIC, SUBS);
    }

    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "ConsumerConfiguration must not be null")
    public void testReceiverWhenConsumerConfigurationIsNull() {
        new SparkStreamingPulsarReceiver(new ClientConfiguration(), null, URL, TOPIC, SUBS);
    }

    private static void waitForTransmission() {
        try {
            Thread.sleep(1_000);
        } catch (InterruptedException e) {
        }
    }
}
