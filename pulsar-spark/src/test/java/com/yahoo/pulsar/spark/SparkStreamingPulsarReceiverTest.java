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
package com.yahoo.pulsar.spark;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doNothing;

import com.yahoo.pulsar.client.api.*;
import org.mockito.ArgumentCaptor;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import com.yahoo.pulsar.spark.SparkStreamingPulsarReceiver;

public class SparkStreamingPulsarReceiverTest extends MockedPulsarServiceBaseTest {

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
        String url = "pulsar://127.0.0.1:" + BROKER_PORT + "/";
        String topic = "persistent://p1/c1/ns1/topic1";
        String subs = "sub1";

        SparkStreamingPulsarReceiver receiver = spy(
                new SparkStreamingPulsarReceiver(clientConf, consConf, url, topic, subs));
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
        PulsarClient pulsarClient = PulsarClient.create(url, clientConf);
        Producer producer = pulsarClient.createProducer(topic, new ProducerConfiguration());
        producer.send("pulsar-spark test message".getBytes());
        waitForTransmission();
        receiver.onStop();
        assertEquals(new String(msgCaptor.getValue().getData()), "pulsar-spark test message");
    }

    private static void waitForTransmission() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }
}
