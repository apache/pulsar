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
package org.apache.pulsar.client.api;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.assertEquals;

public class SimpleSchemaTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testString() throws Exception {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic1").subscriptionName("my-subscriber-name").subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic1").create();

        int N = 10;

        for (int i = 0; i < N; i++) {
            producer.send("my-message-" + i);
        }

        for (int i = 0; i < N; i++) {
            Message<String> msg = consumer.receive();
            assertEquals(msg.getValue(), "my-message-" + i);

            consumer.acknowledge(msg);
        }
    }

    @Test
    public void testByteBuffer() throws Exception {
        Consumer<ByteBuffer> consumer = pulsarClient.newConsumer(Schema.BYTE_BUFFER)
                .topic("persistent://my-property/my-ns/my-topic1").subscriptionName("my-subscriber-name").subscribe();
        Producer<ByteBuffer> producer = pulsarClient.newProducer(Schema.BYTE_BUFFER)
                .topic("persistent://my-property/my-ns/my-topic1").create();

        int N = 10;

        for (int i = 0; i < N; i++) {
            byte[] data = String.format("my-message-%d", i).getBytes();
            producer.send(ByteBuffer.wrap(data));
        }

        for (int i = 0; i < N; i++) {
            Message<ByteBuffer> msg = consumer.receive();
            assertEquals(msg.getValue().array(), String.format("my-message-%d", i).getBytes());
            consumer.acknowledge(msg);
        }
    }
}
