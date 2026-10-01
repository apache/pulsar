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
package org.apache.pulsar.client.api;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class BytesKeyTest extends SharedPulsarBaseTest {

    private void byteKeysTest(boolean batching) throws Exception {
        Random r = new Random(0);
        String topic = newTopicName();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
            .topic(topic)
            .subscriptionName("my-subscriber-name").subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
            .enableBatching(batching)
            .batchingMaxPublishDelay(Long.MAX_VALUE, TimeUnit.SECONDS)
            .batchingMaxMessages(Integer.MAX_VALUE)
            .topic(topic).create();

        byte[] byteKey = new byte[1000];
        r.nextBytes(byteKey);
        producer.newMessage().keyBytes(byteKey).value("TestMessage").sendAsync();
        producer.flush();

        Message<String> m = consumer.receive();
        Assert.assertEquals(m.getValue(), "TestMessage");
        Assert.assertEquals(m.getKeyBytes(), byteKey);
        Assert.assertTrue(m.hasBase64EncodedKey());
    }

    @Test
    public void testBytesKeyBatch() throws Exception {
        byteKeysTest(true);
    }

    @Test
    public void testBytesKeyNoBatch() throws Exception {
        byteKeysTest(false);
    }
}
