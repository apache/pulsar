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
package org.apache.pulsar.client.impl.schema;

import static org.testng.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DefaultSchemasTest {
    private PulsarClient client;

    private static final String TEST_TOPIC = "persistent://sample/standalone/ns1/test-topic";

    @BeforeClass
    public void setup() throws PulsarClientException {
        client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
    }

    @Test
    public void testConsumerInstantiation() {
        ConsumerBuilder<String> stringConsumerBuilder = client.newConsumer(new StringSchema())
                .topic(TEST_TOPIC);
        Assert.assertNotNull(stringConsumerBuilder);
    }

    @Test
    public void testProducerInstantiation() {
        ProducerBuilder<String> stringProducerBuilder = client.newProducer(new StringSchema())
                .topic(TEST_TOPIC);
        Assert.assertNotNull(stringProducerBuilder);
    }

    @Test
    public void testReaderInstantiation() {
        ReaderBuilder<String> stringReaderBuilder = client.newReader(new StringSchema())
                .topic(TEST_TOPIC);
        Assert.assertNotNull(stringReaderBuilder);
    }

    @Test
    public void testStringSchema() throws Exception {
        String testString = "hello world";
        byte[] testBytes = testString.getBytes(StandardCharsets.UTF_8);
        StringSchema stringSchema = new StringSchema();
        assertEquals(testString, stringSchema.decode(testBytes));
        assertEquals(stringSchema.encode(testString), testBytes);

         byte[] bytes2 = testString.getBytes(StandardCharsets.UTF_16);
        StringSchema stringSchemaUtf16 = new StringSchema(StandardCharsets.UTF_16);
        assertEquals(testString, stringSchemaUtf16.decode(bytes2));
        assertEquals(stringSchemaUtf16.encode(testString), bytes2);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws PulsarClientException {
        client.close();
    }
}
