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
package org.apache.pulsar.client.kafka.compat.tests;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class KafkaApiTest extends BrokerTestBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testSimpleProducerConsumer() throws Exception {
        String topic = "persistent://sample/standalone/ns/testSimpleProducerConsumer";

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", brokerUrl.toString());
        producerProperties.put("key.serializer", IntegerSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());
        Producer<Integer, String> producer = new KafkaProducer<>(producerProperties);

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", brokerUrl.toString());
        consumerProperties.put("group.id", "my-subscription-name");
        consumerProperties.put("key.deserializer", IntegerDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("enable.auto.commit", "true");
        Consumer<Integer, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Arrays.asList(topic));

        List<Long> offsets = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            RecordMetadata md = producer.send(new ProducerRecord<Integer, String>(topic, i, "hello-" + i)).get();
            offsets.add(md.offset());
            log.info("Published message at {}", Long.toHexString(md.offset()));
        }

        producer.flush();
        producer.close();

        for (int i = 0; i < 10; i++) {
            ConsumerRecords<Integer, String> records = consumer.poll(1000);
            assertEquals(records.count(), 1);

            int idx = i;
            records.forEach(record -> {
                log.info("Received record: {}", record);
                assertEquals(record.key().intValue(), idx);
                assertEquals(record.value(), "hello-" + idx);
                assertEquals(record.offset(), offsets.get(idx).longValue());
            });
        }

        consumer.close();
    }

    private static final Logger log = LoggerFactory.getLogger(KafkaApiTest.class);
}
