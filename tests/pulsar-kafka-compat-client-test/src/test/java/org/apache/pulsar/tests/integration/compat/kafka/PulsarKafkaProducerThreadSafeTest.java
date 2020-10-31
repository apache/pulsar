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
package org.apache.pulsar.tests.integration.compat.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import java.util.Properties;
/**
 * A test that tests if {@link PulsarKafkaProducer} is thread safe.
 */
public class PulsarKafkaProducerThreadSafeTest extends PulsarStandaloneTestSuite {
    private Producer producer;

    private static String getPlainTextServiceUrl() {
        return container.getPlainTextServiceUrl();
    }

    @BeforeTest
    private void setup() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", getPlainTextServiceUrl());
        producerProperties.put("key.serializer", IntegerSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProperties);
    }

    /**
     * This test run 10 times in threadPool witch size is 5.
     * Different threads have same producer and different topics witch is based on thread time.
     * This test will be failed when producer failed to send if PulsarKafkaProducer is not thread safe.
     */
    @Test(threadPoolSize = 5, invocationCount = 10)
    public void testPulsarKafkaProducerThreadSafe() {
        String topic1 = "persistent://public/default/topic-" + System.currentTimeMillis();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic1, "Hello");
        producer.send(record);
    }
}
