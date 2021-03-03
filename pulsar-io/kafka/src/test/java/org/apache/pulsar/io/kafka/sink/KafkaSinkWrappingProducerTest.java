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

package org.apache.pulsar.io.kafka.sink;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.pulsar.io.kafka.KafkaAbstractSink;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class KafkaSinkWrappingProducerTest {

    @Test
    public void SmokeTest() throws Exception {
        Properties props = new Properties();
        Path file = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
        props.put("file", file.toString());
        Producer<String, String> producer =
                KafkaAbstractSink.createKafkaSinkWrappingProducer(
                    "org.apache.kafka.connect.file.FileStreamSinkConnector",
                        props,
                        Schema.STRING_SCHEMA,
                        Schema.STRING_SCHEMA
                );

        ProducerRecord<String, String> record = new ProducerRecord<>("test",
                "key", "value");

        final AtomicInteger status = new AtomicInteger(0);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                status.incrementAndGet();
            } else {
                System.out.println(exception.toString());
                exception.printStackTrace();

                status.decrementAndGet();
            }
        });

        assertEquals(1, status.get());

        producer.close();

        List<String> lines = Files.readAllLines(file, StandardCharsets.US_ASCII);
        assertEquals("value", lines.get(0));

        Files.delete(file);
    }

}
