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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.file.FileStreamSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.pulsar.io.kafka.KafkaAbstractSink;
import org.apache.pulsar.io.kafka.ProducerRecordWithSchema;
import org.junit.Test;
import org.testng.collections.Maps;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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

    @Test
    public void BytesRecordWithSchemaTest() throws Exception {
        Properties props = new Properties();
        Path file = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
        props.put("file", file.toString());
        // configure with wrong schema, schema from ProducerRecordWithSchema should be used
        Producer<String, byte[]> producer =
                KafkaAbstractSink.createKafkaSinkWrappingProducer(
                        SchemaedFileStreamSinkConnector.class.getCanonicalName(),
                        props,
                        Schema.INT8_SCHEMA,
                        Schema.BOOLEAN_SCHEMA
                );

        ProducerRecord<String, byte[]> record = new ProducerRecordWithSchema<>("test",
                "key",
                "val".getBytes(StandardCharsets.US_ASCII),
                Schema.STRING_SCHEMA,
                Schema.BYTES_SCHEMA);

        final AtomicInteger status = new AtomicInteger(0);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                status.incrementAndGet();
            } else {
                status.decrementAndGet();
            }
        });

        assertEquals(1, status.get());

        producer.close();

        List<String> lines = Files.readAllLines(file, StandardCharsets.US_ASCII);
        ObjectMapper om = new ObjectMapper();
        Map<String, Object> result = om.readValue(lines.get(0), new TypeReference<Map<String, Object>>(){});

        assertEquals("key", result.get("key"));
        assertEquals("val", result.get("value"));
        assertEquals("STRING", result.get("keySchema"));
        assertEquals("BYTES", result.get("valueSchema"));

        Files.delete(file);
    }


}
