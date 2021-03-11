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

package org.apache.pulsar.io.kafka.connect;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class KafkaSinkWrappingProducerTest extends ProducerConsumerBase  {

    private String offsetTopicName =  "persistent://my-property/my-ns/kafka-connect-sink-offset";

    private Path file;
    private Properties props;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        file = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

        props = new Properties();
        props.put("file", file.toString());
        props.put(PulsarKafkaWorkerConfig.PULSAR_SERVICE_URL_CONFIG, brokerUrl.toString());
        props.put(PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG, offsetTopicName);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (file != null) {
            //Files.delete(file);
        }

        super.internalCleanup();
    }

    @Test
    public void smokeTest() throws Exception {
        Producer<String, String> producer =
                KafkaSinkWrappingProducer.create(
                    "org.apache.kafka.connect.file.FileStreamSinkConnector",
                        props,
                        Schema.STRING_SCHEMA,
                        Schema.STRING_SCHEMA);

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

    }

    @Test
    public void bytesRecordWithSchemaTest() throws Exception {
        // configure with wrong schema, schema from ProducerRecordWithSchema should be used
        Producer<String, byte[]> producer =
                KafkaSinkWrappingProducer.create(
                        SchemaedFileStreamSinkConnector.class.getCanonicalName(),
                        props,
                        Schema.INT8_SCHEMA,
                        Schema.BOOLEAN_SCHEMA);

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
    }

    @Test
    public void bytesRecordWithOutSchemaTest() throws Exception {
        Schema keySchema = (Schema)Schema.class
                .getField("STRING_SCHEMA").get(null);
        Schema valueSchema = (Schema)Schema.class
                .getField("BYTES_SCHEMA").get(null);

        // configure with default schema
        Producer<String, byte[]> producer =
                KafkaSinkWrappingProducer.create(
                        SchemaedFileStreamSinkConnector.class.getCanonicalName(),
                        props,
                        keySchema,
                        valueSchema);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>("test",
                "key",
                "val".getBytes(StandardCharsets.US_ASCII));

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
    }

    @Test
    public void offsetTest() throws Exception {
        Producer<String, byte[]> producer =
                KafkaSinkWrappingProducer.create(
                        SchemaedFileStreamSinkConnector.class.getCanonicalName(),
                        props,
                        Schema.STRING_SCHEMA,
                        Schema.BYTES_SCHEMA);
        KafkaSinkWrappingProducer<String, byte[]> kswp = (KafkaSinkWrappingProducer<String, byte[]>)producer;
        // offset is -1 before any data is written
        assertEquals(-1L, kswp.currentOffset("test", 0));

        ProducerRecord<String, byte[]> record = new ProducerRecordWithSchema<>("test",
                "key",
                "val".getBytes(StandardCharsets.US_ASCII),
                Schema.STRING_SCHEMA,
                Schema.BYTES_SCHEMA);

        producer.send(record);
        producer.flush();
        // offset is 0 for the first written record
        assertEquals(0L, kswp.currentOffset("test", 0));

        producer.send(record);
        producer.flush();
        // offset is 1 for the second written record
        assertEquals(1L, kswp.currentOffset("test", 0));

        producer.close();

        // close the producer, open again
        producer = KafkaSinkWrappingProducer.create(
                        SchemaedFileStreamSinkConnector.class.getCanonicalName(),
                        props,
                        Schema.STRING_SCHEMA,
                        Schema.BYTES_SCHEMA);
        kswp = (KafkaSinkWrappingProducer<String, byte[]>)producer;
        // offset is 1 after reopening teh producer
        assertEquals(1L, kswp.currentOffset("test", 0));

        producer.send(record);
        producer.flush();
        // offset is 2 for the next written record
        assertEquals(2L, kswp.currentOffset("test", 0));

        producer.close();
    }
}
