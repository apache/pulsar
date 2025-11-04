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
package org.apache.pulsar.io.kafka.connect;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.connect.KafkaConnectSource.KafkaSourceRecord;
import org.awaitility.Awaitility;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test the implementation of {@link KafkaConnectSource}.
 */
@Slf4j
public class KafkaConnectSourceTest extends ProducerConsumerBase {

    private String offsetTopicName;
    // The topic to publish data to, for kafkaSource
    private String topicName;
    private KafkaConnectSource kafkaConnectSource;
    private File tempFile;
    private SourceContext context;
    private PulsarClient client;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        this.offsetTopicName = "persistent://my-property/my-ns/kafka-connect-source-offset";
        this.topicName = "persistent://my-property/my-ns/kafka-connect-source";
        tempFile = File.createTempFile("some-file-name", null);
        tempFile.deleteOnExit();

        this.context = mock(SourceContext.class);
        this.client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .build();
        when(context.getPulsarClient()).thenReturn(this.client);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (this.client != null) {
            this.client.close();
        }
        tempFile.delete();
        super.internalCleanup();
    }

    @Test
    public void testOpenAndReadConnectorConfig() throws Exception {
        Map<String, Object> config = getConfig();
        config.put(AbstractKafkaConnectSource.CONNECTOR_CLASS,
                "org.apache.kafka.connect.file.FileStreamSourceConnector");

        testOpenAndReadTask(config);
    }

    private OffsetStorageWriter wrapOffsetWriterWithSpy(KafkaConnectSource source) throws IllegalAccessException {
        OffsetStorageWriter original = source.getOffsetWriter();
        OffsetStorageWriter spy = org.mockito.Mockito.spy(original);
        FieldUtils.writeField(source, "offsetWriter", spy, true);
        return spy;
    }


    @Test(timeOut = 30000)
    public void testFlushWhenAllMessagesFilteredWithoutBlocking() throws Exception {

        Map<String, Object> config = getConfig();
        config.put(TaskConfig.TASK_CLASS_CONFIG, "org.apache.kafka.connect.file.FileStreamSourceTask");

        config.put("transforms", "Filter");
        config.put("transforms.Filter.type", "org.apache.kafka.connect.transforms.Filter");
        config.put("transforms.Filter.predicate", "DropMeTopic");

        config.put("predicates", "DropMeTopic");
        config.put("predicates.DropMeTopic.type", "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");
        config.put("predicates.DropMeTopic.pattern", ".*my-property/my-ns/kafka-connect-source.*");

        kafkaConnectSource = new KafkaConnectSource();
        kafkaConnectSource.open(config, context);

        OffsetStorageWriter spyWriter = wrapOffsetWriterWithSpy(kafkaConnectSource);

        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write("first\n".getBytes());
            os.flush();
            os.write("second\n".getBytes());
            os.flush();
        }

        Thread t = new Thread(() -> {
            try {
                kafkaConnectSource.read();
            } catch (Exception e) {
                // Ignore, it just needs the loop running through a batch where every message is filtered out
                log.error("Exception in read thread", e);
            }
        });
        t.setDaemon(true);
        t.start();

        try {
            // First ensure offsets are being written for filtered records.
            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        Mockito.verify(spyWriter, Mockito.atLeastOnce())
                                .offset(ArgumentMatchers.any(), ArgumentMatchers.any());
                    });

            // Then ensure a flush cycle is triggered without waiting for any acks (since all are filtered).
            Awaitility.await()
                    .atMost(java.time.Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        Mockito.verify(spyWriter, Mockito.atLeastOnce())
                                .beginFlush();
                        Mockito.verify(spyWriter, Mockito.atLeastOnce())
                                .doFlush(ArgumentMatchers.any());
                    });
        } finally {
            kafkaConnectSource.close();
            t.interrupt();
        }
    }

    @Test(timeOut = 40000)
    public void testNoFlushUntilAck() throws Exception {
        Map<String, Object> config = getConfig();
        config.put(TaskConfig.TASK_CLASS_CONFIG, "org.apache.kafka.connect.file.FileStreamSourceTask");

        kafkaConnectSource = new KafkaConnectSource();
        kafkaConnectSource.open(config, context);

        OffsetStorageWriter spyWriter = wrapOffsetWriterWithSpy(kafkaConnectSource);

        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write("one\n".getBytes());
            os.flush();
        }

        Record<KeyValue<byte[], byte[]>> readRecord = kafkaConnectSource.read();

        // Verify no flush happens while there is one outstanding record (no ack yet)
        verify(spyWriter, timeout(10000).times(0)).beginFlush();
        verify(spyWriter, timeout(10000).times(0))
                .doFlush(ArgumentMatchers.any());

        readRecord.ack();

        // Verify flush happens after ack
        verify(spyWriter, timeout(10000).atLeastOnce()).beginFlush();
        verify(spyWriter, timeout(10000).atLeastOnce())
                .doFlush(ArgumentMatchers.any());

        kafkaConnectSource.close();
    }

    @Test(timeOut = 40000)
    public void testPartialAckNoFlush_ThenFlushOnAllAck() throws Exception {
        Map<String, Object> config = getConfig();
        config.put(TaskConfig.TASK_CLASS_CONFIG, "org.apache.kafka.connect.file.FileStreamSourceTask");

        kafkaConnectSource = new KafkaConnectSource();
        kafkaConnectSource.open(config, context);

        OffsetStorageWriter spyWriter = wrapOffsetWriterWithSpy(kafkaConnectSource);

        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write("first\n".getBytes());
            os.flush();
            os.write("second\n".getBytes());
            os.flush();
        }

        Record<KeyValue<byte[], byte[]>> r1 = kafkaConnectSource.read();
        Record<KeyValue<byte[], byte[]>> r2 = kafkaConnectSource.read();

        // Ack only the first; one outstanding remains -> no flush should happen
        r1.ack();
        verify(spyWriter, timeout(10000).times(0)).beginFlush();
        verify(spyWriter, timeout(10000).times(0))
                .doFlush(ArgumentMatchers.any());

        // Ack the second; outstanding reaches zero -> flush should be triggered
        r2.ack();
        verify(spyWriter, timeout(10000).atLeastOnce()).beginFlush();
        verify(spyWriter, timeout(10000).atLeastOnce())
                .doFlush(ArgumentMatchers.any());

        kafkaConnectSource.close();
    }

    @Test(timeOut = 20000)
    public void testAckFlush() throws Exception {
        Map<String, Object> config = getConfig();
        config.put(TaskConfig.TASK_CLASS_CONFIG, "org.apache.kafka.connect.file.FileStreamSourceTask");

        kafkaConnectSource = new KafkaConnectSource();
        kafkaConnectSource.open(config, context);

        OffsetStorageWriter spyWriter = wrapOffsetWriterWithSpy(kafkaConnectSource);

        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write("first\n".getBytes());
            os.flush();
            os.write("second\n".getBytes());
            os.flush();
        }

        Record<KeyValue<byte[], byte[]>> r1 = kafkaConnectSource.read();
        Record<KeyValue<byte[], byte[]>> r2 = kafkaConnectSource.read();

        // Ack both, no outstanding remains -> flush should happen
        r1.ack();
        r2.ack();

        verify(spyWriter, timeout(10000).atLeastOnce()).beginFlush();
        verify(spyWriter, timeout(10000).atLeastOnce())
                .doFlush(ArgumentMatchers.any());

        kafkaConnectSource.close();
    }


    @Test
    public void testOpenAndReadTaskDirect() throws Exception {
        Map<String, Object> config = getConfig();

        config.put(TaskConfig.TASK_CLASS_CONFIG,
                "org.apache.kafka.connect.file.FileStreamSourceTask");

        testOpenAndReadTask(config);
    }

    @Test
    void testTransformation() throws Exception {
        Map<String, Object> config = setupTransformConfig(false, false);
        runTransformTest(config, true);
    }

    @Test
    void testTransformationWithPredicate() throws Exception {
        Map<String, Object> config = setupTransformConfig(true, false);
        runTransformTest(config, true);
    }

    @Test
    void testTransformationWithNegatedPredicate() throws Exception {
        Map<String, Object> config = setupTransformConfig(true, true);
        runTransformTest(config, false);
    }

    @Test
    void testShortTopicNames() throws Exception {
        Map<String, Object> config = getConfig();
        config.put(TaskConfig.TASK_CLASS_CONFIG, "org.apache.kafka.connect.file.FileStreamSourceTask");
        config.put(PulsarKafkaWorkerConfig.TOPIC_NAMESPACE_CONFIG, "default-tenant/default-ns");

        runTopicNameTest(config, "a-topic", "persistent://default-tenant/default-ns/a-topic");
    }

    @Test
    void testFullyQualifiedTopicNames() throws Exception {
        Map<String, Object> config = getConfig();
        config.put(TaskConfig.TASK_CLASS_CONFIG, "org.apache.kafka.connect.file.FileStreamSourceTask");
        config.put(PulsarKafkaWorkerConfig.TOPIC_NAMESPACE_CONFIG, "default-tenant/default-ns");

        runTopicNameTest(config, "persistent://a-tenant/a-ns/a-topic", "persistent://a-tenant/a-ns/a-topic");
    }

    private void runTopicNameTest(Map<String, Object> config, String topicName, String expectedDestinationTopicName)
            throws Exception {
        config.put(TaskConfig.TASK_CLASS_CONFIG, "org.apache.kafka.connect.file.FileStreamSourceTask");
        config.put(PulsarKafkaWorkerConfig.TOPIC_NAMESPACE_CONFIG, "default-tenant/default-ns");

        kafkaConnectSource = new KafkaConnectSource();
        kafkaConnectSource.open(config, context);

        Map<String, Object> sourcePartition = new HashMap<>();
        Map<String, Object> sourceOffset = new HashMap<>();
        Map<String, Object> value = new HashMap<>();
        sourcePartition.put("test", "test");
        sourceOffset.put("test", 0);
        value.put("myField", "42");
        SourceRecord srcRecord = new SourceRecord(
                sourcePartition, sourceOffset, topicName, null,
                null, null, null, value
        );

        KafkaSourceRecord record = kafkaConnectSource.processSourceRecord(srcRecord);

        assertEquals(Optional.of(expectedDestinationTopicName), record.destinationTopic);
    }

    private Map<String, Object> setupTransformConfig(boolean withPredicate, boolean negated) {
        Map<String, Object> config = getConfig();
        config.put(TaskConfig.TASK_CLASS_CONFIG, "org.apache.kafka.connect.file.FileStreamSourceTask");

        if (withPredicate) {
            config.put("predicates", "TopicMatch");
            config.put("predicates.TopicMatch.type", "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");
            config.put("predicates.TopicMatch.pattern", "test-topic");
        }

        config.put("transforms", "Cast");
        config.put("transforms.Cast.type", "org.apache.kafka.connect.transforms.Cast$Value");
        config.put("transforms.Cast.spec", "myField:int32");

        if (withPredicate) {
            config.put("transforms.Cast.predicate", "TopicMatch");
            if (negated) {
                config.put("transforms.Cast.negate", "true");
            }
        }

        return config;
    }

    private void runTransformTest(Map<String, Object> config, boolean expectTransformed) throws Exception {
        kafkaConnectSource = new KafkaConnectSource();
        kafkaConnectSource.open(config, context);

        Map<String, Object> value = new HashMap<>();
        value.put("myField", "42");
        SourceRecord record = new SourceRecord(
                null, null, "test-topic", null,
                null, null, null, value
        );

        SourceRecord transformed = kafkaConnectSource.applyTransforms(record);

        @SuppressWarnings("unchecked")
        Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();
        assertNotNull(transformedValue);

        if (expectTransformed) {
            assertEquals(42, ((Number) transformedValue.get("myField")).intValue());
            assertTrue(transformedValue.get("myField") instanceof Number);
        } else {
            assertEquals("42", transformedValue.get("myField"));
            assertTrue(transformedValue.get("myField") instanceof String);
        }
    }

    private Map<String, Object> getConfig() {
        Map<String, Object> config = new HashMap<>();

        config.put(PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG,
                "org.apache.kafka.connect.storage.StringConverter");
        config.put(PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG,
                "org.apache.kafka.connect.storage.StringConverter");

        config.put(PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG, offsetTopicName);

        config.put(FileStreamSourceConnector.TOPIC_CONFIG, topicName);
        config.put(FileStreamSourceConnector.FILE_CONFIG, tempFile.getAbsoluteFile().toString());
        config.put(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG,
                String.valueOf(FileStreamSourceConnector.DEFAULT_TASK_BATCH_SIZE));
        return config;
    }

    private void testOpenAndReadTask(Map<String, Object> config) throws Exception {
        kafkaConnectSource = new KafkaConnectSource();
        kafkaConnectSource.open(config, context);

        // use FileStreamSourceConnector, each line is a record, need "\n" and end of each record.
        OutputStream os = Files.newOutputStream(tempFile.toPath());

        String line1 = "This is the first line\n";
        os.write(line1.getBytes());
        os.flush();
        log.info("write 2 lines.");

        String line2 = "This is the second line\n";
        os.write(line2.getBytes());
        os.flush();

        log.info("finish write, will read 2 lines");

        // Note: FileStreamSourceTask read the whole line as Value, and set Key as null.
        Record<KeyValue<byte[], byte[]>> record = kafkaConnectSource.read();
        String readBack1 = new String(record.getValue().getValue());
        assertTrue(line1.contains(readBack1));
        assertNull(record.getValue().getKey());
        log.info("read line1: {}", readBack1);
        record.ack();

        record = kafkaConnectSource.read();
        String readBack2 = new String(record.getValue().getValue());
        assertTrue(line2.contains(readBack2));
        assertNull(record.getValue().getKey());
        assertTrue(record.getPartitionId().isPresent());
        assertFalse(record.getPartitionIndex().isPresent());
        log.info("read line2: {}", readBack2);
        record.ack();

        String line3 = "This is the 3rd line\n";
        os.write(line3.getBytes());
        os.flush();

        record = kafkaConnectSource.read();
        String readBack3 = new String(record.getValue().getValue());
        assertTrue(line3.contains(readBack3));
        assertNull(record.getValue().getKey());
        log.info("read line3: {}", readBack3);
        record.ack();
    }
}
