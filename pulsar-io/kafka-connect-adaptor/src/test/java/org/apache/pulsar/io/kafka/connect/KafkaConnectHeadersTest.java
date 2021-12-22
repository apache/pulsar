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

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.SourceContext;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;


@Slf4j
public class KafkaConnectHeadersTest extends ProducerConsumerBase {
    private Map<String, Object> config = new HashMap<>();
    private String sourceOffsetTopicName;
    // The topic to publish data to, for kafkaSource
    private String topicName;
    private KafkaConnectSource kafkaConnectSource;
    private File tempFile;
    private SourceContext sourceContext;
    private PulsarClient client;

    private String sinkOffsetTopicName = "persistent://my-property/my-ns/kafka-connect-sink-offset";

    private Path file;
    private Map<String, Object> props;
    private SinkContext sinkContext;


    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        config.put(TaskConfig.TASK_CLASS_CONFIG,
                "org.apache.kafka.connect.file.FileStreamSourceTask");
        config.put(PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG,
                "org.apache.kafka.connect.storage.StringConverter");
        config.put(PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG,
                "org.apache.pulsar.io.kafka.connect.StringConverterWithHeaders");

        this.sourceOffsetTopicName = "persistent://my-property/my-ns/kafka-connect-source-offset";
        config.put(PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG, sourceOffsetTopicName);

        this.topicName = "persistent://my-property/my-ns/kafka-connect-source";
        config.put(FileStreamSourceConnector.TOPIC_CONFIG, topicName);
        tempFile = File.createTempFile("some-file-name", null);
        config.put(FileStreamSourceConnector.FILE_CONFIG, tempFile.getAbsoluteFile().toString());
        config.put(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG,
                String.valueOf(FileStreamSourceConnector.DEFAULT_TASK_BATCH_SIZE));

        this.sourceContext = mock(SourceContext.class);

        file = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

        props = Maps.newHashMap();
        props.put("topic", "test-topic");
        props.put("offsetStorageTopic", sinkOffsetTopicName);
        props.put("kafkaConnectorSinkClass",
                "org.apache.kafka.connect.file.FileStreamSinkConnector");

        Map<String, String> kafkaConnectorProps = Maps.newHashMap();
        kafkaConnectorProps.put("file", file.toString());
        props.put("kafkaConnectorConfigProperties", kafkaConnectorProps);

        this.sinkContext = mock(SinkContext.class);

        this.client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .build();
        when(sinkContext.getSubscriptionType()).thenReturn(SubscriptionType.Failover);
        when(sinkContext.getPulsarClient()).thenReturn(this.client);
        when(sourceContext.getPulsarClient()).thenReturn(this.client);

    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (this.client != null) {
            this.client.close();
        }
        tempFile.delete();

        if (file != null && Files.exists(file)) {
            Files.delete(file);
        }

        super.internalCleanup();
    }

    protected void completedFlush(Throwable error, Void result) {
        if (error != null) {
            log.error("Failed to flush {} offsets to storage: ", this, error);
        } else {
            log.info("Finished flushing {} offsets to storage", this);
        }
    }

    @Test
    public void testSourceOpenAndRead() throws Exception {
        kafkaConnectSource = new KafkaConnectSource();
        kafkaConnectSource.open(config, sourceContext);

        // use FileStreamSourceConnector, each line is a record, need "\n" and end of each record.
        OutputStream os = Files.newOutputStream(tempFile.toPath());

        String line1 = "This is the first line\n";
        os.write(line1.getBytes());
        os.flush();

        // Note: FileStreamSourceTask read the whole line as Value, and set Key as null.
        Record<KeyValue<byte[], byte[]>> record = kafkaConnectSource.read();
        String readBack1 = new String(record.getValue().getValue());
        assertTrue(line1.contains(readBack1));
        assertNull(record.getValue().getKey());
        log.info("read line1: {}", readBack1);
        record.ack();

        assertTrue(record.getProperties().get("test-header").contains("test-header-value"));
    }

    private GenericRecord getGenericRecord(Object value, Schema schema) {
        final GenericRecord rec;
        if (value instanceof GenericRecord) {
            rec = (GenericRecord) value;
        } else {
            rec = MockGenericObjectWrapper.builder()
                    .nativeObject(value)
                    .schemaType(schema != null ? schema.getSchemaInfo().getType() : null)
                    .schemaVersion(new byte[]{1}).build();
        }
        return rec;
    }

    @Test
    public void testSink() throws Exception {
        KafkaConnectSink sink = new KafkaConnectSink();
        sink.open(props, sinkContext);SinkTask task = spy(sink.task);
        ArgumentCaptor<Collection<SinkRecord>> recordsCapture =
                ArgumentCaptor.forClass(Collection.class);
        doCallRealMethod().when(task).put(recordsCapture.capture());
        sink.task = task;
        Schema schema = Schema.STRING;

        final GenericRecord rec = getGenericRecord("This is a test", schema);
        Message msg = mock(MessageImpl.class);
        when(msg.getValue()).thenReturn(rec);
        when(msg.getKey()).thenReturn("key");
        when(msg.hasKey()).thenReturn(true);
        when(msg.getProperties()).thenReturn(ImmutableMap.of("test-header", "test-value"));
        when(msg.getMessageId()).thenReturn(new MessageIdImpl(1, 0, 0));


        final AtomicInteger status = new AtomicInteger(0);
        Record<GenericObject> record = PulsarRecord.<String>builder()
                .topicName("fake-topic")
                .message(msg)
                .schema(schema)
                .ackFunction(status::incrementAndGet)
                .failFunction(status::decrementAndGet)
                .build();

        sink.write(record);
        sink.flush();

        assertEquals(status.get(), 1);
        assertTrue(recordsCapture.getValue() != null);
        assertTrue(!recordsCapture.getValue().isEmpty());
        assertTrue("test-value".equals(recordsCapture
                .getValue()
                .iterator()
                .next()
                .headers()
                .lastWithName("test-header")
                .value()));
        sink.close();
    }
}
