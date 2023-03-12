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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test the implementation of {@link KafkaConnectSource}.
 */
@Slf4j
public class KafkaConnectSourceTest extends ProducerConsumerBase  {

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

    @Test
    public void testOpenAndReadTaskDirect() throws Exception {
        Map<String, Object> config = getConfig();

        config.put(TaskConfig.TASK_CLASS_CONFIG,
                "org.apache.kafka.connect.file.FileStreamSourceTask");

        testOpenAndReadTask(config);
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
