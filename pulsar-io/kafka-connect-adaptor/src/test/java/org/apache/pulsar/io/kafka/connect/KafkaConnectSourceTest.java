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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.connect.file.FileStreamSourceTask.FILENAME_FIELD;
import static org.apache.kafka.connect.file.FileStreamSourceTask.POSITION_FIELD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.file.FileStreamSourceTask;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.util.Callback;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test the implementation of {@link KafkaConnectSource}.
 */
@Slf4j
public class KafkaConnectSourceTest extends ProducerConsumerBase  {

    private Map<String, Object> config = new HashMap<>();
    private String offsetTopicName;
    // The topic to publish data to, for kafkaSource
    private String topicName;
    private KafkaConnectSource kafkaConnectSource;
    private File tempFile;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        config.put(TaskConfig.TASK_CLASS_CONFIG, org.apache.kafka.connect.file.FileStreamSourceTask.class);
        config.put(PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG, org.apache.kafka.connect.storage.StringConverter.class);
        config.put(PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, org.apache.kafka.connect.storage.StringConverter.class);

        this.offsetTopicName = "persistent://my-property/my-ns/kafka-connect-source-offset";
        config.put(PulsarKafkaWorkerConfig.PULSAR_SERVICE_URL_CONFIG, brokerUrl.toString());
        config.put(PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG, offsetTopicName);

        this.topicName = "persistent://my-property/my-ns/kafka-connect-source";
        config.put(FileStreamSourceConnector.TOPIC_CONFIG, topicName);
        tempFile = File.createTempFile("some-file-name", null);
        config.put(FileStreamSourceConnector.FILE_CONFIG, tempFile.getAbsoluteFile().toString());

    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        tempFile.delete();
        super.internalCleanup();
    }

    @Test
    public void testOpenAndRead() throws Exception {
        kafkaConnectSource = new KafkaConnectSource();
        kafkaConnectSource.open(config, null);

        OutputStream os = Files.newOutputStream(tempFile.toPath());
        String line1 = "This is the first line\n";
        os.write(line1.getBytes());
        os.flush();
        log.info("write 2 lines.");
        kafkaConnectSource.getOffsetWriter().offset(
            Collections.singletonMap(FILENAME_FIELD, config.get(FileStreamSourceConnector.FILE_CONFIG).toString()),
            Collections.singletonMap(POSITION_FIELD, /*tempFile.getTotalSpace()*/0L));

        // offset of second line
        long offset = tempFile.getTotalSpace();
        String line2 = "This is the second line\n";
        os.write(line2.getBytes());
        os.flush();
        kafkaConnectSource.getOffsetWriter().offset(
            Collections.singletonMap(FILENAME_FIELD, config.get(FileStreamSourceConnector.FILE_CONFIG).toString()),
            Collections.singletonMap(POSITION_FIELD, offset));

        log.info("finish write, will read 2 lines");

        Record<byte[]> record = kafkaConnectSource.read();
        assertTrue(line1.contains(new String(record.getValue())));

        record = kafkaConnectSource.read();
        assertTrue(line2.contains(new String(record.getValue())));
    }
}
