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
package org.apache.pulsar.io.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.io.cassandra.producers.InputTopicProducerThread;
import org.apache.pulsar.io.cassandra.producers.ReadingSchemaRecordProducer;
import org.yaml.snakeyaml.Yaml;

/**
 * Useful for testing within IDE.
 *
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CassandraSinkExec {

    public static final String BROKER_URL = "pulsar://localhost:6650";
    public static final String INPUT_TOPIC = "persistent://public/default/air-quality-reading-generic";

    public static final String CONFIG_FILE = "cassandra-sink-config.yaml";

    public static void main(String[] args) throws Exception {

        SinkConfig config = getSinkConfig();

        final LocalRunner localRunner =
                LocalRunner.builder()
                        .brokerServiceUrl(BROKER_URL)
                        .sinkConfig(config)
                        .build();

        localRunner.start(false);

        sendData();
        TimeUnit.MINUTES.sleep(10);

        localRunner.stop();

        System.exit(0);
    }

    private static SinkConfig getSinkConfig() throws IOException {
        SinkConfig sinkConfig = SinkConfig.builder()
                .autoAck(true)
                .cleanupSubscription(Boolean.TRUE)
                .configs(getConfigs())
                .className(CassandraGenericRecordSink.class.getName())
                .inputs(Collections.singletonList(INPUT_TOPIC))
                .name("CassandraSink")
                .build();

        return sinkConfig;
    }

    private static Map<String, Object> getConfigs() throws IOException {
        Map<String, Object> configs = new HashMap<String, Object>();

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource(CONFIG_FILE).getFile());

        try (FileInputStream fis = new FileInputStream(file)) {
            configs = new Yaml().load(fis);
        } catch (IOException ex) {
            throw ex;
        }

        return configs;
    }

    private static void sendData() throws InterruptedException {
        TimeUnit.SECONDS.sleep(10);
        InputTopicProducerThread writer = new ReadingSchemaRecordProducer(BROKER_URL, INPUT_TOPIC);
        writer.run();
    }
}
