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
package org.apache.pulsar.io.debezium.mysql;

import java.util.Map;
import java.util.UUID;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.debezium.PulsarDatabaseHistory;
import org.apache.pulsar.io.kafka.connect.KafkaConnectSource;
import org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig;

/**
 * A pulsar source that runs
 */
@Slf4j
public class DebeziumMysqlSource extends KafkaConnectSource {
    static private final String OFFSET_TOPIC_PREFIX = "mysql-offset-topic-";
    static private final String HISTORY_TOPIC_PREFIX = "mysql-history-topic-";

    static private final String DEFAULT_TASK = "io.debezium.connector.mysql.MySqlConnectorTask";
    static private final String DEFAULT_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    static private final String DEFAULT_HISTORY = "org.apache.pulsar.io.debezium.PulsarDatabaseHistory";

    private final String uuid = UUID.randomUUID().toString();

    private static void throwExceptionIfConfigNotMatch(Map<String, Object> config,
                                                       String key,
                                                       String expect) throws IllegalArgumentException {
        Object orig = config.get(key);
        if (orig == null) {
            config.put(key, expect);
            return;
        }

        // throw exception if value not match
        if (!orig.equals(expect)) {
            throw new IllegalArgumentException("Expected " + expect + " but has " + orig);
        }
    }

    private static void setConfigIfNull(Map<String, Object> config, String key, String value) {
        Object orig = config.get(key);
        if (orig == null) {
            config.put(key, value);
        }
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        // connector task
        throwExceptionIfConfigNotMatch(config, TaskConfig.TASK_CLASS_CONFIG, DEFAULT_TASK);

        // key.converter
        setConfigIfNull(config, PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER);
        // value.converter
        setConfigIfNull(config, PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER);

        // database.history implementation class
        setConfigIfNull(config, MySqlConnectorConfig.DATABASE_HISTORY.name(), DEFAULT_HISTORY);
        // database.history.pulsar.topic: history topic name
        setConfigIfNull(config, PulsarDatabaseHistory.TOPIC.name(), HISTORY_TOPIC_PREFIX + uuid);
        // offset.storage.topic: offset topic name
        setConfigIfNull(config, PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG, OFFSET_TOPIC_PREFIX + uuid);

        String serviceUrl = (String) config.get(PulsarKafkaWorkerConfig.PULSAR_SERVICE_URL_CONFIG);
        if (serviceUrl == null) {
            throw new IllegalArgumentException("Pulsar service URL not provided.");
        }
        // database.history.pulsar.service.url, this is set as the value of pulsar.service.url if null.
        setConfigIfNull(config, PulsarDatabaseHistory.SERVICE_URL.name(), serviceUrl);

        super.open(config, sourceContext);
    }

}
