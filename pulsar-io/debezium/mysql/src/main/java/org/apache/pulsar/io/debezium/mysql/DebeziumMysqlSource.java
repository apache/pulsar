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

    static private String OFFSET_TOPIC_PREFIX = "mysql-offset-topic-";
    static private String HISTORY_TOPIC_PREFIX = "mysql-history-topic-";

    private final String uuid = UUID.randomUUID().toString();


    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        config.put(
            TaskConfig.TASK_CLASS_CONFIG,
            "io.debezium.connector.mysql.MySqlConnectorTask");
        config.put(
            PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG,
            "org.apache.kafka.connect.json.JsonConverter");
        config.put(
            PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG,
            "org.apache.kafka.connect.json.JsonConverter");

        // database.history
        config.put(
            MySqlConnectorConfig.DATABASE_HISTORY.name(),
            "org.apache.pulsar.io.debezium.PulsarDatabaseHistory");
        // database.history.pulsar.topic
        config.put(
            PulsarDatabaseHistory.TOPIC.name(),
            HISTORY_TOPIC_PREFIX + uuid);
        // database.history.pulsar.service.url, this is set as the value of pulsar.service.url
        config.put(
            PulsarDatabaseHistory.SERVICE_URL.name(),
            config.get(PulsarKafkaWorkerConfig.PULSAR_SERVICE_URL_CONFIG));

        // offset.storage.topic
        config.put(
            PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG,
            OFFSET_TOPIC_PREFIX + uuid);

        super.open(config, sourceContext);
    }

}
