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
package org.apache.pulsar.io.debezium;

import io.debezium.relational.history.DatabaseHistory;
import java.util.Map;

import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.connect.KafkaConnectSource;
import org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig;

public abstract class DebeziumSource extends KafkaConnectSource {
    private static final String DEFAULT_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static final String DEFAULT_HISTORY = "org.apache.pulsar.io.debezium.PulsarDatabaseHistory";
    private static final String DEFAULT_OFFSET_TOPIC = "debezium-offset-topic";
    private static final String DEFAULT_HISTORY_TOPIC = "debezium-history-topic";

    public static void throwExceptionIfConfigNotMatch(Map<String, Object> config,
                                                       String key,
                                                       String value) throws IllegalArgumentException {
        Object orig = config.get(key);
        if (orig == null) {
            config.put(key, value);
            return;
        }

        // throw exception if value not match
        if (!orig.equals(value)) {
            throw new IllegalArgumentException("Expected " + value + " but has " + orig);
        }
    }

    public static void setConfigIfNull(Map<String, Object> config, String key, String value) {
        Object orig = config.get(key);
        if (orig == null) {
            config.put(key, value);
        }
    }

    // namespace for output topics, default value is "tenant/namespace"
    public static String topicNamespace(SourceContext sourceContext) {
        String tenant = sourceContext.getTenant();
        String namespace = sourceContext.getNamespace();

        return (StringUtils.isEmpty(tenant) ? TopicName.PUBLIC_TENANT : tenant) + "/" +
            (StringUtils.isEmpty(namespace) ? TopicName.DEFAULT_NAMESPACE : namespace);
    }

    public abstract void setDbConnectorTask(Map<String, Object> config) throws Exception;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        setDbConnectorTask(config);

        // key.converter
        setConfigIfNull(config, PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER);
        // value.converter
        setConfigIfNull(config, PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER);

        // database.history : implementation class for database history.
        setConfigIfNull(config, HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY.name(), DEFAULT_HISTORY);

        // database.history.pulsar.service.url
        String pulsarUrl = (String) config.get(PulsarDatabaseHistory.SERVICE_URL.name());
        if (StringUtils.isEmpty(pulsarUrl)) {
            throw new IllegalArgumentException("Pulsar service URL for History Database not provided.");
        }

        String topicNamespace = topicNamespace(sourceContext);
        // topic.namespace
        setConfigIfNull(config, PulsarKafkaWorkerConfig.TOPIC_NAMESPACE_CONFIG, topicNamespace);

        String sourceName = sourceContext.getSourceName();
        // database.history.pulsar.topic: history topic name
        setConfigIfNull(config, PulsarDatabaseHistory.TOPIC.name(),
            topicNamespace + "/" + sourceName + "-" + DEFAULT_HISTORY_TOPIC);
        // offset.storage.topic: offset topic name
        setConfigIfNull(config, PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG,
            topicNamespace + "/" + sourceName + "-" + DEFAULT_OFFSET_TOPIC);

        config.put(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING + "pulsar.client.builder",
                SerDeUtils.serialize(sourceContext.getPulsarClientBuilder()));

        super.open(config, sourceContext);
    }

}
