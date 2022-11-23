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

package io.debezium.connector.mysql.signal;

import static org.apache.commons.lang.StringUtils.isBlank;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotChangeEventSource;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.signal.ExecuteSnapshot;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Threads;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.io.debezium.SerDeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * Pulsar signal thread.
 *
 * @param <T>
 */
public class PulsarSignalThread<T extends DataCollectionId> {
    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "signal.";
    public static final Field SIGNAL_TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "pulsar.topic")
            .withDisplayName("Signal pulsar topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic for the signals to the connector")
            .withValidation(Field::isRequired);
    public static final Field SERVICE_URL = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "pulsar.service.url")
            .withDisplayName("Pulsar service url")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Pulsar service url")
            .withValidation(Field::isRequired);
    public static final Field CLIENT_BUILDER = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "pulsar.client.builder")
            .withDisplayName("Pulsar client builder")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Pulsar client builder")
            .withValidation(Field::isOptional);
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSignalThread.class);
    private final ExecutorService signalTopicListenerExecutor;
    private final String topicName;
    private final String connectorName;
    private final MySqlReadOnlyIncrementalSnapshotChangeEventSource<T> eventSource;
    private final Consumer<String> signalsConsumer;

    public PulsarSignalThread(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                              MySqlReadOnlyIncrementalSnapshotChangeEventSource<T> eventSource) {
        String signalName = "pulsar-signal";
        this.connectorName = connectorConfig.getLogicalName();
        this.signalTopicListenerExecutor = Threads.newSingleThreadExecutor(
                connectorType, connectorName, signalName, true
        );
        Configuration signalConfig = connectorConfig.getConfig().subset(CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .withDefault(PulsarSignalThread.SIGNAL_TOPIC, connectorName + "-signal")
                .build();
        this.eventSource = eventSource;
        this.topicName = signalConfig.getString(SIGNAL_TOPIC);
        String serviceUrl = signalConfig.getString(SERVICE_URL);
        String clientBuilderBase64Encoded = signalConfig.getString(CLIENT_BUILDER);
        if (isBlank(clientBuilderBase64Encoded) && isBlank(signalConfig.getString(SERVICE_URL))) {
            throw new IllegalArgumentException("Neither Pulsar Service URL nor ClientBuilder provided.");
        }
        ClientBuilder clientBuilder = PulsarClient.builder();
        if (!isBlank(clientBuilderBase64Encoded)) {
            clientBuilder = (ClientBuilder) SerDeUtils.deserialize(
                    clientBuilderBase64Encoded,
                    clientBuilder.getClass().getClassLoader()
            );
        } else {
            clientBuilder.serviceUrl(serviceUrl);
        }
        try {
            PulsarClient pulsarClient = clientBuilder.build();
            this.signalsConsumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic(topicName)
                    .subscriptionName(this.connectorName)
                    .subscribe();
        } catch (PulsarClientException e) {
            LOGGER.error("Pulsar client new consumer failed ", e);
            throw new RuntimeException(e);
        }
        LOGGER.info("Subscribing to signals pulsar topic '{}'", topicName);
    }

    public void start() {
        signalTopicListenerExecutor.submit(this::monitorSignals);
    }

    private void monitorSignals() {
        while (true) {
            Message<String> record = null;
            try {
                record = signalsConsumer.receive();
            } catch (PulsarClientException e) {
                LOGGER.error("Signals processing was interrupted", e);
                throw new RuntimeException(e);
            }
            try {
                processSignal(record);
            } catch (final InterruptedException e) {
                LOGGER.error("Signals processing was interrupted", e);
                try {
                    signalsConsumer.close();
                } catch (PulsarClientException ex) {
                }
                return;
            } catch (final Exception e) {
                LOGGER.error("Skipped signal due to an error '{}'", record, e);
            }
        }
    }

    private void processSignal(Message<String> record) throws IOException, InterruptedException {
        if (!connectorName.equals(record.getKey())) {
            LOGGER.info("Signal key '{}' doesn't match the connector's name '{}'", record.getKey(), connectorName);
            return;
        }
        String value = record.getValue();
        LOGGER.trace("Processing signal: {}", value);
        final Document jsonData =
                (value == null || value.isEmpty()) ? Document.create() : DocumentReader.defaultReader().read(value);
        String type = jsonData.getString("type");
        Document data = jsonData.getDocument("data");
        if (ExecuteSnapshot.NAME.equals(type)) {
            executeSnapshot(data, record.getMessageId());
        } else {
            LOGGER.warn("Unknown signal type {}", type);
        }
    }

    private void executeSnapshot(Document data, MessageId signalOffset) {
        final List<String> dataCollections = ExecuteSnapshot.getDataCollections(data);
        if (dataCollections != null) {
            ExecuteSnapshot.SnapshotType snapshotType = ExecuteSnapshot.getSnapshotType(data);
            LOGGER.info("Requested '{}' snapshot of data collections '{}'", snapshotType, dataCollections);
            if (snapshotType == ExecuteSnapshot.SnapshotType.INCREMENTAL) {
                eventSource.enqueueDataCollectionNamesToSnapshot(dataCollections, signalOffset);
            }
        }
    }

    public void seek(String messageId) {
        try {
            MessageId cursor = MessageId.fromByteArray(messageId.getBytes(StandardCharsets.UTF_8));
            signalsConsumer.seek(cursor);
        } catch (Exception e) {
            LOGGER.error("Pulsar signal seek error {}", messageId, e);
            throw new RuntimeException(e);
        }
    }
}