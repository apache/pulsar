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

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.pulsar.io.common.IOConfigUtils.loadConfigFromJsonString;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

/**
 * A {@link DatabaseHistory} implementation that records schema changes as normal pulsar messages on the specified
 * topic, and that recovers the history by establishing a Kafka Consumer re-processing all messages on that topic.
 */
@Slf4j
@ThreadSafe
public final class PulsarDatabaseHistory extends AbstractDatabaseHistory {

    public static final Field TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "pulsar.topic")
        .withDisplayName("Database history topic name")
        .withType(Type.STRING)
        .withWidth(Width.LONG)
        .withImportance(Importance.HIGH)
        .withDescription("The name of the topic for the database schema history")
        .withValidation(Field::isRequired);

    public static final Field SERVICE_URL = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "pulsar.service.url")
        .withDisplayName("Pulsar service url")
        .withType(Type.STRING)
        .withWidth(Width.LONG)
        .withImportance(Importance.HIGH)
        .withDescription("Pulsar service url")
        .withValidation(Field::isOptional);

    public static final Field CLIENT_BUILDER = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "pulsar.client.builder")
        .withDisplayName("Pulsar client builder")
        .withType(Type.STRING)
        .withWidth(Width.LONG)
        .withImportance(Importance.HIGH)
        .withDescription("Pulsar client builder")
        .withValidation(Field::isOptional);

    public static final Field READER_CONFIG = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "pulsar.reader.config")
            .withDisplayName("Extra configs of the reader")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The configs of the reader for the database schema history topic, "
                    + "in the form of a JSON string with key-value pairs")
            .withDefault((String) null)
            .withValidation(Field::isOptional);

    public static final Field.Set ALL_FIELDS = Field.setOf(
        TOPIC,
        SERVICE_URL,
        CLIENT_BUILDER,
        DatabaseHistory.NAME,
        READER_CONFIG);

    private final DocumentReader reader = DocumentReader.defaultReader();
    private String topicName;
    private Map<String, Object> readerConfigMap = new HashMap<>();
    private String dbHistoryName;
    private ClientBuilder clientBuilder;
    private volatile PulsarClient pulsarClient;
    private volatile Producer<String> producer;

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            DatabaseHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        if (!config.validateAndRecord(ALL_FIELDS, logger::error)) {
            throw new IllegalArgumentException("Error configuring an instance of "
                + getClass().getSimpleName() + "; check the logs for details");
        }
        this.topicName = config.getString(TOPIC);
        try {
            this.readerConfigMap = loadConfigFromJsonString(config.getString(READER_CONFIG));
        } catch (JsonProcessingException exception) {
            log.warn("The provided reader configs are invalid, "
                    + "will not passing any extra config to the reader builder.", exception);
        }

        String clientBuilderBase64Encoded = config.getString(CLIENT_BUILDER);
        if (isBlank(clientBuilderBase64Encoded) && isBlank(config.getString(SERVICE_URL))) {
            throw new IllegalArgumentException("Neither Pulsar Service URL nor ClientBuilder provided.");
        }
        this.clientBuilder = PulsarClient.builder();
        if (!isBlank(clientBuilderBase64Encoded)) {
            // deserialize the client builder to the same classloader
            this.clientBuilder = (ClientBuilder) SerDeUtils.deserialize(clientBuilderBase64Encoded,
                    this.clientBuilder.getClass().getClassLoader());
        } else {
            this.clientBuilder.serviceUrl(config.getString(SERVICE_URL));
        }

        // Copy the relevant portions of the configuration and add useful defaults ...
        this.dbHistoryName = config.getString(DatabaseHistory.NAME, UUID.randomUUID().toString());

        log.info("Configure to store the debezium database history {} to pulsar topic {}",
            dbHistoryName, topicName);
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();

        // try simple to publish an empty string to create topic
        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING).topic(topicName).create()) {
            p.send("");
        } catch (PulsarClientException pce) {
            log.error("Failed to initialize storage", pce);
            throw new RuntimeException("Failed to initialize storage", pce);
        }
    }

    void setupClientIfNeeded() {
        if (null == this.pulsarClient) {
            try {
                pulsarClient = clientBuilder.build();
            } catch (PulsarClientException e) {
                throw new RuntimeException("Failed to create pulsar client to pulsar cluster", e);
            }
        }
    }

    void setupProducerIfNeeded() {
        setupClientIfNeeded();
        if (null == this.producer) {
            try {
                this.producer = pulsarClient.newProducer(Schema.STRING)
                    .topic(topicName)
                    .producerName(dbHistoryName)
                    .blockIfQueueFull(true)
                    .create();
            } catch (PulsarClientException e) {
                log.error("Failed to create pulsar producer to topic '{}'", topicName);
                throw new RuntimeException("Failed to create pulsar producer to topic '"
                    + topicName, e);
            }
        }
    }

    @Override
    public void start() {
        super.start();
        setupProducerIfNeeded();
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        if (this.producer == null) {
            throw new IllegalStateException("No producer is available. Ensure that 'start()'"
                    + " is called before storing database history records.");
        }
        if (log.isTraceEnabled()) {
            log.trace("Storing record into database history: {}", record);
        }
        try {
            producer.send(record.toString());
        } catch (PulsarClientException e) {
            throw new DatabaseHistoryException(e);
        }
    }

    @Override
    public void stop() {
        try {
            if (this.producer != null) {
                try {
                    producer.flush();
                } catch (PulsarClientException pce) {
                    // ignore the error to ensure the client is eventually closed
                } finally {
                    this.producer.close();
                }
                this.producer = null;
            }
            if (this.pulsarClient != null) {
                pulsarClient.close();
                this.pulsarClient = null;
            }
        } catch (PulsarClientException pe) {
            log.warn("Failed to closing pulsar client", pe);
        }
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        setupClientIfNeeded();
        try (Reader<String> historyReader = createHistoryReader()) {
            log.info("Scanning the database history topic '{}'", topicName);

            // Read all messages in the topic ...
            MessageId lastProcessedMessageId = null;

            // read the topic until the end
            while (historyReader.hasMessageAvailable()) {
                Message<String> msg = historyReader.readNext();
                try {
                    if (null == lastProcessedMessageId || lastProcessedMessageId.compareTo(msg.getMessageId()) < 0) {
                        if (!isBlank(msg.getValue())) {
                            HistoryRecord recordObj = new HistoryRecord(reader.read(msg.getValue()));
                            if (log.isTraceEnabled()) {
                                log.trace("Recovering database history: {}", recordObj);
                            }
                            if (!recordObj.isValid()) {
                                log.warn("Skipping invalid database history record '{}'. This is often not an issue,"
                                                + " but if it happens repeatedly please check the '{}' topic.",
                                    recordObj, topicName);
                            } else {
                                records.accept(recordObj);
                                log.trace("Recovered database history: {}", recordObj);
                            }
                        }
                        lastProcessedMessageId = msg.getMessageId();
                    }
                } catch (IOException ioe) {
                    log.error("Error while deserializing history record '{}'", msg.getValue(), ioe);
                } catch (final Exception e) {
                    throw e;
                }
            }
            log.info("Successfully completed scanning the database history topic '{}'", topicName);
        } catch (IOException ioe) {
            log.error("Encountered issues on recovering history records", ioe);
            throw new RuntimeException("Encountered issues on recovering history records", ioe);
        }
    }

    @Override
    public boolean exists() {
        setupClientIfNeeded();
        try (Reader<String> historyReader = createHistoryReader()) {
            return historyReader.hasMessageAvailable();
        } catch (IOException e) {
            log.error("Encountered issues on checking existence of database history", e);
            throw new RuntimeException("Encountered issues on checking existence of database history", e);
        }
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public String toString() {
        if (topicName != null) {
            return "Pulsar topic (" + topicName + ")";
        }
        return "Pulsar topic";
    }

    @VisibleForTesting
    Reader<String> createHistoryReader() throws PulsarClientException {
        return pulsarClient.newReader(Schema.STRING)
                .topic(topicName)
                .startMessageId(MessageId.earliest)
                .loadConf(readerConfigMap)
                .create();
    }
}
