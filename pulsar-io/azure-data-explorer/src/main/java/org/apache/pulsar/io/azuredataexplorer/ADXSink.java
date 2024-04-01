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
package org.apache.pulsar.io.azuredataexplorer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.ManagedStreamingIngestClient;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.TableReportIngestionResult;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import java.io.ByteArrayInputStream;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.jetbrains.annotations.NotNull;

@Connector(
        name = "adx",
        type = IOType.SINK,
        help = "The ADXSink is used for moving messages from Pulsar to ADX.",
        configClass = ADXSinkConfig.class
)
@Slf4j
public class ADXSink implements Sink<byte[]> {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    IngestionProperties ingestionProperties;
    private IngestClient ingestClient;
    private List<Record<byte[]>> incomingRecordsList;
    private int batchSize;
    private long batchTimeMs;
    private ScheduledExecutorService adxSinkExecutor;
    private int maxRetryAttempts;
    private long retryBackOffTime;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        log.info("Open ADX Sink");
        // Azure data explorer, initializations
        ADXSinkConfig adxConfig = ADXSinkConfig.load(config, sinkContext);
        adxConfig.validate();
        ConnectionStringBuilder kcsb = getConnectionStringBuilder(adxConfig);
        if (kcsb == null) {
            throw new Exception("Kusto Connection String NULL");
        }
        log.debug("ConnectionString created: {}.", kcsb);
        ingestClient = adxConfig.getManagedIdentityId() != null
                ? IngestClientFactory.createManagedStreamingIngestClient(kcsb) :
                IngestClientFactory.createClient(kcsb);
        ingestionProperties = new IngestionProperties(adxConfig.getDatabase(), adxConfig.getTable());
        ingestionProperties.setIngestionMapping(adxConfig.getMappingRefName(),
                getParseMappingRefType(adxConfig.getMappingRefType()));
        ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
        ingestionProperties.setFlushImmediately(adxConfig.isFlushImmediately());
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.MULTIJSON);
        log.debug("Ingestion Properties:  {}", ingestionProperties.toString());

        maxRetryAttempts = adxConfig.getMaxRetryAttempts() + 1;
        retryBackOffTime = adxConfig.getRetryBackOffTime();
        /*incoming records list will hold incoming messages,
         flushExecutor executes the flushData according to batch time */
        batchSize = adxConfig.getBatchSize();
        batchTimeMs = adxConfig.getBatchTimeMs();
        incomingRecordsList = new ArrayList<>();
        adxSinkExecutor = Executors.newScheduledThreadPool(1);
        adxSinkExecutor.scheduleAtFixedRate(this::sinkData, batchTimeMs, batchTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(Record<byte[]> record) {
        int runningSize;
        synchronized (this) {
            incomingRecordsList.add(record);
            runningSize = incomingRecordsList.size();
        }
        if (runningSize == batchSize) {
            adxSinkExecutor.execute(this::sinkData);
        }
    }

    private void sinkData() {
        final List<Record<byte[]>> recordsToSink;
        synchronized (this) {
            if (incomingRecordsList.isEmpty()) {
                return;
            }
            recordsToSink = incomingRecordsList;
            incomingRecordsList = new ArrayList<>();
        }

        List<ADXPulsarEvent> eventsToSink = new LinkedList<>();
        for (Record<byte[]> record : recordsToSink) {
            try {
                eventsToSink.add(getADXPulsarEvent(record));
            } catch (Exception ex) {
                record.fail();
                log.error("Failed to collect the record for ADX cluster.", ex);
            }
        }
        try {
            for (int retryAttempts = 0; true; retryAttempts++) {
                try {
                    StreamSourceInfo streamSourceInfo =
                            new StreamSourceInfo(new ByteArrayInputStream(mapper.writeValueAsBytes(eventsToSink)));
                    IngestionResult ingestionResult =
                            ingestClient.ingestFromStream(streamSourceInfo, ingestionProperties);
                    if (ingestionResult instanceof TableReportIngestionResult) {
                        // If TableReportIngestionResult returned then the ingestion status is from streaming ingest
                        IngestionStatus ingestionStatus = ingestionResult.getIngestionStatusCollection().get(0);
                        if (!hasStreamingSucceeded(ingestionStatus)) {
                            retryAttempts += ManagedStreamingIngestClient.ATTEMPT_COUNT;
                            backOffForRemainingAttempts(retryAttempts, null, recordsToSink);
                            continue;
                        }
                        recordsToSink.forEach(Record::ack);
                    }
                    return;
                } catch (IngestionServiceException exception) {
                    Throwable innerException = exception.getCause();
                    if (innerException instanceof KustoDataExceptionBase
                            && ((KustoDataExceptionBase) innerException).isPermanent()) {
                        recordsToSink.forEach(Record::fail);
                        throw new ConnectException(exception.getMessage());
                    }
                    // retrying transient exceptions
                    backOffForRemainingAttempts(retryAttempts, exception, recordsToSink);
                } catch (IngestionClientException | URISyntaxException exception) {
                    recordsToSink.forEach(Record::fail);
                    throw new ConnectException(exception.getMessage());
                }
            }

        } catch (Exception ex) {
            log.error("Failed to publish the message to ADX cluster", ex);
        }
    }

    private boolean hasStreamingSucceeded(@NotNull IngestionStatus status) {
        switch (status.status) {
            case Succeeded:
            case Queued:
            case Pending:
                return true;
            case Skipped:
            case PartiallySucceeded:
                String failureStatus = status.getFailureStatus();
                String details = status.getDetails();
                UUID ingestionSourceId = status.getIngestionSourceId();
                log.warn("A batch of streaming records has {} ingestion: table:{}, database:{}, operationId: {},"
                                + "ingestionSourceId: {}{}{}.\n"
                                + "Status is final and therefore ingestion won't be retried and data won't reach dlq",
                        status.getStatus(),
                        status.getTable(),
                        status.getDatabase(),
                        status.getOperationId(),
                        ingestionSourceId,
                        (StringUtils.isNotEmpty(failureStatus) ? (", failure: " + failureStatus) : ""),
                        (StringUtils.isNotEmpty(details) ? (", details: " + details) : ""));
                return true;
            case Failed:
        }
        return false;
    }

    private void backOffForRemainingAttempts(int retryAttempts, Exception exception, List<Record<byte[]>> records)
            throws PulsarClientException.ConnectException {
        if (retryAttempts < maxRetryAttempts) {
            long sleepTimeMs = retryBackOffTime;
            log.error(
                    "Failed to ingest records into Kusto, backing off and retrying ingesting records "
                            + "after {} milliseconds.",
                    sleepTimeMs);
            try {
                TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
                throw new InterruptedException();
            } catch (InterruptedException interruptedErr) {
                records.forEach(Record::fail);
                throw new PulsarClientException.ConnectException(String.format(
                        "Retrying ingesting records into KustoDB was interrupted after retryAttempts=%s",
                        retryAttempts + 1)
                );
            }
        } else {
            records.forEach(Record::fail);
            throw new PulsarClientException.ConnectException(
                    String.format("Retry attempts exhausted, failed to ingest records into KustoDB. Exception: %s",
                            exception.getMessage()));
        }
    }

    private @NotNull ADXPulsarEvent getADXPulsarEvent(@NotNull Record<byte[]> record) throws Exception {
        ADXPulsarEvent event = new ADXPulsarEvent();
        record.getEventTime().ifPresent(time -> event.setEventTime(Instant.ofEpochMilli(time)));
        record.getKey().ifPresent(event::setKey);
        record.getMessage().ifPresent(message -> event.setProducerName(message.getProducerName()));
        record.getMessage().ifPresent(message -> event.setSequenceId(message.getSequenceId()));
        event.setValue(new String(record.getValue(), StandardCharsets.UTF_8));
        event.setProperties(new ObjectMapper().writeValueAsString(record.getProperties()));
        return event;
    }

    private IngestionMapping.IngestionMappingKind getParseMappingRefType(String mappingRefType) {
        if (mappingRefType == null || mappingRefType.isEmpty()) {
            return null;
        }
        return switch (mappingRefType) {
            case "CSV" -> IngestionMapping.IngestionMappingKind.CSV;
            case "AVRO" -> IngestionMapping.IngestionMappingKind.AVRO;
            case "JSON" -> IngestionMapping.IngestionMappingKind.JSON;
            case "PARQUET" -> IngestionMapping.IngestionMappingKind.PARQUET;
            default -> null;
        };
    }

    private ConnectionStringBuilder getConnectionStringBuilder(@NotNull ADXSinkConfig adxConfig) {

        if (adxConfig.getManagedIdentityId() != null) {
            if ("system".equalsIgnoreCase(adxConfig.getManagedIdentityId())) {
                return ConnectionStringBuilder.createWithAadManagedIdentity(adxConfig.getClusterUrl());
            }
            ConnectionStringBuilder.createWithAadManagedIdentity(adxConfig.getClusterUrl(),
                    adxConfig.getManagedIdentityId());
        }
        return ConnectionStringBuilder.createWithAadApplicationCredentials(adxConfig.getClusterUrl(),
                adxConfig.getAppId(), adxConfig.getAppKey(), adxConfig.getTenantId());
    }

    @Override
    public void close() throws Exception {
        ingestClient.close();
        adxSinkExecutor.shutdown();
        try {
            if (!adxSinkExecutor.awaitTermination(2 * batchTimeMs, TimeUnit.MILLISECONDS)) {
                adxSinkExecutor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            adxSinkExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("Kusto ingest client closed.");
    }
}