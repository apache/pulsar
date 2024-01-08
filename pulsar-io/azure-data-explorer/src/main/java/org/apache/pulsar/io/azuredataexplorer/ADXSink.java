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
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ADXSink implements Sink<byte[]> {

    private IngestClient ingestClient;
    IngestionProperties ingestionProperties;
    private static final Logger LOG = LoggerFactory.getLogger(ADXSink.class);
    private List<Record<byte[]>> incomingRecordsList;
    private int batchSize;
    private ScheduledExecutorService adxSinkExecutor;
    private final ObjectMapper mapper = com.microsoft.azure.kusto.data.Utils.getObjectMapper();
    private int maxRetryAttempts;
    private long retryBackOffTime;
    private boolean dlqEnabled = false;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {

        // Azure data explorer, initializations
        ADXSinkConfig adxconfig = ADXSinkConfig.load(config);
        ConnectionStringBuilder kcsb = getConnectionStringBuilder(adxconfig);
        if (kcsb == null) {
            throw new Exception("Kusto Connection String NULL");
        }
        LOG.debug(String.format("ConnectionString created: %s.", kcsb));
        ingestClient = adxconfig.isManagedIngestion() ? IngestClientFactory.createManagedStreamingIngestClient(kcsb) :
                IngestClientFactory.createClient(kcsb);
        ingestionProperties = new IngestionProperties(adxconfig.getDatabase(), adxconfig.getTable());
        ingestionProperties.setIngestionMapping(adxconfig.getMappingRefName(),
                getParseMappingRefType(adxconfig.getMappingRefType()));
        ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
        ingestionProperties.setFlushImmediately(adxconfig.isFlushImmediately());
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.MULTIJSON);
        LOG.debug("Ingestion Properties:  " + ingestionProperties.toString());

        maxRetryAttempts = adxconfig.getMaxRetryAttempts() + 1;
        retryBackOffTime = adxconfig.getRetryBackOffTime();
        /*incoming records list will hold incoming messages,
         flushExecutor executes the flushData according to batch time */
        batchSize = adxconfig.getBatchSize();
        long batchTimeMs = adxconfig.getBatchTimeMs();
        incomingRecordsList = new ArrayList<>();
        adxSinkExecutor = Executors.newScheduledThreadPool(1);
        adxSinkExecutor.scheduleAtFixedRate(this::sinkData, batchTimeMs, batchTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(Record<byte[]> record) {
        int runningSize = 0;
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
                LOG.error("Failed to collect the record for ADX cluster.", ex);
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
            LOG.error("Failed to publish the message to ADX cluster.", ex);
        }
    }

    private boolean hasStreamingSucceeded(IngestionStatus status) {
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
                LOG.warn("A batch of streaming records has {} ingestion: table:{}, database:{}, operationId: {},"
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
            LOG.error(
                    "Failed to ingest records into Kusto, backing off and retrying ingesting records "
                            + "after {} milliseconds.",
                    sleepTimeMs);
            try {
                TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
                throw new InterruptedException();
            } catch (InterruptedException interruptedErr) {
                if (dlqEnabled) {
                    records.forEach(Record::fail);
                }
                throw new PulsarClientException.ConnectException(String.format(
                        "Retrying ingesting records into KustoDB was interuppted after retryAttempts=%s",
                        retryAttempts + 1)
                );
            }
        } else {
            if (dlqEnabled) {
                records.forEach(Record::fail);
            }
            throw new PulsarClientException.ConnectException(
                    String.format("Retry attempts exhausted, failed to ingest records into KustoDB. Exception: %s",
                            exception.getMessage()));
        }
    }

    private ADXPulsarEvent getADXPulsarEvent(Record<byte[]> record) throws Exception {
        ADXPulsarEvent event = new ADXPulsarEvent();
        record.getEventTime().ifPresent(time -> event.setEventTime(Instant.ofEpochMilli(time)));
        record.getKey().ifPresent(key -> event.setKey(key));
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
        switch (mappingRefType) {
            case "CSV" : return IngestionMapping.IngestionMappingKind.CSV;
            case "AVRO" : return IngestionMapping.IngestionMappingKind.AVRO;
            case "JSON" : return IngestionMapping.IngestionMappingKind.JSON;
            case "PARQUET" : return IngestionMapping.IngestionMappingKind.PARQUET;
            default : return IngestionMapping.IngestionMappingKind.CSV;
        }
    }

    private ConnectionStringBuilder getConnectionStringBuilder(ADXSinkConfig adxconfig) {

        if (adxconfig.getManagedIdentityId() != null) {
            if ("system".equalsIgnoreCase(adxconfig.getManagedIdentityId())) {
                return ConnectionStringBuilder.createWithAadManagedIdentity(adxconfig.getClusterUrl());
            }
            ConnectionStringBuilder.createWithAadManagedIdentity(adxconfig.getClusterUrl(),
                    adxconfig.getManagedIdentityId());
        }
        return ConnectionStringBuilder.createWithAadApplicationCredentials(adxconfig.getClusterUrl(),
                adxconfig.getAppId(), adxconfig.getAppKey(), adxconfig.getTenantId());
    }

    @Override
    public void close() throws Exception {
        ingestClient.close();
        LOG.info("Kusto ingest client closed.");
    }
}