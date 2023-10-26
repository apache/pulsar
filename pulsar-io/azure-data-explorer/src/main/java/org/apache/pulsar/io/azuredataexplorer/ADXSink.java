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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ADXSink implements Sink<byte[]> {

    IngestClient client;
    IngestionProperties ingestionProperties;
    private static final Logger LOG = LoggerFactory.getLogger(ADXSink.class);
    private List<Record<byte[]>> incomingRecordsList;
    private int batchSize;
    private ScheduledExecutorService adxSinkExecutor;

    ObjectMapper mapper;


    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {

        // Azure data explorer, initializations
        ADXSinkConfig adxconfig = ADXSinkConfig.load(config);
        ConnectionStringBuilder kcsb = getConnectionStringBuilder(adxconfig, getAuthenticationMode(adxconfig));
        if (kcsb == null) throw new Exception("Kusto Connection String NULL");
        LOG.debug("ConnectionString created:  " + kcsb);
        client = adxconfig.isManagedIngestion() ? IngestClientFactory.createManagedStreamingIngestClient(kcsb) : IngestClientFactory.createClient(kcsb);
        ingestionProperties = new IngestionProperties(adxconfig.getDatabase(),
                adxconfig.getTable());
        ingestionProperties.setIngestionMapping(adxconfig.getMappingRefName(), getParseMappingRefType(adxconfig.getMappingRefType()));
        ingestionProperties.setFlushImmediately(adxconfig.isFlushImmediately());
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
        LOG.debug("Ingestion Properties:  " + ingestionProperties.toString());

        //incoming records list will hold incoming messages, flushExecutor executes the flushData according to batch time
        batchSize = adxconfig.getBatchSize();
        long batchTimeMs = adxconfig.getBatchTimeMs();
        incomingRecordsList = new ArrayList<>();
        adxSinkExecutor = Executors.newScheduledThreadPool(1);
        adxSinkExecutor.scheduleAtFixedRate(this::sinkData, batchTimeMs, batchTimeMs, TimeUnit.MILLISECONDS);
        mapper = new ObjectMapper();

    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
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
            if (incomingRecordsList.isEmpty()) return;
            recordsToSink = incomingRecordsList;
            incomingRecordsList = new ArrayList<>();
        }

        for (Record<byte[]> record : recordsToSink) {
            try {
                ADXPulsarEvent event = getADXPulsarEvent(record);
                StreamSourceInfo streamSourceInfo = new StreamSourceInfo(new ByteArrayInputStream(mapper.writeValueAsString(event).getBytes()));
                OperationStatus status = client.ingestFromStream(streamSourceInfo,
                        ingestionProperties).getIngestionStatusCollection().get(0).status;
                record.ack();
                LOG.info("Record sent to ADX sink");
            } catch (Exception ex) {
                record.fail();
                LOG.error("Failed to publish the message to ADX cluster.", ex);
            }
        }
        recordsToSink.clear();
    }

    private ADXPulsarEvent getADXPulsarEvent(Record<byte[]> record) throws Exception {
        ADXPulsarEvent event = new ADXPulsarEvent();
        record.getEventTime().ifPresent(time -> event.EventTime = new Timestamp(time));
        record.getKey().ifPresent(key -> event.Key = key);
        record.getMessage().ifPresent(message -> event.ProducerName = message.getProducerName());
        record.getMessage().ifPresent(message -> event.SequenceId = message.getSequenceId());
        event.Value = new String(record.getValue(), StandardCharsets.UTF_8);
        event.Properties = new ObjectMapper().writeValueAsString(record.getProperties());
        return event;
    }

    private IngestionMapping.IngestionMappingKind getParseMappingRefType(String mappingRefType) {
        if (mappingRefType == null || mappingRefType.isEmpty()) return null;
        return switch (mappingRefType) {
            case "CSV" -> IngestionMapping.IngestionMappingKind.CSV;
            case "AVRO" -> IngestionMapping.IngestionMappingKind.AVRO;
            case "JSON" -> IngestionMapping.IngestionMappingKind.JSON;
            case "PARQUET" -> IngestionMapping.IngestionMappingKind.PARQUET;
            default -> IngestionMapping.IngestionMappingKind.CSV;
        };
    }

    private AuthenticationMode getAuthenticationMode(ADXSinkConfig adxconfig) {

        if (!(adxconfig.getManagedIdentityId() == null)) {
            return AuthenticationMode.ManagedIdentity;
        }
        return AuthenticationMode.AadApplicationKey;
    }

    private ConnectionStringBuilder getConnectionStringBuilder(ADXSinkConfig adxconfig, AuthenticationMode authenticationMode) {

        switch (authenticationMode) {
            case AadApplicationKey -> {
                return ConnectionStringBuilder.createWithAadApplicationCredentials(adxconfig.getClusterUrl(), adxconfig.getAppId(), adxconfig.getAppKey(), adxconfig.getTenantId());
            }
            case ManagedIdentity -> {
                if (adxconfig.getManagedIdentityId().equals("system")) {
                    return ConnectionStringBuilder.createWithAadManagedIdentity(adxconfig.getClusterUrl());
                }
                return ConnectionStringBuilder.createWithAadManagedIdentity(adxconfig.getClusterUrl(), adxconfig.getManagedIdentityId());
            }
            case UserPromptAuthentication -> {

                return ConnectionStringBuilder.createWithUserPrompt(adxconfig.getClusterUrl());
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        client.close();
        LOG.info("Kusto ingest client closed.");
    }

    enum AuthenticationMode {
        AadApplicationKey,
        ManagedIdentity,
        UserPromptAuthentication,

    }
}