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

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.io.core.SinkContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class ADXSinkE2ETest {

    String database;
    String cluster;
    String authorityId;
    String appId;
    String appKey;
    private final String table = "ADXPulsarTest_" + ThreadLocalRandom.current().nextInt(0, 100);
    private Client kustoAdminClient = null;
    Map<String, Object> configs;

    @BeforeMethod
    public void setUp() throws Exception {
        cluster = System.getenv("kustoCluster");
        database = System.getenv("kustoDatabase");
        authorityId = System.getenv("kustoAadAuthorityID");
        appId = System.getenv("kustoAadAppId");
        appKey = System.getenv("kustoAadAppSecret");

        if(cluster == null){
            throw new SkipException("Skipping tests because environment vars was not accessible.");
        }

        configs = new HashMap<>();
        configs.put("clusterUrl", cluster);
        configs.put("database", database);
        configs.put("table", table);
        configs.put("batchTimeMs", 1000);
        configs.put("flushImmediately", true);
        configs.put("appId", appId);
        configs.put("appKey", appKey);
        configs.put("tenantId", authorityId);
        configs.put("maxRetryAttempts", 3);
        configs.put("retryBackOffTime", 100);

        ConnectionStringBuilder engineKcsb =
                ConnectionStringBuilder.createWithAadApplicationCredentials(ADXSinkUtils.getQueryEndpoint(cluster),
                        appId, appKey, authorityId);
        kustoAdminClient = ClientFactory.createClient(engineKcsb);
        String createTableCommand = ".create table " + table +
                " ( key:string , value:string, eventTime:datetime , producerName:string , sequenceId:long ,properties:dynamic )";
        log.info("Creating test table {} ", table);
        kustoAdminClient.execute(database, createTableCommand);
        kustoAdminClient.execute(database, generateAlterIngestionBatchingPolicyCommand(database,
                "{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024}"));
        log.info("Ingestion policy on table {} altered",table);
    }

    private String generateAlterIngestionBatchingPolicyCommand(String entityName, String targetBatchingPolicy) {
        return ".alter database " + entityName + " policy ingestionbatching ```" + targetBatchingPolicy + "```";
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        try {
            log.warn("Dropping test table {} ", table);
            kustoAdminClient.execute(".drop table " + table + " ifexists");
        } catch (Exception ignore) {
        }
    }

    @Test
    public void testOpenAndWriteSink() throws Exception {

        ADXSink sink = new ADXSink();
        sink.open(configs, Mockito.mock(SinkContext.class));
        int writeCount = 50;

        for (int i = 0; i < writeCount; i++) {
            Record<byte[]> record = build("key_" + i, "test data from ADX Pulsar Sink_" + i);
            sink.write(record);
        }
        Thread.sleep(40000);
        KustoOperationResult result = kustoAdminClient.execute(database, table + " | count");
        KustoResultSetTable mainTableResult = result.getPrimaryResults();
        mainTableResult.next();
        int actualRowsCount = mainTableResult.getInt(0);
        Assert.assertEquals(actualRowsCount, writeCount);
        kustoAdminClient.execute(database, ".clear table " + table + "  data");
        sink.close();
    }

    @Test
    public void testOpenAndWriteSinkWithTimeouts() throws Exception {
        ADXSink sink = new ADXSink();
        sink.open(configs, Mockito.mock(SinkContext.class));
        int writeCount = 9;

        for (int i = 0; i < writeCount; i++) {
            Record<byte[]> record = build("key_" + i, "test data from ADX Pulsar Sink_" + i);
            sink.write(record);
        }
        Thread.sleep(40000);
        KustoOperationResult result = kustoAdminClient.execute(database, table + " | count");
        KustoResultSetTable mainTableResult = result.getPrimaryResults();
        mainTableResult.next();
        int actualRowsCount = mainTableResult.getInt(0);
        Assert.assertEquals(actualRowsCount, writeCount);

        sink.close();
    }

    private Record<byte[]> build(String key, String value) {
        return new SinkRecord<byte[]>(new Record<>() {

            @Override
            public byte[] getValue() {
                return value.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Optional<String> getDestinationTopic() {
                return Optional.of("destination-topic");
            }

            @Override
            public Optional<Long> getEventTime() {
                return Optional.of(System.currentTimeMillis());
            }

            @Override
            public Optional<String> getKey() {
                return Optional.of("key-" + key);
            }

            @Override
            public Map<String, String> getProperties() {
                return new HashMap<>();
            }
        }, value.getBytes(StandardCharsets.UTF_8));
    }
}
