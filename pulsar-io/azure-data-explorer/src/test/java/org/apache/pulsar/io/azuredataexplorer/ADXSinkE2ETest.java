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
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ADXSinkE2ETest {

    private String table = "ADXPulsarTest_" + ThreadLocalRandom.current().nextInt(0, 100);

    private Client kustoAdminClient = null;
    String database;

    @BeforeMethod
    public void setUp() throws Exception {

        Objects.requireNonNull(System.getenv("kustoDatabase"), "kustoDatabase not set.");
        Objects.requireNonNull(System.getenv("kustoCluster"), "kustoCluster not set.");
        Objects.requireNonNull(System.getenv("kustoAadAuthorityID"), "kustoAadAuthorityID not set.");
        Objects.requireNonNull(System.getenv("kustoAadAppId"), "kustoAadAppId not set.");
        Objects.requireNonNull(System.getenv("kustoAadAppSecret"), "kustoAadAppSecret not set.");

        database = System.getenv("kustoDatabase");
        String cluster = System.getenv("kustoCluster");
        String authorityId = System.getenv("kustoAadAuthorityID");
        String appId = System.getenv("kustoAadAppId");
        String appkey = System.getenv("kustoAadAppSecret");

        ConnectionStringBuilder engineKcsb =
                ConnectionStringBuilder.createWithAadApplicationCredentials(ADXSinkUtils.getQueryEndpoint(cluster),
                        appId, appkey, authorityId);
        kustoAdminClient = ClientFactory.createClient(engineKcsb);

        kustoAdminClient.execute(database, generateAlterIngestionBatchingPolicyCommand(database,
                "{\"MaximumBatchingTimeSpan\":\"00:00:10\", \"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024}"));

        String createTableCommand = ".create table " + table +
                " ( key:string , value:string, eventTime:datetime , producerName:string , sequenceId:long ,properties:dynamic )";
        kustoAdminClient.execute(database, createTableCommand);
    }

    private String generateAlterIngestionBatchingPolicyCommand(String entityName, String targetBatchingPolicy) {
        return ".alter database " + entityName + " policy ingestionbatching ```" + targetBatchingPolicy + "```";
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        try {
            kustoAdminClient.execute(".drop table " + table + " ifexists");
        } catch (Exception ignore) {
        }
    }
    @Test
    public void TestOpenAndWriteSink() throws Exception {


        Map<String, Object> configs = new HashMap<>();
        configs.put("clusterUrl", System.getenv("kustoCluster"));
        configs.put("database", System.getenv("kustoDatabase"));
        configs.put("table", table);
        configs.put("batchTimeMs", 1000);
        configs.put("flushImmediately", true);
        configs.put("appId", System.getenv("kustoAadAppId"));
        configs.put("appKey", System.getenv("kustoAadAppSecret"));
        configs.put("tenantId", System.getenv("kustoAadAuthorityID"));
        configs.put("maxRetryAttempts", 3);
        configs.put("retryBackOffTime", 100);
        /*configs.put("serviceUrl","pulsar://localhost:3000");
        configs.put("dlqTopic","something-DLQ");*/

        ADXSink sink = new ADXSink();
        sink.open(configs, null);
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

        sink.close();
    }

    @Test
    public void TestOpenAndWriteSinkWithTimeouts() throws Exception {


        Map<String, Object> configs = new HashMap<>();
        configs.put("clusterUrl", System.getenv("kustoCluster"));
        configs.put("database", System.getenv("kustoDatabase"));
        configs.put("table", table);
        configs.put("batchSize",10);
        configs.put("batchTimeMs", 5000);
        configs.put("flushImmediately", true);
        configs.put("appId", System.getenv("kustoAadAppId"));
        configs.put("appKey", System.getenv("kustoAadAppSecret"));
        configs.put("tenantId", System.getenv("kustoAadAuthorityID"));

        ADXSink sink = new ADXSink();
        sink.open(configs, null);
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
        SinkRecord<byte[]> record = new SinkRecord<>(new Record<>() {

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
                return new HashMap<String, String>();
            }
        }, value.getBytes(StandardCharsets.UTF_8));

        return record;
    }
}
