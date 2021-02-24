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
package org.apache.pulsar.tests.integration.presto;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestBasicPresto extends TestPulsarSQLBase {

    private static final int NUM_OF_STOCKS = 10;

    @BeforeClass
    public void setupPresto() throws Exception {
        log.info("[TestBasicPresto] setupPresto...");
        pulsarCluster.startPrestoWorker();
    }

    @AfterClass
    public void teardownPresto() {
        log.info("[TestBasicPresto] tearing down...");
        pulsarCluster.stopPrestoWorker();
    }

    @Test
    public void testSimpleSQLQueryBatched() throws Exception {
        TopicName topicName = TopicName.get("public/default/stocks_batched_" + randomName(5));
        pulsarSQLBasicTest(topicName, true, false);
    }

    @Test
    public void testSimpleSQLQueryNonBatched() throws Exception {
        TopicName topicName = TopicName.get("public/default/stocks_nonbatched_" + randomName(5));
        pulsarSQLBasicTest(topicName, false, false);
    }

    @DataProvider(name = "keyValueEncodingType")
    public Object[][] keyValueEncodingType() {
        return new Object[][] { { KeyValueEncodingType.INLINE }, { KeyValueEncodingType.SEPARATED } };
    }

    @Test(dataProvider = "keyValueEncodingType")
    public void testKeyValueSchema(KeyValueEncodingType type) throws Exception {
        waitPulsarSQLReady();
        TopicName topicName = TopicName.get("public/default/stocks" + randomName(20));
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        Producer<KeyValue<Stock,Stock>> producer = pulsarClient.newProducer(Schema
                .KeyValue(Schema.AVRO(Stock.class), Schema.AVRO(Stock.class), type))
                .topic(topicName.toString())
                .create();

        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            int j = 100 * i;
            final Stock stock1 = new Stock(j, "STOCK_" + j , 100.0 + j * 10);
            final Stock stock2 = new Stock(i, "STOCK_" + i , 100.0 + i * 10);
            producer.send(new KeyValue<>(stock1, stock2));
        }

        producer.flush();

        validateMetadata(topicName);

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    ContainerExecResult containerExecResult = execQuery(
                            String.format("select * from pulsar.\"%s\".\"%s\" order by entryid;",
                                    topicName.getNamespace(), topicName.getLocalName()));
                    assertThat(containerExecResult.getExitCode()).isEqualTo(0);
                    log.info("select sql query output \n{}", containerExecResult.getStdout());
                    String[] split = containerExecResult.getStdout().split("\n");
                    assertThat(split.length).isEqualTo(NUM_OF_STOCKS);
                    String[] split2 = containerExecResult.getStdout().split("\n|,");
                    for (int i = 0; i < NUM_OF_STOCKS; ++i) {
                        int j = 100 * i;
                        assertThat(split2).contains("\"" + i + "\"");
                        assertThat(split2).contains("\"" + "STOCK_" + i + "\"");
                        assertThat(split2).contains("\"" + (100.0 + i * 10) + "\"");

                        assertThat(split2).contains("\"" + j + "\"");
                        assertThat(split2).contains("\"" + "STOCK_" + j + "\"");
                        assertThat(split2).contains("\"" + (100.0 + j * 10) + "\"");
                    }
                }
        );

    }

    @Override
    protected int prepareData(TopicName topicName, boolean isBatch, boolean useNsOffloadPolices) throws Exception {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        Producer<Stock> producer = pulsarClient.newProducer(JSONSchema.of(Stock.class))
                .topic(topicName.toString())
                .enableBatching(isBatch)
                .create();

        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            final Stock stock = new Stock(i,"STOCK_" + i , 100.0 + i * 10);
            producer.send(stock);
        }
        producer.flush();
        return NUM_OF_STOCKS;
    }
}
