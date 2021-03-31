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

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Test basic Pulsar SQL query, the Pulsar SQL is standalone mode.
 */
@Slf4j
public class TestBasicPresto extends TestPulsarSQLBase {

    private static final int NUM_OF_STOCKS = 10;

    @BeforeClass
    public void setupPresto() throws Exception {
        log.info("[TestBasicPresto] setupPresto...");
        pulsarCluster.startPrestoWorker();
    }

    @AfterClass(alwaysRun = true)
    public void teardownPresto() {
        log.info("[TestBasicPresto] tearing down...");
        pulsarCluster.stopPrestoWorker();
    }

    @DataProvider(name = "schemaProvider")
    public Object[][] schemaProvider() {
        return new Object[][] {
                { Schema.BYTES},
                { Schema.BYTEBUFFER},
                { Schema.STRING},
                { AvroSchema.of(Stock.class)},
                { JSONSchema.of(Stock.class)},
                { ProtobufNativeSchema.of(StockProtoMessage.Stock.class)},
                { Schema.KeyValue(Schema.AVRO(Stock.class), Schema.AVRO(Stock.class), KeyValueEncodingType.INLINE) },
                { Schema.KeyValue(Schema.AVRO(Stock.class), Schema.AVRO(Stock.class), KeyValueEncodingType.SEPARATED) }
        };
    }

    @Test
    public void testSimpleSQLQueryBatched() throws Exception {
        TopicName topicName = TopicName.get("public/default/stocks_batched_" + randomName(5));
        pulsarSQLBasicTest(topicName, true, false, JSONSchema.of(Stock.class));
    }

    @Test
    public void testSimpleSQLQueryNonBatched() throws Exception {
        TopicName topicName = TopicName.get("public/default/stocks_nonbatched_" + randomName(5));
        pulsarSQLBasicTest(topicName, false, false, JSONSchema.of(Stock.class));
    }

    @Test(dataProvider = "schemaProvider")
    public void testForSchema(Schema schema) throws Exception {
        String schemaFlag;
        if (schema.getSchemaInfo().getType().isStruct()) {
            schemaFlag = schema.getSchemaInfo().getType().name();
        } else if(schema.getSchemaInfo().getType().equals(SchemaType.KEY_VALUE)) {
            schemaFlag = schema.getSchemaInfo().getType().name() + "_"
                    + ((KeyValueSchema) schema).getKeyValueEncodingType();
        } else {
            // Because some schema types are same(such as BYTES and BYTEBUFFER), so use the schema name as flag.
            schemaFlag = schema.getSchemaInfo().getName();
        }
        String topic = String.format("public/default/schema_%s_test_%s", schemaFlag, randomName(5)).toLowerCase();
        pulsarSQLBasicTest(TopicName.get(topic), false, false, schema);
    }

    @Test
    public void testForUppercaseTopic() throws Exception {
        TopicName topicName = TopicName.get("public/default/case_UPPER_topic_" + randomName(5));
        pulsarSQLBasicTest(topicName, false, false, JSONSchema.of(Stock.class));
    }

    @Test
    public void testForDifferentCaseTopic() throws Exception {
        String tableName = "diff_case_topic_" + randomName(5);

        String topic1 = "public/default/" + tableName.toUpperCase();
        TopicName topicName1 = TopicName.get(topic1);
        prepareData(topicName1, false, false, JSONSchema.of(Stock.class));

        String topic2 = "public/default/" + tableName;
        TopicName topicName2 = TopicName.get(topic2);
        prepareData(topicName2, false, false, JSONSchema.of(Stock.class));

        try {
            String query = "select * from pulsar.\"public/default\".\"" + tableName + "\"";
            execQuery(query);
            Assert.fail("The testForDifferentCaseTopic query [" + query + "] should be failed.");
        } catch (ContainerExecException e) {
            log.warn("Expected exception. result stderr: {}", e.getResult().getStderr(), e);
            assertTrue(e.getResult().getStderr().contains("There are multiple topics"));
            assertTrue(e.getResult().getStderr().contains(topic1));
            assertTrue(e.getResult().getStderr().contains(topic2));
            assertTrue(e.getResult().getStderr().contains("matched the table name public/default/" + tableName));
        }
    }

    @Override
    protected int prepareData(TopicName topicName,
                              boolean isBatch,
                              boolean useNsOffloadPolices,
                              Schema schema) throws Exception {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        if (schema.getSchemaInfo().getName().equals(Schema.BYTES.getSchemaInfo().getName())) {
            prepareDataForBytesSchema(pulsarClient, topicName, isBatch);
        } else if (schema.getSchemaInfo().getName().equals(Schema.BYTEBUFFER.getSchemaInfo().getName())) {
            prepareDataForByteBufferSchema(pulsarClient, topicName, isBatch);
        } else if (schema.getSchemaInfo().getType().equals(SchemaType.STRING)) {
            prepareDataForStringSchema(pulsarClient, topicName, isBatch);
        } else if (schema.getSchemaInfo().getType().equals(SchemaType.JSON)
                || schema.getSchemaInfo().getType().equals(SchemaType.AVRO)) {
            prepareDataForStructSchema(pulsarClient, topicName, isBatch, schema);
        } else if (schema.getSchemaInfo().getType().equals(SchemaType.PROTOBUF_NATIVE)) {
            prepareDataForProtobufNativeSchema(pulsarClient, topicName, isBatch, schema);
        } else if (schema.getSchemaInfo().getType().equals(SchemaType.KEY_VALUE)) {
            prepareDataForKeyValueSchema(pulsarClient, topicName, schema);
        }

        return NUM_OF_STOCKS;
    }

    private void prepareDataForBytesSchema(PulsarClient pulsarClient,
                                           TopicName topicName,
                                           boolean isBatch) throws PulsarClientException {
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topicName.toString())
                .enableBatching(isBatch)
                .create();

        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            producer.send(("bytes schema test" + i).getBytes());
        }
        producer.flush();
    }

    private void prepareDataForByteBufferSchema(PulsarClient pulsarClient,
                                                TopicName topicName,
                                                boolean isBatch) throws PulsarClientException {
        @Cleanup
        Producer<ByteBuffer> producer = pulsarClient.newProducer(Schema.BYTEBUFFER)
                .topic(topicName.toString())
                .enableBatching(isBatch)
                .create();

        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            producer.send(ByteBuffer.wrap(("bytes schema test" + i).getBytes()));
        }
        producer.flush();
    }

    private void prepareDataForStringSchema(PulsarClient pulsarClient,
                                            TopicName topicName,
                                            boolean isBatch) throws PulsarClientException {
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName.toString())
                .enableBatching(isBatch)
                .create();

        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            producer.send("string" + i);
        }
        producer.flush();
    }

    private void prepareDataForStructSchema(PulsarClient pulsarClient,
                                            TopicName topicName,
                                            boolean isBatch,
                                            Schema<Stock> schema) throws Exception {
        @Cleanup
        Producer<Stock> producer = pulsarClient.newProducer(schema)
                .topic(topicName.toString())
                .enableBatching(isBatch)
                .create();

        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            final Stock stock = new Stock(i,"STOCK_" + i , 100.0 + i * 10);
            producer.send(stock);
        }
        producer.flush();
    }

    private void prepareDataForProtobufNativeSchema(PulsarClient pulsarClient,
                                            TopicName topicName,
                                            boolean isBatch,
                                            Schema<StockProtoMessage.Stock> schema) throws Exception {
        @Cleanup
        Producer<StockProtoMessage.Stock> producer = pulsarClient.newProducer(schema)
                .topic(topicName.toString())
                .enableBatching(isBatch)
                .create();

        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            final StockProtoMessage.Stock stock = StockProtoMessage.Stock.newBuilder().
                    setEntryId(i).setSymbol("STOCK_" + i).setSharePrice(100.0 + i * 10).build();
            producer.send(stock);
        }
        producer.flush();
    }

    private void prepareDataForKeyValueSchema(PulsarClient pulsarClient,
                                              TopicName topicName,
                                              Schema<KeyValue<Stock, Stock>> schema) throws Exception {
        @Cleanup
        Producer<KeyValue<Stock,Stock>> producer = pulsarClient.newProducer(schema)
                .topic(topicName.toString())
                .create();

        for (int i = 0 ; i < NUM_OF_STOCKS; ++i) {
            int j = 100 * i;
            final Stock stock1 = new Stock(j, "STOCK_" + j , 100.0 + j * 10);
            final Stock stock2 = new Stock(i, "STOCK_" + i , 100.0 + i * 10);
            producer.send(new KeyValue<>(stock1, stock2));
        }
    }

    @Override
    protected void validateContent(int messageNum, String[] contentArr, Schema schema) {
        switch (schema.getSchemaInfo().getType()) {
            case BYTES:
                log.info("Skip validate content for BYTES schema type.");
                break;
            case STRING:
                validateContentForStringSchema(messageNum, contentArr);
                log.info("finish validate content for STRING schema type.");
                break;
            case JSON:
            case AVRO:
            case PROTOBUF_NATIVE:
                validateContentForStructSchema(messageNum, contentArr);
                log.info("finish validate content for {} schema type.", schema.getSchemaInfo().getType());
                break;
            case KEY_VALUE:
                validateContentForKeyValueSchema(messageNum, contentArr);
                log.info("finish validate content for KEY_VALUE {} schema type.",
                        ((KeyValueSchema) schema).getKeyValueEncodingType());
        }
    }

    private void validateContentForStringSchema(int messageNum, String[] contentArr) {
        for (int i = 0; i < messageNum; i++) {
            assertThat(contentArr).contains("\"string" + i + "\"");
        }
    }

    private void validateContentForStructSchema(int messageNum, String[] contentArr) {
        for (int i = 0; i < messageNum; ++i) {
            assertThat(contentArr).contains("\"" + i + "\"");
            assertThat(contentArr).contains("\"" + "STOCK_" + i + "\"");
            assertThat(contentArr).contains("\"" + (100.0 + i * 10) + "\"");
        }
    }

    private void validateContentForKeyValueSchema(int messageNum, String[] contentArr) {
        for (int i = 0; i < messageNum; ++i) {
            int j = 100 * i;
            assertThat(contentArr).contains("\"" + i + "\"");
            assertThat(contentArr).contains("\"" + "STOCK_" + i + "\"");
            assertThat(contentArr).contains("\"" + (100.0 + i * 10) + "\"");

            assertThat(contentArr).contains("\"" + j + "\"");
            assertThat(contentArr).contains("\"" + "STOCK_" + j + "\"");
            assertThat(contentArr).contains("\"" + (100.0 + j * 10) + "\"");
        }
    }

}
