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
package org.apache.pulsar.io.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class HttpSinkTest {

    WireMockServer server;

    @BeforeClass
    public void setUp() {
        server = new WireMockServer(0);
        server.start();
        configureFor(server.port());
        stubFor(post(urlPathEqualTo("/"))
            .willReturn(aResponse().withStatus(200)));
    }

    @AfterClass
    public void tearDown() {
        server.stop();
    }

    @DataProvider(name = "primitives")
    public Object[][] primitives() {
        return new Object[][]{
            new Object[] {Schema.STRING, "test-string", "\"test-string\""},
            new Object[] {Schema.INT8, (byte) 42, "42"},
            new Object[] {Schema.INT16, (short) 42, "42"},
            new Object[] {Schema.INT32, 42, "42"},
            new Object[] {Schema.INT64, 42L, "42"},
            new Object[] {Schema.BOOL, true, "true"},
            new Object[] {Schema.FLOAT, 0.1F, "0.1"},
            new Object[] {Schema.DOUBLE, 0.1D, "0.1"},
            new Object[] {Schema.DATE, new Date(1662418008047L), "1662418008047"},
            new Object[] {Schema.TIME, new Time(0, 46, 48), "\"00:46:48\""},
            new Object[] {Schema.TIMESTAMP, new Timestamp(1662418008047L), "1662418008047"},
            new Object[] {Schema.INSTANT, Instant.ofEpochMilli(1662418008047L), "1662418008.047000000"},
            new Object[] {Schema.LOCAL_DATE, LocalDate.of(2022, 1, 1), "[2022,1,1]"},
            new Object[] {Schema.LOCAL_TIME, LocalTime.of(11, 12), "[11,12]"},
            new Object[] {Schema.LOCAL_DATE_TIME, new Timestamp(1662418008047L), "1662418008047"},
        };
    }

    @Test(dataProvider = "primitives")
    public void testPrimitives(Schema<?> schema, Object value, String responseBody) throws Exception {
        GenericObject genericObject = new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return null;
            }

            @Override
            public Object getNativeObject() {
                return value;
            }
        };
        test(schema, genericObject, responseBody);
    }

    @DataProvider(name = "schema")
    public Object[][] schema() {
        return new Object[][]{
            new Object[]{Schema.JSON(Object.class)},
            new Object[]{Schema.AVRO(Object.class)},
        };
    }

    @Test(dataProvider = "schema")
    public void testGenericRecord(Schema<?> schema) throws Exception {
        SchemaType schemaType = schema.getSchemaInfo().getType();
        RecordSchemaBuilder valueSchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("value");
        valueSchemaBuilder.field("c").type(SchemaType.STRING).optional().defaultValue(null);
        valueSchemaBuilder.field("d").type(SchemaType.INT32).optional().defaultValue(null);
        RecordSchemaBuilder udtSchemaBuilder = SchemaBuilder.record("type1");
        udtSchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
        udtSchemaBuilder.field("b").type(SchemaType.BOOLEAN).optional().defaultValue(null);
        udtSchemaBuilder.field("d").type(SchemaType.DOUBLE).optional().defaultValue(null);
        udtSchemaBuilder.field("f").type(SchemaType.FLOAT).optional().defaultValue(null);
        udtSchemaBuilder.field("i").type(SchemaType.INT32).optional().defaultValue(null);
        udtSchemaBuilder.field("l").type(SchemaType.INT64).optional().defaultValue(null);
        GenericSchema<GenericRecord> udtGenericSchema = Schema.generic(udtSchemaBuilder.build(schemaType));
        valueSchemaBuilder.field("e", udtGenericSchema).type(schemaType).optional().defaultValue(null);
        GenericSchema<GenericRecord> valueSchema = Schema.generic(valueSchemaBuilder.build(schemaType));

        GenericRecord valueGenericRecord = valueSchema.newRecordBuilder()
            .set("c", "1")
            .set("d", 1)
            .set("e", udtGenericSchema.newRecordBuilder()
                .set("a", "a")
                .set("b", true)
                .set("d", 1.0)
                .set("f", 1.0f)
                .set("i", 1)
                .set("l", 10L)
                .build())
            .build();

        String responseBody =
            "{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,\"i\":1,\"l\":10}}";
        test(schema, valueGenericRecord, responseBody);
    }

    @Test
    public void testKeyValuePrimitives() throws Exception {
        Schema<KeyValue<String, String>> keyValueSchema = KeyValueSchemaImpl.of(Schema.STRING, Schema.STRING);
        GenericObject genericObject = new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return null;
            }

            @Override
            public Object getNativeObject() {
                return new KeyValue<>("test-key", "test-value");
            }
        };
        test(keyValueSchema, genericObject, "{\"value\":\"test-value\",\"key\":\"test-key\"}");
    }

    @Test(dataProvider = "schema")
    public void testKeyValueGenericRecord(Schema<?> schema) throws Exception {
        SchemaType schemaType = schema.getSchemaInfo().getType();
        RecordSchemaBuilder keySchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("key");
        keySchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
        keySchemaBuilder.field("b").type(SchemaType.INT32).optional().defaultValue(null);
        GenericSchema<GenericRecord> keySchema = Schema.generic(keySchemaBuilder.build(schemaType));
        GenericRecord keyGenericRecord = keySchema.newRecordBuilder()
            .set("a", "1")
            .set("b", 1)
            .build();

        RecordSchemaBuilder valueSchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("value");
        valueSchemaBuilder.field("c").type(SchemaType.STRING).optional().defaultValue(null);
        valueSchemaBuilder.field("d").type(SchemaType.INT32).optional().defaultValue(null);
        RecordSchemaBuilder udtSchemaBuilder = SchemaBuilder.record("type1");
        udtSchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
        udtSchemaBuilder.field("b").type(SchemaType.BOOLEAN).optional().defaultValue(null);
        udtSchemaBuilder.field("d").type(SchemaType.DOUBLE).optional().defaultValue(null);
        udtSchemaBuilder.field("f").type(SchemaType.FLOAT).optional().defaultValue(null);
        udtSchemaBuilder.field("i").type(SchemaType.INT32).optional().defaultValue(null);
        udtSchemaBuilder.field("l").type(SchemaType.INT64).optional().defaultValue(null);
        GenericSchema<GenericRecord> udtGenericSchema = Schema.generic(udtSchemaBuilder.build(schemaType));
        valueSchemaBuilder.field("e", udtGenericSchema).type(schemaType).optional().defaultValue(null);
        GenericSchema<GenericRecord> valueSchema = Schema.generic(valueSchemaBuilder.build(schemaType));

        GenericRecord valueGenericRecord = valueSchema.newRecordBuilder()
            .set("c", "1")
            .set("d", 1)
            .set("e", udtGenericSchema.newRecordBuilder()
                .set("a", "a")
                .set("b", true)
                .set("d", 1.0)
                .set("f", 1.0f)
                .set("i", 1)
                .set("l", 10L)
                .build())
            .build();

        Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema = Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.INLINE);
        KeyValue<GenericRecord, GenericRecord> keyValue = new KeyValue<>(keyGenericRecord, valueGenericRecord);
        GenericObject genericObject = new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return SchemaType.KEY_VALUE;
            }

            @Override
            public Object getNativeObject() {
                return keyValue;
            }
        };
        String responseBody = "{\"value\":{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,"
            + "\"i\":1,\"l\":10}},\"key\":{\"a\":\"1\",\"b\":1}}";
        test(keyValueSchema, genericObject, responseBody);
    }

    private void test(Schema<?> schema, GenericObject genericObject, String responseBody) throws Exception {
        HttpSink httpSink = new HttpSink();
        Map<String, Object> config = new HashMap<>();
        config.put("url", server.baseUrl());
        Map<String, String> headers = new HashMap<>();
        headers.put("header-name", "header-value");
        config.put("headers", headers);
        httpSink.open(config, null);

        long now = 1662418008000L;
        Map<String, String> messageProperties = new HashMap<>();
        messageProperties.put("prop-name", "prop-value");

        Record<GenericObject> record = new Record<>() {
            @Override
            public GenericObject getValue() {
                return genericObject;
            }

            @Override
            public Schema getSchema() {
                return schema;
            }

            @Override
            public Optional<Long> getEventTime() {
                return Optional.of(now);
            }

            @Override
            public Map<String, String> getProperties() {
                return messageProperties;
            }

            @Override
            public Optional<String> getTopicName() {
                return Optional.of("test-topic");
            }

            @Override
            public Optional<String> getKey() {
                return Optional.of("test-key");
            }

            @Override
            public Optional<Message<GenericObject>> getMessage() {
                return Optional.of(new Message<>() {

                    @Override
                    public Map<String, String> getProperties() {
                        return null;
                    }

                    @Override
                    public boolean hasProperty(String name) {
                        return false;
                    }

                    @Override
                    public String getProperty(String name) {
                        return null;
                    }

                    @Override
                    public byte[] getData() {
                        return new byte[0];
                    }

                    @Override
                    public int size() {
                        return 0;
                    }

                    @Override
                    public GenericObject getValue() {
                        return null;
                    }

                    @Override
                    public MessageId getMessageId() {
                        return new MessageIdImpl(1, 2, 3);
                    }

                    @Override
                    public long getPublishTime() {
                        return now + 1;
                    }

                    @Override
                    public long getEventTime() {
                        return 0;
                    }

                    @Override
                    public long getSequenceId() {
                        return 0;
                    }

                    @Override
                    public String getProducerName() {
                        return null;
                    }

                    @Override
                    public boolean hasKey() {
                        return false;
                    }

                    @Override
                    public String getKey() {
                        return null;
                    }

                    @Override
                    public boolean hasBase64EncodedKey() {
                        return false;
                    }

                    @Override
                    public byte[] getKeyBytes() {
                        return new byte[0];
                    }

                    @Override
                    public boolean hasOrderingKey() {
                        return false;
                    }

                    @Override
                    public byte[] getOrderingKey() {
                        return new byte[0];
                    }

                    @Override
                    public String getTopicName() {
                        return null;
                    }

                    @Override
                    public Optional<EncryptionContext> getEncryptionCtx() {
                        return Optional.empty();
                    }

                    @Override
                    public int getRedeliveryCount() {
                        return 0;
                    }

                    @Override
                    public byte[] getSchemaVersion() {
                        return new byte[0];
                    }

                    @Override
                    public boolean isReplicated() {
                        return false;
                    }

                    @Override
                    public String getReplicatedFrom() {
                        return null;
                    }

                    @Override
                    public void release() {

                    }

                    @Override
                    public boolean hasBrokerPublishTime() {
                        return false;
                    }

                    @Override
                    public Optional<Long> getBrokerPublishTime() {
                        return Optional.empty();
                    }

                    @Override
                    public boolean hasIndex() {
                        return false;
                    }

                    @Override
                    public Optional<Long> getIndex() {
                        return Optional.empty();
                    }
                });
            }
        };
        httpSink.write(record);

        verify(postRequestedFor(urlEqualTo("/"))
            .withRequestBody(equalTo(responseBody))
            .withHeader("Content-Type", equalTo("application/json"))
            .withHeader("header-name", equalTo("header-value"))
            .withHeader("PulsarTopic", equalTo("test-topic"))
            .withHeader("PulsarKey", equalTo("test-key"))
            .withHeader("PulsarEventTime", equalTo("1662418008000"))
            .withHeader("PulsarPublishTime", equalTo("1662418008001"))
            .withHeader("PulsarMessageId", equalTo("CAEQAhgDMAA="))
            .withHeader("PulsarProperties-prop-name", equalTo("prop-value"))
        );
    }

    @Test(expectedExceptions = IOException.class)
    public void testRequestFailure() throws Exception {
        stubFor(post(urlPathEqualTo("/"))
            .willReturn(aResponse().withStatus(500)));

        testKeyValuePrimitives();
    }
}
