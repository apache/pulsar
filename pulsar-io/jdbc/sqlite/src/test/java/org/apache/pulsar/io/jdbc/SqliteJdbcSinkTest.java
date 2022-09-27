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

package org.apache.pulsar.io.jdbc;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
/**
 * Jdbc Sink test
 */
@Slf4j
public class SqliteJdbcSinkTest {
    private final SqliteUtils sqliteUtils = new SqliteUtils(UUID.randomUUID().toString());
    private BaseJdbcAutoSchemaSink jdbcSink;
    private final String tableName = "TestOpenAndWriteSink";

    /**
     * A Simple class to test jdbc class
     */
    @Data
    public static class Foo {
        private String field1;
        private String field2;
        private int field3;
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        sqliteUtils.setUp();
        sqliteUtils.createTable(
                "CREATE TABLE " + tableName + "(" +
                        "    field1  TEXT," +
                        "    field2  TEXT," +
                        "    field3 INTEGER," +
                        "PRIMARY KEY (field1));"
        );

        // prepare data for update sql
        String updateSql = "insert into " + tableName + " values('ValueOfField4', 'ValueOfField4', 4)";
        sqliteUtils.execute(updateSql);

        // prepare data for delete sql
        String deleteSql = "insert into " + tableName + " values('ValueOfField5', 'ValueOfField5', 5)";
        sqliteUtils.execute(deleteSql);
        Map<String, Object> conf;

        String jdbcUrl = sqliteUtils.sqliteUri();

        conf = Maps.newHashMap();
        conf.put("jdbcUrl", jdbcUrl);
        conf.put("tableName", tableName);
        conf.put("key", "field3");
        conf.put("nonKey", "field1,field2");
        // change batchSize to 1, to flush on each write.
        conf.put("batchSize", 1);

        jdbcSink = new SqliteJdbcAutoSchemaSink();

        // open should succeed
        jdbcSink.open(conf, null);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        jdbcSink.close();
        sqliteUtils.tearDown();
    }

    private void testOpenAndWriteSinkNullValue(Map<String, String> actionProperties) throws Exception {
        Message<GenericObject> insertMessage = mock(MessageImpl.class);
        GenericSchema<GenericRecord> genericAvroSchema;
        // prepare a foo Record
        Foo insertObj = new Foo();
        insertObj.setField1("ValueOfField1");
        // Not setting field2
        // Field1 is the key and field3 is used for selecting records 
        insertObj.setField3(3);
        AvroSchema<Foo> schema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(true).build());

        byte[] insertBytes = schema.encode(insertObj);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericObject> insertRecord = PulsarRecord.<GenericObject>builder()
            .message(insertMessage)
            .topicName("fake_topic_name")
            .ackFunction(() -> future.complete(null))
            .build();

        genericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());
        when(insertMessage.getValue()).thenReturn(genericAvroSchema.decode(insertBytes));
        when(insertMessage.getProperties()).thenReturn(actionProperties);
        log.info("foo:{}, Message.getValue: {}, record.getValue: {}",
                insertObj.toString(),
                insertMessage.getValue().toString(),
                insertRecord.getValue().toString());

        // write should success.
        jdbcSink.write(insertRecord);
        log.info("executed write");
        // sleep to wait backend flush complete
        future.get(1, TimeUnit.SECONDS);

        // value has been written to db, read it out and verify.
        String querySql = "SELECT * FROM " + tableName + " WHERE field3=3";
        int count = sqliteUtils.select(querySql, (resultSet) -> {
            Assert.assertEquals(insertObj.getField1(), resultSet.getString(1));
            Assert.assertNull(insertObj.getField2());
            Assert.assertEquals(insertObj.getField3(), resultSet.getInt(3));
        });
        Assert.assertEquals(count, 1);

    }

    private void testOpenAndWriteSinkJson(Map<String, String> actionProperties) throws Exception {
        Message<GenericObject> insertMessage = mock(MessageImpl.class);
        GenericSchema<GenericRecord> genericAvroSchema;
        // prepare a foo Record
        Foo insertObj = new Foo();
        insertObj.setField1("ValueOfField1");
        insertObj.setField2("ValueOfField2");
        insertObj.setField3(3);
        JSONSchema<Foo> schema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(true).build());

        byte[] insertBytes = schema.encode(insertObj);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericObject> insertRecord = PulsarRecord.<GenericObject>builder()
            .message(insertMessage)
            .topicName("fake_topic_name")
            .ackFunction(() -> future.complete(null))
            .build();

        GenericSchema<GenericRecord> decodeSchema = GenericSchemaImpl.of(schema.getSchemaInfo());
        when(insertMessage.getValue()).thenReturn(decodeSchema.decode(insertBytes));
        when(insertMessage.getProperties()).thenReturn(actionProperties);
        log.info("foo:{}, Message.getValue: {}, record.getValue: {}",
                insertObj.toString(),
                insertMessage.getValue().toString(),
                insertRecord.getValue().toString());

        // write should success.
        jdbcSink.write(insertRecord);
        log.info("executed write");
        // sleep to wait backend flush complete
        future.get(1, TimeUnit.SECONDS);

        // value has been written to db, read it out and verify.
        String querySql = "SELECT * FROM " + tableName + " WHERE field3=3";
        int count = sqliteUtils.select(querySql, (resultSet) -> {
            Assert.assertEquals(insertObj.getField1(), resultSet.getString(1));
            Assert.assertEquals(insertObj.getField2(), resultSet.getString(2));
            Assert.assertEquals(insertObj.getField3(), resultSet.getInt(3));
        });
        Assert.assertEquals(count, 1);

    }

    private void testOpenAndWriteSinkNullValueJson(Map<String, String> actionProperties) throws Exception {
        Message<GenericObject> insertMessage = mock(MessageImpl.class);
        GenericSchema<GenericRecord> genericAvroSchema;
        // prepare a foo Record
        Foo insertObj = new Foo();
        insertObj.setField1("ValueOfField1");
        // Not setting field2
        // Field1 is the key and field3 is used for selecting records 
        insertObj.setField3(3);
        JSONSchema<Foo> schema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(true).build());

        byte[] insertBytes = schema.encode(insertObj);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericObject> insertRecord = PulsarRecord.<GenericObject>builder()
            .message(insertMessage)
            .topicName("fake_topic_name")
            .ackFunction(() -> future.complete(null))
            .build();

        GenericSchema<GenericRecord> decodeSchema = GenericSchemaImpl.of(schema.getSchemaInfo());
        when(insertMessage.getValue()).thenReturn(decodeSchema.decode(insertBytes));
        when(insertMessage.getProperties()).thenReturn(actionProperties);
        log.info("foo:{}, Message.getValue: {}, record.getValue: {}",
                insertObj.toString(),
                insertMessage.getValue().toString(),
                insertRecord.getValue().toString());

        // write should success.
        jdbcSink.write(insertRecord);
        log.info("executed write");
        // sleep to wait backend flush complete
        // sleep to wait backend flush complete
        future.get(1, TimeUnit.SECONDS);

        // value has been written to db, read it out and verify.
        String querySql = "SELECT * FROM " + tableName + " WHERE field3=3";
        int count = sqliteUtils.select(querySql, (resultSet) -> {
            Assert.assertEquals(insertObj.getField1(), resultSet.getString(1));
            Assert.assertNull(insertObj.getField2());
            Assert.assertEquals(insertObj.getField3(), resultSet.getInt(3));
        });
        Assert.assertEquals(count, 1);

    }

    private void testOpenAndWriteSink(Map<String, String> actionProperties) throws Exception {
        Message<GenericObject> insertMessage = mock(MessageImpl.class);
        GenericSchema<GenericRecord> genericAvroSchema;
        // prepare a foo Record
        Foo insertObj = new Foo();
        insertObj.setField1("ValueOfField1");
        insertObj.setField2("ValueOfField2");
        insertObj.setField3(3);
        AvroSchema<Foo> schema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        byte[] insertBytes = schema.encode(insertObj);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericObject> insertRecord = PulsarRecord.<GenericObject>builder()
            .message(insertMessage)
            .topicName("fake_topic_name")
            .ackFunction(() -> future.complete(null))
            .build();

        genericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());
        when(insertMessage.getValue()).thenReturn(genericAvroSchema.decode(insertBytes));
        when(insertMessage.getProperties()).thenReturn(actionProperties);
        log.info("foo:{}, Message.getValue: {}, record.getValue: {}",
                insertObj.toString(),
                insertMessage.getValue().toString(),
                insertRecord.getValue().toString());

        // write should success.
        jdbcSink.write(insertRecord);
        log.info("executed write");
        // sleep to wait backend flush complete
        future.get(1, TimeUnit.SECONDS);

        // value has been written to db, read it out and verify.
        String querySql = "SELECT * FROM " + tableName + " WHERE field3=3";
        int count = sqliteUtils.select(querySql, (resultSet) -> {
            Assert.assertEquals(insertObj.getField1(), resultSet.getString(1));
            Assert.assertEquals(insertObj.getField2(), resultSet.getString(2));
            Assert.assertEquals(insertObj.getField3(), resultSet.getInt(3));
        });
        Assert.assertEquals(count, 1);

    }

    @Test
    public void TestInsertAction() throws Exception {
        testOpenAndWriteSink(ImmutableMap.of("ACTION", "INSERT"));
    }

    @Test
    public void TestNoAction() throws Exception {
        testOpenAndWriteSink(ImmutableMap.of());
    }

    @Test
    public void TestNoActionNullValue() throws Exception {
        testOpenAndWriteSinkNullValue(ImmutableMap.of("ACTION", "INSERT"));
    }

    @Test
    public void TestNoActionNullValueJson() throws Exception {
        testOpenAndWriteSinkNullValueJson(ImmutableMap.of("ACTION", "INSERT"));
    }

    @Test
    public void TestNoActionJson() throws Exception {
        testOpenAndWriteSinkJson(ImmutableMap.of("ACTION", "INSERT"));
    }

    @Test
    public void TestUnknownAction() throws Exception {
        Record<GenericObject> recordRecord = mock(Record.class);
        when(recordRecord.getProperties()).thenReturn(ImmutableMap.of("ACTION", "UNKNOWN"));
        CompletableFuture<Void> future = new CompletableFuture<>();
        doAnswer(a -> future.complete(null)).when(recordRecord).fail();
        jdbcSink.write(recordRecord);
        future.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void TestUpdateAction() throws Exception {

        AvroSchema<Foo> schema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Foo updateObj = new Foo();
        updateObj.setField1("ValueOfField3");
        updateObj.setField2("ValueOfField3");
        updateObj.setField3(4);

        byte[] updateBytes = schema.encode(updateObj);
        Message<GenericObject> updateMessage = mock(MessageImpl.class);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericObject> updateRecord = PulsarRecord.<GenericObject>builder()
                .message(updateMessage)
                .topicName("fake_topic_name")
                .ackFunction(() -> future.complete(null))
                .build();

        GenericSchema<GenericRecord> updateGenericAvroSchema;
        updateGenericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());

        Map<String, String> updateProperties = Maps.newHashMap();
        updateProperties.put("ACTION", "UPDATE");
        when(updateMessage.getValue()).thenReturn(updateGenericAvroSchema.decode(updateBytes));
        when(updateMessage.getProperties()).thenReturn(updateProperties);
        log.info("foo:{}, Message.getValue: {}, record.getValue: {}",
                updateObj.toString(),
                updateMessage.getValue().toString(),
                updateRecord.getValue().toString());

        jdbcSink.write(updateRecord);
        future.get(1, TimeUnit.SECONDS);

        // value has been written to db, read it out and verify.
        String updateQuerySql = "SELECT * FROM " + tableName + " WHERE field3=4";
        sqliteUtils.select(updateQuerySql, (resultSet) -> {
            Assert.assertEquals(updateObj.getField1(), resultSet.getString(1));
            Assert.assertEquals(updateObj.getField2(), resultSet.getString(2));
            Assert.assertEquals(updateObj.getField3(), resultSet.getInt(3));
        });
    }

    @Test
    public void TestDeleteAction() throws Exception {

        AvroSchema<Foo> schema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Foo deleteObj = new Foo();
        deleteObj.setField3(5);

        byte[] deleteBytes = schema.encode(deleteObj);
        Message<GenericObject> deleteMessage = mock(MessageImpl.class);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericObject> deleteRecord = PulsarRecord.<GenericObject>builder()
                .message(deleteMessage)
                .topicName("fake_topic_name")
                .ackFunction(() -> future.complete(null))
                .build();

        GenericSchema<GenericRecord> deleteGenericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());

        Map<String, String> deleteProperties = Maps.newHashMap();
        deleteProperties.put("ACTION", "DELETE");
        when(deleteMessage.getValue()).thenReturn(deleteGenericAvroSchema.decode(deleteBytes));
        when(deleteMessage.getProperties()).thenReturn(deleteProperties);
        log.info("foo:{}, Message.getValue: {}, record.getValue: {}",
                deleteObj.toString(),
                deleteMessage.getValue().toString(),
                deleteRecord.getValue().toString());

        jdbcSink.write(deleteRecord);
        future.get(1, TimeUnit.SECONDS);

        // value has been written to db, read it out and verify.
        String deleteQuerySql = "SELECT * FROM " + tableName + " WHERE field3=5";
        Assert.assertEquals(sqliteUtils.select(deleteQuerySql, (resultSet) -> {}), 0);
    }

    private static class MockKeyValueGenericRecord implements Record<GenericObject> {

        public MockKeyValueGenericRecord(Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema) {
            this.keyValueSchema = keyValueSchema;
        }

        int ackCount;
        int failCount;
        AtomicReference<KeyValue> keyValueHolder = new AtomicReference<>();
        Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema;

        private final GenericObject genericObject = new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return SchemaType.KEY_VALUE;
            }

            @Override
            public Object getNativeObject() {
                return keyValueHolder.get();
            }
        };
        @Override
        public Optional<String> getTopicName() {
            return Optional.of("topic");
        }

        @Override
        public org.apache.pulsar.client.api.Schema getSchema() {
            return keyValueSchema;
        }

        @Override
        public GenericObject getValue() {
            return genericObject;
        }

        public void setKeyValue(KeyValue kv) {
            keyValueHolder.set(kv);
        }

        @Override
        public void ack() {
            ackCount++;
        }

        @Override
        public void fail() {
            failCount++;
        }
    }

    @Data
    @AllArgsConstructor
    static class InsertModeTestConfig {
        SchemaType schemaType;
        JdbcSinkConfig.InsertMode insertMode;
    }

    @DataProvider(name = "insertModes")
    public Object[] schemaType() {
        List<SchemaType> schemas = Arrays.asList(SchemaType.JSON, SchemaType.AVRO);
        JdbcSinkConfig.InsertMode[] insertModes = JdbcSinkConfig.InsertMode.values();
        Object[] result = new Object[schemas.size() * insertModes.length];
        int i = 0;
        for (SchemaType schema : schemas) {
            for (JdbcSinkConfig.InsertMode insertMode : insertModes) {
                result[i++] = new InsertModeTestConfig(schema, insertMode);
            }
        }
        return result;
    }


    @Test(dataProvider = "insertModes")
    public void testInsertMode(InsertModeTestConfig config) throws Exception {
        jdbcSink.close();
        final JdbcSinkConfig.InsertMode insertMode = config.getInsertMode();
        final SchemaType schemaType = config.getSchemaType();

        final String tableName = "kvtable_insertmode";
        RecordSchemaBuilder keySchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("key");
        keySchemaBuilder.field("key").type(SchemaType.STRING).optional().defaultValue(null);
        GenericSchema<GenericRecord> keySchema = Schema.generic(keySchemaBuilder.build(schemaType));
        GenericRecord keyGenericRecord = keySchema.newRecordBuilder()
                .set("key", "mykey")
                .build();

        RecordSchemaBuilder valueSchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("value");
        valueSchemaBuilder.field("string").type(SchemaType.STRING).optional().defaultValue(null);
        valueSchemaBuilder.field("stringutf8").type(SchemaType.STRING).optional().defaultValue(null);
        valueSchemaBuilder.field("nulltext").type(SchemaType.STRING).optional().defaultValue(null);
        valueSchemaBuilder.field("int").type(SchemaType.INT32).optional().defaultValue(null);
        valueSchemaBuilder.field("bool").type(SchemaType.BOOLEAN).optional().defaultValue(null);
        valueSchemaBuilder.field("double").type(SchemaType.DOUBLE).optional().defaultValue(null);
        valueSchemaBuilder.field("float").type(SchemaType.FLOAT).optional().defaultValue(null);
        valueSchemaBuilder.field("long").type(SchemaType.INT64).optional().defaultValue(null);
        GenericSchema<GenericRecord> valueSchema = Schema.generic(valueSchemaBuilder.build(schemaType));

        Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema = Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.INLINE);
        MockKeyValueGenericRecord genericObjectRecord = new MockKeyValueGenericRecord(keyValueSchema);
        genericObjectRecord.setKeyValue(new KeyValue<>(keyGenericRecord, valueSchema.newRecordBuilder()
                .set("string", "thestring")
                .set("stringutf8", schemaType == SchemaType.AVRO ? new Utf8("thestringutf8"): "thestringutf8")
                .set("int", Integer.MAX_VALUE)
                .set("bool", true)
                .set("double", Double.MAX_VALUE)
                .set("float", Float.MAX_VALUE)
                .set("long", Long.MIN_VALUE)
                .build()));

        sqliteUtils.createTable(
                "CREATE TABLE " + tableName + " (" +
                        "    key  TEXT," +
                        "    int  INTEGER," +
                        "    string TEXT," +
                        "    stringutf8 TEXT," +
                        "    nulltext  TEXT," +
                        "    bool  NUMERIC," +
                        "    double NUMERIC," +
                        "    float NUMERIC," +
                        "    long INTEGER," +
                        "PRIMARY KEY (key));"
        );
        String jdbcUrl = sqliteUtils.sqliteUri();

        Map<String, Object> conf = Maps.newHashMap();
        conf.put("jdbcUrl", jdbcUrl);
        conf.put("tableName", tableName);
        conf.put("key", "key");
        conf.put("nonKey", "long,int,double,float,bool,nulltext,string,stringutf8");
        // change batchSize to 1, to flush on each write.
        conf.put("batchSize", 1);
        conf.put("insertMode", insertMode.toString());
        try (SqliteJdbcAutoSchemaSink kvSchemaJdbcSink = new SqliteJdbcAutoSchemaSink();) {
            kvSchemaJdbcSink.open(conf, null);
            kvSchemaJdbcSink.write(genericObjectRecord);

            Awaitility.await().untilAsserted(() -> {
                final int count = sqliteUtils.select("select int,string,stringutf8,bool,double,float," +
                        "long,nulltext from " + tableName + " where key='mykey'", (resultSet) -> {
                    int index = 1;
                    Assert.assertEquals(resultSet.getInt(index++), Integer.MAX_VALUE);
                    Assert.assertEquals(resultSet.getString(index++), "thestring");
                    Assert.assertEquals(resultSet.getString(index++), "thestringutf8");
                    Assert.assertEquals(resultSet.getBoolean(index++), true);
                    Assert.assertEquals(resultSet.getDouble(index++), Double.MAX_VALUE);
                    Assert.assertEquals(resultSet.getFloat(index++), Float.MAX_VALUE);
                    Assert.assertEquals(resultSet.getLong(index++), Long.MIN_VALUE);
                    Assert.assertNull(resultSet.getString(index++));
                });
                if (insertMode == JdbcSinkConfig.InsertMode.INSERT
                        || insertMode == JdbcSinkConfig.InsertMode.UPSERT) {
                    Assert.assertEquals(count, 1);
                } else {
                    Assert.assertEquals(count, 0);
                }
            });
        }

        if (insertMode == JdbcSinkConfig.InsertMode.UPDATE) {
            conf.put("insertMode", JdbcSinkConfig.InsertMode.INSERT);
            try (SqliteJdbcAutoSchemaSink kvSchemaJdbcSink = new SqliteJdbcAutoSchemaSink();) {
                kvSchemaJdbcSink.open(conf, null);
                kvSchemaJdbcSink.write(genericObjectRecord);
                Awaitility.await().untilAsserted(() -> {
                    final int count = sqliteUtils.select("select key from " + tableName + " where key='mykey'", (resultSet) -> {
                    });
                    Assert.assertEquals(count, 1);
                });
            }
        }
        conf.put("insertMode", insertMode.toString());
        try (SqliteJdbcAutoSchemaSink kvSchemaJdbcSink = new SqliteJdbcAutoSchemaSink();) {
            kvSchemaJdbcSink.open(conf, null);
            genericObjectRecord = new MockKeyValueGenericRecord(keyValueSchema);
            genericObjectRecord.setKeyValue(new KeyValue<>(keyGenericRecord, valueSchema.newRecordBuilder()
                    .set("string", "thestring_updated")
                    .set("stringutf8", schemaType == SchemaType.AVRO ?
                            new Utf8("thestringutf8_updated"): "thestringutf8_updated")
                    .set("int", Integer.MIN_VALUE)
                    .set("bool", false)
                    .set("double", Double.MIN_VALUE)
                    .set("float", Float.MIN_VALUE)
                    .set("long", Long.MAX_VALUE)
                    .set("nulltext", "nomore-null")
                    .build()));
            kvSchemaJdbcSink.write(genericObjectRecord);

            Awaitility.await().untilAsserted(() -> {
                final int count = sqliteUtils.select("select int,string,stringutf8,bool,double,float," +
                        "long,nulltext from " + tableName + "  where key='mykey'", (resultSet) -> {
                    int index = 1;
                    if (insertMode == JdbcSinkConfig.InsertMode.INSERT) {
                        Assert.assertEquals(resultSet.getInt(index++), Integer.MAX_VALUE);
                        Assert.assertEquals(resultSet.getString(index++), "thestring");
                        Assert.assertEquals(resultSet.getString(index++), "thestringutf8");
                        Assert.assertEquals(resultSet.getBoolean(index++), true);
                        Assert.assertEquals(resultSet.getDouble(index++), Double.MAX_VALUE);
                        Assert.assertEquals(resultSet.getFloat(index++), Float.MAX_VALUE);
                        Assert.assertEquals(resultSet.getLong(index++), Long.MIN_VALUE);
                        Assert.assertNull(resultSet.getString(index++));
                    } else {
                        Assert.assertEquals(resultSet.getInt(index++), Integer.MIN_VALUE);
                        Assert.assertEquals(resultSet.getString(index++), "thestring_updated");
                        Assert.assertEquals(resultSet.getString(index++), "thestringutf8_updated");
                        Assert.assertEquals(resultSet.getBoolean(index++), false);
                        Assert.assertEquals(resultSet.getDouble(index++), Double.MIN_VALUE);
                        Assert.assertEquals(resultSet.getFloat(index++), Float.MIN_VALUE);
                        Assert.assertEquals(resultSet.getLong(index++), Long.MAX_VALUE);
                        Assert.assertEquals(resultSet.getString(index++), "nomore-null");
                    }
                });
                if (insertMode == JdbcSinkConfig.InsertMode.INSERT) {
                    Assert.assertEquals(count, 1);
                } else if (insertMode == JdbcSinkConfig.InsertMode.UPSERT) {
                    Assert.assertEquals(count, 1);
                } else {
                    Assert.assertEquals(count, 1);
                }
            });
        }
    }

    @Data
    @AllArgsConstructor
    static class NullValueActionTestConfig {
        SchemaType schemaType;
        JdbcSinkConfig.NullValueAction nullValueAction;
    }
    @DataProvider(name = "nullValueActions")
    public Object[] nullValueActions() {
        List<SchemaType> schemas = Arrays.asList(SchemaType.JSON, SchemaType.AVRO);
        JdbcSinkConfig.NullValueAction[] nullValueActions = JdbcSinkConfig.NullValueAction.values();
        Object[] result = new Object[schemas.size() * nullValueActions.length];
        int i = 0;
        for (SchemaType schema : schemas) {
            for (JdbcSinkConfig.NullValueAction nullValueAction : nullValueActions) {
                result[i++] = new NullValueActionTestConfig(schema, nullValueAction);
            }
        }
        return result;
    }

    @Test(dataProvider = "nullValueActions")
    public void testNullValueAction(NullValueActionTestConfig config) throws Exception {
        jdbcSink.close();
        final SchemaType schemaType = config.getSchemaType();
        final JdbcSinkConfig.NullValueAction nullValueAction = config.getNullValueAction();
        final String tableName = "kvtable_nullvalue";

        RecordSchemaBuilder keySchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("key");
        keySchemaBuilder.field("key").type(SchemaType.STRING).optional().defaultValue(null);
        GenericSchema<GenericRecord> keySchema = Schema.generic(keySchemaBuilder.build(schemaType));
        GenericRecord keyGenericRecord = keySchema.newRecordBuilder()
                .set("key", "mykey")
                .build();

        RecordSchemaBuilder valueSchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("value");
        valueSchemaBuilder.field("string").type(SchemaType.STRING).optional().defaultValue(null);
        GenericSchema<GenericRecord> valueSchema = Schema.generic(valueSchemaBuilder.build(schemaType));

        Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema = Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.INLINE);
        MockKeyValueGenericRecord genericObjectRecord = new MockKeyValueGenericRecord(keyValueSchema);

        sqliteUtils.createTable("CREATE TABLE " + tableName + " (" +
                        "    key  TEXT," +
                        "    string TEXT," +
                        "PRIMARY KEY (key));"
        );
        String jdbcUrl = sqliteUtils.sqliteUri();

        Map<String, Object> conf = Maps.newHashMap();
        conf.put("jdbcUrl", jdbcUrl);
        conf.put("tableName", tableName);
        conf.put("key", "key");
        conf.put("nonKey", "string");
        conf.put("batchSize", 3);
        conf.put("insertMode", JdbcSinkConfig.InsertMode.UPSERT.toString());
        conf.put("nullValueAction", nullValueAction.toString());
        try (SqliteJdbcAutoSchemaSink kvSchemaJdbcSink = new SqliteJdbcAutoSchemaSink();) {
            kvSchemaJdbcSink.open(conf, null);
            genericObjectRecord.setKeyValue(new KeyValue<>(keyGenericRecord, valueSchema.newRecordBuilder()
                    .set("string", "thestring")
                    .build()));
            kvSchemaJdbcSink.write(genericObjectRecord);

            genericObjectRecord = new MockKeyValueGenericRecord(keyValueSchema);
            genericObjectRecord.setKeyValue(new KeyValue<>(keySchema.newRecordBuilder()
                    .set("key", "mykey2")
                    .build(), valueSchema.newRecordBuilder()
                    .set("string", "thestring")
                    .build()));
            kvSchemaJdbcSink.write(genericObjectRecord);

            genericObjectRecord = new MockKeyValueGenericRecord(keyValueSchema);
            genericObjectRecord.setKeyValue(new KeyValue<>(keyGenericRecord, null));
            kvSchemaJdbcSink.write(genericObjectRecord);

            Awaitility.await().untilAsserted(() -> {
                final int count = sqliteUtils.select("select key,string from " + tableName, (resultSet) -> {
                    int index = 1;
                    final String key = resultSet.getString(index++);
                    final String value = resultSet.getString(index++);
                    if (key.equals("mykey2")) {
                        Assert.assertEquals(value, "thestring");
                    } else {
                        throw new IllegalStateException();
                    }
                });
                if (nullValueAction == JdbcSinkConfig.NullValueAction.DELETE) {
                    Assert.assertEquals(count, 1);
                } else if (nullValueAction == JdbcSinkConfig.NullValueAction.FAIL) {
                    Assert.assertEquals(count, 0);
                } else {
                    throw new IllegalStateException();
                }
            });

        }
    }

}
