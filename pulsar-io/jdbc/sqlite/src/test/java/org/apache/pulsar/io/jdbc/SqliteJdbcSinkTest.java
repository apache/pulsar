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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Jdbc Sink test
 */
@Slf4j
public class SqliteJdbcSinkTest {
    private final SqliteUtils sqliteUtils = new SqliteUtils(getClass().getSimpleName());
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

    @BeforeMethod
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

        // open should success
        jdbcSink.open(conf, null);


    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        sqliteUtils.tearDown();
        jdbcSink.close();
    }

    private void testOpenAndWriteSinkNullValue(Map<String, String> actionProperties) throws Exception {
        Message<GenericRecord> insertMessage = mock(MessageImpl.class);
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
        Record<GenericRecord> insertRecord = PulsarRecord.<GenericRecord>builder()
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
        Message<GenericRecord> insertMessage = mock(MessageImpl.class);
        GenericSchema<GenericRecord> genericAvroSchema;
        // prepare a foo Record
        Foo insertObj = new Foo();
        insertObj.setField1("ValueOfField1");
        insertObj.setField2("ValueOfField2");
        insertObj.setField3(3);
        JSONSchema<Foo> schema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(true).build());

        byte[] insertBytes = schema.encode(insertObj);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericRecord> insertRecord = PulsarRecord.<GenericRecord>builder()
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
        Message<GenericRecord> insertMessage = mock(MessageImpl.class);
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
        Record<GenericRecord> insertRecord = PulsarRecord.<GenericRecord>builder()
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
            Assert.assertNull(insertObj.getField2());
            Assert.assertEquals(insertObj.getField3(), resultSet.getInt(3));
        });
        Assert.assertEquals(count, 1);

    }

    private void testOpenAndWriteSink(Map<String, String> actionProperties) throws Exception {
        Message<GenericRecord> insertMessage = mock(MessageImpl.class);
        GenericSchema<GenericRecord> genericAvroSchema;
        // prepare a foo Record
        Foo insertObj = new Foo();
        insertObj.setField1("ValueOfField1");
        insertObj.setField2("ValueOfField2");
        insertObj.setField3(3);
        AvroSchema<Foo> schema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        byte[] insertBytes = schema.encode(insertObj);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericRecord> insertRecord = PulsarRecord.<GenericRecord>builder()
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
        Record<GenericRecord> recordRecord = mock(Record.class);
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
        Message<GenericRecord> updateMessage = mock(MessageImpl.class);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericRecord> updateRecord = PulsarRecord.<GenericRecord>builder()
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
        Message<GenericRecord> deleteMessage = mock(MessageImpl.class);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericRecord> deleteRecord = PulsarRecord.<GenericRecord>builder()
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

}
