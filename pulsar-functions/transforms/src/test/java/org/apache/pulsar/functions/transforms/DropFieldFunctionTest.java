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
package org.apache.pulsar.functions.transforms;

import static org.apache.pulsar.functions.transforms.Utils.createTestAvroKeyValueRecord;
import static org.apache.pulsar.functions.transforms.Utils.getRecord;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.assertNull;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

public class DropFieldFunctionTest {

    @Test
    void testInvalidConfig() {
        Map<String, Object> config = ImmutableMap.of("value-fields", 42);
        Utils.TestContext context = new Utils.TestContext(createTestAvroKeyValueRecord(), config);

        DropFieldFunction dropFieldFunction = new DropFieldFunction();
        assertThrows(IllegalArgumentException.class, () -> dropFieldFunction.initialize(context));

    }

    @Test
    void testAvro() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);
        recordSchemaBuilder.field("lastName").type(SchemaType.STRING);
        recordSchemaBuilder.field("age").type(SchemaType.INT32);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord = genericSchema.newRecordBuilder()
                .set("firstName", "Jane")
                .set("lastName", "Doe")
                .set("age", 42)
                .build();

        Record<GenericRecord> record = new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");
        Map<String, Object> config = ImmutableMap.of("value-fields", "firstName,lastName");
        Utils.TestContext context = new Utils.TestContext(record, config);

        DropFieldFunction dropFieldFunction = new DropFieldFunction();
        dropFieldFunction.initialize(context);
        dropFieldFunction.process(genericRecord, context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        assertEquals(message.getKey(), "test-key");

        GenericData.Record read = getRecord(message.getSchema(), (byte[]) message.getValue());
        assertEquals(read.get("age"), 42);
        assertNull(read.getSchema().getField("firstName"));
        assertNull(read.getSchema().getField("lastName"));
    }

    @Test
    void testKeyValueAvro() throws Exception {
        Record<GenericObject> record = createTestAvroKeyValueRecord();
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "valueField1,valueField2",
                "key-fields", "keyField1,keyField2");
        Utils.TestContext context = new Utils.TestContext(record, config);

        DropFieldFunction dropFieldFunction = new DropFieldFunction();
        dropFieldFunction.initialize(context);
        dropFieldFunction.process(record.getValue(), context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
        KeyValue messageValue = (KeyValue) message.getValue();

        GenericData.Record keyAvroRecord = getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
        assertEquals(keyAvroRecord.get("keyField3"), new Utf8("key3"));
        assertNull(keyAvroRecord.getSchema().getField("keyField1"));
        assertNull(keyAvroRecord.getSchema().getField("keyField2"));

        GenericData.Record valueAvroRecord =
                getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());
        assertEquals(valueAvroRecord.get("valueField3"), new Utf8("value3"));
        assertNull(valueAvroRecord.getSchema().getField("valueField1"));
        assertNull(valueAvroRecord.getSchema().getField("valueField2"));

        assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
    }

    @Test
    void testAvroNotModified() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
        recordSchemaBuilder.field("firstName").type(SchemaType.STRING);
        recordSchemaBuilder.field("lastName").type(SchemaType.STRING);
        recordSchemaBuilder.field("age").type(SchemaType.INT32);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        GenericRecord genericRecord = genericSchema.newRecordBuilder()
                .set("firstName", "Jane")
                .set("lastName", "Doe")
                .set("age", 42)
                .build();

        Record<GenericRecord> record = new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");
        Map<String, Object> config = ImmutableMap.of("value-fields", "other");
        Utils.TestContext context = new Utils.TestContext(record, config);

        DropFieldFunction dropFieldFunction = new DropFieldFunction();
        dropFieldFunction.initialize(context);
        dropFieldFunction.process(genericRecord, context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        assertSame(message.getSchema(), record.getSchema());
        assertSame(message.getValue(), record.getValue());
    }

    @Test
    void testKeyValueAvroNotModified() throws Exception {
        Record<GenericObject> record = createTestAvroKeyValueRecord();
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "otherValue",
                "key-fields", "otherKey");
        Utils.TestContext context = new Utils.TestContext(record, config);

        DropFieldFunction dropFieldFunction = new DropFieldFunction();
        dropFieldFunction.initialize(context);
        dropFieldFunction.process(record.getValue(), context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
        KeyValue messageValue = (KeyValue) message.getValue();

        KeyValueSchema recordSchema = (KeyValueSchema) record.getSchema();
        KeyValue recordValue = (KeyValue) record.getValue().getNativeObject();
        assertSame(messageSchema.getKeySchema(), recordSchema.getKeySchema());
        assertSame(messageSchema.getValueSchema(), recordSchema.getValueSchema());
        assertSame(messageValue.getKey(), recordValue.getKey());
        assertSame(messageValue.getValue(), recordValue.getValue());
    }

    @Test
    void testKeyValueAvroCached() throws Exception {
        Record<GenericObject> record = createTestAvroKeyValueRecord();
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "valueField1,valueField2",
                "key-fields", "keyField1,keyField2");
        Utils.TestContext context = new Utils.TestContext(record, config);

        DropFieldFunction dropFieldFunction = new DropFieldFunction();
        dropFieldFunction.initialize(context);
        dropFieldFunction.process(record.getValue(), context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();

        Record<GenericObject> newRecord = createTestAvroKeyValueRecord();

        context.setCurrentRecord(newRecord);

        dropFieldFunction.process(newRecord.getValue(), context);

        message = context.getOutputMessage();
        KeyValueSchema newMessageSchema = (KeyValueSchema) message.getSchema();

        // Schema was modified by process operation
        KeyValueSchema recordSchema = (KeyValueSchema) record.getSchema();
        assertNotSame(messageSchema.getKeySchema().getNativeSchema().get(), recordSchema.getKeySchema().getNativeSchema().get());
        assertNotSame(messageSchema.getValueSchema().getNativeSchema().get(), recordSchema.getValueSchema().getNativeSchema().get());

        // Multiple process output the same cached schema
        assertSame(messageSchema.getKeySchema().getNativeSchema().get(),
                newMessageSchema.getKeySchema().getNativeSchema().get());
        assertSame(messageSchema.getValueSchema().getNativeSchema().get(),
                newMessageSchema.getValueSchema().getNativeSchema().get());
    }

    @Test
    void testPrimitives() throws Exception {
        Record<GenericObject> record = new Utils.TestRecord<>(
                Schema.STRING,
                AutoConsumeSchema.wrapPrimitiveObject("value", SchemaType.STRING, new byte[]{}),
                "test-key");
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "value",
                "key-fields", "key");
        Utils.TestContext context = new Utils.TestContext(record, config);

        DropFieldFunction dropFieldFunction = new DropFieldFunction();
        dropFieldFunction.initialize(context);
        dropFieldFunction.process(record.getValue(), context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();

        assertSame(message.getSchema(), record.getSchema());
        assertSame(message.getValue(), record.getValue().getNativeObject());
    }


    @Test
    void testKeyValuePrimitives() throws Exception {
        Schema<KeyValue<String, Integer>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);

        KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);

        Record<GenericObject> record = new Utils.TestRecord<>(
                keyValueSchema,
                AutoConsumeSchema.wrapPrimitiveObject(keyValue, SchemaType.KEY_VALUE, new byte[]{}),
                null);
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "value",
                "key-fields", "key");
        Utils.TestContext context = new Utils.TestContext(record, config);

        DropFieldFunction dropFieldFunction = new DropFieldFunction();
        dropFieldFunction.initialize(context);
        dropFieldFunction.process(record.getValue(), context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
        KeyValue messageValue = (KeyValue) message.getValue();

        KeyValueSchema recordSchema = (KeyValueSchema) record.getSchema();
        KeyValue recordValue = ((KeyValue) record.getValue().getNativeObject());
        assertSame(messageSchema.getKeySchema(), recordSchema.getKeySchema());
        assertSame(messageSchema.getValueSchema(), recordSchema.getValueSchema());
        assertSame(messageValue.getKey(), recordValue.getKey());
        assertSame(messageValue.getValue(), recordValue.getValue());
    }
}