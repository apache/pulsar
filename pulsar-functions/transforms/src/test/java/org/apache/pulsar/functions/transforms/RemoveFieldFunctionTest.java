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

public class RemoveFieldFunctionTest {

    @Test
    void testInvalidConfig() {
        Map<String, Object> config = ImmutableMap.of("value-fields", 42);
        Utils.TestContext context = new Utils.TestContext(createTestAvroKeyValueRecord(), config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        assertThrows(IllegalArgumentException.class, () -> removeFieldFunction.initialize(context));

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

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(genericRecord, context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        assertEquals(message.getKey(), "test-key");

        GenericData.Record read = getRecord(message.getSchema(), (byte[]) message.getValue());
        assertEquals(read.get("age"), 42);
        assertNull(read.getSchema().getField("firstName"));
        assertNull(read.getSchema().getField("lastName"));
    }

    @Test
    void testKeyValueAvro() throws Exception {
        Record<KeyValue<GenericRecord, GenericRecord>> record = createTestAvroKeyValueRecord();
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "valueField1,valueField2",
                "key-fields", "keyField1,keyField2");
        Utils.TestContext context = new Utils.TestContext(record, config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

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

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(genericRecord, context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        assertSame(message.getSchema(), record.getSchema());
        assertSame(message.getValue(), record.getValue());
    }

    @Test
    void testKeyValueAvroNotModified() throws Exception {
        Record<KeyValue<GenericRecord, GenericRecord>> record = createTestAvroKeyValueRecord();
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "otherValue",
                "key-fields", "otherKey");
        Utils.TestContext context = new Utils.TestContext(record, config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
        KeyValue messageValue = (KeyValue) message.getValue();

        KeyValueSchema recordSchema = (KeyValueSchema) record.getSchema();
        KeyValue recordValue = record.getValue();
        assertSame(messageSchema.getKeySchema(), recordSchema.getKeySchema());
        assertSame(messageSchema.getValueSchema(), recordSchema.getValueSchema());
        assertSame(messageValue.getKey(), ((GenericRecord) recordValue.getKey()).getNativeObject());
        assertSame(messageValue.getValue(), ((GenericRecord) recordValue.getValue()).getNativeObject());
    }

    @Test
    void testKeyValueAvroCached() throws Exception {
        Record<KeyValue<GenericRecord, GenericRecord>> record = createTestAvroKeyValueRecord();
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "valueField1,valueField2",
                "key-fields", "keyField1,keyField2");
        Utils.TestContext context = new Utils.TestContext(record, config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();

        Record<KeyValue<GenericRecord, GenericRecord>> newRecord = createTestAvroKeyValueRecord();

        context.setCurrentRecord(newRecord);

        removeFieldFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(newRecord.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

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
        Record<String> record = new Utils.TestRecord<>(Schema.STRING, "value", "test-key");
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "value",
                "key-fields", "key");
        Utils.TestContext context = new Utils.TestContext(record, config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.STRING, new byte[]{}),
                context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();

        assertSame(message.getSchema(), record.getSchema());
        assertSame(message.getValue(), record.getValue());
    }


    @Test
    void testKeyValuePrimitives() throws Exception {
        Schema<KeyValue<String, Integer>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);

        KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);

        Record<KeyValue<String, Integer>> record = new Utils.TestRecord<>(keyValueSchema, keyValue, null);
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "value",
                "key-fields", "key");
        Utils.TestContext context = new Utils.TestContext(record, config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
        KeyValue messageValue = (KeyValue) message.getValue();

        KeyValueSchema recordSchema = (KeyValueSchema) record.getSchema();
        KeyValue recordValue = record.getValue();
        assertSame(messageSchema.getKeySchema(), recordSchema.getKeySchema());
        assertSame(messageSchema.getValueSchema(), recordSchema.getValueSchema());
        assertSame(messageValue.getKey(), recordValue.getKey());
        assertSame(messageValue.getValue(), recordValue.getValue());
    }
}