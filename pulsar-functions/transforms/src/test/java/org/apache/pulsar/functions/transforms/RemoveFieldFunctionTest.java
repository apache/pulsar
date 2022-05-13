package org.apache.pulsar.functions.transforms;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertNotSame;
import static org.testng.AssertJUnit.assertNull;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import org.testng.annotations.Test;

public class RemoveFieldFunctionTest {

    @Test
    void testAvro() throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("record");
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

        Record<GenericRecord> record = new TestRecord<>(genericSchema, genericRecord, "test-key");
        Map<String, Object> config = ImmutableMap.of("value-fields", "firstName,lastName");
        TestContext context = new TestContext(record, config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(genericRecord, context);

        TestTypedMessageBuilder<?> message = context.getOutputMessage();
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
        TestContext context = new TestContext(record, config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        TestTypedMessageBuilder<?> message = context.getOutputMessage();
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
    void testKeyValueAvroNotModified() throws Exception {
        Record<KeyValue<GenericRecord, GenericRecord>> record = createTestAvroKeyValueRecord();
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "otherValue",
                "key-fields", "otherKey");
        TestContext context = new TestContext(record, config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        TestTypedMessageBuilder<?> message = context.getOutputMessage();
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
        TestContext context = new TestContext(record, config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        TestTypedMessageBuilder<?> message = context.getOutputMessage();
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
    void testKeyValuePrimitives() throws Exception {
        Schema<KeyValue<String, Integer>> keyValueSchema =
                Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);

        KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);

        Record<KeyValue<String, Integer>> record = new TestRecord<>(keyValueSchema, keyValue, null);
        Map<String, Object> config = ImmutableMap.of(
                "value-fields", "value",
                "key-fields", "key");
        TestContext context = new TestContext(record, config);

        RemoveFieldFunction removeFieldFunction = new RemoveFieldFunction();
        removeFieldFunction.initialize(context);
        removeFieldFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        TestTypedMessageBuilder<?> message = context.getOutputMessage();
        KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
        KeyValue messageValue = (KeyValue) message.getValue();

        KeyValueSchema recordSchema = (KeyValueSchema) record.getSchema();
        KeyValue recordValue = record.getValue();
        assertSame(messageSchema.getKeySchema(), recordSchema.getKeySchema());
        assertSame(messageSchema.getValueSchema(), recordSchema.getValueSchema());
        assertSame(messageValue.getKey(), recordValue.getKey());
        assertSame(messageValue.getValue(), recordValue.getValue());
    }

    private Record<KeyValue<GenericRecord, GenericRecord>> createTestAvroKeyValueRecord() {
        RecordSchemaBuilder keySchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("record");
        keySchemaBuilder.field("keyField1").type(SchemaType.STRING);
        keySchemaBuilder.field("keyField2").type(SchemaType.STRING);
        keySchemaBuilder.field("keyField3").type(SchemaType.STRING);
        GenericSchema<GenericRecord> keySchema = Schema.generic(keySchemaBuilder.build(SchemaType.AVRO));

        RecordSchemaBuilder valueSchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("record");
        valueSchemaBuilder.field("valueField1").type(SchemaType.STRING);
        valueSchemaBuilder.field("valueField2").type(SchemaType.STRING);
        valueSchemaBuilder.field("valueField3").type(SchemaType.STRING);
        GenericSchema<GenericRecord> valueSchema = Schema.generic(valueSchemaBuilder.build(SchemaType.AVRO));

        GenericRecord keyRecord = keySchema.newRecordBuilder()
                .set("keyField1", "key1")
                .set("keyField2", "key2")
                .set("keyField3", "key3")
                .build();

        GenericRecord valueRecord = valueSchema.newRecordBuilder()
                .set("valueField1", "value1")
                .set("valueField2", "value2")
                .set("valueField3", "value3")
                .build();

        Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema =
                Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.SEPARATED);

        KeyValue<GenericRecord, GenericRecord> keyValue = new KeyValue<>(keyRecord, valueRecord);

        return new TestRecord<>(keyValueSchema, keyValue, null);
    }

    private GenericData.Record getRecord(Schema<?> schema, byte[] value)
            throws IOException {
        DatumReader<GenericData.Record> reader =
                new GenericDatumReader<>((org.apache.avro.Schema) schema.getNativeSchema().get());
        Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
        return reader.read(null, decoder);
    }

    private static class TestRecord<T> implements Record<T> {
        private final Schema<T> schema;
        private final T value;
        private final String key;

        private TestRecord(Schema<T> schema, T value, String key) {
            this.schema = schema;
            this.value = value;
            this.key = key;
        }

        @Override
        public Optional<String> getKey() {
            return Optional.ofNullable(key);
        }

        @Override
        public Schema<T> getSchema() {
            return schema;
        }

        @Override
        public T getValue() {
            return value;
        }
    }

    private static class TestTypedMessageBuilder<T> implements TypedMessageBuilder<T> {
        private final String topic;
        private final Schema<T> schema;
        private T value;
        private String key;
        private Map<String, String> properties = new HashMap<>();

        private TestTypedMessageBuilder(String topic, Schema<T> schema) {
            this.topic = topic;
            this.schema = schema;
        }

        @Override
        public MessageId send() {
            return null;
        }

        @Override
        public CompletableFuture<MessageId> sendAsync() {
            return null;
        }

        @Override
        public TestTypedMessageBuilder<T> key(String key) {
            this.key = key;
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> keyBytes(byte[] key) {
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> orderingKey(byte[] orderingKey) {
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> value(T value) {
            this.value = value;
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> property(String name, String value) {
            this.properties.put(name, value);
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> eventTime(long timestamp) {
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> sequenceId(long sequenceId) {
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> replicationClusters(List<String> clusters) {
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> disableReplication() {
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> deliverAt(long timestamp) {
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> deliverAfter(long delay, TimeUnit unit) {
            return this;
        }

        @Override
        public TestTypedMessageBuilder<T> loadConf(Map<String, Object> config) {
            return this;
        }

        public T getValue() {
            return value;
        }

        public String getKey() {
            return key;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public String getTopic() {
            return topic;
        }

        public Schema<T> getSchema() {
            return schema;
        }
    }

    public static class TestContext implements Context {
        private Record<?> currentRecord;
        private final Map<String, Object> userConfig;
        private TestTypedMessageBuilder<?> outputMessage;

        public TestContext(Record<?> currentRecord, Map<String, Object> userConfig) {
            this.currentRecord = currentRecord;
            this.userConfig = userConfig;
        }

        public void setCurrentRecord(Record<?> currentRecord) {
            this.currentRecord = currentRecord;
        }

        @Override
        public Collection<String> getInputTopics() {
            return null;
        }

        @Override
        public String getOutputTopic() {
            return "test-context-topic";
        }

        @Override
        public Record<?> getCurrentRecord() {
            return currentRecord;
        }

        @Override
        public String getOutputSchemaType() {
            return null;
        }

        @Override
        public String getFunctionName() {
            return null;
        }

        @Override
        public String getFunctionId() {
            return null;
        }

        @Override
        public String getFunctionVersion() {
            return null;
        }

        @Override
        public Map<String, Object> getUserConfigMap() {
            return null;
        }

        @Override
        public Optional<Object> getUserConfigValue(String key) {
            return Optional.ofNullable(userConfig.get(key));
        }

        @Override
        public Object getUserConfigValueOrDefault(String key, Object defaultValue) {
            return null;
        }

        @Override
        public PulsarAdmin getPulsarAdmin() {
            return null;
        }

        @Override
        public <X> CompletableFuture<Void> publish(String topicName, X object, String schemaOrSerdeClassName) {
            return null;
        }

        @Override
        public <X> CompletableFuture<Void> publish(String topicName, X object) {
            return null;
        }

        @Override
        public <X> TypedMessageBuilder<X> newOutputMessage(String topicName, Schema<X> schema) {
            this.outputMessage = new TestTypedMessageBuilder<>(topicName, schema);
            return (TypedMessageBuilder<X>) outputMessage;
        }

        public TestTypedMessageBuilder<?> getOutputMessage() {
            return outputMessage;
        }

        @Override
        public <X> ConsumerBuilder<X> newConsumerBuilder(Schema<X> schema) {
            return null;
        }

        @Override
        public String getTenant() {
            return null;
        }

        @Override
        public String getNamespace() {
            return null;
        }

        @Override
        public int getInstanceId() {
            return 0;
        }

        @Override
        public int getNumInstances() {
            return 0;
        }

        @Override
        public Logger getLogger() {
            return null;
        }

        @Override
        public String getSecret(String secretName) {
            return null;
        }

        @Override
        public void putState(String key, ByteBuffer value) {}

        @Override
        public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
            return null;
        }

        @Override
        public ByteBuffer getState(String key) {
            return null;
        }

        @Override
        public CompletableFuture<ByteBuffer> getStateAsync(String key) {
            return null;
        }

        @Override
        public void deleteState(String key) {}

        @Override
        public CompletableFuture<Void> deleteStateAsync(String key) {
            return null;
        }

        @Override
        public void incrCounter(String key, long amount) {}

        @Override
        public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
            return null;
        }

        @Override
        public long getCounter(String key) {
            return 0;
        }

        @Override
        public CompletableFuture<Long> getCounterAsync(String key) {
            return null;
        }

        @Override
        public void recordMetric(String metricName, double value) {}

    }

}