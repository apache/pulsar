package org.apache.pulsar.functions.transforms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;

@Slf4j
public class TransformUtils {

    public static void sendMessage(Context context, TransformResult transformResult) throws IOException {
        org.apache.avro.generic.GenericRecord avroValue = transformResult.getAvroValue();
        Schema schema = transformResult.getSchema();
        Object nativeObject = transformResult.getValue();
        Schema outputSchema = schema;
        // GenericRecord must stay wrapped while primitives and KeyValue must be unwrapped
        Object outputObject = schema.getSchemaInfo().getType().isStruct()
                ? context.getCurrentRecord().getValue()
                : nativeObject;
        if (avroValue != null) {
            Schema newValueSchema = Schema.NATIVE_AVRO(avroValue.getSchema());
            if (schema instanceof KeyValueSchema && nativeObject instanceof KeyValue) {
                KeyValueSchema kvSchema = (KeyValueSchema) schema;
                Schema keySchema = kvSchema.getKeySchema();
                outputSchema = Schema.KeyValue(keySchema, newValueSchema, kvSchema.getKeyValueEncodingType());
                outputObject = new KeyValue(((KeyValue) nativeObject).getKey(), serializeGenericRecord(avroValue));
            } else if (schema.getSchemaInfo().getType() == SchemaType.AVRO) {
                outputSchema = newValueSchema;
                outputObject = serializeGenericRecord(avroValue);
            }
        }
        log.info("output {} schema {}", outputObject, outputSchema);
        context.newOutputMessage(context.getOutputTopic(), outputSchema)
                .value(outputObject).send();
    }

    private static byte[] serializeGenericRecord(org.apache.avro.generic.GenericRecord record) throws IOException {
        GenericDatumWriter writer = new GenericDatumWriter(record.getSchema());
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
        writer.write(record, encoder);
        return oo.toByteArray();
    }

    public static TransformResult dropValueField(String fieldName, TransformResult record) {
        Schema<?> schema = record.getSchema();
        if (schema instanceof KeyValueSchema && record.getValue() instanceof KeyValue)  {
            KeyValueSchema kvSchema = (KeyValueSchema) schema;
            Schema valueSchema = kvSchema.getValueSchema();
            if (valueSchema.getSchemaInfo().getType() == SchemaType.AVRO) {
                org.apache.avro.generic.GenericRecord newRecord;
                if (record.getAvroValue() != null) {
                    newRecord = dropField(fieldName, record.getAvroValue());
                } else {
                    KeyValue originalObject = (KeyValue) record.getValue();
                    GenericRecord value = (GenericRecord) originalObject.getValue();
                    org.apache.avro.generic.GenericRecord genericRecord =
                            (org.apache.avro.generic.GenericRecord) value.getNativeObject();
                    newRecord = dropField(fieldName, genericRecord);
                }
                return new TransformResult(schema, record.getValue(), newRecord);
            }
        } else if (schema.getSchemaInfo().getType() == SchemaType.AVRO) {
            org.apache.avro.generic.GenericRecord newRecord;
            if (record.getAvroValue() != null) {
                newRecord = dropField(fieldName, record.getAvroValue());
            } else {
                org.apache.avro.generic.GenericRecord genericRecord =
                        (org.apache.avro.generic.GenericRecord) record.getValue();
                newRecord = dropField(fieldName, genericRecord);
            }
            return new TransformResult(schema, record.getValue(), newRecord);
        }
        return record;
    }

    private static org.apache.avro.generic.GenericRecord dropField(String fieldName,
                                                                   org.apache.avro.generic.GenericRecord record) {
        org.apache.avro.Schema avroSchema = record.getSchema();
        if (avroSchema.getField(fieldName) != null) {
            org.apache.avro.Schema modified = org.apache.avro.Schema.createRecord(
                    avroSchema.getName(), avroSchema.getDoc(), avroSchema.getNamespace(), avroSchema.isError(),
                    avroSchema.getFields().
                            stream()
                            .filter(f -> !f.name().equals(fieldName))
                            .map(f -> new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(),
                                    f.order()))
                            .collect(Collectors.toList()));

            org.apache.avro.generic.GenericRecord newRecord = new GenericData.Record(modified);
            for (org.apache.avro.Schema.Field field : modified.getFields()) {
                newRecord.put(field.name(), record.get(field.name()));
            }
            return newRecord;
        }
        return record;
    }
}
