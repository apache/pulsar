package org.apache.pulsar.functions.transforms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;

@Slf4j
public class TransformUtils {

    public static TransformRecord newTransformRecord(Schema<?> schema, Object nativeObject) {
        TransformRecord transformRecord;
        if (schema instanceof KeyValueSchema && nativeObject instanceof KeyValue) {
            KeyValueSchema kvSchema = (KeyValueSchema) schema;
            KeyValue kv = (KeyValue) nativeObject;
            transformRecord = new TransformRecord(
                    kvSchema.getKeySchema(),
                    kv.getKey(),
                    kvSchema.getValueSchema(),
                    kv.getValue(),
                    kvSchema.getKeyValueEncodingType()
            );
        } else {
            transformRecord = new TransformRecord(schema, nativeObject);
        }
        return transformRecord;
    }

    public static void sendMessage(Context context, TransformRecord transformRecord) throws IOException {
        if (transformRecord.getKeyModified()
                && transformRecord.getKeySchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            GenericRecord genericRecord = (GenericRecord) transformRecord.getKeyObject();
            transformRecord.setKeySchema(Schema.NATIVE_AVRO(genericRecord.getSchema()));
            transformRecord.setKeyObject(serializeGenericRecord(genericRecord));
        }
        if (transformRecord.getValueModified()
                && transformRecord.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            GenericRecord genericRecord = (GenericRecord) transformRecord.getValueObject();
            transformRecord.setValueSchema(Schema.NATIVE_AVRO(genericRecord.getSchema()));
            transformRecord.setValueObject(serializeGenericRecord(genericRecord));
        }

        Schema outputSchema;
        Object outputObject;
        if (transformRecord.getKeySchema() != null) {
            outputSchema = Schema.KeyValue(
                    transformRecord.getKeySchema(),
                    transformRecord.getValueSchema(),
                    transformRecord.getKeyValueEncodingType()
            );
            outputObject = new KeyValue(transformRecord.getKeyObject(), transformRecord.getValueObject());
        } else {
            outputSchema = transformRecord.getValueSchema();
            // GenericRecord must stay wrapped while primitives must be unwrapped
            outputObject = !transformRecord.getValueModified()
                    && transformRecord.getValueSchema().getSchemaInfo().getType().isStruct()
                    ? context.getCurrentRecord().getValue()
                    : transformRecord.getValueObject();
        }

        if (log.isDebugEnabled()) {
            log.debug("output {} schema {}", outputObject, outputSchema);
        }
        context.newOutputMessage(context.getOutputTopic(), outputSchema)
                .value(outputObject).send();
    }

    public static byte[] serializeGenericRecord(org.apache.avro.generic.GenericRecord record) throws IOException {
        GenericDatumWriter writer = new GenericDatumWriter(record.getSchema());
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
        writer.write(record, encoder);
        return oo.toByteArray();
    }

    public static void dropValueFields(List<String> fields, TransformRecord record) {
        if (record.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            org.apache.avro.generic.GenericRecord newRecord =
                    dropFields(fields, (org.apache.avro.generic.GenericRecord) record.getValueObject());
            record.setValueModified(true);
            record.setValueObject(newRecord);
        }
    }

    public static void dropKeyFields(List<String> fields, TransformRecord record) {
        if (record.getKeyObject() != null && record.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            org.apache.avro.generic.GenericRecord newRecord =
                    dropFields(fields, (org.apache.avro.generic.GenericRecord) record.getKeyObject());
            record.setKeyModified(true);
            record.setKeyObject(newRecord);
        }
    }

    private static org.apache.avro.generic.GenericRecord dropFields(List<String> fields,
                                                                    org.apache.avro.generic.GenericRecord record) {
        org.apache.avro.Schema avroSchema = record.getSchema();
        if (fields.stream().anyMatch(field -> avroSchema.getField(field) != null)) {
            org.apache.avro.Schema modified = org.apache.avro.Schema.createRecord(
                    avroSchema.getName(), avroSchema.getDoc(), avroSchema.getNamespace(), avroSchema.isError(),
                    avroSchema.getFields()
                            .stream()
                            .filter(f -> fields.contains(f.name()))
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
