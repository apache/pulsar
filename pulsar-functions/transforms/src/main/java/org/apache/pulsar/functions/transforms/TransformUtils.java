package org.apache.pulsar.functions.transforms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

    public static TransformResult newTransformResult(Schema<?> schema, Object nativeObject) {
        TransformResult transformResult;
        if (schema instanceof KeyValueSchema && nativeObject instanceof KeyValue) {
            KeyValueSchema kvSchema = (KeyValueSchema) schema;
            KeyValue kv = (KeyValue) nativeObject;
            transformResult = new TransformResult(kvSchema.getKeySchema(), kv.getKey(), kvSchema.getValueSchema(), kv.getValue(), kvSchema.getKeyValueEncodingType());
        } else {
            transformResult = new TransformResult(schema, nativeObject);
        }
        return transformResult;
    }

    public static void sendMessage(Context context, TransformResult transformResult) throws IOException {
        if (transformResult.getKeyModified() && transformResult.getKeySchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            org.apache.avro.generic.GenericRecord genericRecord = (org.apache.avro.generic.GenericRecord) transformResult.getKeyObject();
            transformResult.setKeySchema(Schema.NATIVE_AVRO(genericRecord.getSchema()));
            transformResult.setKeyObject(serializeGenericRecord(genericRecord));
        }
        if (transformResult.getValueModified() && transformResult.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            org.apache.avro.generic.GenericRecord genericRecord = (GenericRecord) transformResult.getValueObject();
            transformResult.setValueSchema(Schema.NATIVE_AVRO(genericRecord.getSchema()));
            transformResult.setValueObject(serializeGenericRecord(genericRecord));
        }

        Schema outputSchema;
        Object outputObject;
        if (transformResult.getKeySchema() != null) {
            outputSchema = Schema.KeyValue(transformResult.getKeySchema(), transformResult.getValueSchema(), transformResult.getKeyValueEncodingType());
            outputObject = new KeyValue(transformResult.getKeyObject(), transformResult.getValueObject());
        } else {
            outputSchema = transformResult.getValueSchema();
            // GenericRecord must stay wrapped while primitives must be unwrapped
            outputObject = !transformResult.getValueModified() && transformResult.getValueSchema().getSchemaInfo().getType().isStruct()
                    ? context.getCurrentRecord().getValue()
                    : transformResult.getValueObject();
        }

        log.info("output {} schema {}", outputObject, outputSchema);
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

    public static void dropValueField(String fieldName, TransformResult record) {
        if (record.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            org.apache.avro.generic.GenericRecord newRecord =
                    dropField(fieldName, (org.apache.avro.generic.GenericRecord) record.getValueObject());
            record.setValueModified(true);
            record.setValueObject(newRecord);
        }
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
