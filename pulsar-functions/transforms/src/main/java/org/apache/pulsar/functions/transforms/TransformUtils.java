package org.apache.pulsar.functions.transforms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.common.schema.SchemaType;

public class TransformUtils {
    public static byte[] serializeGenericRecord(org.apache.avro.generic.GenericRecord record) throws IOException {
        GenericDatumWriter writer = new GenericDatumWriter(record.getSchema());
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
        writer.write(record, encoder);
        return oo.toByteArray();
    }

    public static void dropValueFields(List<String> fields, TransformContext record) {
        if (record.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            org.apache.avro.generic.GenericRecord newRecord =
                    dropFields(fields, (org.apache.avro.generic.GenericRecord) record.getValueObject());
            record.setValueModified(true);
            record.setValueObject(newRecord);
        }
    }

    public static void dropKeyFields(List<String> fields, TransformContext record) {
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
