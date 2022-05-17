package org.apache.pulsar.functions.transforms;

import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

@Slf4j
public class MergeKeyValueFunction extends AbstractTransformStepFunction {

    @Override
    public void process(TransformContext transformContext) {
        Schema<?> keySchema = transformContext.getKeySchema();
        if (keySchema == null) {
            return;
        }
        if (keySchema.getSchemaInfo().getType() == SchemaType.AVRO
                && transformContext.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            GenericRecord avroKeyRecord = (GenericRecord) transformContext.getKeyObject();
            org.apache.avro.Schema avroKeySchema = avroKeyRecord.getSchema();
            org.apache.avro.Schema modified;

            GenericRecord avroValueRecord = (GenericRecord) transformContext.getValueObject();
            org.apache.avro.Schema avroValueSchema = avroValueRecord.getSchema();

            List<String> valueSchemaFieldNames = avroValueSchema.getFields().stream()
                    .map(org.apache.avro.Schema.Field::name)
                    .collect(Collectors.toList());
            List<org.apache.avro.Schema.Field> fields =
                    avroKeySchema.getFields().stream()
                            .filter(field -> !valueSchemaFieldNames.contains(field.name()))
                            .map(f -> new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(),
                                    f.defaultVal(),
                                    f.order()))
                            .collect(Collectors.toList());
            fields.addAll(avroValueSchema.getFields().stream()
                    .map(f -> new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(),
                            f.order()))
                    .collect(Collectors.toList()));

            // TODO: add cache
            modified = org.apache.avro.Schema.createRecord(
                    //TODO: how do we merge those fields ?
                    avroKeySchema.getName(), avroKeySchema.getDoc(), avroKeySchema.getNamespace(),
                    avroKeySchema.isError(),
                    fields);
            GenericRecord newRecord = new GenericData.Record(modified);
            for (String fieldName : valueSchemaFieldNames) {
                newRecord.put(fieldName, avroValueRecord.get(fieldName));
            }
            for (org.apache.avro.Schema.Field field : avroKeySchema.getFields()) {
                newRecord.put(field.name(), avroKeyRecord.get(field.name()));
            }
            transformContext.setValueObject(newRecord);
            transformContext.setValueModified(true);
        }
    }
}
