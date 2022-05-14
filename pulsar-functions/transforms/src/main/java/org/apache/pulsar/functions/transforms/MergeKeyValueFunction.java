package org.apache.pulsar.functions.transforms;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

@Slf4j
public class MergeKeyValueFunction implements Function<GenericObject, Void>, TransformStep {

    @Override
    public Void process(GenericObject input, Context context) throws Exception {
        Record<?> currentRecord = context.getCurrentRecord();
        Schema<?> schema = currentRecord.getSchema();
        Object nativeObject = input.getNativeObject();
        if (log.isDebugEnabled()) {
            log.debug("apply to {} {}", input, nativeObject);
            log.debug("record with schema {} version {} {}", schema,
                    currentRecord.getMessage().get().getSchemaVersion(),
                    currentRecord);
        }

        TransformContext transformContext = new TransformContext(context, nativeObject);
        process(transformContext);
        transformContext.send();
        return null;
    }

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

            List<String> valueSchemaFieldNames = avroValueSchema.getFields().stream().map(org.apache.avro.Schema.Field::name).collect(Collectors.toList());
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
            transformContext.setKeySchema(null);
            transformContext.setKeyObject(null);
            transformContext.setKeyModified(true);
            transformContext.setValueObject(newRecord);
            transformContext.setValueModified(true);
        }
    }
}
