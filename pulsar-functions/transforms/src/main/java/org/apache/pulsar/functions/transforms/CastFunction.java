package org.apache.pulsar.functions.transforms;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

@Slf4j
public class CastFunction implements Function<GenericObject, Void>, TransformStep {

    private SchemaType keySchemaType;
    private SchemaType valueSchemaType;

    public CastFunction() {}

    public CastFunction(SchemaType keySchemaType, SchemaType valueSchemaType) {
        this.keySchemaType = keySchemaType;
        this.valueSchemaType = valueSchemaType;
    }

    @Override
    public void initialize(Context context) throws Exception {
        keySchemaType = getConfig(context, "key-schema-type");
        valueSchemaType = getConfig(context, "value-schema-type");
    }

    private SchemaType getConfig(Context context, String fieldName) {
        return context.getUserConfigValue(fieldName)
                .map(fields -> {
                    if (fields instanceof String) {
                        return SchemaType.valueOf((String) fields);
                    }
                    throw new IllegalArgumentException(fieldName + " must be of type String");
                })
                .orElse(null);
    }

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
        if (transformContext.getKeySchema() != null) {
            Object outputKeyObject = transformContext.getKeyObject();
            Schema<?> outputSchema = transformContext.getKeySchema();
            if (keySchemaType == SchemaType.STRING) {
                outputSchema = Schema.STRING;
                outputKeyObject = outputKeyObject.toString();
            }
            transformContext.setKeySchema(outputSchema);
            transformContext.setKeyObject(outputKeyObject);
        }
        if (valueSchemaType == SchemaType.STRING) {
            transformContext.setValueSchema(Schema.STRING);
            transformContext.setValueObject(transformContext.getValueObject().toString());
        }
    }
}
