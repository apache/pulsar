package org.apache.pulsar.functions.transforms;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.Context;

@Data
@AllArgsConstructor
public class TransformRecord {
    private Context context;
    private Schema<?> keySchema;
    private Object keyObject;
    private Boolean keyModified;
    private Schema<?> valueSchema;
    private Object valueObject;
    private Boolean valueModified;
    private KeyValueEncodingType keyValueEncodingType;
    private String outputTopic;

    public TransformRecord(Context context, Schema<?> keySchema, Object key, Schema<?> valueSchema, Object value,
                           KeyValueEncodingType keyValueEncodingType) {
        this(context, keySchema, key, false, valueSchema, value,
                false, keyValueEncodingType, context.getOutputTopic());
    }

    public TransformRecord(Context context, Schema<?> schema, Object value) {
        this(context, null, null, schema, value, null);
    }
}
