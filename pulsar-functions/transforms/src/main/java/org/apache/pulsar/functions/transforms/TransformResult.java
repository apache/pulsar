package org.apache.pulsar.functions.transforms;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Value;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

@Data
@AllArgsConstructor
public class TransformResult {
    private Schema<?> keySchema;
    private Object keyObject;
    private Boolean keyModified;
    private Schema<?> valueSchema;
    private Object valueObject;
    private Boolean valueModified;
    private KeyValueEncodingType keyValueEncodingType;

    public TransformResult(Schema keySchema, Object key, Schema valueSchema, Object value, KeyValueEncodingType keyValueEncodingType) {
        this(keySchema, key, false, valueSchema, value, false, keyValueEncodingType);
    }

    public TransformResult(Schema<?> schema, Object value) {
        this(null, null, schema, value, null);
    }
}
