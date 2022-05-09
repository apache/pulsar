package org.apache.pulsar.functions.transforms;

import lombok.Getter;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.Schema;

@Getter
public class TransformResult {
    private final Schema<?> schema;
    private final GenericRecord avroValue;
    private final Object value;

    public TransformResult(Schema<?> schema, Object value) {
        this(schema, value, null);
    }

    public TransformResult(Schema<?> schema, Object value, GenericRecord avroValue) {
        this.schema = schema;
        this.value = value;
        this.avroValue = avroValue;
    }
}
