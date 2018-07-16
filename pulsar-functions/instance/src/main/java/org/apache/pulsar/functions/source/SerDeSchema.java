package org.apache.pulsar.functions.source;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.SerDe;

@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class SerDeSchema<T> implements Schema<T> {

    private final SerDe<T> serDe;

    @Override
    public byte[] encode(T value) {
        return serDe.serialize(value);
    }

    @Override
    public T decode(byte[] bytes) {
        return serDe.deserialize(bytes);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        // Do not persist schema information
        return null;
    }

}
