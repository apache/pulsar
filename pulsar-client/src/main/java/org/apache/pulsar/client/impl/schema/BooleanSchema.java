package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * @Author: wpl
 */
public class BooleanSchema implements Schema<Boolean> {
    public static BooleanSchema of() {
        return INSTANCE;
    }

    private static final BooleanSchema INSTANCE = new BooleanSchema();
    private static final SchemaInfo SCHEMA_INFO = new SchemaInfo()
            .setName("INT8")
            .setType(SchemaType.INT8)
            .setSchema(new byte[0]);

    @Override
    public void validate(byte[] message) {
        if (message.length != 1) {
            throw new SchemaSerializationException("Size of data received by BooleanSchema is not 1");
        }
    }

    @Override
    public byte[] encode(Boolean message) {
        return new byte[]{(byte)(message ? -1 : 0)};
    }

    @Override
    public Boolean decode(byte[] bytes) {
        if (bytes.length != 1) {
            throw new IllegalArgumentException("bytes Array has wrong size: " + bytes.length);
        } else {
            return bytes[0] != 0;
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
