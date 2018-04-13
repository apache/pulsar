package org.apache.pulsar.client.api.schemas;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

public class IntegerSchema implements Schema<Integer> {
    @Override
    public byte[] encode(Integer message) {
        byte[] bytes = new byte[1];
        bytes[0] = message.byteValue();
        return bytes;
    }

    @Override
    public Integer decode(byte[] bytes) {
        Byte b = bytes[0];
        return b.intValue();
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return new SchemaInfo();
    }
}
