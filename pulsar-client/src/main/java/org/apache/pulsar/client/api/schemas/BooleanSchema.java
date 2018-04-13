package org.apache.pulsar.client.api.schemas;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

public class BooleanSchema implements Schema<Boolean> {
    public Boolean decode(byte[] bytes) {
        return bytes[0] == 1;
    }

    public byte[] encode(Boolean msg) {
        return msg ? new byte[]{1} : new byte[]{0};
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return new SchemaInfo();
    }
}
