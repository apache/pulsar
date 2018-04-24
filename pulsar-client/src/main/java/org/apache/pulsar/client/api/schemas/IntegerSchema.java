package org.apache.pulsar.client.api.schemas;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.ByteBuffer;

public class IntegerSchema implements Schema<Integer> {
    public IntegerSchema() {}

    public byte[] encode(Integer i) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(i);
        return buf.array();
    }

    public Integer decode(byte[] data) {
        Byte b = data[0];
        return b.intValue();
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setName("Integer");
        schemaInfo.setType(SchemaType.NONE);
        return schemaInfo;
    }
}
