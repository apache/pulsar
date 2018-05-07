package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.nio.ByteBuffer;

public class ByteBufferSchema implements Schema<ByteBuffer> {
    @Override
    public byte[] encode(ByteBuffer message) {
        return message.array();
    }

    @Override
    public ByteBuffer decode(byte[] bytes) {
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return null;
    }
}
