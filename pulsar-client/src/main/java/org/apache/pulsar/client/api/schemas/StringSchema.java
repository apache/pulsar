package org.apache.pulsar.client.api.schemas;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.nio.charset.StandardCharsets;

public class StringSchema implements Schema<String> {
    public byte[] encode(String message) {
        return message.getBytes(StandardCharsets.UTF_8);
    }

    public String decode(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public SchemaInfo getSchemaInfo() {
        return new SchemaInfo();
    }
}
