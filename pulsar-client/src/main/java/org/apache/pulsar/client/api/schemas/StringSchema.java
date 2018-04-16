package org.apache.pulsar.client.api.schemas;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringSchema implements Schema<String> {
    private final Charset charset;
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    public StringSchema() {
        this.charset = DEFAULT_CHARSET;
    }

    public StringSchema(Charset charset) {
        this.charset = charset;
    }

    public byte[] encode(String message) {
        return message.getBytes(charset);
    }

    public String decode(byte[] bytes) {
        return new String(bytes, charset);
    }

    public SchemaInfo getSchemaInfo() {
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setName("String");
        schemaInfo.setType(SchemaType.NONE);
        return schemaInfo;
    }
}
