package org.apache.pulsar.broker.service.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * {@link KeyValueSchemaCompatibilityCheck} for {@link SchemaType#KEY_VALUE}.
 */
public class KeyValueSchemaCompatibilityCheck extends AvroSchemaBasedCompatibilityCheck {


    private KeyValue<byte[], byte[]> decode(byte[] bytes, SchemaData schemaData) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int keyLength = byteBuffer.getInt();
        byte[] keySchema = new byte[keyLength];
        byteBuffer.get(keySchema);

        int valueLength = byteBuffer.getInt();
        byte[] valueSchema = new byte[valueLength];
        byteBuffer.get(valueSchema);
        return new KeyValue<>(keySchema, valueSchema);
    }

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.KEY_VALUE;
    }


    @Override
    public boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        KeyValue<byte[], byte[]> fromKeyValue = this.decode(from.getData(), from);
        KeyValue<byte[], byte[]> toKeyValue = this.decode(to.getData(), to);

        SchemaData fromKeySchemaData = SchemaData.builder().data(fromKeyValue.getKey())
                .type(SchemaType.valueOf(from.getProps().get("key.schema.type"))).build();
        SchemaData fromValueSchemaData = SchemaData.builder().data(fromKeyValue.getValue())
                .type(SchemaType.valueOf(from.getProps().get("value.schema.type"))).build();
        SchemaData toKeySchemaData = SchemaData.builder().data(toKeyValue.getKey())
                .type(SchemaType.valueOf(to.getProps().get("key.schema.type"))).build();
        SchemaData toValueSchemaData = SchemaData.builder().data(toKeyValue.getValue())
                .type(SchemaType.valueOf(to.getProps().get("value.schema.type"))).build();
        if (SchemaType.valueOf(to.getProps().get("key.schema.type")) == SchemaType.AVRO) {
            if (super.isCompatible(fromKeySchemaData, toKeySchemaData, strategy)
                    && super.isCompatible(fromValueSchemaData, toValueSchemaData, strategy)) {
                return true;
            }
            return false;
        } else if (SchemaType.valueOf(to.getProps().get("value.schema.type")) == SchemaType.JSON) {
            JsonSchemaCompatibilityCheck jsonSchemaCompatibilityCheck = new JsonSchemaCompatibilityCheck();
            if (jsonSchemaCompatibilityCheck.isCompatible(fromKeySchemaData, fromValueSchemaData, strategy)
                    && jsonSchemaCompatibilityCheck.isCompatible(fromValueSchemaData, toValueSchemaData, strategy)) {
                return true;
            }
            return false;
        }
        return false;
    }


}
