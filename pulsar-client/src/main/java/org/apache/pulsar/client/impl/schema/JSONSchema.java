package org.apache.pulsar.client.impl.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public class JSONSchema<T> implements Schema<T> {

    private final SchemaInfo info;
    private final ObjectMapper objectMapper;
    private final Class<T> pojo;

    private JSONSchema(SchemaInfo info, Class<T> pojo, ObjectMapper objectMapper) {
        this.info = info;
        this.pojo = pojo;
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] encode(T message) throws SchemaSerializationException {
        try {
            return objectMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public T decode(byte[] bytes) throws SchemaSerializationException {
        try {
            return objectMapper.readValue(new String(bytes), pojo);
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return info;
    }

    public static <T> JSONSchema<T> of(Class<T> pojo) throws JsonProcessingException {
        return of(pojo, Collections.emptyMap());
    }

    public static <T> JSONSchema<T> of(Class<T> pojo, Map<String, String> properties) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
        JsonNode schema = schemaGen.generateJsonSchema(pojo);

        SchemaInfo info = new SchemaInfo();
        info.setName("");
        info.setProperties(properties);
        info.setType(SchemaType.JSON);
        info.setSchema(mapper.writeValueAsBytes(schema));
        return new JSONSchema<>(info, pojo, mapper);
    }
}
