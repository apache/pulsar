package org.apache.pulsar.client.impl.schema.generic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaReader;

import java.io.IOException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class GenericJsonReader implements SchemaReader {

    private final ObjectMapper objectMapper;
    private final byte[] schemaVersion;
    private final List<Field> fields;
    public GenericJsonReader(){
        this.fields = null;
        this.schemaVersion = new byte[10];
        this.objectMapper = new ObjectMapper();
    }

    public GenericJsonReader(byte[] schemaVersion, List<Field> fields){
        this.objectMapper = new ObjectMapper();
        this.fields = fields;
        this.schemaVersion = schemaVersion;
    }
    @Override
    public GenericRecord read(byte[] bytes) {
        try {
            JsonNode jn = objectMapper.readTree(new String(bytes, UTF_8));
            return new GenericJsonRecord(schemaVersion, fields, jn);
        } catch (IOException ioe) {
            throw new SchemaSerializationException(ioe);
        }
    }
}
