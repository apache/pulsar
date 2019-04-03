package org.apache.pulsar.client.impl.schema.generic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaWriter;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

public class GenericJsonWriter implements SchemaWriter {

    private final ObjectMapper objectMapper;

    public GenericJsonWriter(){
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] write(Object pojo) {
        checkArgument(pojo instanceof GenericAvroRecord);
        GenericJsonRecord gjr = (GenericJsonRecord) pojo;
        try {
            return objectMapper.writeValueAsBytes(gjr.getJsonNode().toString());
        } catch (IOException ioe) {
            throw new SchemaSerializationException(ioe);
        }
    }
}
