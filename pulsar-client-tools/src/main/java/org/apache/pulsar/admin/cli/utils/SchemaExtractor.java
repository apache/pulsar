package org.apache.pulsar.admin.cli.utils;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.schema.JsonSchema;

import java.io.IOException;

public class SchemaExtractor {

    public static String getJsonSchema(Class clazz) throws IOException {

        org.codehaus.jackson.map.ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationConfig.Feature.WRITE_ENUMS_USING_TO_STRING, true);

        JsonSchema schema = mapper.generateJsonSchema(clazz);

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
    }

    public static String getAvroSchema(Class clazz) {

        Schema schema = ReflectData.get().getSchema(clazz);

        return schema.toString();
    }
}
