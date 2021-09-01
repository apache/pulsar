package org.apache.pulsar.functions.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.proto.Request;
import org.junit.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

@Slf4j
public class TopicSchemaTest {

    @Test
    public void testGetSchema() {
        TopicSchema topicSchema = new TopicSchema(null);

        String TOPIC = "public/default/test";
        Schema<?> schema = topicSchema.getSchema(TOPIC + "1", DummyClass.class, Optional.of(SchemaType.JSON));
        assertEquals(schema.getClass(), JSONSchema.class);

        schema = topicSchema.getSchema(TOPIC + "2", DummyClass.class, Optional.of(SchemaType.AVRO));
        assertEquals(schema.getClass(), AvroSchema.class);

        // use an arbitrary protobuf class for testing purpose
        schema = topicSchema.getSchema(TOPIC + "3", Request.ServiceRequest.class, Optional.of(SchemaType.PROTOBUF));
        assertEquals(schema.getClass(), ProtobufSchema.class);

        schema = topicSchema.getSchema(TOPIC + "4", Request.ServiceRequest.class, Optional.of(SchemaType.PROTOBUF_NATIVE));
        assertEquals(schema.getClass(), ProtobufNativeSchema.class);
    }

    private static class DummyClass {}
}
