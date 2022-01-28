/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
