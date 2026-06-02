/*
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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.api.v5.schema.SchemaInfo;
import org.apache.pulsar.client.api.v5.schema.SchemaType;
import org.testng.annotations.Test;

/**
 * Covers the V5 Schema factories added for the pulsar-client CLI migration:
 * {@link Schema#generic(SchemaInfo)} and {@link Schema#autoProduceBytesOf(Schema)}, plus the
 * {@link SchemaInfo#of} builder they consume.
 */
public class SchemaFactoryTest {

    private static final String AVRO_DEF =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";

    @Test
    public void testSchemaInfoOfRoundTrips() {
        SchemaInfo info = SchemaInfo.of("client", SchemaType.AVRO,
                AVRO_DEF.getBytes(StandardCharsets.UTF_8), null);
        assertEquals(info.name(), "client");
        assertEquals(info.type(), SchemaType.AVRO);
        assertEquals(new String(info.schema(), StandardCharsets.UTF_8), AVRO_DEF);
        assertNotNull(info.properties());
        assertEquals(info.properties().size(), 0);
    }

    @Test
    public void testGenericSchemaFromAvroDefinition() {
        SchemaInfo info = SchemaInfo.of("client", SchemaType.AVRO,
                AVRO_DEF.getBytes(StandardCharsets.UTF_8), null);
        Schema<?> generic = Schema.generic(info);
        assertNotNull(generic);
        assertEquals(generic.schemaInfo().type(), SchemaType.AVRO);
    }

    @Test
    public void testAutoProduceBytesOfWrapsBase() {
        // The CLI wraps a typed base schema so pre-encoded bytes are validated against it.
        Schema<byte[]> wrapped = Schema.autoProduceBytesOf(Schema.string());
        assertNotNull(wrapped);
        // AUTO_PRODUCE_BYTES encodes raw bytes straight through.
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        assertEquals(wrapped.encode(payload), payload);
    }
}
