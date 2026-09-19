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
package org.apache.pulsar.client.cli;


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;
import java.util.Collections;
import java.util.List;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.pulsar.client.api.v5.schema.SchemaType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestCmdProduce {

    private static final String AVRO_DEF = "{\"type\": \"record\",\"namespace\": \"com.example\","
            + "\"name\": \"FullName\", \"fields\": [{ \"name\": \"a\", \"type\": \"string\" },"
            + "{ \"name\": \"b\", \"type\": \"int\" }]}";

    CmdProduce cmdProduce;

    @BeforeMethod
    public void setUp() {
        cmdProduce = new CmdProduce();
        cmdProduce.updateConfig(null, null, "ws://localhost:8080/");
    }

    @Test
    public void testGetWebSocketProduceUri() {
        String topicNameV2 = "persistent://public/default/issue-11067";
        assertEquals(cmdProduce.getWebSocketProduceUri(topicNameV2),
                "ws://localhost:8080/ws/v2/producer/persistent/public/default/issue-11067");
    }

    @Test
    public void testBuildValueSchema() {
        // bytes -> raw BYTES, no native Avro schema.
        CmdProduce.ValueSchema bytes = CmdProduce.buildValueSchema("bytes");
        assertEquals(bytes.schema().schemaInfo().type(), SchemaType.BYTES);
        assertNull(bytes.avroNative());

        // string -> AUTO_PRODUCE_BYTES wrapping string; no native Avro schema.
        CmdProduce.ValueSchema string = CmdProduce.buildValueSchema("string");
        assertNotNull(string.schema());
        assertNull(string.avroNative());

        // avro:<def> -> AUTO_PRODUCE_BYTES wrapping a generic Avro schema; native Avro present.
        CmdProduce.ValueSchema avro = CmdProduce.buildValueSchema("avro:" + AVRO_DEF);
        assertNotNull(avro.schema());
        assertNotNull(avro.avroNative());

        // json:<def> -> AUTO_PRODUCE_BYTES wrapping a generic JSON schema; no native Avro schema.
        CmdProduce.ValueSchema json = CmdProduce.buildValueSchema("json:" + AVRO_DEF);
        assertNotNull(json.schema());
        assertNull(json.avroNative());

        // unknown -> rejected.
        expectThrows(IllegalArgumentException.class, () -> CmdProduce.buildValueSchema("nope"));
    }

    @Test
    public void generateAvroMessageBodies() throws Exception {
        CmdProduce.ValueSchema vs = CmdProduce.buildValueSchema("avro:" + AVRO_DEF);

        List<byte[]> bytes = CmdProduce.generateMessageBodies(List.of("{\"a\":\"stringValue\",\"b\":123}"),
                Collections.emptyList(), vs.avroNative());
        assertEquals(bytes.size(), 1);

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(vs.avroNative());
        GenericRecord record = reader.read(null, DecoderFactory.get().binaryDecoder(bytes.get(0), null));
        assertEquals("stringValue", record.get("a").toString());
        assertEquals(123, record.get("b"));
    }
}
