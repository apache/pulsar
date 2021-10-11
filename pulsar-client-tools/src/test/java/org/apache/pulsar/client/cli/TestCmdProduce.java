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
package org.apache.pulsar.client.cli;


import static org.testng.Assert.assertEquals;

import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestCmdProduce {

    CmdProduce cmdProduce;

    @BeforeMethod
    public void setUp() {
        cmdProduce = new CmdProduce();
        cmdProduce.updateConfig(null, null, "ws://localhost:8080/");
    }

    @Test
    public void testGetWebSocketProduceUri() {
        String topicNameV1 = "persistent://public/cluster/default/issue-11067";
        assertEquals(cmdProduce.getWebSocketProduceUri(topicNameV1),
                "ws://localhost:8080/ws/producer/persistent/public/cluster/default/issue-11067");
        String topicNameV2 = "persistent://public/default/issue-11067";
        assertEquals(cmdProduce.getWebSocketProduceUri(topicNameV2),
                "ws://localhost:8080/ws/v2/producer/persistent/public/default/issue-11067");
    }

    @Test
    public void testBuildSchema() {
        // default
        assertEquals(SchemaType.BYTES, CmdProduce.buildSchema("string", "bytes", CmdProduce.KEY_VALUE_ENCODING_TYPE_NOT_SET).getSchemaInfo().getType());

        // simple key value
        assertEquals(SchemaType.KEY_VALUE, CmdProduce.buildSchema("string", "string", "separated").getSchemaInfo().getType());
        assertEquals(SchemaType.KEY_VALUE, CmdProduce.buildSchema("string", "string", "inline").getSchemaInfo().getType());

        KeyValueSchema<?, ?> composite1 = (KeyValueSchema<?, ?>) CmdProduce.buildSchema("string",
                "json:{\"type\": \"record\",\"namespace\": \"com.example\",\"name\": \"FullName\", \"fields\": [{ \"name\": \"a\", \"type\": \"string\" }]}",
                "inline");
        assertEquals(KeyValueEncodingType.INLINE, composite1.getKeyValueEncodingType());
        assertEquals(SchemaType.STRING, composite1.getKeySchema().getSchemaInfo().getType());
        assertEquals(SchemaType.JSON, composite1.getValueSchema().getSchemaInfo().getType());

        KeyValueSchema<?, ?> composite2 = (KeyValueSchema<?, ?>) CmdProduce.buildSchema(
                "json:{\"type\": \"record\",\"namespace\": \"com.example\",\"name\": \"FullName\", \"fields\": [{ \"name\": \"a\", \"type\": \"string\" }]}",
                "avro:{\"type\": \"record\",\"namespace\": \"com.example\",\"name\": \"FullName\", \"fields\": [{ \"name\": \"a\", \"type\": \"string\" }]}",
                "inline");
        assertEquals(KeyValueEncodingType.INLINE, composite2.getKeyValueEncodingType());
        assertEquals(SchemaType.JSON, composite2.getKeySchema().getSchemaInfo().getType());
        assertEquals(SchemaType.AVRO, composite2.getValueSchema().getSchemaInfo().getType());
    }
}
