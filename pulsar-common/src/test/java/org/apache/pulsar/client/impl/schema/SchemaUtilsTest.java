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
package org.apache.pulsar.client.impl.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

public class SchemaUtilsTest {

    @Test
    public void testJsonifySchemaInfoPreservesNullDefaults() throws Exception {
        SchemaInfo schemaInfo = new SchemaInfoImpl("test-schema", """
                        {
                          "type": "record",
                          "name": "Root",
                          "fields": [
                            {
                              "name": "user_info",
                              "type": {
                                "type": "record",
                                "name": "UserInfo",
                                "fields": [
                                  {"name": "user_id", "type": "string", "default": ""},
                                  {"name": "meter_id", "type": ["null", "string"], "default": null}
                                ]
                              },
                              "default": {"user_id": "", "meter_id": null}
                            }
                          ]
                        }
                        """.getBytes(UTF_8), SchemaType.AVRO, 0, Collections.emptyMap());

        assertNullDefaults(SchemaUtils.jsonifySchemaInfo(schemaInfo, true).getBytes(UTF_8));

        SchemaInfoWithVersion schemaInfoWithVersion = SchemaInfoWithVersion.builder()
                .schemaInfo(schemaInfo)
                .version(0)
                .build();
        assertNullDefaults(SchemaUtils.jsonifySchemaInfoWithVersion(schemaInfoWithVersion).getBytes(UTF_8));
    }

    private static void assertNullDefaults(byte[] json) throws Exception {
        JsonNode schema = ObjectMapperFactory.getMapper().reader().readTree(json).at("/schemaInfo/schema");
        if (schema.isMissingNode()) {
            schema = ObjectMapperFactory.getMapper().reader().readTree(json).at("/schema");
        }

        JsonNode userInfoField = schema.at("/fields/0");
        assertTrue(userInfoField.get("default").has("meter_id"));
        assertTrue(userInfoField.at("/default/meter_id").isNull());

        JsonNode meterIdField = userInfoField.at("/type/fields/1");
        assertEquals(meterIdField.get("name").asText(), "meter_id");
        assertTrue(meterIdField.has("default"));
        assertTrue(meterIdField.get("default").isNull());
    }
}
