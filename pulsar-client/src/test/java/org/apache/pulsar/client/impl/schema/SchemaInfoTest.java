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
package org.apache.pulsar.client.impl.schema;

import com.google.common.collect.Maps;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Unit test {@link org.apache.pulsar.common.schema.SchemaInfo}.
 */
public class SchemaInfoTest {

    private static final String INT32_SCHEMA_INFO = "{\n"
        + "  \"name\": \"INT32\",\n"
        + "  \"schema\": \"\",\n"
        + "  \"type\": \"INT32\",\n"
        + "  \"properties\": {}\n"
        + "}";

    private static final String UTF8_SCHEMA_INFO = "{\n"
        + "  \"name\": \"String\",\n"
        + "  \"schema\": \"\",\n"
        + "  \"type\": \"STRING\",\n"
        + "  \"properties\": {}\n"
        + "}";

    private static final String BAR_SCHEMA_INFO = "{\n"
        + "  \"name\": \"\",\n"
        + "  \"schema\": {\n"
        + "    \"type\": \"record\",\n"
        + "    \"name\": \"Bar\",\n"
        + "    \"namespace\": \"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\n"
        + "    \"fields\": [\n"
        + "      {\n"
        + "        \"name\": \"field1\",\n"
        + "        \"type\": \"boolean\"\n"
        + "      }\n"
        + "    ]\n"
        + "  },\n"
        + "  \"type\": \"JSON\",\n"
        + "  \"properties\": {\n"
        + "    \"__alwaysAllowNull\": \"true\",\n"
        + "    \"__jsr310ConversionEnabled\": \"false\",\n"
        + "    \"bar1\": \"bar-value1\",\n"
        + "    \"bar2\": \"bar-value2\",\n"
        + "    \"bar3\": \"bar-value3\"\n"
        + "  }\n"
        + "}";

    private static final String FOO_SCHEMA_INFO = "{\n"
        + "  \"name\": \"\",\n"
        + "  \"schema\": {\n"
        + "    \"type\": \"record\",\n"
        + "    \"name\": \"Foo\",\n"
        + "    \"namespace\": \"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\n"
        + "    \"fields\": [\n"
        + "      {\n"
        + "        \"name\": \"color\",\n"
        + "        \"type\": [\n"
        + "          \"null\",\n"
        + "          {\n"
        + "            \"type\": \"enum\",\n"
        + "            \"name\": \"Color\",\n"
        + "            \"symbols\": [\n"
        + "              \"RED\",\n"
        + "              \"BLUE\"\n"
        + "            ]\n"
        + "          }\n"
        + "        ]\n"
        + "      },\n"
        + "      {\n"
        + "        \"name\": \"field1\",\n"
        + "        \"type\": [\n"
        + "          \"null\",\n"
        + "          \"string\"\n"
        + "        ]\n"
        + "      },\n"
        + "      {\n"
        + "        \"name\": \"field2\",\n"
        + "        \"type\": [\n"
        + "          \"null\",\n"
        + "          \"string\"\n"
        + "        ]\n"
        + "      },\n"
        + "      {\n"
        + "        \"name\": \"field3\",\n"
        + "        \"type\": \"int\"\n"
        + "      },\n"
        + "      {\n"
        + "        \"name\": \"field4\",\n"
        + "        \"type\": [\n"
        + "          \"null\",\n"
        + "          {\n"
        + "            \"type\": \"record\",\n"
        + "            \"name\": \"Bar\",\n"
        + "            \"fields\": [\n"
        + "              {\n"
        + "                \"name\": \"field1\",\n"
        + "                \"type\": \"boolean\"\n"
        + "              }\n"
        + "            ]\n"
        + "          }\n"
        + "        ]\n"
        + "      },\n"
        + "      {\n"
        + "        \"name\": \"fieldUnableNull\",\n"
        + "        \"type\": \"string\",\n"
        + "        \"default\": \"defaultValue\"\n"
        + "      }\n"
        + "    ]\n"
        + "  },\n"
        + "  \"type\": \"AVRO\",\n"
        + "  \"properties\": {\n"
        + "    \"__alwaysAllowNull\": \"false\",\n"
        + "    \"__jsr310ConversionEnabled\": \"false\",\n"
        + "    \"foo1\": \"foo-value1\",\n"
        + "    \"foo2\": \"foo-value2\",\n"
        + "    \"foo3\": \"foo-value3\"\n"
        + "  }\n"
        + "}";

    private static final String KV_SCHEMA_INFO = "{\n"
        + "  \"name\": \"KeyValue\",\n"
        + "  \"schema\": {\n"
        + "    \"key\": {\n"
        + "      \"name\": \"\",\n"
        + "      \"schema\": {\n"
        + "        \"type\": \"record\",\n"
        + "        \"name\": \"Foo\",\n"
        + "        \"namespace\": \"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\n"
        + "        \"fields\": [\n"
        + "          {\n"
        + "            \"name\": \"color\",\n"
        + "            \"type\": [\n"
        + "              \"null\",\n"
        + "              {\n"
        + "                \"type\": \"enum\",\n"
        + "                \"name\": \"Color\",\n"
        + "                \"symbols\": [\n"
        + "                  \"RED\",\n"
        + "                  \"BLUE\"\n"
        + "                ]\n"
        + "              }\n"
        + "            ]\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"field1\",\n"
        + "            \"type\": [\n"
        + "              \"null\",\n"
        + "              \"string\"\n"
        + "            ]\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"field2\",\n"
        + "            \"type\": [\n"
        + "              \"null\",\n"
        + "              \"string\"\n"
        + "            ]\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"field3\",\n"
        + "            \"type\": \"int\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"field4\",\n"
        + "            \"type\": [\n"
        + "              \"null\",\n"
        + "              {\n"
        + "                \"type\": \"record\",\n"
        + "                \"name\": \"Bar\",\n"
        + "                \"fields\": [\n"
        + "                  {\n"
        + "                    \"name\": \"field1\",\n"
        + "                    \"type\": \"boolean\"\n"
        + "                  }\n"
        + "                ]\n"
        + "              }\n"
        + "            ]\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"fieldUnableNull\",\n"
        + "            \"type\": \"string\",\n"
        + "            \"default\": \"defaultValue\"\n"
        + "          }\n"
        + "        ]\n"
        + "      },\n"
        + "      \"type\": \"AVRO\",\n"
        + "      \"properties\": {\n"
        + "        \"__alwaysAllowNull\": \"false\",\n"
        + "        \"__jsr310ConversionEnabled\": \"false\",\n"
        + "        \"foo1\": \"foo-value1\",\n"
        + "        \"foo2\": \"foo-value2\",\n"
        + "        \"foo3\": \"foo-value3\"\n"
        + "      }\n"
        + "    },\n"
        + "    \"value\": {\n"
        + "      \"name\": \"\",\n"
        + "      \"schema\": {\n"
        + "        \"type\": \"record\",\n"
        + "        \"name\": \"Bar\",\n"
        + "        \"namespace\": \"org.apache.pulsar.client.impl.schema.SchemaTestUtils\",\n"
        + "        \"fields\": [\n"
        + "          {\n"
        + "            \"name\": \"field1\",\n"
        + "            \"type\": \"boolean\"\n"
        + "          }\n"
        + "        ]\n"
        + "      },\n"
        + "      \"type\": \"JSON\",\n"
        + "      \"properties\": {\n"
        + "        \"__alwaysAllowNull\": \"true\",\n"
        + "        \"__jsr310ConversionEnabled\": \"false\",\n"
        + "        \"bar1\": \"bar-value1\",\n"
        + "        \"bar2\": \"bar-value2\",\n"
        + "        \"bar3\": \"bar-value3\"\n"
        + "      }\n"
        + "    }\n"
        + "  },\n"
        + "  \"type\": \"KEY_VALUE\",\n"
        + "  \"properties\": {\n"
        + "    \"key.schema.name\": \"\",\n"
        + "    \"key.schema.properties\": \"{\\\"__alwaysAllowNull\\\":\\\"false\\\",\\\"__jsr310ConversionEnabled\\\":\\\"false\\\",\\\"foo1\\\":\\\"foo-value1\\\",\\\"foo2\\\":\\\"foo-value2\\\",\\\"foo3\\\":\\\"foo-value3\\\"}\",\n"
        + "    \"key.schema.type\": \"AVRO\",\n"
        + "    \"kv.encoding.type\": \"SEPARATED\",\n"
        + "    \"value.schema.name\": \"\",\n"
        + "    \"value.schema.properties\": \"{\\\"__alwaysAllowNull\\\":\\\"true\\\",\\\"__jsr310ConversionEnabled\\\":\\\"false\\\",\\\"bar1\\\":\\\"bar-value1\\\",\\\"bar2\\\":\\\"bar-value2\\\",\\\"bar3\\\":\\\"bar-value3\\\"}\",\n"
        + "    \"value.schema.type\": \"JSON\"\n"
        + "  }\n"
        + "}";

    @DataProvider(name = "schemas")
    public static Object[][] schemas() {
        return new Object[][] {
            {
                Schema.STRING.getSchemaInfo(), UTF8_SCHEMA_INFO
            },
            {
                Schema.INT32.getSchemaInfo(), INT32_SCHEMA_INFO
            },
            {
                KeyValueSchemaInfoTest.FOO_SCHEMA.getSchemaInfo(), FOO_SCHEMA_INFO
            },
            {
                KeyValueSchemaInfoTest.BAR_SCHEMA.getSchemaInfo(), BAR_SCHEMA_INFO
            },
            {
                Schema.KeyValue(
                    KeyValueSchemaInfoTest.FOO_SCHEMA,
                    KeyValueSchemaInfoTest.BAR_SCHEMA,
                    KeyValueEncodingType.SEPARATED
                ).getSchemaInfo(),
                KV_SCHEMA_INFO
            }
        };
    }

    @Test(dataProvider = "schemas")
    public void testSchemaInfoToString(SchemaInfo si, String jsonifiedStr) {
        assertEquals(si.toString(), jsonifiedStr);
    }

    public static class SchemaInfoBuilderTest {

        @Test
        public void testUnsetProperties() {
            final SchemaInfo schemaInfo = SchemaInfo.builder()
                    .type(SchemaType.STRING)
                    .schema(new byte[0])
                    .name("string")
                    .build();

            assertEquals(schemaInfo.getSchema(), new byte[0]);
            assertEquals(schemaInfo.getType(), SchemaType.STRING);
            assertEquals(schemaInfo.getName(), "string");
            assertEquals(schemaInfo.getProperties(), new HashMap<>());
        }

        @Test
        public void testSetProperties() {
            final Map<String, String> map = new HashMap<>();
            map.put("test", "value");
            final SchemaInfo schemaInfo = SchemaInfo.builder()
                    .type(SchemaType.STRING)
                    .schema(new byte[0])
                    .name("string")
                    .properties(map)
                    .build();

            assertEquals(schemaInfo.getSchema(), new byte[0]);
            assertEquals(schemaInfo.getType(), SchemaType.STRING);
            assertEquals(schemaInfo.getName(), "string");
            assertEquals(schemaInfo.getProperties(), new HashMap<>(map));
        }

        @Test
        public void testNullPropertyValue() {
            final Map<String, String> map = new HashMap<>();
            map.put("key", null);

            SchemaInfo si = SchemaInfo.builder()
                    .name("INT32")
                    .schema(new byte[0])
                    .type(SchemaType.INT32)
                    .properties(map)
                    .build();

            // null key will be skipped by Gson when serializing JSON to String
            assertEquals(si.toString(), INT32_SCHEMA_INFO);
        }
    }
}
