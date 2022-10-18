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

import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.KEY_VALUE_SCHEMA_INFO_INCLUDE_PRIMITIVE;
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.KEY_VALUE_SCHEMA_INFO_NOT_INCLUDE_PRIMITIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Bar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test {@link KeyValueSchemaInfoTest}.
 */
@Slf4j
public class KeyValueSchemaInfoTest {

    private static final Map<String, String> FOO_PROPERTIES = new HashMap() {

        private static final long serialVersionUID = 58641844834472929L;

        {
            put("foo1", "foo-value1");
            put("foo2", "foo-value2");
            put("foo3", "foo-value3");
        }

    };

    private static final Map<String, String> BAR_PROPERTIES = new HashMap() {

        private static final long serialVersionUID = 58641844834472929L;

        {
            put("bar1", "bar-value1");
            put("bar2", "bar-value2");
            put("bar3", "bar-value3");
        }

    };

    public static final Schema<Foo> FOO_SCHEMA =
        Schema.AVRO(SchemaDefinition.<Foo>builder()
            .withAlwaysAllowNull(false)
            .withPojo(Foo.class)
            .withProperties(FOO_PROPERTIES)
            .build()
        );
    public static final Schema<Bar> BAR_SCHEMA =
        Schema.JSON(SchemaDefinition.<Bar>builder()
            .withAlwaysAllowNull(true)
            .withPojo(Bar.class)
            .withProperties(BAR_PROPERTIES)
            .build());

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeNonKeyValueSchemaInfo() {
        DefaultImplementation.getDefaultImplementation().decodeKeyValueSchemaInfo(
            FOO_SCHEMA.getSchemaInfo()
        );
    }

    @DataProvider(name = "encodingTypes")
    public Object[][] encodingTypes() {
        return new Object[][] {
            { KeyValueEncodingType.INLINE },
            { KeyValueEncodingType.SEPARATED },
        };
    }

    @Test(dataProvider = "encodingTypes")
    public void encodeDecodeKeyValueSchemaInfo(KeyValueEncodingType encodingType) {
        Schema<KeyValue<Foo, Bar>> kvSchema = Schema.KeyValue(
            FOO_SCHEMA,
            BAR_SCHEMA,
            encodingType
        );
        SchemaInfo kvSchemaInfo = kvSchema.getSchemaInfo();
        assertEquals(
                DefaultImplementation.getDefaultImplementation().decodeKeyValueEncodingType(kvSchemaInfo),
            encodingType);

        SchemaInfo encodedSchemaInfo =
                DefaultImplementation.getDefaultImplementation().encodeKeyValueSchemaInfo(FOO_SCHEMA, BAR_SCHEMA, encodingType);
        assertEquals(encodedSchemaInfo, kvSchemaInfo);
        assertEquals(
                DefaultImplementation.getDefaultImplementation().decodeKeyValueEncodingType(encodedSchemaInfo),
            encodingType);

        KeyValue<SchemaInfo, SchemaInfo> schemaInfoKeyValue =
                DefaultImplementation.getDefaultImplementation().decodeKeyValueSchemaInfo(kvSchemaInfo);

        assertEquals(schemaInfoKeyValue.getKey(), FOO_SCHEMA.getSchemaInfo());
        assertEquals(schemaInfoKeyValue.getValue(), BAR_SCHEMA.getSchemaInfo());
    }

    @Test(dataProvider = "encodingTypes")
    public void encodeDecodeNestedKeyValueSchemaInfo(KeyValueEncodingType encodingType) {
        Schema<KeyValue<String, Bar>> nestedSchema =
            Schema.KeyValue(Schema.STRING, BAR_SCHEMA, KeyValueEncodingType.INLINE);
        Schema<KeyValue<Foo, KeyValue<String, Bar>>> kvSchema = Schema.KeyValue(
            FOO_SCHEMA,
            nestedSchema,
            encodingType
        );
        SchemaInfo kvSchemaInfo = kvSchema.getSchemaInfo();
        assertEquals(
                DefaultImplementation.getDefaultImplementation().decodeKeyValueEncodingType(kvSchemaInfo),
            encodingType);

        SchemaInfo encodedSchemaInfo =
                DefaultImplementation.getDefaultImplementation().encodeKeyValueSchemaInfo(
                FOO_SCHEMA,
                nestedSchema,
                encodingType);
        assertEquals(encodedSchemaInfo, kvSchemaInfo);
        assertEquals(
                DefaultImplementation.getDefaultImplementation().decodeKeyValueEncodingType(encodedSchemaInfo),
            encodingType);

        KeyValue<SchemaInfo, SchemaInfo> schemaInfoKeyValue =
                DefaultImplementation.getDefaultImplementation().decodeKeyValueSchemaInfo(kvSchemaInfo);

        assertEquals(schemaInfoKeyValue.getKey(), FOO_SCHEMA.getSchemaInfo());
        assertEquals(schemaInfoKeyValue.getValue().getType(), SchemaType.KEY_VALUE);
        KeyValue<SchemaInfo, SchemaInfo> nestedSchemaInfoKeyValue =
                DefaultImplementation.getDefaultImplementation().decodeKeyValueSchemaInfo(schemaInfoKeyValue.getValue());

        assertEquals(nestedSchemaInfoKeyValue.getKey(), Schema.STRING.getSchemaInfo());
        assertEquals(nestedSchemaInfoKeyValue.getValue(), BAR_SCHEMA.getSchemaInfo());
    }

    @Test
    public void testKeyValueSchemaInfoBackwardCompatibility() {
        Schema<KeyValue<Foo, Bar>> kvSchema = Schema.KeyValue(
            FOO_SCHEMA,
            BAR_SCHEMA,
            KeyValueEncodingType.SEPARATED
        );

        SchemaInfo oldSchemaInfo = SchemaInfoImpl.builder()
            .name("")
            .type(SchemaType.KEY_VALUE)
            .schema(kvSchema.getSchemaInfo().getSchema())
            .properties(Collections.emptyMap()).build();

        assertEquals(
                DefaultImplementation.getDefaultImplementation().decodeKeyValueEncodingType(oldSchemaInfo),
            KeyValueEncodingType.INLINE);

        KeyValue<SchemaInfo, SchemaInfo> schemaInfoKeyValue =
                DefaultImplementation.getDefaultImplementation().decodeKeyValueSchemaInfo(oldSchemaInfo);
        // verify the key schema
        SchemaInfo keySchemaInfo = schemaInfoKeyValue.getKey();
        assertEquals(
            SchemaType.BYTES, keySchemaInfo.getType()
        );
        assertArrayEquals(
            "Expected schema = " + FOO_SCHEMA.getSchemaInfo().getSchemaDefinition()
                + " but found " + keySchemaInfo.getSchemaDefinition(),
            FOO_SCHEMA.getSchemaInfo().getSchema(),
            keySchemaInfo.getSchema()
        );
        assertFalse(FOO_SCHEMA.getSchemaInfo().getProperties().isEmpty());
        assertTrue(keySchemaInfo.getProperties().isEmpty());
        // verify the value schema
        SchemaInfo valueSchemaInfo = schemaInfoKeyValue.getValue();
        assertEquals(
            SchemaType.BYTES, valueSchemaInfo.getType()
        );
        assertArrayEquals(
            BAR_SCHEMA.getSchemaInfo().getSchema(),
            valueSchemaInfo.getSchema()
        );
        assertFalse(BAR_SCHEMA.getSchemaInfo().getProperties().isEmpty());
        assertTrue(valueSchemaInfo.getProperties().isEmpty());
    }

    @Test
    public void testKeyValueSchemaInfoToString() throws Exception {
        String havePrimitiveType = DefaultImplementation.getDefaultImplementation()
                .convertKeyValueSchemaInfoDataToString(KeyValueSchemaInfo
                        .decodeKeyValueSchemaInfo(Schema.KeyValue(Schema.AVRO(Foo.class), Schema.STRING)
                                .getSchemaInfo()));
        JSONSchemaTest.assertJSONEqual(havePrimitiveType, KEY_VALUE_SCHEMA_INFO_INCLUDE_PRIMITIVE);

        String notHavePrimitiveType = DefaultImplementation.getDefaultImplementation()
                .convertKeyValueSchemaInfoDataToString(KeyValueSchemaInfo
                        .decodeKeyValueSchemaInfo(Schema.KeyValue(Schema.AVRO(Foo.class),
                                Schema.AVRO(Foo.class)).getSchemaInfo()));
        JSONSchemaTest.assertJSONEqual(notHavePrimitiveType, KEY_VALUE_SCHEMA_INFO_NOT_INCLUDE_PRIMITIVE);
    }

}
