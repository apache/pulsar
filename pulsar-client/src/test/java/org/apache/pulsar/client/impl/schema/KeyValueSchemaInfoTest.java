package org.apache.pulsar.client.impl.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.nio.charset.StandardCharsets;
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
        DefaultImplementation.decodeKeyValueSchemaInfo(
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
            DefaultImplementation.decodeKeyValueEncodingType(kvSchemaInfo),
            encodingType);

        SchemaInfo encodedSchemaInfo =
            DefaultImplementation.encodeKeyValueSchemaInfo(FOO_SCHEMA, BAR_SCHEMA, encodingType);
        assertEquals(encodedSchemaInfo, kvSchemaInfo);
        assertEquals(
            DefaultImplementation.decodeKeyValueEncodingType(encodedSchemaInfo),
            encodingType);

        KeyValue<SchemaInfo, SchemaInfo> schemaInfoKeyValue =
            DefaultImplementation.decodeKeyValueSchemaInfo(kvSchemaInfo);

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
            DefaultImplementation.decodeKeyValueEncodingType(kvSchemaInfo),
            encodingType);

        SchemaInfo encodedSchemaInfo =
            DefaultImplementation.encodeKeyValueSchemaInfo(
                FOO_SCHEMA,
                nestedSchema,
                encodingType);
        assertEquals(encodedSchemaInfo, kvSchemaInfo);
        assertEquals(
            DefaultImplementation.decodeKeyValueEncodingType(encodedSchemaInfo),
            encodingType);

        KeyValue<SchemaInfo, SchemaInfo> schemaInfoKeyValue =
            DefaultImplementation.decodeKeyValueSchemaInfo(kvSchemaInfo);

        assertEquals(schemaInfoKeyValue.getKey(), FOO_SCHEMA.getSchemaInfo());
        assertEquals(schemaInfoKeyValue.getValue().getType(), SchemaType.KEY_VALUE);
        KeyValue<SchemaInfo, SchemaInfo> nestedSchemaInfoKeyValue =
            DefaultImplementation.decodeKeyValueSchemaInfo(schemaInfoKeyValue.getValue());

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

        SchemaInfo oldSchemaInfo = new SchemaInfo()
            .setName("")
            .setType(SchemaType.KEY_VALUE)
            .setSchema(kvSchema.getSchemaInfo().getSchema())
            .setProperties(Collections.emptyMap());

        assertEquals(
            DefaultImplementation.decodeKeyValueEncodingType(oldSchemaInfo),
            KeyValueEncodingType.INLINE);

        KeyValue<SchemaInfo, SchemaInfo> schemaInfoKeyValue =
            DefaultImplementation.decodeKeyValueSchemaInfo(oldSchemaInfo);
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

}
