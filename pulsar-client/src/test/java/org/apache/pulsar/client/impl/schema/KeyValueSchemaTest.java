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

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Bar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Color;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;


@Slf4j
public class KeyValueSchemaTest {

    @Test
    public void testAllowNullAvroSchemaCreate() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());

        Schema<KeyValue<Foo, Bar>> keyValueSchema1 = Schema.KeyValue(fooSchema, barSchema);
        Schema<KeyValue<Foo, Bar>> keyValueSchema2 = Schema.KeyValue(Foo.class, Bar.class, SchemaType.AVRO);

        assertEquals(keyValueSchema1.getSchemaInfo().getType(), SchemaType.KEY_VALUE);
        assertEquals(keyValueSchema2.getSchemaInfo().getType(), SchemaType.KEY_VALUE);

        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema1).getKeySchema().getSchemaInfo().getType(),
                SchemaType.AVRO);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema1).getValueSchema().getSchemaInfo().getType(),
                SchemaType.AVRO);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema2).getKeySchema().getSchemaInfo().getType(),
                SchemaType.AVRO);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema2).getValueSchema().getSchemaInfo().getType(),
                SchemaType.AVRO);

        String schemaInfo1 = new String(keyValueSchema1.getSchemaInfo().getSchema());
        String schemaInfo2 = new String(keyValueSchema2.getSchemaInfo().getSchema());
        assertEquals(schemaInfo1, schemaInfo2);
    }

    @Test
    public void testFillParametersToSchemainfo() {
        Map<String, String> keyProperties = Maps.newTreeMap();
        keyProperties.put("foo.key1", "value");
        keyProperties.put("foo.key2", "value");

        Map<String, String> valueProperties = Maps.newTreeMap();
        valueProperties.put("bar.key", "key");

        AvroSchema<Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<Foo>builder()
                        .withPojo(Foo.class)
                        .withProperties(keyProperties)
                        .build());
        AvroSchema<Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<Bar>builder()
                        .withPojo(Bar.class)
                        .withProperties(valueProperties)
                        .build());

        Schema<KeyValue<Foo, Bar>> keyValueSchema1 = Schema.KeyValue(fooSchema, barSchema);

        assertEquals(keyValueSchema1.getSchemaInfo().getProperties().get("key.schema.type"), String.valueOf(SchemaType.AVRO));
        assertEquals(keyValueSchema1.getSchemaInfo().getProperties().get("key.schema.properties"),
                "{\"__alwaysAllowNull\":\"true\",\"__jsr310ConversionEnabled\":\"false\",\"foo.key1\":\"value\",\"foo.key2\":\"value\"}");
        assertEquals(keyValueSchema1.getSchemaInfo().getProperties().get("value.schema.type"), String.valueOf(SchemaType.AVRO));
        assertEquals(keyValueSchema1.getSchemaInfo().getProperties().get("value.schema.properties"),
                "{\"__alwaysAllowNull\":\"true\",\"__jsr310ConversionEnabled\":\"false\",\"bar.key\":\"key\"}");
    }

    @Test
    public void testNotAllowNullAvroSchemaCreate() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).withAlwaysAllowNull(false).build());

        Schema<KeyValue<Foo, Bar>> keyValueSchema1 = Schema.KeyValue(fooSchema, barSchema);
        Schema<KeyValue<Foo, Bar>> keyValueSchema2 = Schema.KeyValue(AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build()),
                AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).withAlwaysAllowNull(false).build()));

        assertEquals(keyValueSchema1.getSchemaInfo().getType(), SchemaType.KEY_VALUE);
        assertEquals(keyValueSchema2.getSchemaInfo().getType(), SchemaType.KEY_VALUE);

        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema1).getKeySchema().getSchemaInfo().getType(),
            SchemaType.AVRO);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema1).getValueSchema().getSchemaInfo().getType(),
            SchemaType.AVRO);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema2).getKeySchema().getSchemaInfo().getType(),
            SchemaType.AVRO);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema2).getValueSchema().getSchemaInfo().getType(),
            SchemaType.AVRO);

        String schemaInfo1 = new String(keyValueSchema1.getSchemaInfo().getSchema());
        String schemaInfo2 = new String(keyValueSchema2.getSchemaInfo().getSchema());
        assertEquals(schemaInfo1, schemaInfo2);
    }

    @Test
    public void testAllowNullJsonSchemaCreate() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());

        Schema<KeyValue<Foo, Bar>> keyValueSchema1 = Schema.KeyValue(fooSchema, barSchema);
        Schema<KeyValue<Foo, Bar>> keyValueSchema2 = Schema.KeyValue(Foo.class, Bar.class, SchemaType.JSON);
        Schema<KeyValue<Foo, Bar>> keyValueSchema3 = Schema.KeyValue(Foo.class, Bar.class);

        assertEquals(keyValueSchema1.getSchemaInfo().getType(), SchemaType.KEY_VALUE);
        assertEquals(keyValueSchema2.getSchemaInfo().getType(), SchemaType.KEY_VALUE);
        assertEquals(keyValueSchema3.getSchemaInfo().getType(), SchemaType.KEY_VALUE);

        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema1).getKeySchema().getSchemaInfo().getType(),
                SchemaType.JSON);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema1).getValueSchema().getSchemaInfo().getType(),
                SchemaType.JSON);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema2).getKeySchema().getSchemaInfo().getType(),
                SchemaType.JSON);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema2).getValueSchema().getSchemaInfo().getType(),
                SchemaType.JSON);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema3).getKeySchema().getSchemaInfo().getType(),
                SchemaType.JSON);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema3).getValueSchema().getSchemaInfo().getType(),
                SchemaType.JSON);

        String schemaInfo1 = new String(keyValueSchema1.getSchemaInfo().getSchema());
        String schemaInfo2 = new String(keyValueSchema2.getSchemaInfo().getSchema());
        String schemaInfo3 = new String(keyValueSchema3.getSchemaInfo().getSchema());
        assertEquals(schemaInfo1, schemaInfo2);
        assertEquals(schemaInfo1, schemaInfo3);
    }

    @Test
    public void testNotAllowNullJsonSchemaCreate() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(
                SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(
                SchemaDefinition.<Bar>builder().withPojo(Bar.class).withAlwaysAllowNull(false).build());

        Schema<KeyValue<Foo, Bar>> keyValueSchema1 = Schema.KeyValue(fooSchema, barSchema);
        Schema<KeyValue<Foo, Bar>> keyValueSchema2 = Schema.KeyValue(JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build()),
                JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).withAlwaysAllowNull(false).build()));

        Schema<KeyValue<Foo, Bar>> keyValueSchema3 = Schema.KeyValue(JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build()),
                JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).withAlwaysAllowNull(false).build()));

        assertEquals(keyValueSchema1.getSchemaInfo().getType(), SchemaType.KEY_VALUE);
        assertEquals(keyValueSchema2.getSchemaInfo().getType(), SchemaType.KEY_VALUE);
        assertEquals(keyValueSchema3.getSchemaInfo().getType(), SchemaType.KEY_VALUE);

        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema1).getKeySchema().getSchemaInfo().getType(),
            SchemaType.JSON);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema1).getValueSchema().getSchemaInfo().getType(),
            SchemaType.JSON);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema2).getKeySchema().getSchemaInfo().getType(),
            SchemaType.JSON);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema2).getValueSchema().getSchemaInfo().getType(),
            SchemaType.JSON);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema3).getKeySchema().getSchemaInfo().getType(),
            SchemaType.JSON);
        assertEquals(((KeyValueSchemaImpl<Foo, Bar>) keyValueSchema3).getValueSchema().getSchemaInfo().getType(),
            SchemaType.JSON);

        String schemaInfo1 = new String(keyValueSchema1.getSchemaInfo().getSchema());
        String schemaInfo2 = new String(keyValueSchema2.getSchemaInfo().getSchema());
        String schemaInfo3 = new String(keyValueSchema3.getSchemaInfo().getSchema());
        assertEquals(schemaInfo1, schemaInfo2);
        assertEquals(schemaInfo1, schemaInfo3);
    }

    @Test
    public void testAllowNullSchemaEncodeAndDecode() {
        Schema keyValueSchema = Schema.KeyValue(Foo.class, Bar.class);

        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo = new Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(Color.RED);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        Assert.assertTrue(encodeBytes.length > 0);

        KeyValue<Foo, Bar> keyValue = (KeyValue<Foo, Bar>) keyValueSchema.decode(encodeBytes);
        Foo fooBack = keyValue.getKey();
        Bar barBack = keyValue.getValue();

        assertEquals(foo, fooBack);
        assertEquals(bar, barBack);
    }

    @Test
    public void testNotAllowNullSchemaEncodeAndDecode() {
        Schema keyValueSchema = Schema.KeyValue(JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build()),
                JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).withAlwaysAllowNull(false).build()));

        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo = new Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(Color.RED);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        Assert.assertTrue(encodeBytes.length > 0);

        KeyValue<Foo, Bar> keyValue = (KeyValue<Foo, Bar>) keyValueSchema.decode(encodeBytes);
        Foo fooBack = keyValue.getKey();
        Bar barBack = keyValue.getValue();

        assertEquals(foo, fooBack);
        assertEquals(bar, barBack);
    }

    @Test
    public void testDefaultKeyValueEncodingTypeSchemaEncodeAndDecode() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());

        Schema<KeyValue<Foo, Bar>> keyValueSchema = Schema.KeyValue(fooSchema, barSchema);

        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo = new Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(Color.RED);

        // Check kv.encoding.type default not set value
        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        Assert.assertTrue(encodeBytes.length > 0);

        KeyValue<Foo, Bar> keyValue = (KeyValue<Foo, Bar>) keyValueSchema.decode(encodeBytes);
        Foo fooBack = keyValue.getKey();
        Bar barBack = keyValue.getValue();

        assertEquals(foo, fooBack);
        assertEquals(bar, barBack);
    }

    @Test
    public void testInlineKeyValueEncodingTypeSchemaEncodeAndDecode() {

        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());

        Schema<KeyValue<Foo, Bar>> keyValueSchema = Schema.KeyValue(fooSchema, barSchema, KeyValueEncodingType.INLINE);


        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo = new Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(Color.RED);

        // Check kv.encoding.type INLINE
        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        Assert.assertTrue(encodeBytes.length > 0);
        KeyValue<Foo, Bar>  keyValue = (KeyValue<Foo, Bar>) keyValueSchema.decode(encodeBytes);
        Foo fooBack = keyValue.getKey();
        Bar barBack = keyValue.getValue();
        assertEquals(foo, fooBack);
        assertEquals(bar, barBack);

    }

    @Test
    public void testSeparatedKeyValueEncodingTypeSchemaEncodeAndDecode() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());

        Schema<KeyValue<Foo, Bar>> keyValueSchema = Schema.KeyValue(fooSchema, barSchema, KeyValueEncodingType.SEPARATED);

        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo = new Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(Color.RED);

        // Check kv.encoding.type SEPARATED
        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        Assert.assertTrue(encodeBytes.length > 0);
        try {
            keyValueSchema.decode(encodeBytes);
            Assert.fail("This method cannot be used under this SEPARATED encoding type");
        } catch (SchemaSerializationException e) {
            Assert.assertTrue(e.getMessage().contains("This method cannot be used under this SEPARATED encoding type"));
        }
        KeyValue<Foo, Bar>  keyValue = ((KeyValueSchemaImpl)keyValueSchema).decode(fooSchema.encode(foo), encodeBytes, null);
        Foo fooBack = keyValue.getKey();
        Bar barBack = keyValue.getValue();
        assertEquals(foo, fooBack);
        assertEquals(bar, barBack);
    }

    @Test
    public void testAllowNullBytesSchemaEncodeAndDecode() {
        AvroSchema<Foo> fooAvroSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barAvroSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());

        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo = new Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(Color.RED);
        foo.setFieldUnableNull("notNull");

        byte[] fooBytes = fooAvroSchema.encode(foo);
        byte[] barBytes = barAvroSchema.encode(bar);

        byte[] encodeBytes = Schema.KV_BYTES().encode(new KeyValue<>(fooBytes, barBytes));
        KeyValue<byte[], byte[]> decodeKV = Schema.KV_BYTES().decode(encodeBytes);

        Foo fooBack = fooAvroSchema.decode(decodeKV.getKey());
        Bar barBack = barAvroSchema.decode(decodeKV.getValue());

        assertEquals(foo, fooBack);
        assertEquals(bar, barBack);
    }

    @Test
    public void testNotAllowNullBytesSchemaEncodeAndDecode() {
        AvroSchema<Foo> fooAvroSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());
        AvroSchema<Bar> barAvroSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).withAlwaysAllowNull(false).build());

        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo = new Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(Color.RED);
        foo.setFieldUnableNull("notNull");

        byte[] fooBytes = fooAvroSchema.encode(foo);
        byte[] barBytes = barAvroSchema.encode(bar);

        byte[] encodeBytes = Schema.KV_BYTES().encode(new KeyValue<>(fooBytes, barBytes));
        KeyValue<byte[], byte[]> decodeKV = Schema.KV_BYTES().decode(encodeBytes);

        Foo fooBack = fooAvroSchema.decode(decodeKV.getKey());
        Bar barBack = barAvroSchema.decode(decodeKV.getValue());

        assertEquals(foo, fooBack);
        assertEquals(bar, barBack);
    }

    @Test
    public void testKeyValueSchemaSeparatedEncoding() {
        KeyValueSchemaImpl<String, String> keyValueSchema = (KeyValueSchemaImpl<String,String>)
                KeyValueSchemaImpl.of(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED);
        KeyValueSchemaImpl<String, String> keyValueSchema2 = (KeyValueSchemaImpl<String,String>)
                AutoConsumeSchema.getSchema(keyValueSchema.getSchemaInfo());
        assertEquals(keyValueSchema.getKeyValueEncodingType(), keyValueSchema2.getKeyValueEncodingType());
    }
}
