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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Bar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Color;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class KeyValueSchemaTest {

    @Test
    public void testAvroSchemaCreate() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(Foo.class);
        AvroSchema<Bar> barSchema = AvroSchema.of(Bar.class);

        Schema<KeyValue<Foo, Bar>> keyValueSchema1 = Schema.KeyValue(fooSchema, barSchema);
        Schema<KeyValue<Foo, Bar>> keyValueSchema2 = Schema.KeyValue(Foo.class, Bar.class, SchemaType.AVRO);

        assertEquals(keyValueSchema1.getSchemaInfo().getType(), SchemaType.KEY_VALUE);
        assertEquals(keyValueSchema2.getSchemaInfo().getType(), SchemaType.KEY_VALUE);

        assertEquals(((KeyValueSchema<Foo, Bar>)keyValueSchema1).getKeySchema().getSchemaInfo().getType(),
            SchemaType.AVRO);
        assertEquals(((KeyValueSchema<Foo, Bar>)keyValueSchema1).getValueSchema().getSchemaInfo().getType(),
            SchemaType.AVRO);
        assertEquals(((KeyValueSchema<Foo, Bar>)keyValueSchema2).getKeySchema().getSchemaInfo().getType(),
            SchemaType.AVRO);
        assertEquals(((KeyValueSchema<Foo, Bar>)keyValueSchema2).getValueSchema().getSchemaInfo().getType(),
            SchemaType.AVRO);

        String schemaInfo1 = new String(keyValueSchema1.getSchemaInfo().getSchema());
        String schemaInfo2 = new String(keyValueSchema2.getSchemaInfo().getSchema());
        assertEquals(schemaInfo1, schemaInfo2);
    }

    @Test
    public void testJsonSchemaCreate() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(Foo.class);
        JSONSchema<Bar> barSchema = JSONSchema.of(Bar.class);

        Schema<KeyValue<Foo, Bar>> keyValueSchema1 = Schema.KeyValue(fooSchema, barSchema);
        Schema<KeyValue<Foo, Bar>> keyValueSchema2 = Schema.KeyValue(Foo.class, Bar.class, SchemaType.JSON);
        Schema<KeyValue<Foo, Bar>> keyValueSchema3 = Schema.KeyValue(Foo.class, Bar.class);

        assertEquals(keyValueSchema1.getSchemaInfo().getType(), SchemaType.KEY_VALUE);
        assertEquals(keyValueSchema2.getSchemaInfo().getType(), SchemaType.KEY_VALUE);
        assertEquals(keyValueSchema3.getSchemaInfo().getType(), SchemaType.KEY_VALUE);

        assertEquals(((KeyValueSchema<Foo, Bar>)keyValueSchema1).getKeySchema().getSchemaInfo().getType(),
            SchemaType.JSON);
        assertEquals(((KeyValueSchema<Foo, Bar>)keyValueSchema1).getValueSchema().getSchemaInfo().getType(),
            SchemaType.JSON);
        assertEquals(((KeyValueSchema<Foo, Bar>)keyValueSchema2).getKeySchema().getSchemaInfo().getType(),
            SchemaType.JSON);
        assertEquals(((KeyValueSchema<Foo, Bar>)keyValueSchema2).getValueSchema().getSchemaInfo().getType(),
            SchemaType.JSON);
        assertEquals(((KeyValueSchema<Foo, Bar>)keyValueSchema3).getKeySchema().getSchemaInfo().getType(),
            SchemaType.JSON);
        assertEquals(((KeyValueSchema<Foo, Bar>)keyValueSchema3).getValueSchema().getSchemaInfo().getType(),
            SchemaType.JSON);

        String schemaInfo1 = new String(keyValueSchema1.getSchemaInfo().getSchema());
        String schemaInfo2 = new String(keyValueSchema2.getSchemaInfo().getSchema());
        String schemaInfo3 = new String(keyValueSchema3.getSchemaInfo().getSchema());
        assertEquals(schemaInfo1, schemaInfo2);
        assertEquals(schemaInfo1, schemaInfo3);
    }

    @Test
    public void testSchemaEncodeAndDecode() {
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

        KeyValue<Foo, Bar> keyValue = (KeyValue<Foo, Bar>)keyValueSchema.decode(encodeBytes);
        Foo fooBack = keyValue.getKey();
        Bar barBack = keyValue.getValue();

        assertEquals(foo, fooBack);
        assertEquals(bar, barBack);
    }

    @Test
    public void testBytesSchemaEncodeAndDecode() {
        AvroSchema<Foo> fooAvroSchema = AvroSchema.of(Foo.class);
        AvroSchema<Bar> barAvroSchema = AvroSchema.of(Bar.class);

        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo = new Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(Color.RED);

        byte[] fooBytes = fooAvroSchema.encode(foo);
        byte[] barBytes = barAvroSchema.encode(bar);

        byte[] encodeBytes = Schema.KV_BYTES.encode(new KeyValue<>(fooBytes, barBytes));
        KeyValue<byte[], byte[]> decodeKV = Schema.KV_BYTES.decode(encodeBytes);

        Foo fooBack = fooAvroSchema.decode(decodeKV.getKey());
        Bar barBack = barAvroSchema.decode(decodeKV.getValue());

        assertEquals(foo, fooBack);
        assertEquals(bar, barBack);
    }
}
