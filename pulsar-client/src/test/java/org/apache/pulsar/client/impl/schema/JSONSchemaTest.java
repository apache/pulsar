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

import java.util.Collections;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Bar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.DerivedFoo;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.NestedBar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.NestedBarList;
import org.apache.pulsar.common.schema.SchemaType;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.json.JSONException;

import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.FOO_FIELDS;
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.SCHEMA_JSON_NOT_ALLOW_NULL;
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.SCHEMA_JSON_ALLOW_NULL;
import static org.testng.Assert.assertEquals;

@Slf4j
public class JSONSchemaTest {

    public static void assertJSONEqual(String s1, String s2) throws JSONException{
        JSONAssert.assertEquals(s1, s2, false);
    }
    @Test
    public void testNotAllowNullSchema() throws JSONException {
        JSONSchema<Foo> jsonSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());
        Assert.assertEquals(jsonSchema.getSchemaInfo().getType(), SchemaType.JSON);
        Schema.Parser parser = new Schema.Parser();
        String schemaJson = new String(jsonSchema.getSchemaInfo().getSchema());
        assertJSONEqual(schemaJson, SCHEMA_JSON_NOT_ALLOW_NULL);
        Schema schema = parser.parse(schemaJson);

        for (String fieldName : FOO_FIELDS) {
            Schema.Field field = schema.getField(fieldName);
            Assert.assertNotNull(field);

            if (field.name().equals("field4")) {
                Assert.assertNotNull(field.schema().getTypes().get(1).getField("field1"));
            }
            if (field.name().equals("fieldUnableNull")) {
                Assert.assertNotNull(field.schema().getType());
            }
        }
    }

    @Test
    public void testAllowNullSchema() throws JSONException {
        JSONSchema<Foo> jsonSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        Assert.assertEquals(jsonSchema.getSchemaInfo().getType(), SchemaType.JSON);
        Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        String schemaJson = new String(jsonSchema.getSchemaInfo().getSchema());
        assertJSONEqual(schemaJson, SCHEMA_JSON_ALLOW_NULL);
        Schema schema = parser.parse(schemaJson);

        for (String fieldName : FOO_FIELDS) {
            Schema.Field field = schema.getField(fieldName);
            Assert.assertNotNull(field);

            if (field.name().equals("field4")) {
                Assert.assertNotNull(field.schema().getTypes().get(1).getField("field1"));
            }
            if (field.name().equals("fieldUnableNull")) {
                Assert.assertNotNull(field.schema().getType());
            }
        }
    }

    @Test
    public void testAllowNullEncodeAndDecode() {
        JSONSchema<Foo> jsonSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Bar bar = new Bar();
        bar.setField1(true);

        Foo foo1 = new Foo();
        foo1.setField1("foo1");
        foo1.setField2("bar1");
        foo1.setField4(bar);
        foo1.setColor(SchemaTestUtils.Color.BLUE);

        Foo foo2 = new Foo();
        foo2.setField1("foo2");
        foo2.setField2("bar2");

        byte[] bytes1 = jsonSchema.encode(foo1);
        Assert.assertTrue(bytes1.length > 0);

        byte[] bytes2 = jsonSchema.encode(foo2);
        Assert.assertTrue(bytes2.length > 0);

        Foo object1 = jsonSchema.decode(bytes1);
        Foo object2 = jsonSchema.decode(bytes2);

        Assert.assertEquals(object1, foo1);
        Assert.assertEquals(object2, foo2);
    }

    @Test
    public void testNotAllowNullEncodeAndDecode() {
        JSONSchema<Foo> jsonSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());

        Foo foo1 = new Foo();
        foo1.setField1("foo1");
        foo1.setField2("bar1");
        foo1.setField4(new Bar());
        foo1.setFieldUnableNull("notNull");

        Foo foo2 = new Foo();
        foo2.setField1("foo2");
        foo2.setField2("bar2");

        byte[] bytes1 = jsonSchema.encode(foo1);
        Foo object1 = jsonSchema.decode(bytes1);
        Assert.assertTrue(bytes1.length > 0);
        assertEquals(object1, foo1);

        try {

            jsonSchema.encode(foo2);

        } catch (Exception e) {
            Assert.assertTrue(e instanceof SchemaSerializationException);
        }

    }

    @Test
    public void testAllowNullNestedClasses() {
        JSONSchema<NestedBar> jsonSchema = JSONSchema.of(SchemaDefinition.<NestedBar>builder().withPojo(NestedBar.class).build());
        JSONSchema<NestedBarList> listJsonSchema = JSONSchema.of(SchemaDefinition.<NestedBarList>builder().withPojo(NestedBarList.class).build());

        Bar bar = new Bar();
        bar.setField1(true);

        NestedBar nested = new NestedBar();
        nested.setField1(true);
        nested.setNested(bar);

        byte[] bytes = jsonSchema.encode(nested);
        Assert.assertTrue(bytes.length > 0);
        Assert.assertEquals(jsonSchema.decode(bytes), nested);

        List<Bar> list = Collections.singletonList(bar);
        NestedBarList nestedList = new NestedBarList();
        nestedList.setField1(true);
        nestedList.setList(list);

        bytes = listJsonSchema.encode(nestedList);
        Assert.assertTrue(bytes.length > 0);

        Assert.assertEquals(listJsonSchema.decode(bytes), nestedList);
    }

    @Test
    public void testNotAllowNullNestedClasses() {
        JSONSchema<NestedBar> jsonSchema = JSONSchema.of(SchemaDefinition.<NestedBar>builder().withPojo(NestedBar.class).withAlwaysAllowNull(false).build());
        JSONSchema<NestedBarList> listJsonSchema = JSONSchema.of(SchemaDefinition.<NestedBarList>builder().withPojo(NestedBarList.class).withAlwaysAllowNull(false).build());

        Bar bar = new Bar();
        bar.setField1(true);

        NestedBar nested = new NestedBar();
        nested.setField1(true);
        nested.setNested(bar);

        byte[] bytes = jsonSchema.encode(nested);
        Assert.assertTrue(bytes.length > 0);
        Assert.assertEquals(jsonSchema.decode(bytes), nested);

        List<Bar> list = Collections.singletonList(bar);
        NestedBarList nestedList = new NestedBarList();
        nestedList.setField1(true);
        nestedList.setList(list);

        bytes = listJsonSchema.encode(nestedList);
        Assert.assertTrue(bytes.length > 0);

        Assert.assertEquals(listJsonSchema.decode(bytes), nestedList);
    }

    @Test
    public void testNotAllowNullCorrectPolymorphism() {
        Bar bar = new Bar();
        bar.setField1(true);

        DerivedFoo derivedFoo = new DerivedFoo();
        derivedFoo.setField1("foo1");
        derivedFoo.setField2("bar2");
        derivedFoo.setField3(4);
        derivedFoo.setField4(bar);
        derivedFoo.setField5("derived1");
        derivedFoo.setField6(2);

        Foo foo = new Foo();
        foo.setField1("foo1");
        foo.setField2("bar2");
        foo.setField3(4);
        foo.setField4(bar);

        SchemaTestUtils.DerivedDerivedFoo derivedDerivedFoo = new SchemaTestUtils.DerivedDerivedFoo();
        derivedDerivedFoo.setField1("foo1");
        derivedDerivedFoo.setField2("bar2");
        derivedDerivedFoo.setField3(4);
        derivedDerivedFoo.setField4(bar);
        derivedDerivedFoo.setField5("derived1");
        derivedDerivedFoo.setField6(2);
        derivedDerivedFoo.setFoo2(foo);
        derivedDerivedFoo.setDerivedFoo(derivedFoo);

        // schema for base class
        JSONSchema<Foo> baseJsonSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        Assert.assertEquals(baseJsonSchema.decode(baseJsonSchema.encode(foo)), foo);
        Assert.assertEquals(baseJsonSchema.decode(baseJsonSchema.encode(derivedFoo)), foo);
        Assert.assertEquals(baseJsonSchema.decode(baseJsonSchema.encode(derivedDerivedFoo)), foo);

        // schema for derived class
        JSONSchema<DerivedFoo> derivedJsonSchema = JSONSchema.of(SchemaDefinition.<DerivedFoo>builder().withPojo(DerivedFoo.class).build());
        Assert.assertEquals(derivedJsonSchema.decode(derivedJsonSchema.encode(derivedFoo)), derivedFoo);
        Assert.assertEquals(derivedJsonSchema.decode(derivedJsonSchema.encode(derivedDerivedFoo)), derivedFoo);

        //schema for derived derived class
        JSONSchema<SchemaTestUtils.DerivedDerivedFoo> derivedDerivedJsonSchema
                = JSONSchema.of(SchemaDefinition.<SchemaTestUtils.DerivedDerivedFoo>builder().withPojo(SchemaTestUtils.DerivedDerivedFoo.class).build());
        Assert.assertEquals(derivedDerivedJsonSchema.decode(derivedDerivedJsonSchema.encode(derivedDerivedFoo)), derivedDerivedFoo);
    }

    @Test(expectedExceptions = SchemaSerializationException.class)
    public void testAllowNullDecodeWithInvalidContent() {
        JSONSchema<Foo> jsonSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        jsonSchema.decode(new byte[0]);
    }

    @Test
    public void testAllowNullCorrectPolymorphism() {
        Bar bar = new Bar();
        bar.setField1(true);

        DerivedFoo derivedFoo = new DerivedFoo();
        derivedFoo.setField1("foo1");
        derivedFoo.setField2("bar2");
        derivedFoo.setField3(4);
        derivedFoo.setField4(bar);
        derivedFoo.setField5("derived1");
        derivedFoo.setField6(2);

        Foo foo = new Foo();
        foo.setField1("foo1");
        foo.setField2("bar2");
        foo.setField3(4);
        foo.setField4(bar);

        SchemaTestUtils.DerivedDerivedFoo derivedDerivedFoo = new SchemaTestUtils.DerivedDerivedFoo();
        derivedDerivedFoo.setField1("foo1");
        derivedDerivedFoo.setField2("bar2");
        derivedDerivedFoo.setField3(4);
        derivedDerivedFoo.setField4(bar);
        derivedDerivedFoo.setField5("derived1");
        derivedDerivedFoo.setField6(2);
        derivedDerivedFoo.setFoo2(foo);
        derivedDerivedFoo.setDerivedFoo(derivedFoo);

        // schema for base class
        JSONSchema<Foo> baseJsonSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());
        Assert.assertEquals(baseJsonSchema.decode(baseJsonSchema.encode(foo)), foo);
        Assert.assertEquals(baseJsonSchema.decode(baseJsonSchema.encode(derivedFoo)), foo);
        Assert.assertEquals(baseJsonSchema.decode(baseJsonSchema.encode(derivedDerivedFoo)), foo);

        // schema for derived class
        JSONSchema<DerivedFoo> derivedJsonSchema = JSONSchema.of(SchemaDefinition.<DerivedFoo>builder().withPojo(DerivedFoo.class).withAlwaysAllowNull(false).build());
        Assert.assertEquals(derivedJsonSchema.decode(derivedJsonSchema.encode(derivedFoo)), derivedFoo);
        Assert.assertEquals(derivedJsonSchema.decode(derivedJsonSchema.encode(derivedDerivedFoo)), derivedFoo);

        //schema for derived derived class
        JSONSchema<SchemaTestUtils.DerivedDerivedFoo> derivedDerivedJsonSchema
                = JSONSchema.of(SchemaDefinition.<SchemaTestUtils.DerivedDerivedFoo>builder().withPojo(SchemaTestUtils.DerivedDerivedFoo.class).withAlwaysAllowNull(false).build());
        Assert.assertEquals(derivedDerivedJsonSchema.decode(derivedDerivedJsonSchema.encode(derivedDerivedFoo)), derivedDerivedFoo);
    }

    @Test(expectedExceptions = SchemaSerializationException.class)
    public void testNotAllowNullDecodeWithInvalidContent() {
        JSONSchema<Foo> jsonSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());
        jsonSchema.decode(new byte[0]);
    }

    @Test
    public void testDecodeByteBuf() {
        JSONSchema<Foo> jsonSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());

        Foo foo1 = new Foo();
        foo1.setField1("foo1");
        foo1.setField2("bar1");
        foo1.setField4(new Bar());
        foo1.setFieldUnableNull("notNull");

        Foo foo2 = new Foo();
        foo2.setField1("foo2");
        foo2.setField2("bar2");

        byte[] bytes1 = jsonSchema.encode(foo1);
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(bytes1.length);
        byteBuf.writeBytes(bytes1);
        Assert.assertTrue(bytes1.length > 0);
        assertEquals(jsonSchema.decode(byteBuf), foo1);

    }
}
