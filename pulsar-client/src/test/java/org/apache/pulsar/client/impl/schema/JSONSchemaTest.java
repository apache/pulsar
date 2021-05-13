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

import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.FOO_FIELDS;
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.SCHEMA_JSON_ALLOW_NULL;
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.SCHEMA_JSON_NOT_ALLOW_NULL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertSame;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Bar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.DerivedFoo;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.NestedBar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.NestedBarList;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.assertj.core.api.Assertions;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.Assert;
import org.testng.annotations.Test;

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

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Seller {
        public String state;
        public String street;
        public long zipCode;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class PC {
        public String brand;
        public String model;
        public int year;
        public GPU gpu;
        public Seller seller;
    }

    private enum GPU {
        AMD, NVIDIA
    }

    @Test
    public void testEncodeAndDecodeObject() throws JsonProcessingException {
        JSONSchema<PC> jsonSchema = JSONSchema.of(SchemaDefinition.<PC>builder().withPojo(PC.class).build());
        PC pc = new PC("dell", "alienware", 2021, GPU.AMD,
                new Seller("WA", "street", 98004));
        byte[] encoded = jsonSchema.encode(pc);
        PC roundtrippedPc = jsonSchema.decode(encoded);
        assertEquals(roundtrippedPc, pc);
    }

    @Test
    public void testGetNativeSchema() throws SchemaValidationException {
        JSONSchema<PC> schema2 = JSONSchema.of(PC.class);
        org.apache.avro.Schema avroSchema2 = (Schema) schema2.getNativeSchema().get();
        assertSame(schema2.schema, avroSchema2);
    }

    @Test
    public void testJsonGenericRecordBuilder() {
        JSONSchema<Seller> sellerJsonSchema = JSONSchema.of(Seller.class);

        RecordSchemaBuilder sellerSchemaBuilder = SchemaBuilder.record("seller");
        sellerSchemaBuilder.field("state").type(SchemaType.STRING);
        sellerSchemaBuilder.field("street").type(SchemaType.STRING);
        sellerSchemaBuilder.field("zipCode").type(SchemaType.INT64);
        SchemaInfo sellerSchemaInfo = sellerSchemaBuilder.build(SchemaType.JSON);
        GenericSchemaImpl sellerGenericSchema = GenericSchemaImpl.of(sellerSchemaInfo);

        JSONSchema<PC> pcJsonSchema = JSONSchema.of(PC.class);

        RecordSchemaBuilder pcSchemaBuilder = SchemaBuilder.record("pc");
        pcSchemaBuilder.field("brand").type(SchemaType.STRING);
        pcSchemaBuilder.field("model").type(SchemaType.STRING);
        pcSchemaBuilder.field("gpu").type(SchemaType.STRING);
        pcSchemaBuilder.field("year").type(SchemaType.INT64);
        pcSchemaBuilder.field("seller", sellerGenericSchema).type(SchemaType.JSON).optional();
        SchemaInfo pcGenericSchemaInfo = pcSchemaBuilder.build(SchemaType.JSON);
        GenericSchemaImpl pcGenericSchema = GenericSchemaImpl.of(pcGenericSchemaInfo);

        Seller seller = new Seller("USA","oakstreet",9999);
        PC pc = new PC("dell","g3",2020, GPU.AMD, seller);

        byte[] bytes = pcJsonSchema.encode(pc);
        Assert.assertTrue(bytes.length > 0);

        Object pc2 = pcJsonSchema.decode(bytes);
        assertEquals(pc, pc2);

        GenericRecord sellerRecord = sellerGenericSchema.newRecordBuilder()
                .set("state", "USA")
                .set("street", "oakstreet")
                .set("zipCode", 9999)
                .build();

        GenericRecord pcRecord = pcGenericSchema.newRecordBuilder()
                .set("brand", "dell")
                .set("model","g3")
                .set("year", 2020)
                .set("gpu", GPU.AMD)
                .set("seller", sellerRecord)
                .build();

        byte[] bytes3 = pcGenericSchema.encode(pcRecord);
        Assert.assertTrue(bytes3.length > 0);
        GenericRecord pc3Record = pcGenericSchema.decode(bytes3);

        for(Field field : pc3Record.getFields()) {
            assertTrue(pcGenericSchema.getFields().contains(field));
        }
        assertEquals("dell", pc3Record.getField("brand"));
        assertEquals("g3", pc3Record.getField("model"));
        assertEquals(2020, pc3Record.getField("year"));
        assertEquals(GPU.AMD.toString(), pc3Record.getField("gpu"));


        GenericRecord seller3Record = (GenericRecord) pc3Record.getField("seller");
        assertEquals("USA", seller3Record.getField("state"));
        assertEquals("oakstreet", seller3Record.getField("street"));
        assertEquals(9999, seller3Record.getField("zipCode"));

        assertTrue(pc3Record instanceof GenericJsonRecord);
        Assertions.assertThatCode(() -> pc3Record.getField("I_DO_NOT_EXIST")).doesNotThrowAnyException();
    }
}
