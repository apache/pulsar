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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.FOO_FIELDS;
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.SCHEMA_AVRO_NOT_ALLOW_NULL;
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.SCHEMA_AVRO_ALLOW_NULL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

import java.util.Arrays;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;

import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Bar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;


@Slf4j
public class AvroSchemaTest {

    @Data
    private static class DefaultStruct {
        int field1;
        String field2;
        Long field3;
    }

    @Data
    private static class StructWithAnnotations {
        int field1;
        @Nullable
        String field2;
        @AvroDefault("\"1000\"")
        Long field3;
    }

    @Test
    public void testSchemaDefinition() throws SchemaValidationException {
        org.apache.avro.Schema schema1 = ReflectData.get().getSchema(DefaultStruct.class);
        AvroSchema<StructWithAnnotations> schema2 = AvroSchema.of(StructWithAnnotations.class);

        String schemaDef1 = schema1.toString();
        String schemaDef2 = new String(schema2.getSchemaInfo().getSchema(), UTF_8);
        assertNotEquals(
            schemaDef1, schemaDef2,
            "schema1 = " + schemaDef1 + ", schema2 = " + schemaDef2);

        SchemaValidator validator = new SchemaValidatorBuilder()
            .mutualReadStrategy()
            .validateLatest();
        try {
            validator.validate(
                schema1,
                Arrays.asList(
                    new Schema.Parser().parse(schemaDef2)
                )
            );
            fail("Should fail on validating incompatible schemas");
        } catch (SchemaValidationException sve) {
            // expected
        }

        AvroSchema<StructWithAnnotations> schema3 = AvroSchema.of(SchemaDefinition.<StructWithAnnotations>builder().withJsonDef(schemaDef1).build());
        String schemaDef3 = new String(schema3.getSchemaInfo().getSchema(), UTF_8);
        assertEquals(schemaDef1, schemaDef3);
        assertNotEquals(schemaDef2, schemaDef3);

        StructWithAnnotations struct = new StructWithAnnotations();
        struct.setField1(5678);
        // schema2 is using the schema generated from POJO,
        // it allows field2 to be nullable, and field3 has default value.
        schema2.encode(struct);
        try {
            // schema3 is using the schema passed in, which doesn't allow nullable
            schema3.encode(struct);
            fail("Should fail to write the record since the provided schema is incompatible");
        } catch (SchemaSerializationException sse) {
            // expected
        }
    }

    @Test
    public void testNotAllowNullSchema() {
        AvroSchema<Foo> avroSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());
        assertEquals(avroSchema.getSchemaInfo().getType(), SchemaType.AVRO);
        Schema.Parser parser = new Schema.Parser();
        String schemaJson = new String(avroSchema.getSchemaInfo().getSchema());
        assertEquals(schemaJson, SCHEMA_AVRO_NOT_ALLOW_NULL);
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
    public void testAllowNullSchema() {
        AvroSchema<Foo> avroSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        assertEquals(avroSchema.getSchemaInfo().getType(), SchemaType.AVRO);
        Schema.Parser parser = new Schema.Parser();
        String schemaJson = new String(avroSchema.getSchemaInfo().getSchema());
        assertEquals(schemaJson, SCHEMA_AVRO_ALLOW_NULL);
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
    public void testNotAllowNullEncodeAndDecode() {
        AvroSchema<Foo> avroSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());

        Foo foo1 = new Foo();
        foo1.setField1("foo1");
        foo1.setField2("bar1");
        foo1.setField4(new Bar());
        foo1.setFieldUnableNull("notNull");

        Foo foo2 = new Foo();
        foo2.setField1("foo2");
        foo2.setField2("bar2");

        byte[] bytes1 = avroSchema.encode(foo1);
        Foo object1 = avroSchema.decode(bytes1);
        Assert.assertTrue(bytes1.length > 0);
        assertEquals(object1, foo1);

        try {

            avroSchema.encode(foo2);

        } catch (Exception e) {
            Assert.assertTrue(e instanceof SchemaSerializationException);
        }

    }

    @Test
    public void testAllowNullEncodeAndDecode() {
        AvroSchema<Foo> avroSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Foo foo1 = new Foo();
        foo1.setField1("foo1");
        foo1.setField2("bar1");
        foo1.setField4(new Bar());

        Foo foo2 = new Foo();
        foo2.setField1("foo2");
        foo2.setField2("bar2");

        byte[] bytes1 = avroSchema.encode(foo1);
        Assert.assertTrue(bytes1.length > 0);

        byte[] bytes2 = avroSchema.encode(foo2);
        Assert.assertTrue(bytes2.length > 0);

        Foo object1 = avroSchema.decode(bytes1);
        Foo object2 = avroSchema.decode(bytes2);

        assertEquals(object1, foo1);
        assertEquals(object2, foo2);

    }


}
