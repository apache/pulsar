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
package org.apache.pulsar.client.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class AvroSchemaTest {

    @Data
    @ToString
    @EqualsAndHashCode
    private static class Foo {
        private String field1;
        private String field2;
        private int field3;
        private Bar field4;
    }

    @Data
    @ToString
    @EqualsAndHashCode
    private static class Bar {
        private boolean field1;
    }

    private static final String SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"org.apache" +
            ".pulsar.client" +
            ".schema.AvroSchemaTest$\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",\"string\"]," +
            "\"default\":null},{\"name\":\"field2\",\"type\":[\"null\",\"string\"],\"default\":null}," +
            "{\"name\":\"field3\",\"type\":\"int\"},{\"name\":\"field4\",\"type\":[\"null\",{\"type\":\"record\"," +
            "\"name\":\"Bar\",\"fields\":[{\"name\":\"field1\",\"type\":\"boolean\"}]}],\"default\":null}]}";

    private static String[] FOO_FIELDS = {
            "field1",
            "field2",
            "field3",
            "field4"
    };

    @Test
    public void testSchema() {
        AvroSchema<Foo> avroSchema = AvroSchema.of(Foo.class);
        assertEquals(avroSchema.getSchemaInfo().getType(), SchemaType.AVRO);
        Schema.Parser parser = new Schema.Parser();
        String schemaJson = new String(avroSchema.getSchemaInfo().getSchema());
        assertEquals(schemaJson, SCHEMA_JSON);
        Schema schema = parser.parse(schemaJson);

        for (String fieldName : FOO_FIELDS) {
            Schema.Field field = schema.getField(fieldName);
            Assert.assertNotNull(field);

            if (field.name().equals("field4")) {
                Assert.assertNotNull(field.schema().getTypes().get(1).getField("field1"));
            }
        }
    }

    @Test
    public void testEncodeAndDecode() {
        AvroSchema<Foo> avroSchema = AvroSchema.of(Foo.class, null);

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

    @Test
    public void testEncodeAndDecodeGenericRecord() {
        AvroSchema<Foo> avroSchema = AvroSchema.of(Foo.class, null);

        GenericAvroSchema genericAvroSchema = new GenericAvroSchema(avroSchema.getSchemaInfo());

        log.info("Avro Schema : {}", genericAvroSchema.getAvroSchema());

        int numRecords = 10;
        for (int i = 0; i < numRecords; i++) {
            Foo foo = new Foo();
            foo.setField1("field-1-" + i);
            foo.setField2("field-2-" + i);
            foo.setField3(i);
            Bar bar = new Bar();
            bar.setField1(i % 2 == 0);
            foo.setField4(bar);

            byte[] data = avroSchema.encode(foo);

            GenericRecord record = genericAvroSchema.decode(data);
            Object field1 = record.getField("field1");
            assertEquals("field-1-" + i, field1, "Field 1 is " + field1.getClass());
            Object field2 = record.getField("field2");
            assertEquals("field-2-" + i, field2, "Field 2 is " + field2.getClass());
            Object field3 = record.getField("field3");
            assertEquals(i, field3, "Field 3 is " + field3.getClass());
            Object field4 = record.getField("field4");
            assertTrue(field4 instanceof GenericRecord);
            GenericRecord field4Record = (GenericRecord) field4;
            assertEquals(i % 2 == 0, field4Record.getField("field1"));
        }
    }
}
