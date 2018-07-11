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
package org.apache.pulsar.client.schemas;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.avro.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JSONSchemaTest {
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
            ".schemas.JSONSchemaTest$\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",\"string\"]," +
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
        JSONSchema<Foo> jsonSchema = JSONSchema.of(Foo.class);
        Assert.assertEquals(jsonSchema.getSchemaInfo().getType(), SchemaType.JSON);
        Schema.Parser parser = new Schema.Parser();
        String schemaJson = new String(jsonSchema.getSchemaInfo().getSchema());
        Assert.assertEquals(schemaJson, SCHEMA_JSON);
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
        JSONSchema<Foo> jsonSchema = JSONSchema.of(Foo.class, null);

        Foo foo1 = new Foo();
        foo1.setField1("foo1");
        foo1.setField2("bar1");
        foo1.setField4(new Bar());

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
}
