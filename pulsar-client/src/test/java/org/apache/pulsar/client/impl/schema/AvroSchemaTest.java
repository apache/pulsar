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
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.SCHEMA_JSON;
import static org.testng.Assert.assertEquals;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Bar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class AvroSchemaTest {

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

}
