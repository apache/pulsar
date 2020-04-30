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
package org.apache.pulsar.client.impl.schema.generic;

import static org.testng.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.FooV2;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class GenericAvroReaderTest {

    private Foo foo;
    private FooV2 fooV2;
    private AvroSchema fooSchemaNotNull;
    private AvroSchema fooSchema;
    private AvroSchema fooV2Schema;
    private AvroSchema fooOffsetSchema;

    @BeforeMethod
    public void setup() {
        fooSchema = AvroSchema.of(Foo.class);

        fooV2Schema = AvroSchema.of(FooV2.class);
        fooSchemaNotNull = AvroSchema.of(SchemaDefinition
                .builder()
                .withAlwaysAllowNull(false)
                .withPojo(Foo.class)
                .build());

        fooOffsetSchema = AvroSchema.of(Foo.class);
        fooOffsetSchema.getAvroSchema().addProp(GenericAvroSchema.OFFSET_PROP, 5);

        foo = new Foo();
        foo.setField1("foo1");
        foo.setField2("bar1");
        foo.setField4(new SchemaTestUtils.Bar());
        foo.setFieldUnableNull("notNull");

        fooV2 = new FooV2();
        fooV2.setField1("foo1");
        fooV2.setField3(10);
    }

    @Test
    public void testGenericAvroReaderByWriterSchema() {
        byte[] fooBytes = fooSchema.encode(foo);

        GenericAvroReader genericAvroSchemaByWriterSchema = new GenericAvroReader(fooSchema.getAvroSchema());
        GenericRecord genericRecordByWriterSchema =  genericAvroSchemaByWriterSchema.read(fooBytes);
        assertEquals(genericRecordByWriterSchema.getField("field1"), "foo1");
        assertEquals(genericRecordByWriterSchema.getField("field2"), "bar1");
        assertEquals(genericRecordByWriterSchema.getField("fieldUnableNull"), "notNull");
    }

    @Test
    public void testGenericAvroReaderByReaderSchema() {
        byte[] fooV2Bytes = fooV2Schema.encode(fooV2);

        GenericAvroReader genericAvroSchemaByReaderSchema = new GenericAvroReader(fooV2Schema.getAvroSchema(), fooSchemaNotNull.getAvroSchema(), new byte[10]);
        GenericRecord genericRecordByReaderSchema = genericAvroSchemaByReaderSchema.read(fooV2Bytes);
        assertEquals(genericRecordByReaderSchema.getField("fieldUnableNull"), "defaultValue");
        assertEquals(genericRecordByReaderSchema.getField("field1"), "foo1");
        assertEquals(genericRecordByReaderSchema.getField("field3"), 10);
    }

    @Test
    public void testOffsetSchema() {
        byte[] fooBytes = fooOffsetSchema.encode(foo);
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeByte(0);
        byteBuf.writeInt(10);
        byteBuf.writeBytes(fooBytes);

        GenericAvroReader reader = new GenericAvroReader(fooOffsetSchema.getAvroSchema());
        assertEquals(reader.getOffset(), 5);
        GenericRecord record = reader.read(byteBuf.array());
        assertEquals(record.getField("field1"), "foo1");
        assertEquals(record.getField("field2"), "bar1");
        assertEquals(record.getField("fieldUnableNull"), "notNull");
    }

}
