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
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.SCHEMA_AVRO_ALLOW_NULL;
import static org.apache.pulsar.client.impl.schema.SchemaTestUtils.SCHEMA_AVRO_NOT_ALLOW_NULL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertSame;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.UUID;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.BufferedBinaryEncoder;
import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.avro.generated.NasaMission;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Bar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.client.impl.schema.reader.JacksonJsonReader;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
import org.apache.pulsar.client.impl.schema.writer.JacksonJsonWriter;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.json.JSONException;
import org.powermock.reflect.Whitebox;
import org.skyscreamer.jsonassert.JSONAssert;
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

    @Data
    private static class SchemaLogicalType{
        @org.apache.avro.reflect.AvroSchema("{\n" +
                "  \"type\": \"bytes\",\n" +
                "  \"logicalType\": \"decimal\",\n" +
                "  \"precision\": 4,\n" +
                "  \"scale\": 2\n" +
                "}")
        BigDecimal decimal;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"int\",\"logicalType\":\"date\"}")
        LocalDate date;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}")
        Instant timestampMillis;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"int\",\"logicalType\":\"time-millis\"}")
        LocalTime timeMillis;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        long timestampMicros;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"time-micros\"}")
        long timeMicros;
    }

    @Data
    private static class JodaTimeLogicalType{
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"int\",\"logicalType\":\"date\"}")
        LocalDate date;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}")
        DateTime timestampMillis;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"int\",\"logicalType\":\"time-millis\"}")
        LocalTime timeMillis;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        long timestampMicros;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"time-micros\"}")
        long timeMicros;
    }

    @Test
    public void testGetNativeSchema() throws SchemaValidationException {
        AvroSchema<StructWithAnnotations> schema2 = AvroSchema.of(StructWithAnnotations.class);
        org.apache.avro.Schema avroSchema2 = (Schema) schema2.getNativeSchema().get();
        assertSame(schema2.schema, avroSchema2);
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
                    new Schema.Parser().setValidateDefaults(false).parse(schemaDef2)
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

    public void assertJSONEquals(String s1, String s2) throws JSONException {
        JSONAssert.assertEquals(s1, s2, false);
    }

    @Test
    public void testNotAllowNullSchema() throws JSONException {
        AvroSchema<Foo> avroSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).withAlwaysAllowNull(false).build());
        assertEquals(avroSchema.getSchemaInfo().getType(), SchemaType.AVRO);
        Schema.Parser parser = new Schema.Parser();
        String schemaJson = new String(avroSchema.getSchemaInfo().getSchema());
        assertJSONEquals(schemaJson, SCHEMA_AVRO_NOT_ALLOW_NULL);
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
        AvroSchema<Foo> avroSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        assertEquals(avroSchema.getSchemaInfo().getType(), SchemaType.AVRO);
        Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        String schemaJson = new String(avroSchema.getSchemaInfo().getSchema());
        assertJSONEquals(schemaJson, SCHEMA_AVRO_ALLOW_NULL);
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

    @Test
    public void testLogicalType() {
        AvroSchema<SchemaLogicalType> avroSchema = AvroSchema.of(SchemaDefinition.<SchemaLogicalType>builder()
                .withPojo(SchemaLogicalType.class).withJSR310ConversionEnabled(true).build());

        SchemaLogicalType schemaLogicalType = new SchemaLogicalType();
        schemaLogicalType.setTimestampMicros(System.currentTimeMillis()*1000);
        schemaLogicalType.setTimestampMillis(Instant.parse("2019-03-26T04:39:58.469Z"));
        schemaLogicalType.setDecimal(new BigDecimal("12.34"));
        schemaLogicalType.setDate(LocalDate.now());
        schemaLogicalType.setTimeMicros(System.currentTimeMillis()*1000);
        schemaLogicalType.setTimeMillis(LocalTime.now().truncatedTo(ChronoUnit.MILLIS));

        byte[] bytes1 = avroSchema.encode(schemaLogicalType);
        Assert.assertTrue(bytes1.length > 0);

        SchemaLogicalType object1 = avroSchema.decode(bytes1);

        assertEquals(object1, schemaLogicalType);

    }

    @Test
    public void testJodaTimeLogicalType() {
        AvroSchema<JodaTimeLogicalType> avroSchema = AvroSchema.of(SchemaDefinition.<JodaTimeLogicalType>builder()
                .withPojo(JodaTimeLogicalType.class).build());
        JodaTimeLogicalType schemaLogicalType = new JodaTimeLogicalType();
        schemaLogicalType.setTimestampMicros(System.currentTimeMillis()*1000);
        schemaLogicalType.setTimestampMillis(new DateTime("2019-03-26T04:39:58.469Z", ISOChronology.getInstanceUTC()));
        schemaLogicalType.setDate(LocalDate.now());
        schemaLogicalType.setTimeMicros(System.currentTimeMillis()*1000);
        schemaLogicalType.setTimeMillis(LocalTime.now().truncatedTo(ChronoUnit.MILLIS));

        byte[] bytes1 = avroSchema.encode(schemaLogicalType);
        Assert.assertTrue(bytes1.length > 0);

        JodaTimeLogicalType object1 = avroSchema.decode(bytes1);

        assertEquals(object1, schemaLogicalType);
    }

  @Test
  public void testDateAndTimestamp() {
    RecordSchemaBuilder recordSchemaBuilder =
        SchemaBuilder.record("org.apache.pulsar.client.avro.generated.NasaMission");
    recordSchemaBuilder.field("id")
        .type(SchemaType.INT32);
    recordSchemaBuilder.field("name")
        .type(SchemaType.STRING);
    recordSchemaBuilder.field("create_year")
        .type(SchemaType.DATE);
    recordSchemaBuilder.field("create_time")
        .type(SchemaType.TIME);
    recordSchemaBuilder.field("create_timestamp")
        .type(SchemaType.TIMESTAMP);
    SchemaInfo schemaInfo = recordSchemaBuilder.build(
        SchemaType.AVRO
    );

    org.apache.avro.Schema recordSchema = new org.apache.avro.Schema.Parser().parse(
        new String(schemaInfo.getSchema(), UTF_8)
    );
    AvroSchema<NasaMission> avroSchema = AvroSchema.of(SchemaDefinition.<NasaMission>builder().withPojo(NasaMission.class).build());
    assertEquals(recordSchema, avroSchema.schema);

    NasaMission nasaMission = NasaMission.newBuilder()
        .setId(1001)
        .setName("one")
        .setCreateYear(LocalDate.now())
        .setCreateTime(LocalTime.now())
        .setCreateTimestamp(Instant.now())
        .build();

    byte[] bytes = avroSchema.encode(nasaMission);
    Assert.assertTrue(bytes.length > 0);

    NasaMission object = avroSchema.decode(bytes);
    assertEquals(object, nasaMission);
  }

    @Test
    public void testDecodeByteBuf() {
        AvroSchema<Foo> avroSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

        Foo foo1 = new Foo();
        foo1.setField1("foo1");
        foo1.setField2("bar1");
        foo1.setField4(new Bar());
        foo1.setFieldUnableNull("notNull");

        Foo foo2 = new Foo();
        foo2.setField1("foo2");
        foo2.setField2("bar2");

        byte[] bytes1 = avroSchema.encode(foo1);
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(bytes1.length);
        byteBuf.writeBytes(bytes1);

        Foo object1 = avroSchema.decode(byteBuf);
        Assert.assertTrue(bytes1.length > 0);
        assertEquals(object1, foo1);
    }

    @Test
    public void discardBufferIfBadAvroData() {
        AvroWriter<NasaMission> avroWriter = new AvroWriter<>(
                ReflectData.AllowNull.get().getSchema(NasaMission.class));

        NasaMission badNasaMissionData = new NasaMission();
        badNasaMissionData.setId(1);
        // set null in the non-null field. The java set will accept it but going ahead, the avro encode will crash.
        badNasaMissionData.setName(null);

        // Because data does not conform to schema expect a crash
        Assert.assertThrows( SchemaSerializationException.class, () -> avroWriter.write(badNasaMissionData));

        // Get the buffered data using powermock
        BinaryEncoder encoder = Whitebox.getInternalState(avroWriter, "encoder");

        // Assert that the buffer position is reset to zero
        Assert.assertEquals(((BufferedBinaryEncoder)encoder).bytesBuffered(), 0);
    }

    @Test
    public void testAvroSchemaUserDefinedReadAndWriter() {
        SchemaReader<Foo> reader = new JacksonJsonReader<>(new ObjectMapper(), Foo.class);
        SchemaWriter<Foo> writer = new JacksonJsonWriter<>(new ObjectMapper());
        SchemaDefinition<Foo> schemaDefinition = SchemaDefinition.<Foo>builder()
                .withPojo(Bar.class)
                .withSchemaReader(reader)
                .withSchemaWriter(writer)
                .build();

        AvroSchema<Foo> schema = AvroSchema.of(schemaDefinition);
        Foo foo = new Foo();
        foo.setColor(SchemaTestUtils.Color.RED);
        String field1 = "test";
        foo.setField1(field1);
        schema.encode(foo);
        foo = schema.decode(schema.encode(foo));
        assertEquals(foo.getColor(), SchemaTestUtils.Color.RED);
        assertEquals(field1, foo.getField1());
    }

    static class MyPojo {
        public UUID uid;
    }

    @Test
    public void testAvroUUID() {
        org.apache.pulsar.client.api.Schema<MyPojo> schema = org.apache.pulsar.client.api.Schema.AVRO(MyPojo.class);
        MyPojo pojo1 = new MyPojo();
        pojo1.uid = UUID.randomUUID();
        MyPojo pojo2 = schema.decode(schema.encode(pojo1));
        assertEquals(pojo1.uid, pojo2.uid);
    }

}
