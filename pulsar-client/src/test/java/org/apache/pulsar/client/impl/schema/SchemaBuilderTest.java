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
import static org.testng.Assert.assertEquals;
import lombok.Data;
import org.apache.avro.reflect.Nullable;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.reader.MultiVersionAvroReader;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

/**
 * Schema Builder Test.
 */
public class SchemaBuilderTest {

    private static class AllOptionalFields {
        @Nullable
        private Integer intField;
        @Nullable
        private Long longField;
        @Nullable
        private String stringField;
        @Nullable
        private Boolean boolField;
        @Nullable
        private Float floatField;
        @Nullable
        private Double doubleField;
    }

    private static class AllPrimitiveFields {
        private int intField;
        private long longField;
        private boolean boolField;
        private float floatField;
        private double doubleField;
    }

    @Data
    private static class People {
        private People1 people1;
        private People2 people2;
        private String name;
    }

    @Data
    private static class People1 {
        private int age;
        private int height;
        private String name;
    }

    @Data
    private static class People2 {
        private int age;
        private int height;
        private String name;
    }

    @Test
    public void testAllOptionalFieldsSchema() {
        RecordSchemaBuilder recordSchemaBuilder =
            SchemaBuilder.record("org.apache.pulsar.client.impl.schema.SchemaBuilderTest.AllOptionalFields");
        recordSchemaBuilder.field("boolField")
                .type(SchemaType.BOOLEAN).optional();
        recordSchemaBuilder.field("doubleField")
                .type(SchemaType.DOUBLE).optional();
        recordSchemaBuilder.field("floatField")
                .type(SchemaType.FLOAT).optional();
        recordSchemaBuilder.field("intField")
            .type(SchemaType.INT32).optional();
        recordSchemaBuilder.field("longField")
            .type(SchemaType.INT64).optional();
        recordSchemaBuilder.field("stringField")
            .type(SchemaType.STRING).optional();
        SchemaInfo schemaInfo = recordSchemaBuilder.build(
            SchemaType.AVRO
        );

        Schema<AllOptionalFields> pojoSchema = Schema.AVRO(AllOptionalFields.class);
        SchemaInfo pojoSchemaInfo = pojoSchema.getSchemaInfo();

        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(
            new String(schemaInfo.getSchema(), UTF_8)
        );
        org.apache.avro.Schema avroPojoSchema = new org.apache.avro.Schema.Parser().parse(
            new String(pojoSchemaInfo.getSchema(), UTF_8)
        );

        assertEquals(avroPojoSchema, avroSchema);
    }

    @Test
    public void testAllPrimitiveFieldsSchema() {
        RecordSchemaBuilder recordSchemaBuilder =
            SchemaBuilder.record("org.apache.pulsar.client.impl.schema.SchemaBuilderTest.AllPrimitiveFields");
        recordSchemaBuilder.field("boolField")
                .type(SchemaType.BOOLEAN);
        recordSchemaBuilder.field("doubleField")
                .type(SchemaType.DOUBLE);
        recordSchemaBuilder.field("floatField")
                .type(SchemaType.FLOAT);
        recordSchemaBuilder.field("intField")
            .type(SchemaType.INT32);
        recordSchemaBuilder.field("longField")
            .type(SchemaType.INT64);
        SchemaInfo schemaInfo = recordSchemaBuilder.build(
            SchemaType.AVRO
        );

        Schema<AllPrimitiveFields> pojoSchema = Schema.AVRO(AllPrimitiveFields.class);
        SchemaInfo pojoSchemaInfo = pojoSchema.getSchemaInfo();

        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(
            new String(schemaInfo.getSchema(), UTF_8)
        );
        org.apache.avro.Schema avroPojoSchema = new org.apache.avro.Schema.Parser().parse(
            new String(pojoSchemaInfo.getSchema(), UTF_8)
        );

        assertEquals(avroPojoSchema, avroSchema);
    }

    @Test
    public void testGenericRecordBuilderByFieldName() {
        RecordSchemaBuilder recordSchemaBuilder =
            SchemaBuilder.record("org.apache.pulsar.client.impl.schema.SchemaBuilderTest.AllPrimitiveFields");
        recordSchemaBuilder.field("intField")
            .type(SchemaType.INT32);
        recordSchemaBuilder.field("longField")
            .type(SchemaType.INT64);
        recordSchemaBuilder.field("boolField")
            .type(SchemaType.BOOLEAN);
        recordSchemaBuilder.field("floatField")
            .type(SchemaType.FLOAT);
        recordSchemaBuilder.field("doubleField")
            .type(SchemaType.DOUBLE);
        SchemaInfo schemaInfo = recordSchemaBuilder.build(
            SchemaType.AVRO
        );
        GenericSchema schema = Schema.generic(schemaInfo);
        GenericRecord record = schema.newRecordBuilder()
            .set("intField", 32)
            .set("longField", 1234L)
            .set("boolField", true)
            .set("floatField", 0.7f)
            .set("doubleField", 1.34d)
            .build();

        byte[] serializedData = schema.encode(record);

        // create a POJO schema to deserialize the serialized data
        Schema<AllPrimitiveFields> pojoSchema = Schema.AVRO(AllPrimitiveFields.class);

        injectWriterSchema(pojoSchema, schema);

        AllPrimitiveFields fields = pojoSchema.decode(serializedData);
        assertEquals(32, fields.intField);
        assertEquals(1234L, fields.longField);
        assertEquals(true, fields.boolField);
        assertEquals(0.7f, fields.floatField);
        assertEquals(1.34d, fields.doubleField);
    }

    @Test
    public void testGenericRecordBuilderByIndex() {
        RecordSchemaBuilder recordSchemaBuilder =
            SchemaBuilder.record("org.apache.pulsar.client.impl.schema.SchemaBuilderTest.AllPrimitiveFields");
        recordSchemaBuilder.field("intField")
            .type(SchemaType.INT32);
        recordSchemaBuilder.field("longField")
            .type(SchemaType.INT64);
        recordSchemaBuilder.field("boolField")
            .type(SchemaType.BOOLEAN);
        recordSchemaBuilder.field("floatField")
            .type(SchemaType.FLOAT);
        recordSchemaBuilder.field("doubleField")
            .type(SchemaType.DOUBLE);
        SchemaInfo schemaInfo = recordSchemaBuilder.build(
            SchemaType.AVRO
        );
        GenericSchema<GenericRecord> schema = Schema.generic(schemaInfo);
        GenericRecord record = schema.newRecordBuilder()
            .set(schema.getFields().get(0), 32)
            .set(schema.getFields().get(1), 1234L)
            .set(schema.getFields().get(2), true)
            .set(schema.getFields().get(3), 0.7f)
            .set(schema.getFields().get(4), 1.34d)
            .build();
        byte[] serializedData = schema.encode(record);

        // create a POJO schema to deserialize the serialized data
        Schema<AllPrimitiveFields> pojoSchema = Schema.AVRO(AllPrimitiveFields.class);

        injectWriterSchema(pojoSchema, schema);

        AllPrimitiveFields fields = pojoSchema.decode(serializedData);

        assertEquals(32, fields.intField);
        assertEquals(1234L, fields.longField);
        assertEquals(true, fields.boolField);
        assertEquals(0.7f, fields.floatField);
        assertEquals(1.34d, fields.doubleField);
    }

    @Test
    public void testGenericRecordBuilderAvroByFieldname() {
        RecordSchemaBuilder people1SchemaBuilder = SchemaBuilder.record(People1.class.getCanonicalName());
        people1SchemaBuilder.field("age").type(SchemaType.INT32);
        people1SchemaBuilder.field("height").type(SchemaType.INT32);
        people1SchemaBuilder.field("name").type(SchemaType.STRING);


        SchemaInfo people1SchemaInfo = people1SchemaBuilder.build(SchemaType.AVRO);
        GenericSchema people1Schema = Schema.generic(people1SchemaInfo);


        GenericRecordBuilder people1RecordBuilder = people1Schema.newRecordBuilder();
        people1RecordBuilder.set("age", 20);
        people1RecordBuilder.set("height", 180);
        people1RecordBuilder.set("name", "people1");
        GenericRecord people1GenericRecord = people1RecordBuilder.build();

        RecordSchemaBuilder people2SchemaBuilder = SchemaBuilder.record(People2.class.getCanonicalName());
        people2SchemaBuilder.field("age").type(SchemaType.INT32);
        people2SchemaBuilder.field("height").type(SchemaType.INT32);
        people2SchemaBuilder.field("name").type(SchemaType.STRING);

        SchemaInfo people2SchemaInfo = people2SchemaBuilder.build(SchemaType.AVRO);
        GenericSchema people2Schema = Schema.generic(people2SchemaInfo);

        GenericRecordBuilder people2RecordBuilder = people2Schema.newRecordBuilder();
        people2RecordBuilder.set("age", 20);
        people2RecordBuilder.set("height", 180);
        people2RecordBuilder.set("name", "people2");
        GenericRecord people2GenericRecord = people2RecordBuilder.build();

        RecordSchemaBuilder peopleSchemaBuilder = SchemaBuilder.record(People.class.getCanonicalName());
        peopleSchemaBuilder.field("people1", people1Schema).type(SchemaType.AVRO);
        peopleSchemaBuilder.field("people2", people2Schema).type(SchemaType.AVRO);
        peopleSchemaBuilder.field("name").type(SchemaType.STRING);


        SchemaInfo schemaInfo = peopleSchemaBuilder.build(SchemaType.AVRO);

        GenericSchema peopleSchema = Schema.generic(schemaInfo);
        GenericRecordBuilder peopleRecordBuilder = peopleSchema.newRecordBuilder();
        peopleRecordBuilder.set("people1", people1GenericRecord);
        peopleRecordBuilder.set("people2", people2GenericRecord);
        peopleRecordBuilder.set("name", "people");
        GenericRecord peopleRecord = peopleRecordBuilder.build();

        byte[] peopleEncode = peopleSchema.encode(peopleRecord);

        GenericRecord people = (GenericRecord) peopleSchema.decode(peopleEncode);

        assertEquals(people.getFields(), peopleRecord.getFields());
        assertEquals((people.getField("name")), peopleRecord.getField("name"));
        assertEquals(((GenericRecord)people.getField("people1")).getField("age"),
                people1GenericRecord.getField("age"));
        assertEquals(((GenericRecord)people.getField("people1")).getField("height"),
                people1GenericRecord.getField("height"));
        assertEquals(((GenericRecord)people.getField("people1")).getField("name"),
                people1GenericRecord.getField("name"));
        assertEquals(((GenericRecord)people.getField("people2")).getField("age"),
                people2GenericRecord.getField("age"));
        assertEquals(((GenericRecord)people.getField("people2")).getField("height"),
                people2GenericRecord.getField("height"));
        assertEquals(((GenericRecord)people.getField("people2")).getField("name"),
                people2GenericRecord.getField("name"));

    }

    @Test
    public void testGenericRecordBuilderAvroByFieldnamePojo() {
        RecordSchemaBuilder people1SchemaBuilder = SchemaBuilder.record(People1.class.getCanonicalName());
        people1SchemaBuilder.field("age").type(SchemaType.INT32);
        people1SchemaBuilder.field("height").type(SchemaType.INT32);
        people1SchemaBuilder.field("name").type(SchemaType.STRING);


        SchemaInfo people1SchemaInfo = people1SchemaBuilder.build(SchemaType.AVRO);
        GenericSchema people1Schema = Schema.generic(people1SchemaInfo);


        GenericRecordBuilder people1RecordBuilder = people1Schema.newRecordBuilder();
        people1RecordBuilder.set("age", 20);
        people1RecordBuilder.set("height", 180);
        people1RecordBuilder.set("name", "people1");
        GenericRecord people1GenericRecord = people1RecordBuilder.build();

        RecordSchemaBuilder people2SchemaBuilder = SchemaBuilder.record(People2.class.getCanonicalName());
        people2SchemaBuilder.field("age").type(SchemaType.INT32);
        people2SchemaBuilder.field("height").type(SchemaType.INT32);
        people2SchemaBuilder.field("name").type(SchemaType.STRING);

        SchemaInfo people2SchemaInfo = people2SchemaBuilder.build(SchemaType.AVRO);
        GenericSchema people2Schema = Schema.generic(people2SchemaInfo);

        GenericRecordBuilder people2RecordBuilder = people2Schema.newRecordBuilder();
        people2RecordBuilder.set("age", 20);
        people2RecordBuilder.set("height", 180);
        people2RecordBuilder.set("name", "people2");
        GenericRecord people2GenericRecord = people2RecordBuilder.build();

        RecordSchemaBuilder peopleSchemaBuilder = SchemaBuilder.record(People.class.getCanonicalName());
        peopleSchemaBuilder.field("people1", people1Schema).type(SchemaType.AVRO);
        peopleSchemaBuilder.field("people2", people2Schema).type(SchemaType.AVRO);
        peopleSchemaBuilder.field("name").type(SchemaType.STRING);


        SchemaInfo schemaInfo = peopleSchemaBuilder.build(SchemaType.AVRO);

        GenericSchema peopleSchema = Schema.generic(schemaInfo);
        GenericRecordBuilder peopleRecordBuilder = peopleSchema.newRecordBuilder();
        peopleRecordBuilder.set("people1", people1GenericRecord);
        peopleRecordBuilder.set("people2", people2GenericRecord);
        peopleRecordBuilder.set("name", "people");
        GenericRecord peopleRecord = peopleRecordBuilder.build();

        byte[] peopleEncode = peopleSchema.encode(peopleRecord);

        Schema<People> peopleDecodeSchema = Schema.AVRO(
                SchemaDefinition.<People>builder().withPojo(People.class).withAlwaysAllowNull(false).build());

        injectWriterSchema(peopleDecodeSchema, peopleSchema);

        People people = peopleDecodeSchema.decode(peopleEncode);

        assertEquals(people.name, peopleRecord.getField("name"));
        assertEquals(people.getPeople1().age, people1GenericRecord.getField("age"));
        assertEquals(people.getPeople1().height, people1GenericRecord.getField("height"));
        assertEquals(people.getPeople1().name, people1GenericRecord.getField("name"));
        assertEquals(people.getPeople2().age, people2GenericRecord.getField("age"));
        assertEquals(people.getPeople2().height, people2GenericRecord.getField("height"));
        assertEquals(people.getPeople2().name, people2GenericRecord.getField("name"));

    }

    @Test
    public void testGenericRecordBuilderAvroByFieldIndex() {
        RecordSchemaBuilder people1SchemaBuilder = SchemaBuilder.record(People1.class.getCanonicalName());
        people1SchemaBuilder.field("age").type(SchemaType.INT32);
        people1SchemaBuilder.field("height").type(SchemaType.INT32);
        people1SchemaBuilder.field("name").type(SchemaType.STRING);


        SchemaInfo people1SchemaInfo = people1SchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> people1Schema = Schema.generic(people1SchemaInfo);


        GenericRecordBuilder people1RecordBuilder = people1Schema.newRecordBuilder();
        people1RecordBuilder.set(people1Schema.getFields().get(0), 20);
        people1RecordBuilder.set(people1Schema.getFields().get(1), 180);
        people1RecordBuilder.set(people1Schema.getFields().get(2), "people1");
        GenericRecord people1GenericRecord = people1RecordBuilder.build();

        RecordSchemaBuilder people2SchemaBuilder = SchemaBuilder.record(People2.class.getCanonicalName());
        people2SchemaBuilder.field("age").type(SchemaType.INT32);
        people2SchemaBuilder.field("height").type(SchemaType.INT32);
        people2SchemaBuilder.field("name").type(SchemaType.STRING);

        SchemaInfo people2SchemaInfo = people2SchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> people2Schema = Schema.generic(people2SchemaInfo);

        GenericRecordBuilder people2RecordBuilder = people2Schema.newRecordBuilder();
        people2RecordBuilder.set(people2Schema.getFields().get(0), 20);
        people2RecordBuilder.set(people2Schema.getFields().get(1), 180);
        people2RecordBuilder.set(people2Schema.getFields().get(2), "people2");
        GenericRecord people2GenericRecord = people2RecordBuilder.build();

        RecordSchemaBuilder peopleSchemaBuilder = SchemaBuilder.record(People.class.getCanonicalName());
        peopleSchemaBuilder.field("people1", people1Schema).type(SchemaType.AVRO);
        peopleSchemaBuilder.field("people2", people2Schema).type(SchemaType.AVRO);
        peopleSchemaBuilder.field("name").type(SchemaType.STRING);


        SchemaInfo schemaInfo = peopleSchemaBuilder.build(SchemaType.AVRO);

        GenericSchema<GenericRecord> peopleSchema = Schema.generic(schemaInfo);
        GenericRecordBuilder peopleRecordBuilder = peopleSchema.newRecordBuilder();
        peopleRecordBuilder.set(peopleSchema.getFields().get(0), people1GenericRecord);
        peopleRecordBuilder.set(peopleSchema.getFields().get(1), people2GenericRecord);
        peopleRecordBuilder.set(peopleSchema.getFields().get(2), "people");
        GenericRecord peopleRecord = peopleRecordBuilder.build();

        byte[] peopleEncode = peopleSchema.encode(peopleRecord);

        GenericRecord people = (GenericRecord) peopleSchema.decode(peopleEncode);

        assertEquals(people.getFields(), peopleRecord.getFields());
        assertEquals((people.getField("name")), peopleRecord.getField("name"));
        assertEquals(((GenericRecord)people.getField("people1")).getField("age"),
                people1GenericRecord.getField("age"));
        assertEquals(((GenericRecord)people.getField("people1")).getField("height"),
                people1GenericRecord.getField("height"));
        assertEquals(((GenericRecord)people.getField("people1")).getField("name"),
                people1GenericRecord.getField("name"));
        assertEquals(((GenericRecord)people.getField("people2")).getField("age"),
                people2GenericRecord.getField("age"));
        assertEquals(((GenericRecord)people.getField("people2")).getField("height"),
                people2GenericRecord.getField("height"));
        assertEquals(((GenericRecord)people.getField("people2")).getField("name"),
                people2GenericRecord.getField("name"));

    }

    @Test
    public void testGenericRecordBuilderAvroByFieldIndexPojo() {
        RecordSchemaBuilder people1SchemaBuilder = SchemaBuilder.record(People1.class.getCanonicalName());
        people1SchemaBuilder.field("age").type(SchemaType.INT32);
        people1SchemaBuilder.field("height").type(SchemaType.INT32);
        people1SchemaBuilder.field("name").type(SchemaType.STRING);


        SchemaInfo people1SchemaInfo = people1SchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> people1Schema = Schema.generic(people1SchemaInfo);


        GenericRecordBuilder people1RecordBuilder = people1Schema.newRecordBuilder();
        people1RecordBuilder.set(people1Schema.getFields().get(0), 20);
        people1RecordBuilder.set(people1Schema.getFields().get(1), 180);
        people1RecordBuilder.set(people1Schema.getFields().get(2), "people1");
        GenericRecord people1GenericRecord = people1RecordBuilder.build();

        RecordSchemaBuilder people2SchemaBuilder = SchemaBuilder.record(People2.class.getCanonicalName());
        people2SchemaBuilder.field("age").type(SchemaType.INT32);
        people2SchemaBuilder.field("height").type(SchemaType.INT32);
        people2SchemaBuilder.field("name").type(SchemaType.STRING);

        SchemaInfo people2SchemaInfo = people2SchemaBuilder.build(SchemaType.AVRO);
        GenericSchema<GenericRecord> people2Schema = Schema.generic(people2SchemaInfo);

        GenericRecordBuilder people2RecordBuilder = people2Schema.newRecordBuilder();
        people2RecordBuilder.set(people2Schema.getFields().get(0), 20);
        people2RecordBuilder.set(people2Schema.getFields().get(1), 180);
        people2RecordBuilder.set(people2Schema.getFields().get(2), "people2");
        GenericRecord people2GenericRecord = people2RecordBuilder.build();

        RecordSchemaBuilder peopleSchemaBuilder = SchemaBuilder.record(People.class.getCanonicalName());
        peopleSchemaBuilder.field("people1", people1Schema).type(SchemaType.AVRO);
        peopleSchemaBuilder.field("people2", people2Schema).type(SchemaType.AVRO);
        peopleSchemaBuilder.field("name").type(SchemaType.STRING);


        SchemaInfo schemaInfo = peopleSchemaBuilder.build(SchemaType.AVRO);

        GenericSchema<GenericRecord> peopleSchema = Schema.generic(schemaInfo);
        GenericRecordBuilder peopleRecordBuilder = peopleSchema.newRecordBuilder();
        peopleRecordBuilder.set(peopleSchema.getFields().get(0), people1GenericRecord);
        peopleRecordBuilder.set(peopleSchema.getFields().get(1), people2GenericRecord);
        peopleRecordBuilder.set(peopleSchema.getFields().get(2), "people");
        GenericRecord peopleRecord = peopleRecordBuilder.build();

        byte[] peopleEncode = peopleSchema.encode(peopleRecord);

        Schema<People> peopleDecodeSchema = Schema.AVRO(
                SchemaDefinition.<People>builder().withPojo(People.class).withAlwaysAllowNull(false).build());

        injectWriterSchema(peopleDecodeSchema, peopleSchema);

        People people = peopleDecodeSchema.decode(peopleEncode);

        assertEquals(people.name, peopleRecord.getField("name"));
        assertEquals(people.getPeople1().age, people1GenericRecord.getField("age"));
        assertEquals(people.getPeople1().height, people1GenericRecord.getField("height"));
        assertEquals(people.getPeople1().name, people1GenericRecord.getField("name"));
        assertEquals(people.getPeople2().age, people2GenericRecord.getField("age"));
        assertEquals(people.getPeople2().height, people2GenericRecord.getField("height"));
        assertEquals(people.getPeople2().name, people2GenericRecord.getField("name"));
    }

    /**
     * Set the schema used to encode the data into the schema that we are using to read.
     * Avro is able to decode successfully the record and apply the contents to the requested
     * schema
     * @param decoder the schema used for reading
     * @param writer the schema used for writing
     */
    private static final void injectWriterSchema(Schema decoder, Schema writer) {
        AvroSchema<?> avroSchema = (AvroSchema<?>) decoder;
        avroSchema.setReader(new MultiVersionAvroReader<>(
                AvroSchema.of(SchemaDefinition.
                        builder()
                        .withJsonDef(writer.getSchemaInfo().getSchemaDefinition())
                        .build())
                        .getAvroSchema(),
                Thread.currentThread().getContextClassLoader(),
                false));
    }
}
