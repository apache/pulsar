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

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

/**
 * Schema Builder Test.
 */
public class SchemaBuilderTest {

    private static class AllOptionalFields {
        private Integer intField;
        private Long longField;
        private String stringField;
        private Boolean boolField;
        private Float floatField;
        private Double doubleField;
    }

    private static class AllPrimitiveFields {
        private int intField;
        private long longField;
        private boolean boolField;
        private float floatField;
        private double doubleField;
    }

    @Test
    public void testAllOptionalFieldsSchema() {
        RecordSchemaBuilder recordSchemaBuilder =
            SchemaBuilder.record("org.apache.pulsar.client.impl.schema.SchemaBuilderTest$.AllOptionalFields");
        recordSchemaBuilder.field("intField")
            .type(SchemaType.INT32).optional();
        recordSchemaBuilder.field("longField")
            .type(SchemaType.INT64).optional();
        recordSchemaBuilder.field("stringField")
            .type(SchemaType.STRING).optional();
        recordSchemaBuilder.field("boolField")
            .type(SchemaType.BOOLEAN).optional();
        recordSchemaBuilder.field("floatField")
            .type(SchemaType.FLOAT).optional();
        recordSchemaBuilder.field("doubleField")
            .type(SchemaType.DOUBLE).optional();
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
            SchemaBuilder.record("org.apache.pulsar.client.impl.schema.SchemaBuilderTest$.AllPrimitiveFields");
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
            SchemaBuilder.record("org.apache.pulsar.client.impl.schema.SchemaBuilderTest$.AllPrimitiveFields");
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
            SchemaBuilder.record("org.apache.pulsar.client.impl.schema.SchemaBuilderTest$.AllPrimitiveFields");
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
            .set(schema.getFields().get(0), 32)
            .set(schema.getFields().get(1), 1234L)
            .set(schema.getFields().get(2), true)
            .set(schema.getFields().get(3), 0.7f)
            .set(schema.getFields().get(4), 1.34d)
            .build();

        byte[] serializedData = schema.encode(record);

        // create a POJO schema to deserialize the serialized data
        Schema<AllPrimitiveFields> pojoSchema = Schema.AVRO(AllPrimitiveFields.class);
        AllPrimitiveFields fields = pojoSchema.decode(serializedData);

        assertEquals(32, fields.intField);
        assertEquals(1234L, fields.longField);
        assertEquals(true, fields.boolField);
        assertEquals(0.7f, fields.floatField);
        assertEquals(1.34d, fields.doubleField);
    }
}
