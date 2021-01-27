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

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;

import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

import java.util.Collection;

import static org.testng.Assert.*;

/**
 * Unit testing generic schemas.
 */
@Slf4j
public class SchemaFileTypeConversionTest {

    @Data
    private static final class Nested {
        private int value;
    }

    @Data
    private static final class MyStruct {
        private String theString;
        private boolean theBool;
        private int theInt;
        private long theLong;
        private double theDouble;
        private float theFloat;
        private byte[] theByteArray;
        private Nested nested;
    }

    @Test
    public void testGenericAvroSchema() {
        Schema<MyStruct> encodeSchema = Schema.AVRO(MyStruct.class);
        GenericSchema decodeSchema = GenericAvroSchema.of(encodeSchema.getSchemaInfo());
        testSchema(decodeSchema, SchemaType.AVRO);
    }

    @Test
    public void testGenericJsonSchema() {
        Schema<MyStruct> encodeSchema = Schema.JSON(MyStruct.class);
        GenericSchema decodeSchema = GenericJsonSchema.of(encodeSchema.getSchemaInfo());
        testSchema(decodeSchema, SchemaType.JSON);
    }

    private void testSchema(GenericSchema decodeSchema, SchemaType schemaTypeForStructs) {
        findField(decodeSchema.getFields(), "theString", SchemaType.STRING);
        findField(decodeSchema.getFields(), "theBool", SchemaType.BOOLEAN);
        findField(decodeSchema.getFields(), "theInt", SchemaType.INT32);
        findField(decodeSchema.getFields(), "theLong", SchemaType.INT64);
        findField(decodeSchema.getFields(), "theDouble", SchemaType.DOUBLE);
        findField(decodeSchema.getFields(), "theFloat", SchemaType.FLOAT);
        findField(decodeSchema.getFields(), "theByteArray", SchemaType.BYTES);
        Field nested = findField(decodeSchema.getFields(), "nested", schemaTypeForStructs);
        GenericSchema genericSchema = (GenericSchema) nested.getSchema();
        findField(genericSchema.getFields(), "value", SchemaType.INT32);
    }

    private Field findField(Collection<Field> fields, String name, SchemaType type) {
        for (Field f : fields) {
             if (f.getName().equals(name)) {
                SchemaType schemaType = f.getSchema().getSchemaInfo().getType();
                if (schemaType.equals(type)) {
                    return f;
                }
            }
        }
        fail("cannot find field "+name+" of type "+type);
        return null;
    }


}
