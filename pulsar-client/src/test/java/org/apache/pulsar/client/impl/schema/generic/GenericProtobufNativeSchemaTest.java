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

import com.google.protobuf.Descriptors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.client.schema.proto.Test.TestMessage;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.testng.Assert.assertEquals;

@Slf4j
public class GenericProtobufNativeSchemaTest {

    private TestMessage message;
    private GenericRecord genericmessage;
    private GenericProtobufNativeSchema genericProtobufNativeSchema;
    private ProtobufNativeSchema<TestMessage> clazzBasedProtobufNativeSchema;

    @BeforeMethod
    public void init() {
        clazzBasedProtobufNativeSchema = ProtobufNativeSchema.of(SchemaDefinition.<TestMessage>builder()
                .withPojo(TestMessage.class).build());
        genericProtobufNativeSchema = (GenericProtobufNativeSchema) GenericProtobufNativeSchema.of(clazzBasedProtobufNativeSchema.getSchemaInfo());

    }

    private static void assertField(String name, Consumer<Descriptors.FieldDescriptor> checker, List<Field> fields) {
        Field field = fields.stream().filter(f -> f.getName().equals(name)).findFirst().get();
        Descriptors.FieldDescriptor avroField = field.unwrap(Descriptors.FieldDescriptor.class);
        checker.accept(avroField);
    }

    @Test
    public void testAllowUnwrapFieldSchema() {
        List<Field> fields = genericProtobufNativeSchema.getFields();
        Assert.assertEquals(7, fields.size());
        fields.forEach(f -> {
            Descriptors.FieldDescriptor unwrap = f.unwrap(Descriptors.FieldDescriptor.class);
            log.info("field {} unwrap {} {}", f, unwrap, unwrap.getType());
        });

        assertField("stringField", (f) -> {
            Assert.assertEquals(Descriptors.FieldDescriptor.Type.STRING, f.getType());
        }, fields);
        assertField("doubleField", (f) -> {
            Assert.assertEquals(Descriptors.FieldDescriptor.Type.DOUBLE, f.getType());
        }, fields);
        assertField("intField", (f) -> {
            Assert.assertEquals(Descriptors.FieldDescriptor.Type.INT32, f.getType());
        }, fields);
        assertField("testEnum", (f) -> {
            Assert.assertEquals(Descriptors.FieldDescriptor.Type.ENUM, f.getType());
        }, fields);
        assertField("nestedField", (f) -> {
            Assert.assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, f.getType());
        }, fields);
        assertField("externalMessage", (f) -> {
            Assert.assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, f.getType());
        }, fields);
        assertField("repeatedField", (f) -> {
            Assert.assertEquals(Descriptors.FieldDescriptor.Type.STRING, f.getType());
        }, fields);
    }

    @Test
    public void testGenericReaderByClazzBasedWriterSchema() {
        message = TestMessage.newBuilder().setStringField(STRING_FIELD_VLUE).setDoubleField(DOUBLE_FIELD_VLUE).build();
        byte[] clazzBasedProtobufBytes = clazzBasedProtobufNativeSchema.encode(message);
        GenericRecord genericRecord = genericProtobufNativeSchema.decode(clazzBasedProtobufBytes);
        assertEquals(genericRecord.getField("stringField"), STRING_FIELD_VLUE);
        assertEquals(genericRecord.getField("doubleField"), DOUBLE_FIELD_VLUE);
    }

    @Test
    public void testClazzBasedReaderByClazzGenericWriterSchema() {
        genericmessage = genericProtobufNativeSchema.newRecordBuilder().set("stringField", STRING_FIELD_VLUE).set("doubleField", DOUBLE_FIELD_VLUE).build();
        byte[] messageBytes = genericProtobufNativeSchema.encode(genericmessage);
        message = clazzBasedProtobufNativeSchema.decode(messageBytes);
        assertEquals(message.getStringField(), STRING_FIELD_VLUE);
        assertEquals(message.getDoubleField(), DOUBLE_FIELD_VLUE);
    }

    private final static String STRING_FIELD_VLUE = "stringFieldValue";
    private final static double DOUBLE_FIELD_VLUE = 0.2D;

}
