/*
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
package org.apache.pulsar.broker.service.schema;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class KeyValueSchemaCompatibilityCheckTest {

    private final Map<SchemaType, SchemaCompatibilityCheck> checkers = new HashMap<>();

    @Data
    private static class Foo {
        private String field1;
        private String field2;
        private int field3;
        private KeyValueSchemaCompatibilityCheckTest.Bar field4;
    }

    @Data
    private static class Bar {
        private boolean field1;
    }

    @BeforeClass
    protected void setup() {
        checkers.put(SchemaType.AVRO, new AvroSchemaCompatibilityCheck());
        checkers.put(SchemaType.JSON, new JsonSchemaCompatibilityCheck());
        checkers.put(SchemaType.PROTOBUF_NATIVE, new ProtobufNativeSchemaAdvancedCompatibilityCheck());
        checkers.put(SchemaType.KEY_VALUE, new KeyValueSchemaCompatibilityCheck(checkers));
    }

    @Test
    public void testAdvancedNativeKeyAndValueDelegation() throws Exception {
        SchemaInfo nativeInt = nativeSchemaInfo(FieldDescriptorProto.Type.TYPE_INT32);
        SchemaInfo nativeString = nativeSchemaInfo(FieldDescriptorProto.Type.TYPE_STRING);
        SchemaInfo other = new StringSchema().getSchemaInfo();
        SchemaCompatibilityCheck keyValue = checkers.get(SchemaType.KEY_VALUE);

        Assert.assertFalse(keyValue.isCompatible(keyValue(nativeInt, other), keyValue(nativeString, other),
                SchemaCompatibilityStrategy.BACKWARD));
        Assert.assertFalse(keyValue.isCompatible(keyValue(other, nativeInt), keyValue(other, nativeString),
                SchemaCompatibilityStrategy.BACKWARD));
        Assert.assertFalse(keyValue.isCompatible(keyValue(nativeInt, nativeInt),
                keyValue(nativeString, nativeInt), SchemaCompatibilityStrategy.BACKWARD));
        Assert.assertTrue(keyValue.isCompatible(keyValue(nativeInt, nativeInt),
                keyValue(nativeInt, nativeInt), SchemaCompatibilityStrategy.FULL));

        SchemaData old = keyValue(nativeInt, other);
        SchemaData empty = keyValue(nativeSchemaInfo(null), other);
        SchemaData proposed = keyValue(nativeString, other);
        Assert.assertTrue(keyValue.isCompatible(empty, proposed, SchemaCompatibilityStrategy.BACKWARD));
        Assert.assertFalse(keyValue.isCompatible(List.of(old, empty), proposed,
                SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE));
    }

    private static SchemaData keyValue(SchemaInfo key, SchemaInfo value) {
        return SchemaData.fromSchemaInfo(KeyValueSchemaInfo.encodeKeyValueSchemaInfo("KeyValue", key, value,
                KeyValueEncodingType.INLINE));
    }

    private static SchemaInfo nativeSchemaInfo(FieldDescriptorProto.Type type) throws Exception {
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("Order");
        if (type != null) {
            message.addField(FieldDescriptorProto.newBuilder().setName("id").setNumber(1)
                    .setType(type).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("native-kv.proto")
                .setPackage("example").setSyntax("proto2").addMessageType(message).build();
        byte[] bytes = ProtobufNativeSchemaUtils.serialize(FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName("Order"));
        return SchemaInfoImpl.builder().name("Order").type(SchemaType.PROTOBUF_NATIVE)
                .schema(bytes).properties(Map.of()).build();
    }

    @Test
    public void testCheckKeyValueAvroCompatibilityFull() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder()
                .withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder()
                .withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema).getSchemaInfo()
                        .getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema).getSchemaInfo()
                        .getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FULL));
    }

    @Test
    public void testCheckKeyValueAvroInCompatibilityFull() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema).getSchemaInfo()
                        .getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema).getSchemaInfo()
                        .getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FULL));
    }

    @Test
    public void testCheckKeyValueAvroCompatibilityBackward() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema).getSchemaInfo()
                        .getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema).getSchemaInfo()
                        .getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.BACKWARD));
    }

    @Test
    public void testCheckKeyValueAvroInCompatibilityBackward() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.BACKWARD));
    }

    @Test
    public void testCheckKeyValueAvroCompatibilityForward() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FORWARD));
    }

    @Test
    public void testCheckKeyValueAvroInCompatibilityForward() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FORWARD));
    }

    @Test
    public void testCheckKeyValueJsonCompatibilityFull() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FULL));
    }

    @Test
    public void testCheckKeyValueJsonInCompatibilityFull() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FULL));
    }

    @Test
    public void testCheckKeyValueJsonCompatibilityBackward() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.BACKWARD));
    }

    @Test
    public void testCheckKeyValueJsonInCompatibilityBackWard() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.BACKWARD));
    }

    @Test
    public void testCheckKeyValueJsonCompatibilityForward() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FORWARD));
    }

    @Test
    public void testCheckKeyValueJsonInCompatibilityForward() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FORWARD));
    }

    @Test
    public void testCheckKeyAvroValueJsonCompatibilityFull() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FULL));
    }

    @Test
    public void testCheckKeyAvroValueJsonInCompatibilityFull() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FULL));
    }

    @Test
    public void testCheckKeyAvroValueJsonCompatibilityBackward() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.BACKWARD));
    }

    @Test
    public void testCheckKeyAvroValueJsonInCompatibilityBackward() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.BACKWARD));
    }

    @Test
    public void testCheckKeyAvroValueJsonCompatibilityForward() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FORWARD));
    }

    @Test
    public void testCheckKeyAvroValueJsonInCompatibilityForward() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        JSONSchema<Bar> barSchema = JSONSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        properties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FORWARD));
    }

    @Test
    public void testCheckKeyJsonValueAvroCompatibilityFull() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FULL));
    }

    @Test
    public void testCheckKeyJsonValueAvroInCompatibilityFull() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FULL));
    }

    @Test
    public void testCheckKeyJsonValueAvroCompatibilityBackward() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.BACKWARD));
    }

    @Test
    public void testCheckKeyJsonValueAvroInCompatibilityBackward() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.BACKWARD));
    }


    @Test
    public void testCheckKeyJsonValueAvroCompatibilityForward() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FORWARD));
    }

    @Test
    public void testCheckKeyJsonValueAvroInCompatibilityForward() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> properties = new HashMap<>();
        properties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        properties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, fooSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(properties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FORWARD));
    }

    @Test
    public void testCheckKeyJsonValueAvroKeyTypeInCompatibility() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> fromProperties = new HashMap<>();
        fromProperties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        fromProperties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        Map<String, String> toProperties = new HashMap<>();
        toProperties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        toProperties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(fromProperties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(barSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(toProperties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FORWARD));
    }

    @Test
    public void testCheckKeyJsonValueAvroValueTypeInCompatibility() {
        JSONSchema<Foo> fooSchema = JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> fromProperties = new HashMap<>();
        fromProperties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        fromProperties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        Map<String, String> toProperties = new HashMap<>();
        toProperties.put("key.schema.type", String.valueOf(SchemaType.JSON));
        toProperties.put("value.schema.type", String.valueOf(SchemaType.JSON));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema).getSchemaInfo()
                        .getSchema()).props(fromProperties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, fooSchema).getSchemaInfo()
                        .getSchema()).props(toProperties).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FORWARD));
    }

    @Test
    public void testCheckPropertiesNullTypeCompatibility() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> fromProperties = new HashMap<>();
        fromProperties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        fromProperties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        Map<String, String> toProperties = new HashMap<>();
        toProperties.put("key.schema.type", String.valueOf(SchemaType.AVRO));
        toProperties.put("value.schema.type", String.valueOf(SchemaType.AVRO));
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(fromProperties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(toProperties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FULL));
    }

    @Test
    public void testCheckSchemaTypeNullCompatibility() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        Map<String, String> fromProperties = new HashMap<>();
        Map<String, String> toProperties = new HashMap<>();
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(fromProperties).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).props(toProperties).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.FULL));
    }

    @Test
    public void testCheckSchemaTypeAlwaysCompatibility() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        StringSchema stringSchema = new StringSchema();
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.STRING)
                .data(stringSchema.getSchemaInfo().getSchema()).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema).getSchemaInfo().getSchema()).build();
        Assert.assertTrue(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE));
    }

    @Test
    public void testCheckSchemaTypeOtherCompatibility() {
        AvroSchema<Foo> fooSchema = AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());
        AvroSchema<Bar> barSchema = AvroSchema.of(SchemaDefinition.<Bar>builder().withPojo(Bar.class).build());
        StringSchema stringSchema = new StringSchema();
        SchemaData fromSchemaData = SchemaData.builder().type(SchemaType.STRING)
                .data(stringSchema.getSchemaInfo().getSchema()).build();
        SchemaData toSchemaData = SchemaData.builder().type(SchemaType.KEY_VALUE)
                .data(KeyValueSchemaImpl.of(fooSchema, barSchema)
                        .getSchemaInfo().getSchema()).build();
        Assert.assertFalse(checkers.get(SchemaType.KEY_VALUE)
                .isCompatible(fromSchemaData, toSchemaData, SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE));
    }

}
