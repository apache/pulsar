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
package org.apache.pulsar.client.impl.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.client.schema.proto.Test.SubMessage.NestedMessage;
import org.apache.pulsar.client.schema.proto.Test.TestMessage;
import org.apache.pulsar.common.protocol.schema.ProtobufNativeSchemaData;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtobufNativeSchemaUtilsTest {

    @Test
    public static void testSerialize() {
        byte[] data = ProtobufNativeSchemaUtils.serialize(TestMessage.getDescriptor());
        Descriptors.Descriptor descriptor =  ProtobufNativeSchemaUtils.deserialize(data);
        Assert.assertNotNull(descriptor);
        Assert.assertNotNull(descriptor.findFieldByName("nestedField").getMessageType());
        Assert.assertNotNull(descriptor.findFieldByName("externalMessage").getMessageType());
    }

    @Test
    public static void testNestedMessage() {
        byte[] data = ProtobufNativeSchemaUtils.serialize(NestedMessage.getDescriptor());
        Descriptors.Descriptor descriptor =  ProtobufNativeSchemaUtils.deserialize(data);
        Assert.assertNotNull(descriptor);
    }

    @Test
    public void testLegacyRootNamesRemainReadable() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("legacy.proto").setPackage("a.b")
                .addMessageType(DescriptorProto.newBuilder().setName("Order")).build();
        for (String rootName : List.of("a.b.Order", "Order", "aXb.Order", "a.b.Order.")) {
            ProtobufNativeSchemaData data = ProtobufNativeSchemaData.builder()
                    .fileDescriptorSet(FileDescriptorSet.newBuilder().addFile(file).build().toByteArray())
                    .rootFileDescriptorName(file.getName()).rootMessageTypeName(rootName).build();
            byte[] bytes = ObjectMapperFactory.getMapperWithIncludeAlways().writer().writeValueAsBytes(data);
            Assert.assertEquals(ProtobufNativeSchemaUtils.deserialize(bytes).getFullName(), "a.b.Order", rootName);
        }
    }

    @Test
    public void testNestedRootWithoutPackage() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("nested.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("Outer")
                        .addNestedType(DescriptorProto.newBuilder().setName("Inner")
                                .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(1)
                                        .setType(FieldDescriptorProto.Type.TYPE_STRING))))
                .build();
        Descriptors.Descriptor original = Descriptors.FileDescriptor
                .buildFrom(file, new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("Outer").findNestedTypeByName("Inner");
        byte[] data = ProtobufNativeSchemaUtils.serialize(original);
        assertThat(ProtobufNativeSchemaUtils.deserialize(data).getFullName()).isEqualTo("Outer.Inner");

        GenericProtobufNativeSchema schema = new GenericProtobufNativeSchema(SchemaInfo.builder()
                .type(SchemaType.PROTOBUF_NATIVE).schema(data).build());
        byte[] payload = DynamicMessage.newBuilder(original)
                .setField(original.findFieldByName("value"), "nested value").build().toByteArray();
        assertThat(schema.decode(payload).getField("value")).isEqualTo("nested value");
    }

    @Test
    public void testMissingImportFailsExplicitly() throws Exception {
        FileDescriptorProto missing = FileDescriptorProto.newBuilder().setName("a.proto")
                .setPackage("example").addDependency("missing.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("Order")).build();
        assertThatThrownBy(() -> ProtobufNativeSchemaUtils.deserialize(envelope(missing)))
                .isInstanceOf(SchemaSerializationException.class)
                .hasMessageContaining("Missing imported file descriptor");
    }

    @Test
    public void testCyclicImportsFailExplicitly() throws Exception {
        FileDescriptorProto a = FileDescriptorProto.newBuilder().setName("a.proto")
                .setPackage("example").addDependency("b.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("Order")).build();
        FileDescriptorProto b = FileDescriptorProto.newBuilder().setName("b.proto")
                .addDependency("a.proto").build();
        assertThatThrownBy(() -> ProtobufNativeSchemaUtils.deserialize(envelope(a, b)))
                .isInstanceOf(SchemaSerializationException.class)
                .hasMessageContaining("Cyclic file descriptor imports");
    }

    @Test
    public void testSharedImportIsNotACycle() throws Exception {
        FileDescriptorProto a = FileDescriptorProto.newBuilder().setName("a.proto")
                .setPackage("example").addDependency("b.proto").addDependency("c.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("Order")).build();
        FileDescriptorProto b = FileDescriptorProto.newBuilder().setName("b.proto")
                .addDependency("shared.proto").build();
        FileDescriptorProto c = FileDescriptorProto.newBuilder().setName("c.proto")
                .addDependency("shared.proto").build();
        FileDescriptorProto shared = FileDescriptorProto.newBuilder().setName("shared.proto").build();
        assertThat(ProtobufNativeSchemaUtils.deserialize(envelope(a, b, c, shared)).getFullName())
                .isEqualTo("example.Order");
    }

    @Test
    public void testMissingRootFileFailsExplicitly() {
        assertThatThrownBy(() -> ProtobufNativeSchemaUtils.deserialize(envelope()))
                .isInstanceOf(SchemaSerializationException.class)
                .hasMessageContaining("Missing root file descriptor");
    }

    private static byte[] envelope(FileDescriptorProto... files) throws Exception {
        ProtobufNativeSchemaData data = ProtobufNativeSchemaData.builder()
                .fileDescriptorSet(FileDescriptorSet.newBuilder().addAllFile(List.of(files))
                        .build().toByteArray())
                .rootFileDescriptorName("a.proto").rootMessageTypeName("example.Order").build();
        return ObjectMapperFactory.getMapperWithIncludeAlways().writer().writeValueAsBytes(data);
    }

}
