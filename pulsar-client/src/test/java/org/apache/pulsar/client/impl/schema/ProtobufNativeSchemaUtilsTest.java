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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.Edition;
import com.google.protobuf.DescriptorProtos.FeatureSet;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.JavaFeaturesProto;
import com.google.protobuf.JavaFeaturesProto.JavaFeatures;
import java.util.List;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.protocol.schema.ProtobufNativeSchemaData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtobufNativeSchemaUtilsTest {

    @Test
    public static void testSerialize() {
        byte[] data = ProtobufNativeSchemaUtils.serialize(
                org.apache.pulsar.client.schema.proto.Test.TestMessage.getDescriptor());
        Descriptors.Descriptor descriptor =  ProtobufNativeSchemaUtils.deserialize(data);
        Assert.assertNotNull(descriptor);
        Assert.assertNotNull(descriptor.findFieldByName("nestedField").getMessageType());
        Assert.assertNotNull(descriptor.findFieldByName("externalMessage").getMessageType());
    }

    @Test
    public static void testNestedMessage() {
        byte[] data = ProtobufNativeSchemaUtils.serialize(
                org.apache.pulsar.client.schema.proto.Test.SubMessage.NestedMessage.getDescriptor());
        Descriptors.Descriptor descriptor =  ProtobufNativeSchemaUtils.deserialize(data);
        Assert.assertNotNull(descriptor);
    }

    @Test
    public void testEditionAndJavaFeatureRoundTrip() throws Exception {
        DescriptorProtos.getDescriptor();
        for (Edition edition : new Edition[]{Edition.EDITION_2023, Edition.EDITION_2024}) {
            FeatureSet features = FeatureSet.newBuilder()
                    .setUtf8Validation(FeatureSet.Utf8Validation.NONE)
                    .setExtension(JavaFeaturesProto.java_, JavaFeatures.newBuilder()
                            .setUtf8Validation(JavaFeatures.Utf8Validation.VERIFY).build())
                    .build();
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("edition-" + edition.getNumber() + ".proto")
                    .setPackage("example")
                    .setSyntax("editions")
                    .setEdition(edition)
                    .addDependency(JavaFeaturesProto.getDescriptor().getName())
                    .setOptions(FileOptions.newBuilder().setFeatures(features))
                    .addMessageType(DescriptorProto.newBuilder().setName("Order")
                            .addField(FieldDescriptorProto.newBuilder().setName("name").setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)))
                    .build();
            Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(proto,
                    new Descriptors.FileDescriptor[]{JavaFeaturesProto.getDescriptor()});
            Descriptors.Descriptor restored = ProtobufNativeSchemaUtils.deserialize(
                    ProtobufNativeSchemaUtils.serialize(file.findMessageTypeByName("Order")));
            Assert.assertEquals(restored.getFullName(), "example.Order");
            Assert.assertEquals(restored.getFile().toProto().getEdition(), edition);
            Assert.assertEquals(restored.getFile().toProto().getOptions().getFeatures().getUtf8Validation(),
                    FeatureSet.Utf8Validation.NONE);
            Assert.assertTrue(restored.getFile().toProto().getOptions().getFeatures()
                    .hasExtension(JavaFeaturesProto.java_));
            Assert.assertTrue(restored.findFieldByName("name").needsUtf8Check());
        }
    }

    @Test
    public void testUnresolvedAndCyclicImportsFailExplicitly() throws Exception {
        FileDescriptorProto missing = FileDescriptorProto.newBuilder().setName("a.proto")
                .setPackage("example").addDependency("missing.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("Order")).build();
        Assert.expectThrows(SchemaSerializationException.class,
                () -> ProtobufNativeSchemaUtils.deserialize(envelope(missing)));

        FileDescriptorProto a = missing.toBuilder().clearDependency().addDependency("b.proto").build();
        FileDescriptorProto b = FileDescriptorProto.newBuilder().setName("b.proto")
                .addDependency("a.proto").build();
        Assert.expectThrows(SchemaSerializationException.class,
                () -> ProtobufNativeSchemaUtils.deserialize(envelope(a, b)));
    }

    private static byte[] envelope(FileDescriptorProto... files) throws Exception {
        ProtobufNativeSchemaData data = ProtobufNativeSchemaData.builder()
                .fileDescriptorSet(FileDescriptorSet.newBuilder().addAllFile(List.of(files))
                        .build().toByteArray())
                .rootFileDescriptorName("a.proto").rootMessageTypeName("example.Order").build();
        return ObjectMapperFactory.getMapperWithIncludeAlways().writer().writeValueAsBytes(data);
    }

}
