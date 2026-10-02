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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.Edition;
import com.google.protobuf.DescriptorProtos.FeatureSet;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.JavaFeaturesProto;
import com.google.protobuf.JavaFeaturesProto.JavaFeatures;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

// The broker uses Protobuf v4; client-module tests also compile and run against v3.
@Test(groups = "broker")
public class ProtobufNativeSchemaDeserializationTest {
    @DataProvider
    public Object[][] javaFeatureScopes() {
        return new Object[][]{
                {Edition.EDITION_2023, false},
                {Edition.EDITION_2023, true},
                {Edition.EDITION_2024, false},
                {Edition.EDITION_2024, true}
        };
    }

    @Test(dataProvider = "javaFeatureScopes")
    public void testJavaUtf8OverrideSurvivesDeserialization(Edition edition, boolean fieldOverride) throws Exception {
        DescriptorProtos.getDescriptor();
        FeatureSet javaFeatures = FeatureSet.newBuilder()
                .setExtension(JavaFeaturesProto.java_, JavaFeatures.newBuilder()
                        .setUtf8Validation(JavaFeatures.Utf8Validation.VERIFY).build())
                .build();
        FeatureSet.Builder fileFeatures = FeatureSet.newBuilder().setUtf8Validation(FeatureSet.Utf8Validation.NONE);
        FieldDescriptorProto.Builder field = FieldDescriptorProto.newBuilder().setName("name").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING);
        if (fieldOverride) {
            field.setOptions(FieldOptions.newBuilder().setFeatures(javaFeatures));
        } else {
            fileFeatures.mergeFrom(javaFeatures);
        }
        FileDescriptorProto proto = FileDescriptorProto.newBuilder().setName("edition.proto")
                .setPackage("example").setSyntax("editions").setEdition(edition)
                .addDependency(JavaFeaturesProto.getDescriptor().getName())
                .setOptions(FileOptions.newBuilder().setFeatures(fileFeatures))
                .addMessageType(DescriptorProto.newBuilder().setName("Order").addField(field)).build();
        Descriptor original = FileDescriptor.buildFrom(proto, new FileDescriptor[]{JavaFeaturesProto.getDescriptor()})
                .findMessageTypeByName("Order");
        assertThat(original.findFieldByName("name").needsUtf8Check()).isTrue();

        byte[] data = ProtobufNativeSchemaUtils.serialize(original);
        GenericProtobufNativeSchema schema = new GenericProtobufNativeSchema(SchemaInfo.builder()
                .type(SchemaType.PROTOBUF_NATIVE).schema(data).build());
        Descriptor restored = schema.getProtobufNativeSchema();
        assertThat(restored.getFullName()).isEqualTo("example.Order");
        assertThat(restored.getFile().toProto().getEdition()).isEqualTo(edition);
        assertThat(restored.findFieldByName("name").needsUtf8Check()).isTrue();
        byte[] invalidUtf8 = new byte[]{0x0a, 0x01, (byte) 0xff};
        assertThatThrownBy(() -> schema.decode(invalidUtf8)).isInstanceOf(SchemaSerializationException.class);
        FeatureSet restoredFeatures = fieldOverride
                ? restored.findFieldByName("name").toProto().getOptions().getFeatures()
                : restored.getFile().toProto().getOptions().getFeatures();
        assertThat(restoredFeatures.hasExtension(JavaFeaturesProto.java_)).isTrue();

        byte[] valid = DynamicMessage.newBuilder(original)
                .setField(original.findFieldByName("name"), "valid").build().toByteArray();
        assertThat(schema.decode(valid).getField("name")).isEqualTo("valid");
    }
}
