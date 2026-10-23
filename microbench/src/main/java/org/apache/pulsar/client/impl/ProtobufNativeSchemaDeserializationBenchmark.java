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
package org.apache.pulsar.client.impl;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.Edition;
import com.google.protobuf.DescriptorProtos.FeatureSet;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.JavaFeaturesProto;
import com.google.protobuf.JavaFeaturesProto.JavaFeatures;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** Measures descriptor reconstruction, including extension-registry lookup, on every invocation. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(2)
@State(Scope.Thread)
public class ProtobufNativeSchemaDeserializationBenchmark {
    @Param({"2", "128"})
    public int fieldCount;

    @Param({"plain", "javaFeature"})
    public String features;

    private byte[] schema;

    @Setup
    public void setup() throws Exception {
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("Order");
        for (int number = 1; number <= fieldCount; number++) {
            message.addField(FieldDescriptorProto.newBuilder().setName("field" + number).setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING));
        }
        FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder().setName("benchmark.proto")
                .setPackage("benchmark").addMessageType(message);
        FileDescriptor[] dependencies;
        if (features.equals("javaFeature")) {
            DescriptorProtos.getDescriptor();
            file.setSyntax("editions").setEdition(Edition.EDITION_2023)
                    .addDependency(JavaFeaturesProto.getDescriptor().getName())
                    .setOptions(FileOptions.newBuilder().setFeatures(FeatureSet.newBuilder()
                            .setUtf8Validation(FeatureSet.Utf8Validation.NONE)
                            .setExtension(JavaFeaturesProto.java_, JavaFeatures.newBuilder()
                                    .setUtf8Validation(JavaFeatures.Utf8Validation.VERIFY).build())));
            dependencies = new FileDescriptor[]{JavaFeaturesProto.getDescriptor()};
        } else {
            file.setSyntax("proto2");
            dependencies = new FileDescriptor[0];
        }
        schema = ProtobufNativeSchemaUtils.serialize(
                FileDescriptor.buildFrom(file.build(), dependencies).findMessageTypeByName("Order"));
    }

    @Benchmark
    public Descriptor deserialize() {
        return ProtobufNativeSchemaUtils.deserialize(schema);
    }
}
