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
package org.apache.pulsar.broker.service;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.schema.ProtobufNativeSchemaAdvancedCompatibilityCheck;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@State(Scope.Thread)
public class ProtobufNativeSchemaCompatibilityBenchmark {
    @Param({"compact", "sparse", "wide", "recursive", "history16", "history64"})
    public String scenario;

    private final ProtobufNativeSchemaAdvancedCompatibilityCheck checker =
            new ProtobufNativeSchemaAdvancedCompatibilityCheck();
    private List<SchemaData> history;
    private SchemaData proposed;

    @Setup
    public void setup() throws Exception {
        history = new ArrayList<>();
        switch (scenario) {
            case "compact" -> {
                proposed = schema(2, false, false);
                history.add(proposed);
            }
            case "sparse" -> {
                proposed = schema(2, true, false);
                history.add(proposed);
            }
            case "wide" -> {
                proposed = schema(128, false, false);
                history.add(proposed);
            }
            case "recursive" -> {
                proposed = schema(8, false, true);
                history.add(proposed);
            }
            case "history16", "history64" -> {
                int versions = scenario.equals("history16") ? 16 : 64;
                proposed = schema(versions + 1, false, false);
                for (int count = 1; count <= versions; count++) {
                    history.add(schema(count, false, false));
                }
            }
            default -> throw new IllegalArgumentException(scenario);
        }
    }

    @Benchmark
    public void check() throws IncompatibleSchemaException {
        checker.checkCompatible(history, proposed, SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE);
    }

    private static SchemaData schema(int fields, boolean sparse, boolean recursive) throws Exception {
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("Order");
        for (int index = 1; index <= fields; index++) {
            int number = sparse && index == fields ? 536870911 : index;
            message.addField(FieldDescriptorProto.newBuilder().setName("field" + index).setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_INT32)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }
        if (recursive) {
            message.addField(FieldDescriptorProto.newBuilder().setName("child").setNumber(fields + 1)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".benchmark.Order")
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("benchmark.proto")
                .setPackage("benchmark").setSyntax("proto2").addMessageType(message).build();
        Descriptor descriptor = FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
        return SchemaData.builder().type(SchemaType.PROTOBUF_NATIVE)
                .data(ProtobufNativeSchemaUtils.serialize(descriptor)).build();
    }
}
