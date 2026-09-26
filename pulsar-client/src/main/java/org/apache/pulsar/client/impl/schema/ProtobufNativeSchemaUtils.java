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

import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.CustomLog;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.protocol.schema.ProtobufNativeSchemaData;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Protobuf-Native schema util used for serialize/deserialize
 * between {@link com.google.protobuf.Descriptors.Descriptor} and
 * {@link org.apache.pulsar.common.protocol.schema.ProtobufNativeSchemaData}.
 */
@CustomLog
public class ProtobufNativeSchemaUtils {

    public static byte[] serialize(Descriptors.Descriptor descriptor) {
        byte[] schemaDataBytes;
        try {
            Map<String, FileDescriptorProto> fileDescriptorProtoCache = new HashMap<>();
            //recursively cache all FileDescriptorProto
            serializeFileDescriptor(descriptor.getFile(), fileDescriptorProtoCache);

            //extract root message path
            String rootMessageTypeName = descriptor.getFullName();
            String rootFileDescriptorName = descriptor.getFile().getFullName();
            //build FileDescriptorSet, this is equal to < protoc --include_imports --descriptor_set_out >
            byte[] fileDescriptorSet = FileDescriptorSet.newBuilder().addAllFile(fileDescriptorProtoCache.values())
                    .build().toByteArray();

            //serialize to bytes
            ProtobufNativeSchemaData schemaData = ProtobufNativeSchemaData.builder()
                    .fileDescriptorSet(fileDescriptorSet)
                    .rootFileDescriptorName(rootFileDescriptorName).rootMessageTypeName(rootMessageTypeName).build();
            schemaDataBytes = ObjectMapperFactory.getMapperWithIncludeAlways().writer().writeValueAsBytes(schemaData);
            log.debug().attr("descriptor", descriptor.getFullName())
                    .attr("size", schemaDataBytes.length).log("descriptor serialized");
        } catch (Exception e) {
            log.error().exception(e).log("Failed to serialize protobuf schema");
            throw new SchemaSerializationException(e);
        }
        return schemaDataBytes;
    }

    private static void serializeFileDescriptor(Descriptors.FileDescriptor fileDescriptor,
                                                Map<String, FileDescriptorProto> fileDescriptorCache) {
        fileDescriptor.getDependencies().forEach(dependency -> {
                    if (!fileDescriptorCache.containsKey(dependency.getFullName())) {
                        serializeFileDescriptor(dependency, fileDescriptorCache);
                    }
                }
        );
        String[] unResolvedFileDescriptNames = fileDescriptor.getDependencies().stream().
                filter(item -> !fileDescriptorCache.containsKey(item.getFullName()))
                .map(Descriptors.FileDescriptor::getFullName).toArray(String[]::new);
        if (unResolvedFileDescriptNames.length == 0) {
            fileDescriptorCache.put(fileDescriptor.getFullName(), fileDescriptor.toProto());
        } else {
            throw new SchemaSerializationException(fileDescriptor.getFullName() + " can't resolve dependency '"
                    + Arrays.toString(unResolvedFileDescriptNames) + "'.");
        }
    }

    private static final ObjectReader PROTOBUF_NATIVE_SCHEMADATA_READER = ObjectMapperFactory.getMapper().reader()
            .forType(ProtobufNativeSchemaData.class);

    @SuppressWarnings("deprecation")
    public static Descriptors.Descriptor deserialize(byte[] schemaDataBytes) {
        Descriptors.Descriptor descriptor;
        try {
            ProtobufNativeSchemaData schemaData = PROTOBUF_NATIVE_SCHEMADATA_READER.readValue(schemaDataBytes);

            Map<String, FileDescriptorProto> fileDescriptorProtoCache = new HashMap<>();
            Map<String, Descriptors.FileDescriptor> fileDescriptorCache = new HashMap<>();
            FileDescriptorSet fileDescriptorSet = FileDescriptorSet.parseFrom(
                    schemaData.getFileDescriptorSet(), nativeSchemaExtensions());
            fileDescriptorSet.getFileList().forEach(fileDescriptorProto ->
                    fileDescriptorProtoCache.put(fileDescriptorProto.getName(), fileDescriptorProto));
            FileDescriptorProto rootFileDescriptorProto =
                    fileDescriptorProtoCache.get(schemaData.getRootFileDescriptorName());
            if (rootFileDescriptorProto == null) {
                throw new SchemaSerializationException("Missing root file descriptor");
            }

            //recursively build FileDescriptor
            deserializeFileDescriptor(rootFileDescriptorProto, fileDescriptorCache, fileDescriptorProtoCache,
                    new HashSet<>());
            //extract root fileDescriptor
            Descriptors.FileDescriptor fileDescriptor = fileDescriptorCache.get(schemaData.getRootFileDescriptorName());
            String packagePrefix = fileDescriptor.getPackage().isEmpty() ? "" : fileDescriptor.getPackage() + ".";
            String rootName = schemaData.getRootMessageTypeName();
            if (rootName == null || !rootName.startsWith(packagePrefix)
                    || rootName.length() == packagePrefix.length()) {
                throw new SchemaSerializationException("Root message is outside its descriptor package");
            }
            String[] paths = rootName.substring(packagePrefix.length()).split("\\.");
            //extract root message
            descriptor = fileDescriptor.findMessageTypeByName(paths[0]);
            //extract nested message
            for (int i = 1; i < paths.length; i++) {
                if (descriptor == null) {
                    throw new SchemaSerializationException("Root message was not found");
                }
                descriptor = descriptor.findNestedTypeByName(paths[i]);
            }
            if (descriptor == null || !descriptor.getFullName().equals(rootName)) {
                throw new SchemaSerializationException("Root message was not found");
            }
            log.debug().attr("size", schemaDataBytes.length)
                    .attr("descriptor", descriptor.getFullName()).log("deserialized to descriptor");
        } catch (Exception e) {
            log.error().exception(e).log("Failed to deserialize protobuf schema");
            throw new SchemaSerializationException(e);
        }

        return descriptor;
    }

    private static ExtensionRegistry nativeSchemaExtensions() throws ReflectiveOperationException {
        // Initialize descriptor.proto before registering its Java feature extension.
        DescriptorProtos.getDescriptor();
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        try {
            // Java features are available in Protobuf v4, while the client also supports v3.
            // Derive the package and loader from Protobuf so shaded clients use the matching runtime.
            Class<?> javaFeatures = Class.forName(DescriptorProtos.class.getPackageName() + ".JavaFeaturesProto",
                    true, DescriptorProtos.class.getClassLoader());
            javaFeatures.getMethod("registerAllExtensions", ExtensionRegistry.class).invoke(null, registry);
        } catch (ClassNotFoundException ignored) {
            // Protobuf v3 has no Java feature extension to register.
        }
        return registry;
    }

    private static void deserializeFileDescriptor(FileDescriptorProto fileDescriptorProto,
                                                  Map<String, Descriptors.FileDescriptor> fileDescriptorCache,
                                                  Map<String, FileDescriptorProto> fileDescriptorProtoCache,
                                                  Set<String> visiting) {
        if (fileDescriptorProto == null) {
            throw new SchemaSerializationException("Missing imported file descriptor");
        }
        if (!visiting.add(fileDescriptorProto.getName())) {
            throw new SchemaSerializationException("Cyclic file descriptor imports");
        }
        fileDescriptorProto.getDependencyList().forEach(dependencyFileDescriptorName -> {
            if (!fileDescriptorCache.containsKey(dependencyFileDescriptorName)) {
                FileDescriptorProto dependencyFileDescriptor =
                        fileDescriptorProtoCache.get(dependencyFileDescriptorName);
                deserializeFileDescriptor(dependencyFileDescriptor, fileDescriptorCache, fileDescriptorProtoCache,
                        visiting);
            }
        });

        Descriptors.FileDescriptor[] dependencyFileDescriptors = fileDescriptorProto.getDependencyList().stream()
                .map(dependency -> {
            if (fileDescriptorCache.containsKey(dependency)) {
                return fileDescriptorCache.get(dependency);
            } else {
                throw new SchemaSerializationException("'" + fileDescriptorProto.getName()
                        + "' can't resolve  dependency '" + dependency + "'.");
            }
        }).toArray(Descriptors.FileDescriptor[]::new);

        try {
            Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor
                    .buildFrom(fileDescriptorProto, dependencyFileDescriptors);
            fileDescriptorCache.put(fileDescriptor.getFullName(), fileDescriptor);
            visiting.remove(fileDescriptorProto.getName());
        } catch (Descriptors.DescriptorValidationException e) {
            throw new SchemaSerializationException(e);
        }
    }

}
