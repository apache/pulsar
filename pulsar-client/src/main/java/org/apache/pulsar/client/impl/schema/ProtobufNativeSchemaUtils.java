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

import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.protocol.schema.ProtobufNativeSchemaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protobuf-Native schema util used for serialize/deserialize
 * between {@link com.google.protobuf.Descriptors.Descriptor} and
 * {@link org.apache.pulsar.common.protocol.schema.ProtobufNativeSchemaData}.
 */
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
            schemaDataBytes = new ObjectMapper().writeValueAsBytes(schemaData);
            logger.debug("descriptor '{}' serialized to '{}'.", descriptor.getFullName(), schemaDataBytes);
        } catch (Exception e) {
            e.printStackTrace();
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

    public static Descriptors.Descriptor deserialize(byte[] schemaDataBytes) {
        Descriptors.Descriptor descriptor;
        try {
            ProtobufNativeSchemaData schemaData = new ObjectMapper()
                    .readValue(schemaDataBytes, ProtobufNativeSchemaData.class);

            Map<String, FileDescriptorProto> fileDescriptorProtoCache = new HashMap<>();
            Map<String, Descriptors.FileDescriptor> fileDescriptorCache = new HashMap<>();
            FileDescriptorSet fileDescriptorSet = FileDescriptorSet.parseFrom(schemaData.getFileDescriptorSet());
            fileDescriptorSet.getFileList().forEach(fileDescriptorProto ->
                    fileDescriptorProtoCache.put(fileDescriptorProto.getName(), fileDescriptorProto));
            FileDescriptorProto rootFileDescriptorProto =
                    fileDescriptorProtoCache.get(schemaData.getRootFileDescriptorName());

            //recursively build FileDescriptor
            deserializeFileDescriptor(rootFileDescriptorProto, fileDescriptorCache, fileDescriptorProtoCache);
            //extract root fileDescriptor
            Descriptors.FileDescriptor fileDescriptor = fileDescriptorCache.get(schemaData.getRootFileDescriptorName());
            //trim package
            String[] paths = StringUtils.removeFirst(schemaData.getRootMessageTypeName(), fileDescriptor.getPackage())
                    .replaceFirst("\\.", "").split("\\.");
            //extract root message
            descriptor = fileDescriptor.findMessageTypeByName(paths[0]);
            //extract nested message
            for (int i = 1; i < paths.length; i++) {
                descriptor = descriptor.findNestedTypeByName(paths[i]);
            }
            logger.debug("deserialize '{}' to descriptor: '{}'.", schemaDataBytes, descriptor.getFullName());
        } catch (Exception e) {
            e.printStackTrace();
            throw new SchemaSerializationException(e);
        }

        return descriptor;
    }

    private static void deserializeFileDescriptor(FileDescriptorProto fileDescriptorProto,
                                                  Map<String, Descriptors.FileDescriptor> fileDescriptorCache,
                                                  Map<String, FileDescriptorProto> fileDescriptorProtoCache) {
        fileDescriptorProto.getDependencyList().forEach(dependencyFileDescriptorName -> {
            if (!fileDescriptorCache.containsKey(dependencyFileDescriptorName)) {
                FileDescriptorProto dependencyFileDescriptor =
                        fileDescriptorProtoCache.get(dependencyFileDescriptorName);
                deserializeFileDescriptor(dependencyFileDescriptor, fileDescriptorCache, fileDescriptorProtoCache);
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
        } catch (Descriptors.DescriptorValidationException e) {
            e.printStackTrace();
            throw new SchemaSerializationException(e);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(ProtobufNativeSchemaUtils.class);

}
