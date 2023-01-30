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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.reader.ProtobufNativeReader;
import org.apache.pulsar.client.impl.schema.writer.ProtobufNativeWriter;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * A schema implementation to deal with protobuf generated messages.
 */
public class ProtobufNativeSchema<T extends GeneratedMessageV3> extends AbstractStructSchema<T> {

    public static final String PARSING_INFO_PROPERTY = "__PARSING_INFO__";

    @Getter
    @AllArgsConstructor
    public static class ProtoBufParsingInfo {
        private final int number;
        private final String name;
        private final String type;
        private final String label;
        // For future nested fields
        private final Map<String, Object> definition;
    }

    private static <T> Descriptors.Descriptor createProtobufNativeSchema(Class<T> pojo) {
        try {
            Method method = pojo.getMethod("getDescriptor");
            Descriptors.Descriptor descriptor = (Descriptors.Descriptor) method.invoke(null);
            return descriptor;
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private ProtobufNativeSchema(SchemaInfo schemaInfo, T protoMessageInstance) {
        super(schemaInfo);
        setReader(new ProtobufNativeReader<>(protoMessageInstance));
        setWriter(new ProtobufNativeWriter<>());
        // update properties with protobuf related properties
        // set protobuf parsing info
        Map<String, String> allProperties = new HashMap<>(schemaInfo.getProperties());
        allProperties.put(PARSING_INFO_PROPERTY, getParsingInfo(protoMessageInstance));
        ((SchemaInfoImpl) schemaInfo).setProperties(allProperties);
    }

    private String getParsingInfo(T protoMessageInstance) {
        List<ProtoBufParsingInfo> protoBufParsingInfos = new LinkedList<>();
        protoMessageInstance.getDescriptorForType().getFields().forEach(new Consumer<Descriptors.FieldDescriptor>() {
            @Override
            public void accept(Descriptors.FieldDescriptor fieldDescriptor) {
                protoBufParsingInfos.add(new ProtoBufParsingInfo(fieldDescriptor.getNumber(),
                        fieldDescriptor.getName(), fieldDescriptor.getType().name(),
                        fieldDescriptor.toProto().getLabel().name(), null));
            }
        });

        try {
            return ObjectMapperFactory.getMapperWithIncludeAlways().writer().writeValueAsString(protoBufParsingInfos);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public Descriptors.Descriptor getProtobufNativeSchema() {
        return ProtobufNativeSchemaUtils.deserialize(this.schemaInfo.getSchema());
    }

    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(getProtobufNativeSchema());
    }

    public static <T extends GeneratedMessageV3> ProtobufNativeSchema<T> of(Class<T> pojo) {
        return of(pojo, new HashMap<>());
    }

    public static <T> ProtobufNativeSchema ofGenericClass(Class<T> pojo, Map<String, String> properties) {
        SchemaDefinition<T> schemaDefinition = SchemaDefinition.<T>builder().withPojo(pojo)
                .withProperties(properties).build();
        return ProtobufNativeSchema.of(schemaDefinition);
    }

    public static <T> ProtobufNativeSchema of(SchemaDefinition<T> schemaDefinition) {
        Class<T> pojo = schemaDefinition.getPojo();

        if (!GeneratedMessageV3.class.isAssignableFrom(pojo)) {
            throw new IllegalArgumentException(GeneratedMessageV3.class.getName()
                    + " is not assignable from " + pojo.getName());
        }
        Descriptors.Descriptor descriptor = createProtobufNativeSchema(schemaDefinition.getPojo());

        SchemaInfo schemaInfo = SchemaInfoImpl.builder()
                .schema(ProtobufNativeSchemaUtils.serialize(descriptor))
                .type(SchemaType.PROTOBUF_NATIVE)
                .name("")
                .properties(schemaDefinition.getProperties())
                .build();
        try {
            return new ProtobufNativeSchema(schemaInfo,
                    (GeneratedMessageV3) pojo.getMethod("getDefaultInstance").invoke(null));
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T extends GeneratedMessageV3> ProtobufNativeSchema<T> of(
            Class pojo, Map<String, String> properties) {
        return ofGenericClass(pojo, properties);
    }
}
