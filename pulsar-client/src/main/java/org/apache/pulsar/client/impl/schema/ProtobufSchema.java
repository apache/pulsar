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

import static java.nio.charset.StandardCharsets.UTF_8;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.reader.ProtobufReader;
import org.apache.pulsar.client.impl.schema.writer.ProtobufWriter;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A schema implementation to deal with protobuf generated messages.
 */
public class ProtobufSchema<T extends com.google.protobuf.GeneratedMessageV3> extends AvroBaseStructSchema<T> {

    public static final String PARSING_INFO_PROPERTY = "__PARSING_INFO__";

    @Getter
    @AllArgsConstructor
    public static class ProtoBufParsingInfo {
        private final int number;
        private final String name;
        private final String type;
        private final String label;
        // For future nested fields
        private final Map <String, Object> definition;
    }

    private static <T> org.apache.avro.Schema createProtobufAvroSchema(Class<T> pojo) {
        return ProtobufData.get().getSchema(pojo);
    }

    private ProtobufSchema(SchemaInfo schemaInfo, T protoMessageInstance) {
        super(schemaInfo);
        setReader(new ProtobufReader<>(protoMessageInstance));
        setWriter(new ProtobufWriter<>());
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
            return new ObjectMapper().writeValueAsString(protoBufParsingInfos);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends com.google.protobuf.GeneratedMessageV3> ProtobufSchema<T> of(Class<T> pojo) {
        return of(pojo, new HashMap<>());
    }

    public static <T> ProtobufSchema ofGenericClass(Class<T> pojo, Map<String, String> properties) {
        SchemaDefinition<T> schemaDefinition = SchemaDefinition.<T>builder().withPojo(pojo)
                .withProperties(properties).build();
        return ProtobufSchema.of(schemaDefinition);
    }

    public static <T> ProtobufSchema of(SchemaDefinition<T> schemaDefinition) {
        Class<T> pojo = schemaDefinition.getPojo();

        if (!com.google.protobuf.GeneratedMessageV3.class.isAssignableFrom(pojo)) {
            throw new IllegalArgumentException(com.google.protobuf.GeneratedMessageV3.class.getName()
                    + " is not assignable from " + pojo.getName());
        }

            SchemaInfo schemaInfo = SchemaInfoImpl.builder()
                    .schema(createProtobufAvroSchema(schemaDefinition.getPojo()).toString().getBytes(UTF_8))
                    .type(SchemaType.PROTOBUF)
                    .name("")
                    .properties(schemaDefinition.getProperties())
                    .build();

        try {
            return new ProtobufSchema(schemaInfo,
                (GeneratedMessageV3) pojo.getMethod("getDefaultInstance").invoke(null));
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T extends com.google.protobuf.GeneratedMessageV3> ProtobufSchema<T> of(
            Class pojo, Map<String, String> properties){
        return ofGenericClass(pojo, properties);
    }
}
