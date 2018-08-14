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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Parser;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.avro.protobuf.ProtobufDatumReader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class ProtobufSchema<T extends com.google.protobuf.GeneratedMessageV3> implements Schema<T> {

    private SchemaInfo schemaInfo;
    private Parser<T> tParser;
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

    private ProtobufSchema(Map<String, String> properties, Class<T> pojo) {
        try {
            T protoMessageInstance = (T) pojo.getMethod("getDefaultInstance").invoke(null);
            tParser = (Parser<T>) protoMessageInstance.getParserForType();

            this.schemaInfo = new SchemaInfo();
            this.schemaInfo.setName("");

            Map<String, String> allProperties = new HashMap<>();
            allProperties.putAll(properties);
            // set protobuf parsing info
            allProperties.put(PARSING_INFO_PROPERTY, getParsingInfo(protoMessageInstance));

            this.schemaInfo.setProperties(allProperties);
            this.schemaInfo.setType(SchemaType.PROTOBUF);
            ProtobufDatumReader datumReader = new ProtobufDatumReader(pojo);
            org.apache.avro.Schema schema = datumReader.getSchema();
            this.schemaInfo.setSchema(schema.toString().getBytes());

        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
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

    @Override
    public byte[] encode(T message) {
        return message.toByteArray();
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            return this.tParser.parseFrom(bytes);
        } catch (Exception e) {
            throw new RuntimeException(new SchemaSerializationException(e));
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    public static <T extends com.google.protobuf.GeneratedMessageV3> ProtobufSchema<T> of(Class<T> pojo) {
        return of(pojo, Collections.emptyMap());
    }

    public static ProtobufSchema ofGenericClass(Class pojo, Map<String, String> properties) {
        if (!com.google.protobuf.GeneratedMessageV3.class.isAssignableFrom(pojo)) {
            throw new IllegalArgumentException(com.google.protobuf.GeneratedMessageV3.class.getName()
                    + " is not assignable from " + pojo.getName());
        }
        return new ProtobufSchema<>(properties, pojo);
    }

    public static <T extends com.google.protobuf.GeneratedMessageV3> ProtobufSchema<T> of(
            Class<T> pojo, Map<String, String> properties){
        return ofGenericClass(pojo, properties);
    }
}
