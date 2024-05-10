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

import static org.apache.pulsar.client.impl.schema.ProtobufNativeSchema.ProtoBufParsingInfo;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.pulsar.broker.service.schema.exceptions.ProtoBufCanReadCheckException;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;

@Slf4j
public class ProtobufNativeSchemaBreakCheckUtils {

    public static void checkSchemaCompatibility(Descriptors.Descriptor writtenSchema,
                                                Descriptors.Descriptor readSchema)
            throws ProtoBufCanReadCheckException {
        String writtenSchemaRootName = writtenSchema.getName();
        String readSchemaRootName = readSchema.getName();
        if (!writtenSchemaRootName.equals(readSchemaRootName)) {
            throw new ProtoBufCanReadCheckException("Protobuf root message isn't allow change!");
        }

        Map<String, List<ProtobufNativeSchema.ProtoBufParsingInfo>> writtenSchemaAllProto = new HashMap<>();
        ProtobufNativeSchemaUtils.getSchemaDependenciesFileDescriptorCache(writtenSchema)
                .forEach((s, fileDescriptorProto) -> {
                    ProtobufNativeSchemaUtils.coverAllNestedAndEnumFileDescriptor(fileDescriptorProto,
                            writtenSchemaAllProto);
                });

        Map<String, List<ProtobufNativeSchema.ProtoBufParsingInfo>> readSchemaAllProto = new HashMap<>();
        ProtobufNativeSchemaUtils.getSchemaDependenciesFileDescriptorCache(readSchema)
                .forEach((s, fileDescriptorProto) -> {
                    ProtobufNativeSchemaUtils.coverAllNestedAndEnumFileDescriptor(fileDescriptorProto,
                            readSchemaAllProto);
                });

        List<ProtoBufParsingInfo> writtenRootProtoBufParsingInfos = writtenSchemaAllProto.get(writtenSchemaRootName);
        List<ProtoBufParsingInfo> readRootProtoBufParsingInfos = readSchemaAllProto.get(readSchemaRootName);
        // root check first
        check(writtenRootProtoBufParsingInfos, readRootProtoBufParsingInfos);

        for (String writtenSchemaMessageName : writtenSchemaAllProto.keySet()) {
            // skip root
            if (!writtenSchemaMessageName.equals(writtenSchemaRootName)
                    && readSchemaAllProto.containsKey(writtenSchemaMessageName)) {
                List<ProtoBufParsingInfo> writtenProtoBufParsingInfoList =
                        writtenSchemaAllProto.get(writtenSchemaMessageName);
                List<ProtoBufParsingInfo> readProtoBufParsingInfoList =
                        readSchemaAllProto.get(writtenSchemaMessageName);
                check(writtenProtoBufParsingInfoList, readProtoBufParsingInfoList);
            }
        }
    }

    private static void check(List<ProtoBufParsingInfo> writtenSchemaFieldInfoList,
                              List<ProtoBufParsingInfo> readSchemaFieldInfoList)
            throws ProtoBufCanReadCheckException {
        List<ProtoBufParsingInfo> readSchemaRequiredFields = new LinkedList<>();
        List<ProtoBufParsingInfo> writtenSchemaRequiredFields = new LinkedList<>();
        Map<Integer, ProtoBufParsingInfo> readSchemaFieldInfoByFieldNumberMap = new HashMap<>();
        Map<Integer, ProtoBufParsingInfo> writtenSchemaFieldInfoByFieldNumberMap = new HashMap<>();
        Map<String, ProtoBufParsingInfo> writtenSchemaFieldInfoByFieldNameMap = new HashMap<>();

        readSchemaFieldInfoList.forEach(readSchemaFieldInfo -> {
            if (readSchemaFieldInfo.getLabel()
                    .equals(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED.name())) {
                readSchemaRequiredFields.add(readSchemaFieldInfo);
            }
            readSchemaFieldInfoByFieldNumberMap.put(readSchemaFieldInfo.getNumber(), readSchemaFieldInfo);
        });
        writtenSchemaFieldInfoList.forEach(writtenSchemaFieldInfo -> {
            if (writtenSchemaFieldInfo.getLabel()
                    .equals(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED.name())) {
                writtenSchemaRequiredFields.add(writtenSchemaFieldInfo);
            }
            writtenSchemaFieldInfoByFieldNumberMap.put(writtenSchemaFieldInfo.getNumber(), writtenSchemaFieldInfo);
            writtenSchemaFieldInfoByFieldNameMap.put(writtenSchemaFieldInfo.getName(), writtenSchemaFieldInfo);
        });

        if (!ListUtils.isEqualList(readSchemaRequiredFields, writtenSchemaRequiredFields)) {
            throw new ProtoBufCanReadCheckException("Required Fields have been modified.");
        }

        int readSchemaMaxFieldNumber = readSchemaFieldInfoList.stream()
                .mapToInt(ProtoBufParsingInfo::getNumber).max().orElse(0);
        int writtenSchemaMaxFieldNumber = writtenSchemaFieldInfoList.stream()
                .mapToInt(ProtoBufParsingInfo::getNumber).max().orElse(0);
        int maxFieldNumber = Math.max(readSchemaMaxFieldNumber, writtenSchemaMaxFieldNumber);
        readSchemaFieldInfoList = fillUpProtoBufParsingInfoList(readSchemaFieldInfoByFieldNumberMap,
                maxFieldNumber);
        writtenSchemaFieldInfoList = fillUpProtoBufParsingInfoList(writtenSchemaFieldInfoByFieldNumberMap,
                maxFieldNumber);

        ProtoBufParsingInfo readSchemaFieldInfo;
        ProtoBufParsingInfo writtenSchemaFieldInfo;
        for (int i = 0; i < maxFieldNumber; i++) {
            readSchemaFieldInfo = readSchemaFieldInfoList.get(i);
            writtenSchemaFieldInfo = writtenSchemaFieldInfoList.get(i);
            if (readSchemaFieldInfo != null && !readSchemaFieldInfo.equals(writtenSchemaFieldInfo)) {
                if (writtenSchemaFieldInfo != null
                        && readSchemaFieldInfo.getNumber() == writtenSchemaFieldInfo.getNumber()
                        && readSchemaFieldInfo.getName().equals(writtenSchemaFieldInfo.getName())
                        && !readSchemaFieldInfo.getType().equals(writtenSchemaFieldInfo.getType())) {
                    log.warn("The field type for a field with number {} has been changed.",
                            readSchemaFieldInfo.getNumber());
                } else if (writtenSchemaFieldInfoByFieldNameMap.containsKey(readSchemaFieldInfo.getName())
                        && writtenSchemaFieldInfoByFieldNameMap.get(readSchemaFieldInfo.getName()).getNumber()
                        != readSchemaFieldInfo.getNumber()) {
                    throw new ProtoBufCanReadCheckException("The field number of the field have been changed.");
                }  else if (writtenSchemaFieldInfoByFieldNumberMap.containsKey(readSchemaFieldInfo.getNumber())
                        && !writtenSchemaFieldInfoByFieldNumberMap.get(readSchemaFieldInfo.getNumber()).getName()
                        .equals(readSchemaFieldInfo.getName())) {
                    throw new ProtoBufCanReadCheckException("The field name of the field have been changed.");
                } else if (writtenSchemaFieldInfo == null && !readSchemaFieldInfo.isHasDefaultValue()) {
                    throw new ProtoBufCanReadCheckException("No default value fields have been added or removed.");
                }
            } else if (readSchemaFieldInfo != null && readSchemaFieldInfo.equals(writtenSchemaFieldInfo)
                    && (readSchemaFieldInfo.getType()
                    .equals(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM.name())
                    || readSchemaFieldInfo.getType()
                    .equals(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE.name()))) {
                String[] readSchemaFieldEnumName = readSchemaFieldInfo.getTypeName().split("\\.");
                String[] writtenSchemaFieldEnumName = writtenSchemaFieldInfo.getTypeName().split("\\.");
                if (!readSchemaFieldEnumName[readSchemaFieldEnumName.length - 1]
                        .equals(writtenSchemaFieldEnumName[writtenSchemaFieldEnumName.length - 1])) {
                    throw new ProtoBufCanReadCheckException("The field type name have been changed.");
                }
            }
        }
    }

    private static List<ProtoBufParsingInfo> fillUpProtoBufParsingInfoList(
            Map<Integer, ProtoBufParsingInfo> protoBufParsingInfoMap, int maxCapacity) {
        List<ProtoBufParsingInfo> fullProtoBufParsingInfoList = new LinkedList<>();
        for (int i = 0; i < maxCapacity; i++) {
            int currentFieldNumber = i + 1;
            fullProtoBufParsingInfoList.add(protoBufParsingInfoMap.getOrDefault(currentFieldNumber, null));
        }
        return fullProtoBufParsingInfoList;
    }

}
