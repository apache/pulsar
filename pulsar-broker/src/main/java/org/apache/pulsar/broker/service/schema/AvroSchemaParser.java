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
package org.apache.pulsar.broker.service.schema;

import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

import org.apache.avro.Schema;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@UtilityClass
public class AvroSchemaParser {

    private static final String NAMESPACE_MARKER = "NS";

    static Schema parseSchema(byte[] serializedSchema) {
        Schema.Parser parser = new Schema.Parser();
        String schemaDef = removeNamespace(serializedSchema);
        return parser.parse(schemaDef);
    }

    /**
     * Remove the namespace attributes from the Avro schema definition.
     */
    @SneakyThrows
    static String removeNamespace(byte[] serializedSchema) {
        Map<String, Object> schemaDef = ObjectMapperFactory.getThreadLocal().readValue(serializedSchema,
                new TypeReference<TreeMap<String, Object>>() {
                });
        removeNamespace(schemaDef);
        return ObjectMapperFactory.getThreadLocal().writeValueAsString(schemaDef);
    }

    /**
     * Recursively remove the namespace from schema definition
     */
    @SuppressWarnings("unchecked")
    private static void removeNamespace(Map<String, Object> schemaDef) {
        if ("record".equals(schemaDef.get("type"))) {
            schemaDef.put("namespace", NAMESPACE_MARKER);
            Object fieldsObj = schemaDef.get("fields");
            if (fieldsObj instanceof List) {
                List<Map<String, Object>> fields = (List<Map<String, Object>>) fieldsObj;
                fields.forEach(field -> removeNamespace(field));
            }
        }
    }
}
