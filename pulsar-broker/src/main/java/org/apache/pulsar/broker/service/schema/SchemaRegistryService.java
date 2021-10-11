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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.broker.service.schema.validator.SchemaRegistryServiceWithSchemaDataValidator;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SchemaRegistryService extends SchemaRegistry {
    Logger LOG = LoggerFactory.getLogger(SchemaRegistryService.class);
    long NO_SCHEMA_VERSION = -1L;

    static Map<SchemaType, SchemaCompatibilityCheck> getCheckers(Set<String> checkerClasses) throws Exception {
        Map<SchemaType, SchemaCompatibilityCheck> checkers = Maps.newHashMap();
        for (String className : checkerClasses) {
            final Class<?> checkerClass = Class.forName(className);
            SchemaCompatibilityCheck instance = (SchemaCompatibilityCheck) checkerClass
                    .getDeclaredConstructor().newInstance();
            checkers.put(instance.getSchemaType(), instance);
        }
        return checkers;
    }

    static SchemaRegistryService create(SchemaStorage schemaStorage, Set<String> schemaRegistryCompatibilityCheckers) {
        if (schemaStorage != null) {
            try {
                Map<SchemaType, SchemaCompatibilityCheck> checkers = getCheckers(schemaRegistryCompatibilityCheckers);
                checkers.put(SchemaType.KEY_VALUE, new KeyValueSchemaCompatibilityCheck(checkers));
                return SchemaRegistryServiceWithSchemaDataValidator.of(
                        new SchemaRegistryServiceImpl(schemaStorage, checkers));
            } catch (Exception e) {
                LOG.warn("Unable to create schema registry storage, defaulting to empty storage", e);
            }
        }
        return new DefaultSchemaRegistryService();
    }

    void close() throws Exception;
}
