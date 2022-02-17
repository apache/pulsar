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

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.schema.validator.SchemaRegistryServiceWithSchemaDataValidator;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SchemaRegistryService extends SchemaRegistry {
    Logger LOG = LoggerFactory.getLogger(SchemaRegistryService.class);
    long NO_SCHEMA_VERSION = -1L;

    static SchemaRegistryService create(SchemaStorage schemaStorage, String schemaRegistryClassName) {
        if (schemaStorage != null) {
            try {
                SchemaRegistryService schemaRegistryService = (SchemaRegistryService) Class
                      .forName(schemaRegistryClassName).getDeclaredConstructor()
                      .newInstance();

                return SchemaRegistryServiceWithSchemaDataValidator.of(schemaRegistryService);
            } catch (Exception e) {
                LOG.warn("Unable to create schema registry storage, defaulting to empty storage", e);
            }
        }
        return new DefaultSchemaRegistryService();
    }

    void initialize(ServiceConfiguration configuration, SchemaStorage schemaStorage) throws PulsarServerException;

    void close() throws Exception;
}
