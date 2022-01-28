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

import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

public interface SchemaCompatibilityCheck {

    SchemaType getSchemaType();

    /**
     * @param from     the current schema i.e. schema that the broker has
     * @param to       the future schema i.e. the schema sent by the producer
     * @param strategy the strategy to use when comparing schemas
     */
    void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException;

    /**
     * @param from     the current schemas i.e. schemas that the broker has
     * @param to       the future schema i.e. the schema sent by the producer
     * @param strategy the strategy to use when comparing schemas
     */
    void checkCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException;

    default boolean isCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        try {
            checkCompatible(from, to, strategy);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    default boolean isCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy) {
        try {
            checkCompatible(from, to, strategy);
            return true;
        } catch (IncompatibleSchemaException e) {
            return false;
        }
    }

    SchemaCompatibilityCheck DEFAULT = new SchemaCompatibilityCheck() {

        @Override
        public SchemaType getSchemaType() {
            return SchemaType.NONE;
        }

        @Override
        public void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
                throws IncompatibleSchemaException {
            if (strategy == SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE) {
                throw new IncompatibleSchemaException("Schema compatibility strategy is ALWAYS_INCOMPATIBLE");
            }
        }

        @Override
        public void checkCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy)
                throws IncompatibleSchemaException {
            if (strategy == SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE) {
                throw new IncompatibleSchemaException("Schema compatibility strategy is ALWAYS_INCOMPATIBLE");
            }
        }

    };
}
