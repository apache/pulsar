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

import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

public class ExternalSchemaCompatibilityCheck implements SchemaCompatibilityCheck {

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.EXTERNAL;
    }

    @Override
    public void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        if (strategy == SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE) {
            return;
        }
        if ((SchemaType.EXTERNAL.equals(from.getType()) || SchemaType.EXTERNAL.equals(to.getType()))
                && !from.getType().equals(to.getType())) {
            throw new IncompatibleSchemaException("External schema is not compatible with the other schema types.");
        }
    }

    @Override
    public void checkCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        if (strategy == SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE) {
            return;
        }
        while (from.iterator().hasNext()) {
            SchemaData fromSchema = from.iterator().next();
            checkCompatible(fromSchema, to, strategy);
        }
    }

}
