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
package org.apache.pulsar.broker.service.schema.validator;

import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.common.protocol.schema.SchemaData;

/**
 * Validate if the primitive schema is in expected form.
 */
class PrimitiveSchemaDataValidator implements SchemaDataValidator {

    public static PrimitiveSchemaDataValidator of() {
        return INSTANCE;
    }

    private static final PrimitiveSchemaDataValidator INSTANCE = new PrimitiveSchemaDataValidator();

    private PrimitiveSchemaDataValidator() {}

    @Override
    public void validate(SchemaData schemaData) throws InvalidSchemaDataException {
        byte[] data = schemaData.getData();
        if (null != data && data.length > 0) {
            throw new InvalidSchemaDataException("Invalid schema definition data for primitive schemas :"
                + "length of schema data should be zero, but " + data.length + " bytes is found");
        }
    }
}
