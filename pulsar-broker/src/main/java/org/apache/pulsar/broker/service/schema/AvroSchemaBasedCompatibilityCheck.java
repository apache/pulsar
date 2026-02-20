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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.Collections;
import java.util.LinkedList;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.validator.StructSchemaDataValidator;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;

/**
 * The abstract implementation of {@link SchemaCompatibilityCheck} using Avro Schema.
 */
@Slf4j
abstract class AvroSchemaBasedCompatibilityCheck implements SchemaCompatibilityCheck {

    @Override
    public void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        checkCompatible(Collections.singletonList(from), to, strategy);
    }

    @Override
    public void checkCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        LinkedList<Schema> fromList = new LinkedList<>();
        checkArgument(from != null, "check compatibility list is null");
        try {
            for (SchemaData schemaData : from) {
                Schema.Parser parser =
                        new Schema.Parser(StructSchemaDataValidator.COMPATIBLE_NAME_VALIDATOR);
                parser.setValidateDefaults(false);
                fromList.addFirst(parser.parse(new String(schemaData.getData(), UTF_8)));
            }
            Schema.Parser parser = new Schema.Parser(StructSchemaDataValidator.COMPATIBLE_NAME_VALIDATOR);
            parser.setValidateDefaults(false);
            Schema toSchema = parser.parse(new String(to.getData(), UTF_8));
            SchemaValidator schemaValidator = createSchemaValidator(strategy);
            schemaValidator.validate(toSchema, fromList);
        } catch (SchemaParseException e) {
            log.warn("Error during schema parsing: {}", e.getMessage());
            throw new IncompatibleSchemaException(e);
        } catch (SchemaValidationException e) {
            String msg = String.format("Error during schema compatibility check with strategy %s: %s: %s",
                    strategy, e.getClass().getName(), e.getMessage());
            log.warn(msg);
            throw new IncompatibleSchemaException(msg, e);
        }
    }

    static SchemaValidator createSchemaValidator(SchemaCompatibilityStrategy compatibilityStrategy) {
        final SchemaValidatorBuilder validatorBuilder = new SchemaValidatorBuilder();
        switch (compatibilityStrategy) {
            case BACKWARD_TRANSITIVE:
                return createLatestOrAllValidator(validatorBuilder.canReadStrategy(), false);
            case BACKWARD:
                return createLatestOrAllValidator(validatorBuilder.canReadStrategy(), true);
            case FORWARD_TRANSITIVE:
                return createLatestOrAllValidator(validatorBuilder.canBeReadStrategy(), false);
            case FORWARD:
                return createLatestOrAllValidator(validatorBuilder.canBeReadStrategy(), true);
            case FULL_TRANSITIVE:
                return createLatestOrAllValidator(validatorBuilder.mutualReadStrategy(), false);
            case FULL:
                return createLatestOrAllValidator(validatorBuilder.mutualReadStrategy(), true);
            case ALWAYS_COMPATIBLE:
                return AlwaysSchemaValidator.INSTANCE;
            default:
                return NeverSchemaValidator.INSTANCE;
        }
    }

    static SchemaValidator createLatestOrAllValidator(SchemaValidatorBuilder validatorBuilder, boolean onlyLatest) {
        return onlyLatest ? validatorBuilder.validateLatest() : validatorBuilder.validateAll();
    }
}
