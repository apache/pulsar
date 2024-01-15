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
import static com.google.protobuf.Descriptors.Descriptor;
import java.util.Collections;
import java.util.LinkedList;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.ProtoBufCanReadCheckException;
import org.apache.pulsar.broker.service.schema.validator.ProtobufNativeAlwaysCompatibleValidator;
import org.apache.pulsar.broker.service.schema.validator.ProtobufNativeNeverCompatibleValidator;
import org.apache.pulsar.broker.service.schema.validator.ProtobufNativeSchemaValidationStrategy;
import org.apache.pulsar.broker.service.schema.validator.ProtobufNativeSchemaValidator;
import org.apache.pulsar.broker.service.schema.validator.ProtobufNativeSchemaValidatorBuilder;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * The {@link SchemaCompatibilityCheck} implementation for {@link SchemaType#PROTOBUF_NATIVE}.
 */
@Slf4j
public class ProtobufNativeSchemaCompatibilityCheck implements SchemaCompatibilityCheck {

    private String schemaValidatorClassName;

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.PROTOBUF_NATIVE;
    }

    @Override
    public void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        checkCompatible(Collections.singletonList(from), to, strategy);
    }

    @Override
    public void checkCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        checkArgument(from != null, "check compatibility list is null");
        LinkedList<Descriptor> fromList = new LinkedList<>();
        try {
            for (SchemaData schemaData : from) {
                fromList.addFirst(ProtobufNativeSchemaUtils.deserialize(schemaData.getData()));
            }
            Descriptor toDescriptor = ProtobufNativeSchemaUtils.deserialize(to.getData());
            ProtobufNativeSchemaValidator schemaValidator = createSchemaValidator(strategy);
            schemaValidator.validate(fromList, toDescriptor);
        } catch (ProtoBufCanReadCheckException e) {
            throw new IncompatibleSchemaException(e);
        }
    }

    private ProtobufNativeSchemaValidator createSchemaValidator(SchemaCompatibilityStrategy compatibilityStrategy) {
        final ProtobufNativeSchemaValidatorBuilder schemaValidatorBuilder = new ProtobufNativeSchemaValidatorBuilder()
                .validatorClassName(schemaValidatorClassName);
        return switch (compatibilityStrategy) {
            case BACKWARD_TRANSITIVE -> schemaValidatorBuilder
                    .validatorStrategy(ProtobufNativeSchemaValidationStrategy.CanReadExistingStrategy)
                    .isOnlyValidateLatest(false).build();
            case BACKWARD -> schemaValidatorBuilder
                    .validatorStrategy(ProtobufNativeSchemaValidationStrategy.CanReadExistingStrategy)
                    .isOnlyValidateLatest(true).build();
            case FORWARD_TRANSITIVE -> schemaValidatorBuilder
                    .validatorStrategy(ProtobufNativeSchemaValidationStrategy.CanBeReadByExistingStrategy)
                    .isOnlyValidateLatest(false).build();
            case FORWARD -> schemaValidatorBuilder
                    .validatorStrategy(ProtobufNativeSchemaValidationStrategy.CanBeReadByExistingStrategy)
                    .isOnlyValidateLatest(true).build();
            case FULL_TRANSITIVE -> schemaValidatorBuilder
                    .validatorStrategy(ProtobufNativeSchemaValidationStrategy.CanBeReadMutualStrategy)
                    .isOnlyValidateLatest(false).build();
            case FULL -> schemaValidatorBuilder
                    .validatorStrategy(ProtobufNativeSchemaValidationStrategy.CanBeReadMutualStrategy)
                    .isOnlyValidateLatest(true).build();
            case ALWAYS_COMPATIBLE -> ProtobufNativeAlwaysCompatibleValidator.INSTANCE;
            default -> ProtobufNativeNeverCompatibleValidator.INSTANCE;
        };
    }

    public void setProtobufNativeSchemaValidatorClassName(String schemaValidatorClassName) {
        this.schemaValidatorClassName = schemaValidatorClassName;
    }

}
