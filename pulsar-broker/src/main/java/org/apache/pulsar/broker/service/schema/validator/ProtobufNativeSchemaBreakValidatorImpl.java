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
package org.apache.pulsar.broker.service.schema.validator;

import com.google.protobuf.Descriptors;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.schema.ProtobufNativeSchemaBreakCheckUtils;
import org.apache.pulsar.broker.service.schema.exceptions.ProtoBufCanReadCheckException;

@Slf4j
public class ProtobufNativeSchemaBreakValidatorImpl implements ProtobufNativeSchemaValidator {
    private final ProtobufNativeSchemaValidationStrategy strategy;
    private final boolean onlyValidateLatest;

    public ProtobufNativeSchemaBreakValidatorImpl(ProtobufNativeSchemaValidationStrategy strategy,
                                                  boolean onlyValidateLatest) {
        this.strategy = strategy;
        this.onlyValidateLatest = onlyValidateLatest;
    }

    @Override
    public void validate(Iterable<Descriptors.Descriptor> fromDescriptor, Descriptors.Descriptor toDescriptor)
            throws ProtoBufCanReadCheckException {
        if (onlyValidateLatest) {
            validateLatest(fromDescriptor, toDescriptor);
        } else {
            validateAll(fromDescriptor, toDescriptor);
        }
    }

    private void validateAll(Iterable<Descriptors.Descriptor> fromDescriptor, Descriptors.Descriptor toDescriptor)
            throws ProtoBufCanReadCheckException {
        for (Descriptors.Descriptor existing : fromDescriptor) {
            validateWithStrategy(toDescriptor, existing);
        }
    }

    private void validateLatest(Iterable<Descriptors.Descriptor> fromDescriptor, Descriptors.Descriptor toDescriptor)
            throws ProtoBufCanReadCheckException {
        Iterator<Descriptors.Descriptor> schemas = fromDescriptor.iterator();
        if (schemas.hasNext()) {
            Descriptors.Descriptor existing = schemas.next();
            validateWithStrategy(toDescriptor, existing);
        }
    }

    private void validateWithStrategy(Descriptors.Descriptor toValidate, Descriptors.Descriptor fromDescriptor)
            throws ProtoBufCanReadCheckException {
        switch (strategy) {
            case CanReadExistingStrategy -> canRead(fromDescriptor, toValidate);
            case CanBeReadByExistingStrategy -> canRead(toValidate, fromDescriptor);
            case CanBeReadMutualStrategy -> {
                canRead(toValidate, fromDescriptor);
                canRead(fromDescriptor, toValidate);
            }
        }
    }

    private void canRead(Descriptors.Descriptor writtenSchema, Descriptors.Descriptor readSchema)
            throws ProtoBufCanReadCheckException {
        ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writtenSchema, readSchema);
    }
}
