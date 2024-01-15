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

import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.Reflections;

public class ProtobufNativeSchemaValidatorBuilder {
    private ProtobufNativeSchemaValidationStrategy strategy;
    private boolean onlyValidateLatest;
    private String validatorClassName;

    public ProtobufNativeSchemaValidatorBuilder validatorStrategy(
            @NonNull ProtobufNativeSchemaValidationStrategy strategy) {
        this.strategy = strategy;
        return this;
    }

    public ProtobufNativeSchemaValidatorBuilder isOnlyValidateLatest(boolean onlyValidateLatest) {
        this.onlyValidateLatest = onlyValidateLatest;
        return this;
    }

    public ProtobufNativeSchemaValidatorBuilder validatorClassName(String validatorClassName) {
        this.validatorClassName = validatorClassName;
        return this;
    }

    public ProtobufNativeSchemaValidator build() {
        if (StringUtils.isBlank(validatorClassName)) {
            return ProtobufNativeSchemaValidator.DEFAULT;
        } else {
            Object[] params = {strategy, onlyValidateLatest};
            Class[] paramTypes = {ProtobufNativeSchemaValidationStrategy.class, boolean.class};
            return (ProtobufNativeSchemaValidator) Reflections.createInstance(validatorClassName,
                    Thread.currentThread().getContextClassLoader(), params, paramTypes);
        }
    }
}
