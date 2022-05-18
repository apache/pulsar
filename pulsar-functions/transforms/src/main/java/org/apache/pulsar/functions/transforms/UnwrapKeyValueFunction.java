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
package org.apache.pulsar.functions.transforms;

import org.apache.pulsar.functions.api.Context;

public class UnwrapKeyValueFunction extends AbstractTransformStepFunction {

    boolean unwrapKey;

    public UnwrapKeyValueFunction() {}

    public UnwrapKeyValueFunction(boolean unwrapKey) {
        this.unwrapKey = unwrapKey;
    }

    @Override
    public void initialize(Context context) {
        this.unwrapKey = (Boolean) context.getUserConfigValue("unwrapKey")
                .map(value -> {
                    if (!(value instanceof Boolean)) {
                        throw new IllegalArgumentException("unwrapKey param must be of type Boolean");
                    }
                    return value;
                })
                .orElse(false);
    }

    @Override
    public void process(TransformContext transformContext) throws Exception {
        if (transformContext.getKeySchema() != null) {
            if (unwrapKey) {
                transformContext.setValueSchema(transformContext.getKeySchema());
                transformContext.setValueObject(transformContext.getKeyObject());
            }
            // TODO: can we avoid making the conversion to NATIVE_AVRO ?
            transformContext.setValueModified(true);
            transformContext.setKeySchema(null);
            transformContext.setKeyObject(null);
        }
    }
}
