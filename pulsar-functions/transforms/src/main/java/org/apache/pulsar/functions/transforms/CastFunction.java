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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;

@Slf4j
public class CastFunction extends AbstractTransformStepFunction {

    private SchemaType keySchemaType;
    private SchemaType valueSchemaType;

    public CastFunction() {}

    public CastFunction(SchemaType keySchemaType, SchemaType valueSchemaType) {
        this.keySchemaType = keySchemaType;
        this.valueSchemaType = valueSchemaType;
    }

    @Override
    public void initialize(Context context) {
        keySchemaType = getConfig(context, "key-schema-type");
        valueSchemaType = getConfig(context, "value-schema-type");
    }

    private SchemaType getConfig(Context context, String fieldName) {
        return context.getUserConfigValue(fieldName)
                .map(fields -> {
                    if (fields instanceof String) {
                        return SchemaType.valueOf((String) fields);
                    }
                    throw new IllegalArgumentException(fieldName + " must be of type String");
                })
                .orElse(null);
    }

    @Override
    public void process(TransformContext transformContext) {
        if (transformContext.getKeySchema() != null) {
            Object outputKeyObject = transformContext.getKeyObject();
            Schema<?> outputSchema = transformContext.getKeySchema();
            if (keySchemaType == SchemaType.STRING) {
                outputSchema = Schema.STRING;
                outputKeyObject = outputKeyObject.toString();
            }
            transformContext.setKeySchema(outputSchema);
            transformContext.setKeyObject(outputKeyObject);
        }
        if (valueSchemaType == SchemaType.STRING) {
            transformContext.setValueSchema(Schema.STRING);
            transformContext.setValueObject(transformContext.getValueObject().toString());
        }
    }
}
