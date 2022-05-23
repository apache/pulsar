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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;

@Slf4j
public class TransformFunction extends AbstractTransformStepFunction {

    private final List<TransformStep> steps = new ArrayList<>();
    private final Gson gson = new Gson();

    @Override
    public void initialize(Context context) {
        Object config = context.getUserConfigValue("steps")
                .orElseThrow(() -> new IllegalArgumentException("missing required 'steps' parameter"));
        LinkedList<Map<String, Object>> stepsConfig;
        try {
            TypeToken<LinkedList<Map<String, Object>>> typeToken = new TypeToken<LinkedList<Map<String, Object>>>(){};
            stepsConfig = gson.fromJson((gson.toJson(config)), typeToken.getType());
        } catch (Exception e) {
            throw new IllegalArgumentException("could not parse configuration", e);
        }
        for (Map<String, Object> step : stepsConfig) {
            String type = getRequiredStringConfig(step, "type");
            switch (type) {
                case "drop-fields":
                    steps.add(newRemoveFieldFunction(step));
                    break;
                case "cast":
                    steps.add(newCastFunction(step));
                    break;
                case "merge-key-value":
                    steps.add(new MergeKeyValueFunction());
                    break;
                case "unwrap-key-value":
                    steps.add(newUnwrapKeyValueFunction(step));
                    break;
                default:
                    throw new IllegalArgumentException("invalid step type: " + type);
            }
        }

    }

    @Override
    public void process(TransformContext transformContext) throws Exception {
        for (TransformStep step : steps) {
            step.process(transformContext);
        }
    }

    public static DropFieldFunction newRemoveFieldFunction(Map<String, Object> step) {
        String fields = getRequiredStringConfig(step, "fields");
        List<String> fieldList = Arrays.asList(fields.split(","));
        String part = getStringConfig(step, "part");
        if (part == null) {
            return new DropFieldFunction(fieldList, fieldList);
        } else if (part.equals("key")) {
            return new DropFieldFunction(fieldList, new ArrayList<>());
        } else if (part.equals("value")) {
            return new DropFieldFunction(new ArrayList<>(), fieldList);
        } else {
            throw new IllegalArgumentException("invalid 'part' parameter: " + part);
        }
    }

    public static CastFunction newCastFunction(Map<String, Object> step) {
        String schemaTypeParam = getRequiredStringConfig(step, "schema-type");
        SchemaType schemaType = SchemaType.valueOf(schemaTypeParam);
        String part = getStringConfig(step, "part");
        if (part == null) {
            return new CastFunction(schemaType, schemaType);
        } else if (part.equals("key")) {
            return new CastFunction(schemaType, null);
        } else if (part.equals("value")) {
            return new CastFunction(null, schemaType);
        } else {
            throw new IllegalArgumentException("invalid 'part' parameter: " + part);
        }
    }

    private static UnwrapKeyValueFunction newUnwrapKeyValueFunction(Map<String, Object> step) {
        Boolean unwrapKey = getBooleanConfig(step, "unwrap-key");
        return new UnwrapKeyValueFunction(unwrapKey != null && unwrapKey);
    }

    private static String getStringConfig(Map<String, Object> config, String fieldName) {
        Object fieldObject = config.get(fieldName);
        if (fieldObject == null) {
            return null;
        }
        if (fieldObject instanceof String) {
            return (String) fieldObject;
        }
        throw new IllegalArgumentException("field '" + fieldName + "' must be a string");
    }

    private static String getRequiredStringConfig(Map<String, Object> config, String fieldName) {
        Object fieldObject = config.get(fieldName);
        if (fieldObject == null) {
            throw new IllegalArgumentException("missing required '" + fieldName + "' parameter");
        }
        if (fieldObject instanceof String) {
            String field = (String) fieldObject;
            if (field.isEmpty()) {
                throw new IllegalArgumentException("field '" + fieldName + "' must not be empty");
            }
            return field;
        }
        throw new IllegalArgumentException("field '" + fieldName + "' must be a string");
    }

    private static Boolean getBooleanConfig(Map<String, Object> config, String fieldName) {
        Object fieldObject = config.get(fieldName);
        if (fieldObject == null) {
            return null;
        }
        if (fieldObject instanceof Boolean) {
            return (Boolean) fieldObject;
        }
        throw new IllegalArgumentException("field '" + fieldName + "' must be a boolean");
    }
}
