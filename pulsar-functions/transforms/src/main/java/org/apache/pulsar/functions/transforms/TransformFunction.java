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
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;


/**
 * <code>TransformFunction</code> is a {@link Function} that provides an easy way to apply a set of usual basic
 * transformations to the data.
 * <p>
 * It provides the following transformations:
 * <ul>
 * <li><code>cast</code>: modifies the key or value schema to a target compatible schema passed in the
 * <code>schema-type</code> argument.
 * This PR only enables <code>STRING</code> schema-type.
 * The <code>part</code> argument allows to choose on which part to apply between <code>key</code> and
 * <code>value</code>.
 * If <code>part</code> is null or absent the transformations applies to both the key and value.</li>
 * <li><code>drop-fields</code>: drops fields given as a string list in parameter <code>fields</code>.
 * The <code>part</code> argument allows to choose on which part to apply between <code>key</code> and
 * <code>value</code>.
 * If <code>part</code> is null or absent the transformations applies to both the key and value.
 * Currently only AVRO is supported.</li>
 * <li><code>merge-key-value</code>: merges the fields of KeyValue records where both the key and value are
 * structured types of the same schema type. Currently only AVRO is supported.</li>
 * <li><code>unwrap-key-value</code>: if the record is a KeyValue, extract the KeyValue's value and make it the
 * record value. If parameter <code>unwrapKey</code> is present and set to <code>true</code>, extract the
 * KeyValue's key instead.</li>
 * </ul>
 * <p>
 * The <code>TransformFunction</code> reads its configuration as Json from the {@link Context} <code>userConfig</code>
 * in the format:
 *<pre><code class="lang-json">
 * {
 *   "steps": [
 *     {
 *       "type": "drop-fields", "fields": "keyField1,keyField2", "part": "key"
 *     },
 *     {
 *       "type": "merge-key-value"
 *     },
 *     {
 *       "type": "unwrap-key-value"
 *     },
 *     {
 *       "type": "cast", "schema-type": "STRING"
 *     }
 *   ]
 * }
 * </code></pre>
 * @see <a href="https://github.com/apache/pulsar/issues/15902">
 *     PIP-173 : Create a built-in Function implementing the most common basic transformations</a>
 */
@Slf4j
public class TransformFunction implements Function<GenericObject, Void>, TransformStep {

    private final List<TransformStep> steps = new ArrayList<>();
    private final Gson gson = new Gson();

    @Override
    public void initialize(Context context) {
        Object config = context.getUserConfigValue("steps")
                .orElseThrow(() -> new IllegalArgumentException("missing required 'steps' parameter"));
        LinkedList<Map<String, Object>> stepsConfig;
        try {
            TypeToken<LinkedList<Map<String, Object>>> typeToken = new TypeToken<>() {
            };
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
                    steps.add(new MergeKeyValueStep());
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
    public Void process(GenericObject input, Context context) throws Exception {
        Object nativeObject = input.getNativeObject();
        if (log.isDebugEnabled()) {
            Record<?> currentRecord = context.getCurrentRecord();
            log.debug("apply to {} {}", input, nativeObject);
            log.debug("record with schema {} version {} {}", currentRecord.getSchema(),
                    currentRecord.getMessage().get().getSchemaVersion(),
                    currentRecord);
        }

        TransformContext transformContext = new TransformContext(context, nativeObject);
        process(transformContext);
        transformContext.send();
        return null;
    }

    @Override
    public void process(TransformContext transformContext) throws Exception {
        for (TransformStep step : steps) {
            step.process(transformContext);
        }
    }

    public static DropFieldStep newRemoveFieldFunction(Map<String, Object> step) {
        String fields = getRequiredStringConfig(step, "fields");
        List<String> fieldList = Arrays.asList(fields.split(","));
        String part = getStringConfig(step, "part");
        if (part == null) {
            return new DropFieldStep(fieldList, fieldList);
        } else if (part.equals("key")) {
            return new DropFieldStep(fieldList, new ArrayList<>());
        } else if (part.equals("value")) {
            return new DropFieldStep(new ArrayList<>(), fieldList);
        } else {
            throw new IllegalArgumentException("invalid 'part' parameter: " + part);
        }
    }

    public static CastStep newCastFunction(Map<String, Object> step) {
        String schemaTypeParam = getRequiredStringConfig(step, "schema-type");
        SchemaType schemaType = SchemaType.valueOf(schemaTypeParam);
        String part = getStringConfig(step, "part");
        if (part == null) {
            return new CastStep(schemaType, schemaType);
        } else if (part.equals("key")) {
            return new CastStep(schemaType, null);
        } else if (part.equals("value")) {
            return new CastStep(null, schemaType);
        } else {
            throw new IllegalArgumentException("invalid 'part' parameter: " + part);
        }
    }

    private static UnwrapKeyValueStep newUnwrapKeyValueFunction(Map<String, Object> step) {
        Boolean unwrapKey = getBooleanConfig(step, "unwrap-key");
        return new UnwrapKeyValueStep(unwrapKey != null && unwrapKey);
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
