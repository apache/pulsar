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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

/**
 * This function removes a "field" from a message.
 */
@Slf4j
public class RemoveFieldFunction implements Function<GenericObject, Void>, TransformStep {

    private List<String> keyFields;
    private List<String> valueFields;

    public RemoveFieldFunction() {}

    public RemoveFieldFunction(List<String> keyFields, List<String> valueFields) {
        this.keyFields = keyFields;
        this.valueFields = valueFields;
    }

    @Override
    public void initialize(Context context) {
        this.keyFields = getConfig(context, "key-fields");
        this.valueFields = getConfig(context, "value-fields");
    }

    private List<String> getConfig(Context context, String fieldName) {
        return context.getUserConfigValue(fieldName)
                .map(fields -> {
                    if (fields instanceof String) {
                        return Arrays.asList(((String) fields).split(","));
                    }
                    throw new IllegalArgumentException(fieldName + " must be of type String");
                })
                .orElse(new ArrayList<>());
    }

    @Override
    public Void process(GenericObject genericObject, Context context) throws Exception {
        Record<?> currentRecord = context.getCurrentRecord();
        Schema<?> schema = currentRecord.getSchema();
        Object nativeObject = genericObject.getNativeObject();
        if (log.isDebugEnabled()) {
            log.debug("apply to {} {}", genericObject, nativeObject);
            log.debug("record with schema {} version {} {}", schema,
                    currentRecord.getMessage().get().getSchemaVersion(),
                    currentRecord);
        }

        TransformContext transformContext = new TransformContext(context, nativeObject);
        process(transformContext);
        transformContext.send();
        return null;
    }

    @Override
    public void process(TransformContext transformContext) {
        TransformUtils.dropKeyFields(keyFields, transformContext);
        TransformUtils.dropValueFields(valueFields, transformContext);
    }

    public static RemoveFieldFunction of(Map<String, String> step) {
        String fields = step.get("fields");
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("missing required 'fields' parameter");
        }
        List<String> fieldList = Arrays.asList(fields.split(","));
        String part = step.get("part");
        if (part == null) {
            return new RemoveFieldFunction(fieldList, fieldList);
        } else if (part.equals("key")) {
            return new RemoveFieldFunction(fieldList, new ArrayList<>());
        } else if (part.equals("value")) {
            return new RemoveFieldFunction(new ArrayList<>(), fieldList);
        } else {
            throw new IllegalArgumentException("invalid 'part' parameter: " + part);
        }
    }
}