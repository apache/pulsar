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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
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
    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> schemaCache = new HashMap<>();

    public RemoveFieldFunction() {}

    public RemoveFieldFunction(List<String> keyFields, List<String> valueFields) {
        this.keyFields = keyFields;
        this.valueFields = valueFields;
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
        dropKeyFields(keyFields, transformContext);
        dropValueFields(valueFields, transformContext);
    }

    public void dropValueFields(List<String> fields, TransformContext record) {
        if (record.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            org.apache.avro.generic.GenericRecord newRecord =
                    dropFields(fields, (org.apache.avro.generic.GenericRecord) record.getValueObject());
            record.setValueModified(true);
            record.setValueObject(newRecord);
        }
    }

    public void dropKeyFields(List<String> fields, TransformContext record) {
        if (record.getKeyObject() != null && record.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            org.apache.avro.generic.GenericRecord newRecord =
                    dropFields(fields, (org.apache.avro.generic.GenericRecord) record.getKeyObject());
            record.setKeyModified(true);
            record.setKeyObject(newRecord);
        }
    }

    private org.apache.avro.generic.GenericRecord dropFields(List<String> fields,
                                                                    org.apache.avro.generic.GenericRecord record) {
        org.apache.avro.Schema avroSchema = record.getSchema();
        org.apache.avro.Schema modified = schemaCache.get(avroSchema);
        if (modified != null || fields.stream().anyMatch(field -> avroSchema.getField(field) != null)) {
            if (modified == null) {
                modified = org.apache.avro.Schema.createRecord(
                        avroSchema.getName(), avroSchema.getDoc(), avroSchema.getNamespace(), avroSchema.isError(),
                        avroSchema.getFields()
                                .stream()
                                .filter(f -> !fields.contains(f.name()))
                                .map(f -> new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(),
                                        f.defaultVal(),
                                        f.order()))
                                .collect(Collectors.toList()));
                schemaCache.put(avroSchema, modified);
            }

            org.apache.avro.generic.GenericRecord newRecord = new GenericData.Record(modified);
            for (org.apache.avro.Schema.Field field : modified.getFields()) {
                newRecord.put(field.name(), record.get(field.name()));
            }
            return newRecord;
        }
        return record;
    }
}