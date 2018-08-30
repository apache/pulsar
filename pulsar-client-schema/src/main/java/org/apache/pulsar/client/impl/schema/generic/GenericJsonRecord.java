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
package org.apache.pulsar.client.impl.schema.generic;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;

/**
 * Generic json record.
 */
class GenericJsonRecord implements GenericRecord {

    private final List<Field> fields;
    private final Map<String, Object> values;

    GenericJsonRecord(List<Field> fields,
                      Map<String, Object> values) {
        this.fields = fields;
        this.values = values;
    }

    Map<String, Object> asJson() {
        return values;
    }

    @Override
    public List<Field> getFields() {
        return fields;
    }

    @Override
    public Object getField(String fieldName) {
        Object obj = values.get(fieldName);
        if (obj instanceof Map) {
            Map<String, Object> objValues = (Map<String, Object>) obj;
            AtomicInteger idx = new AtomicInteger(0);
            List<Field> fields = objValues.keySet()
                .stream()
                .map(f -> new Field(f, idx.getAndIncrement()))
                .collect(Collectors.toList());
            return new GenericJsonRecord(fields, objValues);
        }
        return obj;
    }
}
