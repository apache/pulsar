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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.schema.Field;

/**
 * Generic json record.
 */
public class GenericJsonRecord extends VersionedGenericRecord {

    private final JsonNode jn;

    GenericJsonRecord(byte[] schemaVersion,
                      List<Field> fields,
                      JsonNode jn) {
        super(schemaVersion, fields);
        this.jn = jn;
    }

    public JsonNode getJsonNode() {
        return jn;
    }

    @Override
    public Object getField(String fieldName) {
        JsonNode fn = jn.get(fieldName);
        if (fn.isContainerNode()) {
            AtomicInteger idx = new AtomicInteger(0);
            List<Field> fields = Lists.newArrayList(fn.fieldNames())
                .stream()
                .map(f -> new Field(f, idx.getAndIncrement()))
                .collect(Collectors.toList());
            return new GenericJsonRecord(schemaVersion, fields, fn);
        } else if (fn.isBoolean()) {
            return fn.asBoolean();
        } else if (fn.isFloatingPointNumber()) {
            return fn.asDouble();
        } else if (fn.isBigInteger()) {
            if (fn.canConvertToLong()) {
                return fn.asLong();
            } else {
                return fn.asText();
            }
        } else if (fn.isNumber()) {
            return fn.numberValue();
        } else {
            return fn.asText();
        }
    }
}
