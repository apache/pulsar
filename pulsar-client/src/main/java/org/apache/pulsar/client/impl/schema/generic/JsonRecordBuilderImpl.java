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
package org.apache.pulsar.client.impl.schema.generic;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;

public class JsonRecordBuilderImpl implements GenericRecordBuilder {

    private final GenericSchemaImpl genericSchema;
    private Map<String, Object> map = new HashMap<>();

    public JsonRecordBuilderImpl(GenericSchemaImpl genericSchema) {
        this.genericSchema = genericSchema;
    }

    /**
     * Sets the value of a field.
     *
     * @param fieldName the name of the field to set.
     * @param value     the value to set.
     * @return a reference to the RecordBuilder.
     */
    @Override
    public GenericRecordBuilder set(String fieldName, Object value) {
        if (value instanceof GenericRecord) {
            if (!(value instanceof GenericJsonRecord)) {
                throw new IllegalArgumentException("JSON Record Builder doesn't support non-JSON record as a field");
            }
            GenericJsonRecord genericJsonRecord = (GenericJsonRecord) value;
            value = genericJsonRecord.getJsonNode();
        }

        map.put(fieldName, value);
        return this;
    }

    /**
     * Sets the value of a field.
     *
     * @param field the field to set.
     * @param value the value to set.
     * @return a reference to the RecordBuilder.
     */
    @Override
    public GenericRecordBuilder set(Field field, Object value) {
        set(field.getName(), value);
        return this;
    }

    /**
     * Clears the value of the given field.
     *
     * @param fieldName the name of the field to clear.
     * @return a reference to the RecordBuilder.
     */
    @Override
    public GenericRecordBuilder clear(String fieldName) {
        map.remove(fieldName);
        return this;
    }

    /**
     * Clears the value of the given field.
     *
     * @param field the field to clear.
     * @return a reference to the RecordBuilder.
     */
    @Override
    public GenericRecordBuilder clear(Field field) {
        clear(field.getName());
        return this;
    }

    @Override
    public GenericRecord build() {
        JsonNode jn = ObjectMapperFactory.getMapperWithIncludeAlways().getObjectMapper().valueToTree(map);
        return new GenericJsonRecord(
                null,
                genericSchema.getFields(),
                jn,
                null
                );
    }
}
