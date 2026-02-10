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
package org.apache.pulsar.io.cassandra.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

public class StringRecordWrapper extends RecordWrapper<String> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private Map<String, Object> valuesMap;

    public StringRecordWrapper(String jsonString) {
        super(jsonString);
        try {
            valuesMap = MAPPER.readValue(jsonString, Map.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Object get(TableMetadataProvider.ColumnId column) {
        return getValueAsExpectedType(valuesMap.get(column.getName()), column);
    }

    @Override
    public boolean containsKey(String name) {
        return valuesMap.containsKey(name);
    }
}
