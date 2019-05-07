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
package org.apache.pulsar.client.impl.schema.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaReader;

import java.io.IOException;

public class JsonReader<T> implements SchemaReader<T> {
    private final Class<T> pojo;
    private final ObjectMapper objectMapper;

    public JsonReader(ObjectMapper objectMapper, Class<T> pojo) {
        this.pojo = pojo;
        this.objectMapper = objectMapper;
    }

    @Override
    public T read(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, this.pojo);
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }
}
