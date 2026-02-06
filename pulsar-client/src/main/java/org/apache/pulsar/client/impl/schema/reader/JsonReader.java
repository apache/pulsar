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
package org.apache.pulsar.client.impl.schema.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reader implementation for reading objects from JSON.
 *
 * @param <T> object type to read
 * @deprecated use {@link JacksonJsonReader} instead.
 */
@Deprecated
public class JsonReader<T> implements SchemaReader<T> {
    private final Class<T> pojo;
    private final ObjectReader objectReader;

    public JsonReader(ObjectMapper objectMapper, Class<T> pojo) {
        this.pojo = pojo;
        this.objectReader = pojo != null ? objectMapper.readerFor(pojo) : objectMapper.reader();
    }

    @Override
    public T read(byte[] bytes, int offset, int length) {
        try {
            return objectReader.readValue(bytes, offset, length);
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public T read(InputStream inputStream) {
        try {
            return objectReader.readValue(inputStream);
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                log.error("JsonReader close inputStream close error", e);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(JsonReader.class);
}
