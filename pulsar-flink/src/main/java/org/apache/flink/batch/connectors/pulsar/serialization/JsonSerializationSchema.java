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
package org.apache.flink.batch.connectors.pulsar.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Json Serialization Schema to serialize Dataset records to Json.
 */
public class JsonSerializationSchema<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = -6938065355389311385L;
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(T t) {
        try {
            return mapper.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error while serializing the record to Json", e);
        }
    }
}
