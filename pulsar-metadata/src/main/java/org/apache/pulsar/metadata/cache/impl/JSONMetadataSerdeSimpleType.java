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
package org.apache.pulsar.metadata.cache.impl;

import com.fasterxml.jackson.databind.JavaType;
import java.io.IOException;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataSerde;

public class JSONMetadataSerdeSimpleType<T> implements MetadataSerde<T> {

    private final JavaType typeRef;

    public JSONMetadataSerdeSimpleType(JavaType typeRef) {
        this.typeRef = typeRef;
    }

    @Override
    public byte[] serialize(T value) throws IOException {
        return ObjectMapperFactory.getThreadLocal().writeValueAsBytes(value);
    }

    @Override
    public T deserialize(byte[] content) throws IOException {
        return ObjectMapperFactory.getThreadLocal().readValue(content, typeRef);
    }
}
