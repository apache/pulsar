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
package org.apache.pulsar.metadata.cache.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.Stat;

public class JSONMetadataSerdeTypeRef<T> implements MetadataSerde<T> {

    private final ObjectReader objectReader;
    private final ObjectWriter objectWriter;

    public JSONMetadataSerdeTypeRef(TypeReference<T> typeRef) {
        this.objectReader = ObjectMapperFactory.getMapper().reader().forType(typeRef);
        this.objectWriter = ObjectMapperFactory.getMapper().writer().forType(typeRef);
    }

    @Override
    public byte[] serialize(String path, T value) throws IOException {
        return objectWriter.writeValueAsBytes(value);
    }

    @Override
    public T deserialize(String path, byte[] content, Stat stat) throws IOException {
        return objectReader.readValue(content);
    }
}
