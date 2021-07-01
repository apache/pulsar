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
package org.apache.pulsar.io.kafka.connect;

import lombok.Builder;
import lombok.Data;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.List;

/**
 * org.apache.pulsar.client.impl.schema.GenericObjectWrapper is not public
 * (and is not intended to be, at least for now).
 * this one implements similar functionality, for testing.
 */
@Data
@Builder
class MockGenericObjectWrapper implements GenericRecord {

    private final Object nativeObject;
    private final SchemaType schemaType;
    private final byte[] schemaVersion;

    @Override
    public List<Field> getFields() {
        return null;
    }

    @Override
    public Object getField(String fieldName) {
        return null;
    }
}