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
package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Implementation of GenericRecord that wraps objects of non Struct types.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class GenericObjectWrapper implements GenericRecord {

    private final Object nativeObject;
    private final SchemaType schemaType;
    private final byte[] schemaVersion;

    static GenericObjectWrapper of(Object nativeObject, SchemaType schemaType, byte[] schemaVersion) {
        return new GenericObjectWrapper(nativeObject, schemaType, schemaVersion);
    }

    private GenericObjectWrapper(Object nativeObject, SchemaType schemaType, byte[] schemaVersion) {
        this.nativeObject = nativeObject;
        this.schemaType = Objects.requireNonNull(schemaType, "SchemaType is required");
        this.schemaVersion = schemaVersion;
    }

    @Override
    public byte[] getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public List<Field> getFields() {
        return Collections.emptyList();
    }

    @Override
    public Object getField(String fieldName) {
        return null;
    }

    @Override
    public SchemaType getSchemaType() {
        return schemaType;
    }

    @Override
    public Object getNativeObject() {
        return nativeObject;
    }

    @Override
    public String toString() {
        return Objects.toString(nativeObject);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nativeObject);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof GenericObjectWrapper)) {
            return false;
        }
        GenericObjectWrapper gw = (GenericObjectWrapper) other;
        return this.schemaType == gw.schemaType &&
                Objects.equals(nativeObject, gw.nativeObject) &&
                Arrays.equals(schemaVersion, gw.schemaVersion);
    }
}
