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
package org.apache.pulsar.common.protocol.schema;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import lombok.EqualsAndHashCode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Schema hash wrapper with a HashCode inner type.
 */
@EqualsAndHashCode
public class SchemaHash {

    private static final HashFunction hashFunction = Hashing.sha256();
    private static final SchemaHash EMPTY_SCHEMA_HASH = new SchemaHash(hashFunction.hashBytes(new byte[0]), null);

    private final HashCode hash;

    private final SchemaType schemaType;

    private SchemaHash(HashCode hash, SchemaType schemaType) {
        this.hash = hash;
        this.schemaType = schemaType;
    }

    public static SchemaHash of(Schema schema) {
        if (schema == null || schema.getSchemaInfo() == null) {
            return EMPTY_SCHEMA_HASH;
        }
        return ((SchemaInfoImpl) schema.getSchemaInfo()).getSchemaHash();
    }

    public static SchemaHash of(SchemaData schemaData) {
        return of(schemaData.getData(), schemaData.getType());
    }

    public static SchemaHash of(SchemaInfo schemaInfo) {
        if (schemaInfo == null) {
            return EMPTY_SCHEMA_HASH;
        }
        return ((SchemaInfoImpl) schemaInfo).getSchemaHash();
    }

    public static SchemaHash empty() {
        return EMPTY_SCHEMA_HASH;
    }

    // Shouldn't call this method frequently, otherwise will bring performance regression
    public static SchemaHash of(byte[] schemaBytes, SchemaType schemaType) {
        return new SchemaHash(hashFunction.hashBytes(schemaBytes == null ? new byte[0] : schemaBytes), schemaType);
    }

    public byte[] asBytes() {
        return hash.asBytes();
    }
}
