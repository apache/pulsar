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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Schema hash wrapper with a HashCode inner type.
 */
@EqualsAndHashCode
public class SchemaHash {

    private static final HashFunction hashFunction = Hashing.sha256();

    private final HashCode hash;

    private final SchemaType schemaType;

    private static final LoadingCache<Pair<byte[], SchemaType>, SchemaHash> cache = Caffeine.newBuilder()
            .maximumSize(10000)
            .build(key ->
                    new SchemaHash(
                            hashFunction.hashBytes(key.getLeft() == null ? new byte[0] : key.getLeft()),
                            key.getRight())
            );

    private SchemaHash(HashCode hash, SchemaType schemaType) {
        this.hash = hash;
        this.schemaType = schemaType;
    }

    public static SchemaHash of(Schema schema) {
        Optional<SchemaInfo> schemaInfo = Optional.ofNullable(schema).map(Schema::getSchemaInfo);
        return of(schemaInfo.map(SchemaInfo::getSchema).orElse(null),
                schemaInfo.map(SchemaInfo::getType).orElse(null));
    }

    public static SchemaHash of(SchemaData schemaData) {
        return of(schemaData.getData(), schemaData.getType());
    }

    public static SchemaHash of(SchemaInfo schemaInfo) {
        return of(schemaInfo == null ? null : schemaInfo.getSchema(),
                schemaInfo == null ? null : schemaInfo.getType());
    }

    public static SchemaHash of(byte[] schemaBytes, SchemaType schemaType) {
        return cache.get(Pair.of(schemaBytes, schemaType));
    }

    public byte[] asBytes() {
        return hash.asBytes();
    }
}
