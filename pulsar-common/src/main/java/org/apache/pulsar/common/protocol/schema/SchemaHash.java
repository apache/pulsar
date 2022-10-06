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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Schema hash wrapper with a HashCode inner type.
 */
@EqualsAndHashCode
@Slf4j
public class SchemaHash {

    private static final HashFunction hashFunction = Hashing.sha256();

    private final HashCode hash;

    private final SchemaType schemaType;

    private static final Cache<Pair<byte[], SchemaType>, SchemaHash> cache = Caffeine.newBuilder()
            .maximumSize(10000).build();

    private SchemaHash(HashCode hash, SchemaType schemaType) {
        this.hash = hash;
        this.schemaType = schemaType;
    }

    public static SchemaHash of(Schema schema) {
        Optional<SchemaInfo> schemaInfo = Optional.ofNullable(schema).map(Schema::getSchemaInfo);
        return of(schemaInfo.map(SchemaInfo::getSchema).orElseGet(() -> new byte[0]),
                schemaInfo.map(SchemaInfo::getType).orElse(null));
    }

    public static SchemaHash of(SchemaData schemaData) {
        return of(schemaData.getData(), schemaData.getType());
    }

    public static SchemaHash of(SchemaInfo schemaInfo) {
        return of(schemaInfo == null ? new byte[0] : schemaInfo.getSchema(),
                schemaInfo == null ? null : schemaInfo.getType());
    }

    public static SchemaHash of(byte[] schemaBytes, SchemaType schemaType) {
        return cache.get(Pair.of(schemaBytes, schemaType),
                schemaTypePair -> new SchemaHash(hashFunction.hashBytes(schemaBytes), schemaType));
    }

    public byte[] asBytes() {
        return hash.asBytes();
    }
}
