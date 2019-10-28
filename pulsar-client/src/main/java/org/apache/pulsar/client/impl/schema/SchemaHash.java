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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import lombok.EqualsAndHashCode;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.Optional;

// TODO(yittg): use same hash function with broker,
//  move it to pulsar-common and provide it to broker also.
@EqualsAndHashCode
public class SchemaHash {
    private static HashFunction hashFunction = Hashing.sha256();

    private final byte[] value;

    private SchemaHash(byte[] value) {
        this.value = value;
    }

    public static SchemaHash of(Schema schema) {
        byte[] schemaBytes = Optional.ofNullable(schema)
                                     .map(Schema::getSchemaInfo)
                                     .map(SchemaInfo::getSchema).orElse(new byte[0]);
        return new SchemaHash(hashFunction.hashBytes(schemaBytes).asBytes());
    }
}
