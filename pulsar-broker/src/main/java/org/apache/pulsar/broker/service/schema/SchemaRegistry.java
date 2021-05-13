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
package org.apache.pulsar.broker.service.schema;

import com.google.common.base.MoreObjects;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

public interface SchemaRegistry extends AutoCloseable {

    CompletableFuture<SchemaAndMetadata> getSchema(String schemaId);

    CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, SchemaVersion version);

    CompletableFuture<List<CompletableFuture<SchemaAndMetadata>>> getAllSchemas(String schemaId);

    CompletableFuture<SchemaVersion> putSchemaIfAbsent(String schemaId, SchemaData schema,
                                                       SchemaCompatibilityStrategy strategy);

    CompletableFuture<SchemaVersion> deleteSchema(String schemaId, String user);

    CompletableFuture<SchemaVersion> deleteSchemaStorage(String schemaId);

    CompletableFuture<SchemaVersion> deleteSchemaStorage(String schemaId, boolean forcefully);

    CompletableFuture<Boolean> isCompatible(String schemaId, SchemaData schema,
                                            SchemaCompatibilityStrategy strategy);

    CompletableFuture<Void> checkCompatible(String schemaId, SchemaData schema,
                                                             SchemaCompatibilityStrategy strategy);

    CompletableFuture<List<SchemaAndMetadata>> trimDeletedSchemaAndGetList(String schemaId);

    CompletableFuture<Long> findSchemaVersion(String schemaId, SchemaData schemaData);

    CompletableFuture<Void> checkConsumerCompatibility(String schemaId, SchemaData schemaData,
                                                       SchemaCompatibilityStrategy strategy);

    CompletableFuture<SchemaVersion> getSchemaVersionBySchemaData(List<SchemaAndMetadata> schemaAndMetadataList,
                                                                  SchemaData schemaData);

    SchemaVersion versionFromBytes(byte[] version);

    class SchemaAndMetadata {
        public final String id;
        public final SchemaData schema;
        public final SchemaVersion version;

        SchemaAndMetadata(String id, SchemaData schema, SchemaVersion version) {
            this.id = id;
            this.schema = schema;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SchemaAndMetadata that = (SchemaAndMetadata) o;
            return version == that.version
                    && Objects.equals(id, that.id)
                    && Objects.equals(schema, that.schema);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, schema, version);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("schema", schema)
                .add("version", version)
                .toString();
        }
    }

}
