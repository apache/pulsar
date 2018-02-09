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
package org.apache.pulsar.broker.schema;

import com.google.common.base.MoreObjects;
import java.util.Map;
import java.util.Objects;
import org.apache.pulsar.common.schema.Schema;

import java.util.concurrent.CompletableFuture;

public interface SchemaRegistry extends AutoCloseable {

    CompletableFuture<SchemaAndMetadata> getSchema(String schemaId);

    CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, long version);

    CompletableFuture<Long> putSchema(String schemaId, Schema schema);

    CompletableFuture<Long> deleteSchema(String schemaId, String user);

    class SchemaAndMetadata {
        public final String id;
        public final Schema schema;
        public final long version;
        public final Map<String, String> metadata;

        public SchemaAndMetadata(String id, Schema schema, long version, Map<String, String> metadata) {
            this.id = id;
            this.schema = schema;
            this.version = version;
            this.metadata = metadata;
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
            return version == that.version &&
                Objects.equals(id, that.id) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(metadata, that.metadata);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, schema, version, metadata);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("schema", schema)
                .add("version", version)
                .add("metadata", metadata)
                .toString();
        }
    }

}
