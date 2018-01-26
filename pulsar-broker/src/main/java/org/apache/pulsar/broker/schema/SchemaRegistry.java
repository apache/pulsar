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

        public SchemaAndMetadata(String id, Schema schema, long version) {
            this.id = id;
            this.schema = schema;
            this.version = version;
        }
    }

}
