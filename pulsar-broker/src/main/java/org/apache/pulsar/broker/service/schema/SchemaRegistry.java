/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.schema;

import java.util.concurrent.CompletableFuture;
import lombok.Data;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaVersion;

public interface SchemaRegistry extends AutoCloseable {

    CompletableFuture<SchemaAndMetadata> getSchema(String schemaId);

    CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, SchemaVersion version);

    CompletableFuture<SchemaVersion> putSchema(String schemaId, Schema schema);

    CompletableFuture<SchemaVersion> deleteSchema(String schemaId, String user);

    SchemaVersion versionFromBytes(byte[] version);

    @Data
    class SchemaAndMetadata {
        public final String id;
        public final Schema schema;
        public final SchemaVersion version;
    }

}
