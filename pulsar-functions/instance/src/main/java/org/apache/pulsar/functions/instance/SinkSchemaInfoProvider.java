/*
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
package org.apache.pulsar.functions.instance;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * SchemaInfo provider that creates a new schema version for each new schema hash.
 */
class SinkSchemaInfoProvider implements SchemaInfoProvider {

  AtomicLong latestVersion = new AtomicLong(0);
  ConcurrentHashMap<SchemaVersion, SchemaInfo> schemaInfos = new ConcurrentHashMap<>();
  ConcurrentHashMap<SchemaHash, SchemaVersion> schemaVersions = new ConcurrentHashMap<>();

  /**
   * Creates a new schema version with the info of the provided schema if the hash of the schema is a new one.
   *
   * @param schema schema for which we create a version
   * @return the version of the schema
   */
  public SchemaVersion addSchemaIfNeeded(Schema<?> schema) {
    SchemaHash schemaHash = SchemaHash.of(schema);
    return schemaVersions.computeIfAbsent(schemaHash, s -> createNewSchemaInfo(schema.getSchemaInfo()));
  }

  private SchemaVersion createNewSchemaInfo(SchemaInfo schemaInfo) {
    long l = latestVersion.incrementAndGet();
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(l);
    BytesSchemaVersion schemaVersion = BytesSchemaVersion.of(buffer.array());
    schemaInfos.put(schemaVersion, schemaInfo);
    return schemaVersion;
  }

  @Override
  public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion) {
    return CompletableFuture.completedFuture(schemaInfos.get(BytesSchemaVersion.of(schemaVersion)));
  }

  @Override
  public CompletableFuture<SchemaInfo> getLatestSchema() {
    long l = latestVersion.get();
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(l);
    SchemaVersion schemaVersion = BytesSchemaVersion.of(buffer.array());
    return CompletableFuture.completedFuture(schemaInfos.get(schemaVersion));
  }

  @Override
  public String getTopicName() {
    return "__INTERNAL__";
  }
}
