package org.apache.pulsar.functions.instance;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.Record;

class SinkSchemaInfoProvider implements SchemaInfoProvider {

  AtomicLong latestVersion = new AtomicLong(0);
  ConcurrentHashMap<SchemaVersion, SchemaInfo> schemaInfos = new ConcurrentHashMap<>();
  ConcurrentHashMap<SchemaHash, SchemaVersion> schemaVersions = new ConcurrentHashMap<>();

  public SchemaVersion getSchemaVersion(Record<?> record) {
    SchemaHash schemaHash = SchemaHash.of(record.getSchema());
    return schemaVersions.computeIfAbsent(schemaHash, s -> {
      long l = latestVersion.incrementAndGet();
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(l);
      SchemaVersion schemaVersion = BytesSchemaVersion.of(buffer.array());
      schemaInfos.put(schemaVersion, record.getSchema().getSchemaInfo());
      return schemaVersion;
    });
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
