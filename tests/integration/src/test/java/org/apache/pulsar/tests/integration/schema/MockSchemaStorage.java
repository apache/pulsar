package org.apache.pulsar.tests.integration.schema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;

public class MockSchemaStorage implements SchemaStorage {
    Map<String, List<Byte[]>> mockSchemaStore = new HashMap<>();

    @Override
    public CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash) {

        List<Byte[]> schemas;
        Long version;

        CompletableFuture<SchemaVersion> result = new CompletableFuture<>();

        if (mockSchemaStore.containsKey(key)) {
            schemas = mockSchemaStore.get(key);
        } else {
            schemas = new ArrayList<>();
        }
        schemas.add(ArrayUtils.toObject(value));
        version = (long) schemas.size();

        mockSchemaStore.put(key, schemas);
        result.complete(new LongSchemaVersion(version));

        return result;
    }

    @Override
    public CompletableFuture<StoredSchema> get(String key, SchemaVersion version) {
        CompletableFuture<StoredSchema> result = new CompletableFuture<>();
        Byte[] schema = mockSchemaStore.get(key).get(ByteBuffer.wrap(version.bytes()).getInt());
        result.complete(new StoredSchema(ArrayUtils.toPrimitive(schema), version));

        return result;
    }

    @Override
    public CompletableFuture<List<CompletableFuture<StoredSchema>>> getAll(String key) {
        CompletableFuture<List<CompletableFuture<StoredSchema>>> finalResult = new CompletableFuture<>();
        List<CompletableFuture<StoredSchema>> result = new ArrayList<>();

        for (int i = 0; i < mockSchemaStore.get(key).size(); i++) {
            result.add(get(key, new LongSchemaVersion(i)));
        }
        finalResult.complete(result);
        return finalResult;
    }

    @Override
    public CompletableFuture<SchemaVersion> delete(String key, boolean forcefully) {
        CompletableFuture<SchemaVersion> result = new CompletableFuture<>();
        int version = mockSchemaStore.get(key).size() - 1;
        mockSchemaStore.get(key).remove(version);
        result.complete(new LongSchemaVersion(version));
        return result;
    }

    @Override
    public CompletableFuture<SchemaVersion> delete(String key) {
        return delete(key, false);
    }

    @Override
    public SchemaVersion versionFromBytes(byte[] version) {
        ByteBuffer bb = ByteBuffer.wrap(version);
        return new LongSchemaVersion(bb.getLong());
    }

    @Override
    public void start() throws Exception {
        mockSchemaStore = new HashMap<>();
    }

    @Override
    public void close() throws Exception {
        // nothing
    }
}
