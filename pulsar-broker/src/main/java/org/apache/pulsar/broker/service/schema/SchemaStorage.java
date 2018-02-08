package org.apache.pulsar.broker.service.schema;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface SchemaStorage {

    CompletableFuture<SchemaVersion> put(String key, byte[] value);

    CompletableFuture<StoredSchema> get(String key, SchemaVersion version);

    CompletableFuture<SchemaVersion> delete(String key);

    void close() throws Exception;

    class StoredSchema {
        public final byte[] data;
        public final SchemaVersion version;
        public final Map<String, String> metadata;

        StoredSchema(byte[] data, SchemaVersion version, Map<String, String> metadata) {
            this.data = data;
            this.version = version;
            this.metadata = metadata;
        }
    }

}
