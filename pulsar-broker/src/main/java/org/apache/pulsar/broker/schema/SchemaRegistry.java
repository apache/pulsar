package org.apache.pulsar.broker.schema;

import org.apache.pulsar.common.schema.Schema;

import java.util.concurrent.CompletableFuture;

public interface SchemaRegistry extends AutoCloseable {

    CompletableFuture<SchemaAndMetadata> getSchema(String schemaId);

    CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, long version);

    CompletableFuture<Long> putSchema(String schemaId, Schema schema);

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
