package org.apache.pulsar.broker.service.schema;

import org.apache.pulsar.broker.schema.SchemaRegistry;
import org.apache.pulsar.common.schema.Schema;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface SchemaRegistryService extends SchemaRegistry {
    void start() throws IOException;

    CompletableFuture<Schema> getSchema(String schemaId);

    CompletableFuture<Schema> getSchema(String schemaId, long version);

    CompletableFuture<Long> putSchema(Schema schema);

    CompletableFuture<Void> deleteSchema(String schemaId);

    void close() throws Exception;
}
