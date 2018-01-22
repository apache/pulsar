package org.apache.pulsar.broker.service;

import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.common.schema.Schema;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class MockSchemaRegistryService implements SchemaRegistryService {
    @Override
    public CompletableFuture<Schema> getSchema(String schemaId) {
        CompletableFuture<Schema> future = new CompletableFuture<>();
        future.complete(null);
        return future;
    }

    @Override
    public CompletableFuture<Schema> getSchema(String schemaId, long version) {
        CompletableFuture<Schema> future = new CompletableFuture<>();
        future.complete(null);
        return future;
    }

    @Override
    public CompletableFuture<Long> putSchema(Schema schema) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        future.complete(-1L);
        return future;    }

    @Override
    public void start() throws IOException {

    }

    @Override
    public void close() throws Exception {

    }
}
