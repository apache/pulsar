package org.apache.pulsar.broker.service;

import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.common.schema.Schema;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class MockSchemaRegistryService implements SchemaRegistryService {
    @Override
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, long version) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Long> putSchema(String schemaId, Schema schema) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Long> deleteSchema(String schemaId, String user) {
        return completedFuture(null);
    }

    @Override
    public void start() throws IOException {

    }

    @Override
    public void close() throws Exception {

    }
}
