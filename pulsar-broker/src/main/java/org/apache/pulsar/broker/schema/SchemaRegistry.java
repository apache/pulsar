package org.apache.pulsar.broker.schema;

import org.apache.pulsar.common.schema.Schema;

import java.util.concurrent.CompletableFuture;

public interface SchemaRegistry extends AutoCloseable {

    CompletableFuture<Schema> getSchema(String schemaId);

    CompletableFuture<Schema> getSchema(String schemaId, long version);

    CompletableFuture<Long> putSchema(Schema schema);

}
