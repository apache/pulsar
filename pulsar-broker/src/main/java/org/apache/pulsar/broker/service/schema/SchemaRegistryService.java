package org.apache.pulsar.broker.service.schema;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.schema.SchemaRegistry;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;

public interface SchemaRegistryService extends SchemaRegistry {
    Schema getSchema(String schemaId);

    Schema getSchema(String schemaId, long version);

    long putSchema(String schemaId, SchemaType type, String schema);

    void close() throws Exception;
}
