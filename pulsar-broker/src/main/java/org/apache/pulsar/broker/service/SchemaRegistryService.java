package org.apache.pulsar.broker.service;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.schema.SchemaRegistry;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;

public class SchemaRegistryService implements SchemaRegistry {
    private final PulsarService pulsar;

    public SchemaRegistryService(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    @Override
    public Schema getSchema(String schemaId) {
        return null;
    }

    @Override
    public Schema getSchema(String schemaId, long version) {
        return null;
    }

    @Override
    public long putSchema(String schemaId, SchemaType type, String schema) {
        return 0;
    }

    @Override
    public void close() throws Exception {

    }
}
