package org.apache.pulsar.broker.service.schema;

import javax.validation.constraints.NotNull;
import org.apache.pulsar.broker.PulsarService;

@SuppressWarnings("unused")
public class BookkeeperSchemaStorageFactory implements SchemaStorageFactory {
    @Override
    @NotNull
    public SchemaStorage create(PulsarService pulsar) throws Exception {
        BookkeeperSchemaStorage service = new BookkeeperSchemaStorage(pulsar);
        service.init();
        return service;
    }
}
