package org.apache.pulsar.broker.service.schema;

import javax.validation.constraints.NotNull;
import org.apache.pulsar.broker.PulsarService;

public interface SchemaStorageFactory {
    @NotNull
    SchemaStorage create(PulsarService pulsar) throws Exception;
}
