package org.apache.pulsar.broker.service.schema;

import org.testng.annotations.Test;

public class KeyValueSchemaCompatibilityCheckTest extends BaseAvroSchemaCompatibilityTest {

    @Override
    public SchemaCompatibilityCheck getSchemaCheck() {
        return new KeyValueSchemaCompatibilityCheck();
    }

//    @Test

}
