package org.apache.pulsar.tests.integration.schema;

import java.nio.charset.StandardCharsets;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.tests.integration.suites.PulsarSchemaTestSuite;
import org.junit.Test;

public class SchemaRegistryTest extends PulsarSchemaTestSuite {

    @Test
    public void testMockSchema() {
        MockSchemaStorage mockSchemaStorage = new MockSchemaStorage();
        mockSchemaStorage.put("k", "val".getBytes(StandardCharsets.UTF_8), ("val".hashCode()+"").getBytes(StandardCharsets.UTF_8));
        mockSchemaStorage.get("k", new LongSchemaVersion(0));
    }
}
