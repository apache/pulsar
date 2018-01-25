package org.apache.pulsar.broker.service;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class SchemaServiceTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void writeAndReadBackSchemaEntry() throws ExecutionException, InterruptedException {

        SchemaRegistryService schemaRegistryService =
            pulsar.getSchemaRegistryService();

        String schemaId = "id1";

        Schema schema = Schema.newBuilder()
            .user("dave")
            .type(SchemaType.JSON)
            .timestamp(Clock.systemUTC().millis())
            .isDeleted(false)
            .id(schemaId)
            .schemaInfo("")
            .data("message { required int64 a = 1};".getBytes())
            .build();

        CompletableFuture<Long> put =
            schemaRegistryService.putSchema(schema);

        long puttedVersion = put.get();
        long expectedVersion = 0;
        assertEquals(expectedVersion, puttedVersion);

        CompletableFuture<Schema> get =
            schemaRegistryService.getSchema(schemaId);

        Schema gotten = get.get();
        //TODO: mock the Clock so I can compare apples to apples
        //assertEquals(schema, gotten);

        CompletableFuture<Void> delete =
            schemaRegistryService.deleteSchema(schemaId, "dave");

        delete.get();

        CompletableFuture<Schema> getAgain =
            schemaRegistryService.getSchema(schemaId);

        Schema gottenAgain = getAgain.get();

        assertTrue(gottenAgain.isDeleted);
    }

}
