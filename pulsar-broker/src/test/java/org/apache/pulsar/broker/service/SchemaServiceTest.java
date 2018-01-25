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
    public void writeAndReacBackSchemaEntry() throws ExecutionException, InterruptedException {
        SchemaRegistryService schemaRegistryService =
            pulsar.getSchemaRegistryService();

        String schemaId = "id1";

        CompletableFuture<Long> put = schemaRegistryService
            .putSchema(
                Schema.newBuilder()
                    .user("dave")
                    .type(SchemaType.JSON)
                    .timestamp(Clock.systemUTC().millis())
                    .isDeleted(false)
                    .id(schemaId)
                    .version(1)
                    .schemaInfo("")
                    .data(new byte[]{})
                    .build()
            );

        Long putted = put.get();
        System.out.println(putted);

        CompletableFuture<Schema> get =
            schemaRegistryService.getSchema(schemaId);

        Schema gotten = get.get();
        System.out.println(gotten);

        CompletableFuture<Long> delete =
            schemaRegistryService.deleteSchema(schemaId, "dave");

        delete.get();

        CompletableFuture<Schema> getAgain =
            schemaRegistryService.getSchema(schemaId);

        Schema gottenAgain = getAgain.get();
        System.out.println(gottenAgain);
    }

}
