package org.apache.pulsar.broker.service.schema;

import org.apache.pulsar.broker.service.schema.validator.SchemaRegistryServiceWithSchemaDataValidator;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.junit.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;

@Test(groups = "broker")
public class SchemaRegistryServiceTest {

   SchemaStorage mockSchemaStorage;
   Set<String> checkers;

   @BeforeMethod
   public void setup() {
      this.mockSchemaStorage = mock(SchemaStorage.class);
      this.checkers = new HashSet<>();
   }

   @Test
   void createCustomRegistry() {
      SchemaRegistryService actual = SchemaRegistryService
            .create("org.apache.pulsar.broker.service.schema.TestSchemaRegistryService", mockSchemaStorage, checkers);
      Assert.assertSame(actual.getClass(), SchemaRegistryServiceWithSchemaDataValidator.class);
   }

   @Test
   void createEmptyRegistry() {
      SchemaRegistryService actual = SchemaRegistryService.create(null, null, null);
      Assert.assertSame(actual.getClass(), DefaultSchemaRegistryService.class);
   }

}

class TestSchemaRegistryService extends DefaultSchemaRegistryService {
   public TestSchemaRegistryService(SchemaStorage schemaStorage, Map checkers) {}
}
