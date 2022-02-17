/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.schema;

import org.apache.pulsar.broker.service.schema.validator.SchemaRegistryServiceWithSchemaDataValidator;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.junit.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
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
            .create("org.apache.pulsar.broker.service.schema.TestSchemaRegistryService");
      Assert.assertSame(actual.getClass(), SchemaRegistryServiceWithSchemaDataValidator.class);
   }

   @Test
   void createEmptyRegistry() {
      SchemaRegistryService actual = SchemaRegistryService.create(null);
      Assert.assertSame(actual.getClass(), DefaultSchemaRegistryService.class);
   }

}

class TestSchemaRegistryService extends DefaultSchemaRegistryService {
   public TestSchemaRegistryService() {}
}
