/*
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.mockito.MockedConstruction;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SchemaRegistryServiceImplTest {

    @Test
    public void testExplicitVersionUsesBookkeeperSchemaReadPath() throws Exception {
        String schemaId = "tenant/namespace/topic";
        LongSchemaVersion version = new LongSchemaVersion(0);
        BookkeeperSchemaStorage storage = mock(BookkeeperSchemaStorage.class);
        when(storage.getAllForVersionedRead(schemaId))
                .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        try (MockedConstruction<SchemaRegistryStats> ignored = mockConstruction(SchemaRegistryStats.class)) {
            SchemaRegistryServiceImpl service = new SchemaRegistryServiceImpl(
                    storage, Map.of(), Clock.systemUTC(), mock(PulsarService.class));

            assertNull(service.getSchema(schemaId, version).get(5, TimeUnit.SECONDS));
            verify(storage).getAllForVersionedRead(schemaId);
            verify(storage, never()).getAll(schemaId);
            verify(storage, never()).get(schemaId, version);

            service.close();
        }
    }

    @Test
    public void testExplicitVersionFallsBackToSchemaStorageGetAll() throws Exception {
        String schemaId = "tenant/namespace/topic";
        LongSchemaVersion version = new LongSchemaVersion(0);
        SchemaStorage storage = mock(SchemaStorage.class);
        when(storage.getAll(schemaId))
                .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        try (MockedConstruction<SchemaRegistryStats> ignored = mockConstruction(SchemaRegistryStats.class)) {
            SchemaRegistryServiceImpl service = new SchemaRegistryServiceImpl(
                    storage, Map.of(), Clock.systemUTC(), mock(PulsarService.class));

            assertNull(service.getSchema(schemaId, version).get(5, TimeUnit.SECONDS));
            verify(storage).getAll(schemaId);
            verify(storage, never()).get(schemaId, version);

            service.close();
        }
    }
}
