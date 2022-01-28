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
package org.apache.pulsar.broker.service.schema.validator;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.broker.service.schema.SchemaRegistry.SchemaAndMetadata;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link SchemaRegistryServiceWithSchemaDataValidator}.
 */
@Test(groups = "broker")
public class SchemaRegistryServiceWithSchemaDataValidatorTest {

    private SchemaRegistryService underlyingService;
    private SchemaRegistryServiceWithSchemaDataValidator service;

    @BeforeMethod
    public void setup() {
        this.underlyingService = mock(SchemaRegistryService.class);
        this.service = SchemaRegistryServiceWithSchemaDataValidator.of(underlyingService);
    }

    @Test
    public void testGetLatestSchema() {
        String schemaId = "test-schema-id";
        CompletableFuture<SchemaAndMetadata> getFuture = new CompletableFuture<>();
        when(underlyingService.getSchema(eq(schemaId))).thenReturn(getFuture);
        assertSame(getFuture, service.getSchema(schemaId));
        verify(underlyingService, times(1)).getSchema(eq(schemaId));
    }

    @Test
    public void testGetSchemaByVersion() {
        String schemaId = "test-schema-id";
        CompletableFuture<SchemaAndMetadata> getFuture = new CompletableFuture<>();
        when(underlyingService.getSchema(eq(schemaId), any(SchemaVersion.class)))
            .thenReturn(getFuture);
        assertSame(getFuture, service.getSchema(schemaId, SchemaVersion.Latest));
        verify(underlyingService, times(1))
            .getSchema(eq(schemaId), same(SchemaVersion.Latest));
    }

    @Test
    public void testDeleteSchema() {
        String schemaId = "test-schema-id";
        String user = "test-user";
        CompletableFuture<SchemaVersion> deleteFuture = new CompletableFuture<>();
        when(underlyingService.deleteSchema(eq(schemaId), eq(user)))
            .thenReturn(deleteFuture);
        assertSame(deleteFuture, service.deleteSchema(schemaId, user));
        verify(underlyingService, times(1))
            .deleteSchema(eq(schemaId), eq(user));
    }

    @Test
    public void testIsCompatibleWithGoodSchemaData() {
        String schemaId = "test-schema-id";
        SchemaCompatibilityStrategy strategy = SchemaCompatibilityStrategy.FULL;
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        when(underlyingService.isCompatible(eq(schemaId), any(SchemaData.class), eq(strategy)))
            .thenReturn(future);
        SchemaData schemaData = SchemaData.builder()
            .type(SchemaType.BOOLEAN)
            .data(new byte[0])
            .build();
        assertSame(future, service.isCompatible(schemaId, schemaData, strategy));
        verify(underlyingService, times(1))
            .isCompatible(eq(schemaId), same(schemaData), eq(strategy));
    }

    @Test
    public void testIsCompatibleWithBadSchemaData() {
        String schemaId = "test-schema-id";
        SchemaCompatibilityStrategy strategy = SchemaCompatibilityStrategy.FULL;
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        when(underlyingService.isCompatible(eq(schemaId), any(SchemaData.class), eq(strategy)))
            .thenReturn(future);
        SchemaData schemaData = SchemaData.builder()
            .type(SchemaType.BOOLEAN)
            .data(new byte[10])
            .build();
        try {
            service.isCompatible(schemaId, schemaData, strategy).get();
            fail("Should fail isCompatible check");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidSchemaDataException);
        }
        verify(underlyingService, times(0))
            .isCompatible(eq(schemaId), same(schemaData), eq(strategy));
    }

    @Test
    public void testPutSchemaIfAbsentWithGoodSchemaData() {
        String schemaId = "test-schema-id";
        SchemaCompatibilityStrategy strategy = SchemaCompatibilityStrategy.FULL;
        CompletableFuture<SchemaVersion> future = new CompletableFuture<>();
        when(underlyingService.putSchemaIfAbsent(eq(schemaId), any(SchemaData.class), eq(strategy)))
            .thenReturn(future);
        SchemaData schemaData = SchemaData.builder()
            .type(SchemaType.BOOLEAN)
            .data(new byte[0])
            .build();
        assertSame(future, service.putSchemaIfAbsent(schemaId, schemaData, strategy));
        verify(underlyingService, times(1))
            .putSchemaIfAbsent(eq(schemaId), same(schemaData), eq(strategy));
    }

    @Test
    public void testPutSchemaIfAbsentWithBadSchemaData() {
        String schemaId = "test-schema-id";
        SchemaCompatibilityStrategy strategy = SchemaCompatibilityStrategy.FULL;
        CompletableFuture<SchemaVersion> future = new CompletableFuture<>();
        when(underlyingService.putSchemaIfAbsent(eq(schemaId), any(SchemaData.class), eq(strategy)))
            .thenReturn(future);
        SchemaData schemaData = SchemaData.builder()
            .type(SchemaType.BOOLEAN)
            .data(new byte[10])
            .build();
        try {
            service.putSchemaIfAbsent(schemaId, schemaData, strategy).get();
            fail("Should fail putSchemaIfAbsent");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidSchemaDataException);
        }
        verify(underlyingService, times(0))
            .putSchemaIfAbsent(eq(schemaId), same(schemaData), eq(strategy));
    }

}
