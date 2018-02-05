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
package org.apache.pulsar.broker.service;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.schema.SchemaRegistry;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchemaServiceTest extends MockedPulsarServiceBaseTest {

    static Clock MockClock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

    String schemaId1 = "id1";
    String schemaId2 = "id2";
    String userId = "user";

    Schema schema1 = Schema.newBuilder()
        .user(userId)
        .type(SchemaType.PROTOBUF)
        .timestamp(MockClock.millis())
        .isDeleted(false)
        .schemaInfo("")
        .data("message { required int64 a = 1};".getBytes())
        .build();

    Schema schema2 = Schema.newBuilder()
        .user(userId)
        .type(SchemaType.PROTOBUF)
        .timestamp(MockClock.millis())
        .isDeleted(false)
        .schemaInfo("")
        .data("message { required int64 b = 1};".getBytes())
        .build();

    Schema schema3 = Schema.newBuilder()
        .user(userId)
        .type(SchemaType.PROTOBUF)
        .timestamp(MockClock.millis())
        .isDeleted(false)
        .schemaInfo("")
        .data("message { required int64 b = 1};".getBytes())
        .build();

    DefaultSchemaRegistryService schemaRegistryService;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        schemaRegistryService =
            new DefaultSchemaRegistryService(pulsar, MockClock);
        schemaRegistryService.init();
        schemaRegistryService.start();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        schemaRegistryService.close();
    }

    @Test
    public void writeReadBackDeleteSchemaEntry() throws Exception {
        putSchema(schemaId1, schema1, 0);

        Schema latest = getLatestSchema(schemaId1, 0);
        assertEquals(schema1, latest);

        deleteScehma(schemaId1, 1);

        Schema latest2 = getLatestSchema(schemaId1, 1);

        assertTrue(latest2.isDeleted);
    }

    @Test
    public void getReturnsTheLastWrittenEntry() throws Exception {
        putSchema(schemaId1, schema1, 0);
        putSchema(schemaId1, schema2, 1);

        Schema latest = getLatestSchema(schemaId1, 1);
        assertEquals(schema2, latest);

    }

    @Test
    public void getByVersionReturnsTheCorrectEntry() throws Exception {
        putSchema(schemaId1, schema1, 0);
        putSchema(schemaId1, schema2, 1);

        Schema version0 = getSchema(schemaId1, 0);
        assertEquals(schema1, version0);
    }

    @Test
    public void getByVersionReturnsTheCorrectEntry2() throws Exception {
        putSchema(schemaId1, schema1, 0);
        putSchema(schemaId1, schema2, 1);

        Schema version1 = getSchema(schemaId1, 1);
        assertEquals(schema2, version1);
    }

    @Test
    public void addLotsOfEntriesThenDelete() throws Exception {
        putSchema(schemaId1, schema1, 0);
        putSchema(schemaId1, schema2, 1);
        putSchema(schemaId1, schema2, 2);
        putSchema(schemaId1, schema2, 3);
        putSchema(schemaId1, schema2, 4);
        putSchema(schemaId1, schema2, 5);
        putSchema(schemaId1, schema1, 6);

        Schema version0 = getSchema(schemaId1, 0);
        assertEquals(schema1, version0);

        Schema version1 = getSchema(schemaId1, 1);
        assertEquals(schema2, version1);

        Schema version2 = getSchema(schemaId1, 2);
        assertEquals(schema2, version2);

        Schema version3 = getSchema(schemaId1, 3);
        assertEquals(schema2, version3);

        Schema version4 = getSchema(schemaId1, 4);
        assertEquals(schema2, version4);

        Schema version5 = getSchema(schemaId1, 5);
        assertEquals(schema2, version5);

        Schema version6 = getSchema(schemaId1, 6);
        assertEquals(schema1, version6);

        deleteScehma(schemaId1, 7);

        Schema version7 = getSchema(schemaId1, 7);
        assertTrue(version7.isDeleted);

    }

    @Test
    public void writeSchemasToDifferentIds() throws Exception {
        Schema schemaWithDifferentId = schema3;

        putSchema(schemaId1, schema1, 0);
        putSchema(schemaId2, schemaWithDifferentId, 0);

        Schema withFirstId = getLatestSchema(schemaId1, 0);
        Schema withDifferentId = getLatestSchema(schemaId2, 0);

        assertEquals(schema1, withFirstId);
        assertEquals(schema3, withDifferentId);
    }

    private void putSchema(String schemaId, Schema schema, long expectedVersion) throws Exception {
        CompletableFuture<Long> put = schemaRegistryService.putSchema(schemaId, schema);
        long newVersion = put.get();
        assertEquals(expectedVersion, newVersion);
    }

    private Schema getLatestSchema(String schemaId, long expectedVersion) throws Exception {
        SchemaRegistry.SchemaAndMetadata schemaAndVersion = schemaRegistryService.getSchema(schemaId).get();
        assertEquals(expectedVersion, schemaAndVersion.version);
        assertEquals(schemaId, schemaAndVersion.id);
        return schemaAndVersion.schema;
    }

    private Schema getSchema(String schemaId, long version) throws Exception {
        SchemaRegistry.SchemaAndMetadata schemaAndVersion = schemaRegistryService.getSchema(schemaId, version).get();
        assertEquals(version, schemaAndVersion.version);
        assertEquals(schemaId, schemaAndVersion.id);
        return schemaAndVersion.schema;
    }

    private long deleteScehma(String schemaId, long expectedVersion) throws Exception{
        long version = schemaRegistryService.deleteSchema(schemaId, userId).get();
        assertEquals(expectedVersion, version);
        return version;
    }
}
