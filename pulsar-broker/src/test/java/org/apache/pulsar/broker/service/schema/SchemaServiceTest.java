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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.schema.SchemaVersion;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchemaServiceTest extends MockedPulsarServiceBaseTest {

    private static Clock MockClock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

    private String schemaId1 = "id1";
    private String schemaId2 = "id2";
    private String userId = "user";

    private Schema schema1 = schema1(version(0));

    private Schema schema2 = schema2(version(0));

    private Schema schema3 = Schema.newBuilder()
        .user(userId)
        .type(SchemaType.PROTOBUF)
        .timestamp(MockClock.millis())
        .isDeleted(false)
        .data("message { required int64 b = 1};".getBytes())
        .build();

    private SchemaRegistryServiceImpl schemaRegistryService;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        BookkeeperSchemaStorage storage = new BookkeeperSchemaStorage(pulsar);
        storage.init();
        storage.start();
        schemaRegistryService = new SchemaRegistryServiceImpl(storage, MockClock);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        schemaRegistryService.close();
    }

    @Test
    public void writeReadBackDeleteSchemaEntry() throws Exception {
        putSchema(schemaId1, schema1, version(0));

        Schema latest = getLatestSchema(schemaId1, version(0));
        assertEquals(schema1, latest);

        deleteSchema(schemaId1, version(1));

        Schema latest2 = getLatestSchema(schemaId1, version(1));

        assertTrue(latest2.isDeleted);
    }

    @Test
    public void getReturnsTheLastWrittenEntry() throws Exception {
        putSchema(schemaId1, schema1, version(0));
        putSchema(schemaId1, schema2, version(1));

        Schema latest = getLatestSchema(schemaId1, version(1));
        assertEquals(schema2(version(1)), latest);

    }

    @Test
    public void getByVersionReturnsTheCorrectEntry() throws Exception {
        putSchema(schemaId1, schema1, version(0));
        putSchema(schemaId1, schema2, version(1));

        Schema version0 = getSchema(schemaId1, version(0));
        assertEquals(schema1, version0);
    }

    @Test
    public void getByVersionReturnsTheCorrectEntry2() throws Exception {
        putSchema(schemaId1, schema1, version(0));
        putSchema(schemaId1, schema2, version(1));

        Schema version1 = getSchema(schemaId1, version(1));
        assertEquals(schema2(version(1)), version1);
    }

    @Test
    public void addLotsOfEntriesThenDelete() throws Exception {
        putSchema(schemaId1, schema1, version(0));
        putSchema(schemaId1, schema2, version(1));
        putSchema(schemaId1, schema2, version(2));
        putSchema(schemaId1, schema2, version(3));
        putSchema(schemaId1, schema2, version(4));
        putSchema(schemaId1, schema2, version(5));
        putSchema(schemaId1, schema1, version(6));

        Schema version0 = getSchema(schemaId1, version(0));
        assertEquals(schema1(version(0)), version0);

        Schema version1 = getSchema(schemaId1, version(1));
        assertEquals(schema2(version(1)), version1);

        Schema version2 = getSchema(schemaId1, version(2));
        assertEquals(schema2(version(2)), version2);

        Schema version3 = getSchema(schemaId1, version(3));
        assertEquals(schema2(version(3)), version3);

        Schema version4 = getSchema(schemaId1, version(4));
        assertEquals(schema2(version(4)), version4);

        Schema version5 = getSchema(schemaId1, version(5));
        assertEquals(schema2(version(5)), version5);

        Schema version6 = getSchema(schemaId1, version(6));
        assertEquals(schema1(version(6)), version6);

        deleteSchema(schemaId1, version(7));

        Schema version7 = getSchema(schemaId1, version(7));
        assertTrue(version7.isDeleted);

    }

    @Test
    public void writeSchemasToDifferentIds() throws Exception {
        Schema schemaWithDifferentId = schema3;

        putSchema(schemaId1, schema1, version(0));
        putSchema(schemaId2, schemaWithDifferentId, version(0));

        Schema withFirstId = getLatestSchema(schemaId1, version(0));
        Schema withDifferentId = getLatestSchema(schemaId2, version(0));

        assertEquals(schema1, withFirstId);
        assertEquals(schema3, withDifferentId);
    }

    private void putSchema(String schemaId, Schema schema, SchemaVersion expectedVersion) throws Exception {
        CompletableFuture<SchemaVersion> put = schemaRegistryService.putSchema(schemaId, schema);
        SchemaVersion newVersion = put.get();
        assertEquals(expectedVersion, newVersion);
    }

    private Schema getLatestSchema(String schemaId, SchemaVersion expectedVersion) throws Exception {
        SchemaRegistry.SchemaAndMetadata schemaAndVersion = schemaRegistryService.getSchema(schemaId).get();
        assertEquals(expectedVersion, schemaAndVersion.version);
        assertEquals(schemaId, schemaAndVersion.id);
        return schemaAndVersion.schema;
    }

    private Schema getSchema(String schemaId, SchemaVersion version) throws Exception {
        SchemaRegistry.SchemaAndMetadata schemaAndVersion = schemaRegistryService.getSchema(schemaId, version).get();
        assertEquals(version, schemaAndVersion.version);
        assertEquals(schemaId, schemaAndVersion.id);
        return schemaAndVersion.schema;
    }

    private void deleteSchema(String schemaId, SchemaVersion expectedVersion) throws Exception {
        SchemaVersion version = schemaRegistryService.deleteSchema(schemaId, userId).get();
        assertEquals(expectedVersion, version);
    }

    private Schema schema1(SchemaVersion version) {
        return Schema.newBuilder()
            .user(userId)
            .type(SchemaType.PROTOBUF)
            .timestamp(MockClock.millis())
            .isDeleted(false)
            .data("message { required int64 a = 1};".getBytes())
            .build();
    }

    private Schema schema2(SchemaVersion version) {
        return Schema.newBuilder()
            .user(userId)
            .type(SchemaType.PROTOBUF)
            .timestamp(MockClock.millis())
            .isDeleted(false)
            .data("message { required int64 b = 1};".getBytes())
            .build();
    }

    private SchemaVersion version(long version) {
        return new LongSchemaVersion(version);
    }
}
