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
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.schema.SchemaRegistry.SchemaAndMetadata;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SchemaServiceTest extends MockedPulsarServiceBaseTest {

    private static final Clock MockClock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

    private final String schemaId1 = "1/2/3/4";
    private static final String userId = "user";

    private static final String schemaJson1 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
    private static final SchemaData schemaData1 = getSchemaData(schemaJson1);

    private static final String schemaJson2 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}," +
                    "{\"name\":\"field2\",\"type\":\"string\",\"default\":\"foo\"}]}";
    private static final SchemaData schemaData2 = getSchemaData(schemaJson2);

    private static final String schemaJson3 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}," +
                    "{\"name\":\"field2\",\"type\":\"string\"}]}";
    private static final SchemaData schemaData3 = getSchemaData(schemaJson3);

    private SchemaRegistryServiceImpl schemaRegistryService;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setSchemaRegistryStorageClassName("org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorageFactory");
        super.internalSetup();
        BookkeeperSchemaStorage storage = new BookkeeperSchemaStorage(pulsar, mockZooKeeper);
        storage.start();
        Map<SchemaType, SchemaCompatibilityCheck> checkMap = new HashMap<>();
        checkMap.put(SchemaType.AVRO, new AvroSchemaCompatibilityCheck());
        schemaRegistryService = new SchemaRegistryServiceImpl(storage, checkMap, MockClock);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        schemaRegistryService.close();
    }

    @Test
    public void writeReadBackDeleteSchemaEntry() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));

        SchemaData latest = getLatestSchema(schemaId1, version(0));
        assertEquals(schemaData1, latest);

        deleteSchema(schemaId1, version(1));

        assertNull(schemaRegistryService.getSchema(schemaId1).get());
    }

    @Test
    public void findSchemaVersionTest() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        assertEquals(0, schemaRegistryService.findSchemaVersion(schemaId1, schemaData1).get().longValue());
    }

    @Test
    public void deleteSchemaAndAddSchema() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        SchemaData latest = getLatestSchema(schemaId1, version(0));
        assertEquals(schemaData1, latest);

        deleteSchema(schemaId1, version(1));

        assertNull(schemaRegistryService.getSchema(schemaId1).get());

        putSchema(schemaId1, schemaData1, version(2));

        latest = getLatestSchema(schemaId1, version(2));
        assertEquals(schemaData1, latest);

    }

    @Test
    public void getReturnsTheLastWrittenEntry() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));

        SchemaData latest = getLatestSchema(schemaId1, version(1));
        assertEquals(schemaData2, latest);

    }

    @Test
    public void getByVersionReturnsTheCorrectEntry() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));

        SchemaData version0 = getSchema(schemaId1, version(0));
        assertEquals(schemaData1, version0);
    }

    @Test
    public void getByVersionReturnsTheCorrectEntry2() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));

        SchemaData version1 = getSchema(schemaId1, version(1));
        assertEquals(schemaData2, version1);
    }

    @Test
    public void getByVersionReturnsTheCorrectEntry3() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));

        SchemaData version1 = getSchema(schemaId1, version(0));
        assertEquals(schemaData1, version1);
    }

    @Test
    public void getAllVersionSchema() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));
        putSchema(schemaId1, schemaData3, version(2));

        List<SchemaData> allSchemas = getAllSchemas(schemaId1);
        assertEquals(schemaData1, allSchemas.get(0));
        assertEquals(schemaData2, allSchemas.get(1));
        assertEquals(schemaData3, allSchemas.get(2));
    }

    @Test
    public void addLotsOfEntriesThenDelete() throws Exception {

        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));
        putSchema(schemaId1, schemaData3, version(2));

        SchemaData version0 = getSchema(schemaId1, version(0));
        assertEquals(schemaData1, version0);

        SchemaData version1 = getSchema(schemaId1, version(1));
        assertEquals(schemaData2, version1);

        SchemaData version2 = getSchema(schemaId1, version(2));
        assertEquals(schemaData3, version2);

        deleteSchema(schemaId1, version(3));

        SchemaRegistry.SchemaAndMetadata version3 = schemaRegistryService.getSchema(schemaId1, version(3)).get();
        assertNull(version3);

    }

    @Test
    public void writeSchemasToDifferentIds() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        String schemaId2 = "id2";
        putSchema(schemaId2, schemaData3, version(0));

        SchemaData withFirstId = getLatestSchema(schemaId1, version(0));
        SchemaData withDifferentId = getLatestSchema(schemaId2, version(0));

        assertEquals(schemaData1, withFirstId);
        assertEquals(schemaData3, withDifferentId);
    }

    @Test
    public void dontReAddExistingSchemaAtRoot() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData1, version(0));
    }

    @Test
    public void trimDeletedSchemaAndGetListTest() throws Exception {
        List<SchemaAndMetadata> list = new ArrayList<>();
        CompletableFuture<SchemaVersion> put = schemaRegistryService.putSchemaIfAbsent(
                schemaId1, schemaData1, SchemaCompatibilityStrategy.FULL);
        SchemaVersion newVersion = put.get();
        list.add(new SchemaAndMetadata(schemaId1, schemaData1, newVersion));
        put = schemaRegistryService.putSchemaIfAbsent(
                schemaId1, schemaData2, SchemaCompatibilityStrategy.FULL);
        newVersion = put.get();
        list.add(new SchemaAndMetadata(schemaId1, schemaData2, newVersion));
        List<SchemaAndMetadata> list1 = schemaRegistryService.trimDeletedSchemaAndGetList(schemaId1).get();
        assertEquals(list.size(), list1.size());
        HashFunction hashFunction = Hashing.sha256();
        for (int i = 0; i < list.size(); i++) {
            SchemaAndMetadata schemaAndMetadata1 = list.get(i);
            SchemaAndMetadata schemaAndMetadata2 = list1.get(i);
            assertEquals(hashFunction.hashBytes(schemaAndMetadata1.schema.getData()).asBytes(),
                    hashFunction.hashBytes(schemaAndMetadata2.schema.getData()).asBytes());
            assertEquals(((LongSchemaVersion)schemaAndMetadata1.version).getVersion()
                    , ((LongSchemaVersion)schemaAndMetadata2.version).getVersion());
            assertEquals(schemaAndMetadata1.id, schemaAndMetadata2.id);
        }
    }

    @Test
    public void dontReAddExistingSchemaInMiddle() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));
        putSchema(schemaId1, schemaData3, version(2));
        putSchema(schemaId1, schemaData2, version(1));
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void checkIsCompatible() throws Exception {
        putSchema(schemaId1, schemaData1, version(0), SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE);
        putSchema(schemaId1, schemaData2, version(1), SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE);

        assertTrue(schemaRegistryService.isCompatible(schemaId1, schemaData3,
                SchemaCompatibilityStrategy.BACKWARD).get());
        assertFalse(schemaRegistryService.isCompatible(schemaId1, schemaData3,
                SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE).get());
        putSchema(schemaId1, schemaData3, version(2), SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE);
    }

    @Test(expectedExceptions = PulsarServerException.class)
    public void testSchemaStorageFailed() throws Exception {
        conf.setSchemaRegistryStorageClassName("Unknown class name");
        restartBroker();
    }

    private void putSchema(String schemaId, SchemaData schema, SchemaVersion expectedVersion) throws Exception {
        putSchema(schemaId, schema, expectedVersion, SchemaCompatibilityStrategy.FULL);
    }

    private void putSchema(String schemaId, SchemaData schema, SchemaVersion expectedVersion,
                           SchemaCompatibilityStrategy strategy) throws ExecutionException, InterruptedException {
        CompletableFuture<SchemaVersion> put = schemaRegistryService.putSchemaIfAbsent(
                schemaId, schema, strategy);
        SchemaVersion newVersion = put.get();
        assertEquals(expectedVersion, newVersion);
    }

    private SchemaData getLatestSchema(String schemaId, SchemaVersion expectedVersion) throws Exception {
        SchemaRegistry.SchemaAndMetadata schemaAndVersion = schemaRegistryService.getSchema(schemaId).get();
        assertEquals(expectedVersion, schemaAndVersion.version);
        assertEquals(schemaId, schemaAndVersion.id);
        return schemaAndVersion.schema;
    }

    private SchemaData getSchema(String schemaId, SchemaVersion version) throws Exception {
        SchemaRegistry.SchemaAndMetadata schemaAndVersion = schemaRegistryService.getSchema(schemaId, version).get();
        assertEquals(version, schemaAndVersion.version);
        assertEquals(schemaId, schemaAndVersion.id);
        return schemaAndVersion.schema;
    }

    private List<SchemaData> getAllSchemas(String schemaId) throws Exception {
        List<SchemaData> result = new ArrayList<>();
        for (CompletableFuture<SchemaRegistry.SchemaAndMetadata> schema :
                schemaRegistryService.getAllSchemas(schemaId).get()) {
            result.add(schema.get().schema);
        }
        return result;
    }

    private void deleteSchema(String schemaId, SchemaVersion expectedVersion) throws Exception {
        SchemaVersion version = schemaRegistryService.deleteSchema(schemaId, userId).get();
        assertEquals(expectedVersion, version);
    }

    private SchemaData randomSchema() {
        UUID randomString = UUID.randomUUID();
        return SchemaData.builder()
            .user(userId)
            .type(SchemaType.JSON)
            .timestamp(MockClock.millis())
            .isDeleted(false)
            .data(randomString.toString().getBytes())
            .props(new TreeMap<>())
            .build();
    }

    private static SchemaData getSchemaData(String schemaJson) {
        return SchemaData.builder().data(schemaJson.getBytes()).type(SchemaType.AVRO).user(userId).build();
    }

    private SchemaVersion version(long version) {
        return new LongSchemaVersion(version);
    }
}
