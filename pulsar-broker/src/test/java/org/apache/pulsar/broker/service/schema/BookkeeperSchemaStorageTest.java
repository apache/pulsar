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

import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorage.bkException;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "broker")
public class BookkeeperSchemaStorageTest extends MockedPulsarServiceBaseTest {

    private static final String CLUSTER_NAME = "test";

    private static final String NAMESPACE_NAME = PUBLIC_TENANT + "/default";

    @Test
    public void testBkException() {
        Exception ex = bkException("test", BKException.Code.ReadException, 1, -1);
        assertEquals("Error while reading ledger -  ledger=1 - operation=test", ex.getMessage());
        ex = bkException("test", BKException.Code.ReadException, 1, 0);
        assertEquals("Error while reading ledger -  ledger=1 - operation=test - entry=0",
                ex.getMessage());
        ex = bkException("test", BKException.Code.QuorumException, 1, -1);
        assertEquals("Invalid quorum size on ensemble size -  ledger=1 - operation=test",
                ex.getMessage());
        ex = bkException("test", BKException.Code.QuorumException, 1, 0);
        assertEquals("Invalid quorum size on ensemble size -  ledger=1 - operation=test - entry=0",
                ex.getMessage());
    }

    @Test
    public void testVersionFromBytes() {
        long version = System.currentTimeMillis();

        ByteBuffer bbPre240 = ByteBuffer.allocate(Long.SIZE);
        bbPre240.putLong(version);
        byte[] versionBytesPre240 = bbPre240.array();

        ByteBuffer bbPost240 = ByteBuffer.allocate(Long.BYTES);
        bbPost240.putLong(version);
        byte[] versionBytesPost240 = bbPost240.array();

        PulsarService mockPulsarService = mock(PulsarService.class);
        when(mockPulsarService.getLocalMetadataStore()).thenReturn(mock(MetadataStoreExtended.class));
        BookkeeperSchemaStorage schemaStorage = new BookkeeperSchemaStorage(mockPulsarService);
        assertEquals(new LongSchemaVersion(version), schemaStorage.versionFromBytes(versionBytesPre240));
        assertEquals(new LongSchemaVersion(version), schemaStorage.versionFromBytes(versionBytesPost240));
    }

    @Test
    public void testTrimDeletedSchemaAndGetList() throws Exception{
        String topicName = "persistent://" + NAMESPACE_NAME + "/tp";
        // Create multi version schema.
        SchemaCompatibilityStrategy originalStrategy =
                admin.namespaces().getPolicies(NAMESPACE_NAME).schema_compatibility_strategy;
        if (originalStrategy != SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE) {
            admin.namespaces()
                    .setSchemaCompatibilityStrategy(NAMESPACE_NAME, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        }
        List<Class> classes = Arrays.asList(Integer.class, Long.class, Boolean.class);
        for (Class c : classes){
            Schema schema = Schema.KeyValue(Integer.class, c);
            Producer producer = pulsarClient
                    .newProducer(schema)
                    .topic(topicName)
                    .create();
            producer.close();
        }
        // Delete the last version schema.
        BookkeeperSchemaStorage schemaStorage = (BookkeeperSchemaStorage) pulsar.getSchemaStorage();
        String schemaId = TopicName.get(topicName).getSchemaName();
        BookkeeperSchemaStorage.LocatorEntry locatorEntry = schemaStorage.getLocator(schemaId).join().get();
        assertEquals(classes.size(), locatorEntry.locator.getIndexList().size());
        mockBookKeeper.deleteLedger(
                locatorEntry.locator.getIndexList().get(classes.size() - 1).getPosition().getLedgerId());
        // Trigger the trim action.
        try {
            schemaStorage.get(schemaId, new LongSchemaVersion(locatorEntry.locator.getInfo().getVersion())).join();
            fail("expect SchemaException: ledger not found");
        } catch (Exception ex){
            assertTrue(ex.getCause() instanceof SchemaException);
            SchemaException schemaException = (SchemaException) ex.getCause();
            assertFalse(schemaException.isRecoverable());
        }
        // Verify schema meta already deleted.
        Awaitility.await().until(() -> {
            assertNull(mockZooKeeper.exists("/schemas/" + topicName.substring("persistent://".length()), false));
            return schemaStorage.getLocator(schemaId).join().isEmpty();
        });
        StoredSchema storedSchema = schemaStorage.get(schemaId, SchemaVersion.Latest).join();
        assertNull(storedSchema);
        // Assert producer work ok.
        for (Class c : classes){
            Schema schema = Schema.KeyValue(Integer.class, c);
            Producer producer = pulsarClient
                    .newProducer(schema)
                    .topic(topicName)
                    .create();
            producer.newMessage().send();
            producer.close();
        }
        assertEquals(classes.size(), locatorEntry.locator.getIndexList().size());
        // Cleanup
        admin.topics().delete(topicName, true);
        if (originalStrategy != SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE) {
            admin.namespaces()
                    .setSchemaCompatibilityStrategy(NAMESPACE_NAME, originalStrategy);
        }
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress())
                .build());
        TenantInfo tenantInfo = TenantInfo.builder().allowedClusters(Collections.singleton(CLUSTER_NAME)).build();
        admin.tenants().createTenant(PUBLIC_TENANT, tenantInfo);
        if (!admin.namespaces().getNamespaces(PUBLIC_TENANT).contains(NAMESPACE_NAME)) {
            admin.namespaces().createNamespace(NAMESPACE_NAME, Sets.newHashSet(CLUSTER_NAME));
        }
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setAutoSkipNonRecoverableData(true);
    }
}
