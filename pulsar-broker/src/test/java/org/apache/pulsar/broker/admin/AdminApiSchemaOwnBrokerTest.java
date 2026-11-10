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
package org.apache.pulsar.broker.admin;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Schema admin api tests that stub the broker's BookKeeper client, so they run on a broker of their own.
 * {@link AdminApiSchemaTest} has the tests that share one.
 */
@Test(groups = "broker-admin")
public class AdminApiSchemaOwnBrokerTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setSystemTopicEnabled(false);
        conf.setTopicLevelPoliciesEnabled(false);
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("schematest", tenantInfo);
        admin.namespaces().createNamespace("schematest/test", Set.of("test"));
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    public static class Foo {
        int intField;
    }

    @Test
    void getTopicIntervalStateIncludeSchemaStoreLedger() throws PulsarAdminException {
        String topicName = "persistent://schematest/test/get-schema-ledger-info";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, "test", MessageId.earliest);
        Schema<Foo> schema = Schema.AVRO(Foo.class);
        admin.schemas().createSchema(topicName, schema.getSchemaInfo());
        long ledgerId = 1;
        long entryId = 10;
        long length = 10;
        doReturn(CompletableFuture.completedFuture(new LedgerMetadata() {
            @Override
            public long getLedgerId() {
                return ledgerId;
            }

            @Override
            public int getEnsembleSize() {
                return 0;
            }

            @Override
            public int getWriteQuorumSize() {
                return 0;
            }

            @Override
            public int getAckQuorumSize() {
                return 0;
            }

            @Override
            public long getLastEntryId() {
                return entryId;
            }

            @Override
            public long getLength() {
                return length;
            }

            @Override
            public boolean hasPassword() {
                return false;
            }

            @Override
            public byte[] getPassword() {
                return new byte[0];
            }

            @Override
            public DigestType getDigestType() {
                return null;
            }

            @Override
            public long getCtime() {
                return 0;
            }

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public Map<String, byte[]> getCustomMetadata() {
                return null;
            }

            @Override
            public List<BookieId> getEnsembleAt(long entryId) {
                return null;
            }

            @Override
            public NavigableMap<Long, ? extends List<BookieId>> getAllEnsembles() {
                return null;
            }

            @Override
            public State getState() {
                return null;
            }

            @Override
            public String toSafeString() {
                return "test";
            }

            @Override
            public int getMetadataFormatVersion() {
                return 0;
            }

            @Override
            public long getCToken() {
                return 0;
            }
        })).when(pulsarTestContext.getBookKeeperClient()).getLedgerMetadata(anyLong());
        PersistentTopicInternalStats persistentTopicInternalStats = admin.topics().getInternalStats(topicName);
        List<PersistentTopicInternalStats.LedgerInfo> list = persistentTopicInternalStats.schemaLedgers;
        assertEquals(list.size(), 1);
        PersistentTopicInternalStats.LedgerInfo ledgerInfo = list.get(0);
        assertEquals(ledgerInfo.ledgerId, ledgerId);
        assertEquals(ledgerInfo.entries, entryId + 1);
        assertEquals(ledgerInfo.size, length);
    }
}
