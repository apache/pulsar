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
package org.apache.pulsar.broker.admin;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for schema admin api.
 */
@Slf4j
@Test(groups = "broker-admin")
public class AdminApiSchemaTest extends MockedPulsarServiceBaseTest {

    final String cluster = "test";
    private final String schemaCompatibilityNamespace = "schematest/test-schema-compatibility-ns";

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster(cluster, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("schematest", tenantInfo);
        admin.namespaces().createNamespace("schematest/test", Sets.newHashSet("test"));
        admin.namespaces().createNamespace("schematest/"+cluster+"/test", Sets.newHashSet("test"));
        admin.namespaces().createNamespace(schemaCompatibilityNamespace, Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    enum ApiVersion{
        V1, V2;
    }

    public static class Foo {
        int intField;
    }

    public static class Foo1 {
        int intField;

        String file1;
    }

    private static final Map<String, String> PROPS;

    static {
        PROPS = new HashMap<>();
        PROPS.put("key1", "value1");
    }

    @DataProvider(name = "schemas")
    public Object[][] schemas() {
        return new Object[][] {
            { Schema.BOOL },
            { Schema.INT8 },
            { Schema.INT16 },
            { Schema.INT32 },
            { Schema.INT64 },
            { StringSchema.utf8() },
            { new StringSchema(US_ASCII) },
            { Schema.FLOAT },
            { Schema.DOUBLE },
            { Schema.DATE },
            { Schema.TIME },
            { Schema.TIMESTAMP },
            { Schema.INSTANT },
            { Schema.LOCAL_DATE},
            { Schema.LOCAL_TIME},
            { Schema.LOCAL_DATE_TIME},
            { Schema.AVRO(
                SchemaDefinition.builder()
                    .withPojo(Foo.class)
                    .withProperties(PROPS)
                    .build()
            ) },
            { Schema.JSON(
                SchemaDefinition.builder()
                    .withPojo(Foo.class)
                    .withProperties(PROPS)
                    .build()
            )},
            { Schema.KeyValue(
                StringSchema.utf8(),
                new StringSchema(US_ASCII)
            )}
        };
    }

    @DataProvider(name = "version")
    public Object[][] versions() {
        return new Object[][] { { ApiVersion.V1 }, { ApiVersion.V2 } };
    }

    @Test(dataProvider = "schemas")
    public void testSchemaInfoApi(Schema<?> schema) throws Exception {
        testSchemaInfoApi(schema, "schematest/test/test-" + schema.getSchemaInfo().getType());
    }

    @Test(dataProvider = "schemas")
    public void testSchemaInfoWithVersionApi(Schema<?> schema) throws Exception {
        testSchemaInfoWithVersionApi(schema, "schematest/test/test-" + schema.getSchemaInfo().getType());
    }

    private <T> void testSchemaInfoApi(Schema<T> schema,
                                       String topicName) throws Exception {
        SchemaInfo si = schema.getSchemaInfo();
        admin.schemas().createSchema(topicName, si);
        log.info("Upload schema to topic {} : {}", topicName, si);

        SchemaInfo readSi = admin.schemas().getSchemaInfo(topicName);
        log.info("Read schema of topic {} : {}", topicName, readSi);

        ((SchemaInfoImpl)readSi).setTimestamp(0);
        assertEquals(readSi, si);

        readSi = admin.schemas().getSchemaInfo(topicName + "-partition-0");
        log.info("Read schema of topic {} : {}", topicName, readSi);

        ((SchemaInfoImpl)readSi).setTimestamp(0);
        assertEquals(readSi, si);

    }

    @Test(dataProvider = "version")
    public void testPostSchemaCompatibilityStrategy(ApiVersion version) throws PulsarAdminException {
        String namespace = format("%s%s%s", "schematest", (ApiVersion.V1.equals(version) ? "/" + cluster + "/" : "/"),
                "test");
        String topicName = "persistent://"+namespace + "/testStrategyChange";
        SchemaInfo fooSchemaInfo = Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(false)
                .withPojo(Foo.class).build())
                .getSchemaInfo();

        admin.schemas().createSchema(topicName, fooSchemaInfo);
        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace, SchemaAutoUpdateCompatibilityStrategy.Backward);
        SchemaInfo foo1SchemaInfo = Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(false)
                .withPojo(Foo1.class).build())
                .getSchemaInfo();

        try {
            admin.schemas().createSchema(topicName, foo1SchemaInfo);
            fail("Should have failed");
        } catch (PulsarAdminException.ConflictException e) {
            assertTrue(e.getMessage().contains("HTTP 409"));
        }

        namespace = "schematest/testnotfound";
        topicName = namespace + "/testStrategyChange";

        try {
            admin.schemas().createSchema(topicName, fooSchemaInfo);
            fail("Should have failed");
        } catch (PulsarAdminException.NotFoundException e) {
            assertTrue(e.getMessage().contains("HTTP 404"));
        }
    }

    private <T> void testSchemaInfoWithVersionApi(Schema<T> schema,
                                       String topicName) throws Exception {
        SchemaInfo si = schema.getSchemaInfo();
        admin.schemas().createSchema(topicName, si);
        log.info("Upload schema to topic {} : {}", topicName, si);

        SchemaInfoWithVersion readSi = admin.schemas().getSchemaInfoWithVersion(topicName);
        log.info("Read schema of topic {} : {}", topicName, readSi);

        ((SchemaInfoImpl)readSi.getSchemaInfo()).setTimestamp(0);
        assertEquals(readSi.getSchemaInfo(), si);
        assertEquals(readSi.getVersion(), 0);

        readSi = admin.schemas().getSchemaInfoWithVersion(topicName + "-partition-0");
        log.info("Read schema of topic {} : {}", topicName, readSi);

        ((SchemaInfoImpl)readSi.getSchemaInfo()).setTimestamp(0);
        assertEquals(readSi.getSchemaInfo(), si);
        assertEquals(readSi.getVersion(), 0);

    }

    @Test(dataProvider = "version")
    public void createKeyValueSchema(ApiVersion version) throws Exception {
        String namespace = format("%s%s%s", "schematest", (ApiVersion.V1.equals(version) ? "/" + cluster + "/" : "/"),
                "test");
        String topicName = "persistent://"+namespace + "/test-key-value-schema";
        Schema keyValueSchema = Schema.KeyValue(Schema.AVRO(Foo.class), Schema.AVRO(Foo.class));
        admin.schemas().createSchema(topicName, keyValueSchema.getSchemaInfo());
        SchemaInfo schemaInfo = admin.schemas().getSchemaInfo(topicName);

        long timestamp = schemaInfo.getTimestamp();
        assertNotEquals(keyValueSchema.getSchemaInfo().getTimestamp(), timestamp);
        assertNotEquals(0, timestamp);

        ((SchemaInfoImpl)keyValueSchema.getSchemaInfo()).setTimestamp(schemaInfo.getTimestamp());
        assertEquals(keyValueSchema.getSchemaInfo(), schemaInfo);

        admin.schemas().createSchema(topicName, keyValueSchema.getSchemaInfo());
        SchemaInfo schemaInfo2 = admin.schemas().getSchemaInfo(topicName);
        assertEquals(timestamp, schemaInfo2.getTimestamp());
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
        })).when(mockBookKeeper).getLedgerMetadata(anyLong());
        PersistentTopicInternalStats persistentTopicInternalStats = admin.topics().getInternalStats(topicName);
        List<PersistentTopicInternalStats.LedgerInfo> list = persistentTopicInternalStats.schemaLedgers;
        assertEquals(list.size(), 1);
        PersistentTopicInternalStats.LedgerInfo ledgerInfo = list.get(0);
        assertEquals(ledgerInfo.ledgerId, ledgerId);
        assertEquals(ledgerInfo.entries, entryId + 1);
        assertEquals(ledgerInfo.size, length);
    }

    @Test
    public void testGetSchemaCompatibilityStrategy() throws PulsarAdminException {
        assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(schemaCompatibilityNamespace),
                SchemaCompatibilityStrategy.UNDEFINED);
    }

    @Test
    public void testGetSchemaAutoUpdateCompatibilityStrategy() throws PulsarAdminException {
        assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(schemaCompatibilityNamespace));
    }

    @Test
    public void testGetSchemaCompatibilityStrategyWhenSetSchemaAutoUpdateCompatibilityStrategy()
            throws PulsarAdminException {
        assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(schemaCompatibilityNamespace),
                SchemaCompatibilityStrategy.UNDEFINED);

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(schemaCompatibilityNamespace,
                SchemaAutoUpdateCompatibilityStrategy.Forward);
        Awaitility.await().untilAsserted(() -> assertEquals(SchemaAutoUpdateCompatibilityStrategy.Forward,
                admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(schemaCompatibilityNamespace)
        ));

        assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(schemaCompatibilityNamespace),
                SchemaCompatibilityStrategy.UNDEFINED);

        admin.namespaces().setSchemaCompatibilityStrategy(schemaCompatibilityNamespace,
                SchemaCompatibilityStrategy.BACKWARD);
        Awaitility.await().untilAsserted(() -> assertEquals(SchemaCompatibilityStrategy.BACKWARD,
                admin.namespaces().getSchemaCompatibilityStrategy(schemaCompatibilityNamespace)));
    }

    @Test
    public void testGetSchemaCompatibilityStrategyWhenSetBrokerLevelAndSchemaAutoUpdateCompatibilityStrategy()
            throws PulsarAdminException {
        pulsar.getConfiguration().setSchemaCompatibilityStrategy(SchemaCompatibilityStrategy.FORWARD);

        assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(schemaCompatibilityNamespace),
                SchemaCompatibilityStrategy.UNDEFINED);

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(schemaCompatibilityNamespace,
                SchemaAutoUpdateCompatibilityStrategy.AlwaysCompatible);
        Awaitility.await().untilAsserted(() -> assertEquals(
                admin.namespaces().getSchemaCompatibilityStrategy(schemaCompatibilityNamespace),
                SchemaCompatibilityStrategy.UNDEFINED));
    }
}
