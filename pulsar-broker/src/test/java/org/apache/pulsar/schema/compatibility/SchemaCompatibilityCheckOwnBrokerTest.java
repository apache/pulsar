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
package org.apache.pulsar.schema.compatibility;

import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Schema compatibility tests that count the ledgers in the broker's mock BookKeeper, so they run on a broker of
 * their own. {@link SchemaCompatibilityCheckTest} has the tests that share one.
 */
@Test(groups = "schema")
public class SchemaCompatibilityCheckOwnBrokerTest extends MockedPulsarServiceBaseTest {
    private static final String CLUSTER_NAME = "test";

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton(CLUSTER_NAME))
                .build();
        admin.tenants().createTenant(PUBLIC_TENANT, tenantInfo);
        String namespaceName = PUBLIC_TENANT + "/" + DEFAULT_NAMESPACE;
        admin.namespaces().createNamespace(namespaceName, Sets.newHashSet(CLUSTER_NAME));
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSchemaLedgerAutoRelease() throws Exception {
        String namespaceName = PUBLIC_TENANT + "/" + DEFAULT_NAMESPACE;
        String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespaceName + "/tp");
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName,
                SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        // Update schema 100 times.
        for (int i = 0; i < 100; i++){
            Schema schema = Schema.JSON(SchemaDefinition.builder()
                    .withJsonDef(String.format("""
                            {
                                "type": "record",
                                "name": "Test_Pojo",
                                "namespace": "org.apache.pulsar.schema.compatibility",
                                "fields": [{
                                    "name": "prop_%s",
                                    "type": ["null", "string"],
                                    "default": null
                                }]
                            }
                            """, i))
                    .build());
            Producer producer = pulsarClient
                    .newProducer(schema)
                    .topic(topicName)
                    .create();
            producer.close();
        }
        // The other ledgers are about 5.
        Assert.assertTrue(pulsarTestContext.getMockBookKeeper().getLedgerMap().values().stream()
                .filter(ledger -> !ledger.isFenced())
                .collect(Collectors.toList()).size() < 20);
        admin.topics().delete(topicName, true);
    }
}
