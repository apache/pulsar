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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@Slf4j
public class AdminApiClusterTest extends MockedPulsarServiceBaseTest {
    private final String CLUSTER = "test";

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        admin.clusters()
                .createCluster(CLUSTER, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testCreateClusterBadRequest() {
        try {
            admin.clusters()
                    .createCluster("bad_request", ClusterData.builder()
                            .serviceUrl("pulsar://example.com").build());
            fail("Unexpected behaviour");
        } catch (PulsarAdminException ex) {
            assertEquals(ex.getStatusCode(), 400);
        }
    }

    @Test
    public void testDeleteNonExistCluster() {
        String cluster = "test-non-exist-cluster-" + UUID.randomUUID();

        assertThrows(PulsarAdminException.NotFoundException.class, () -> admin.clusters().deleteCluster(cluster));
    }

    @Test
    public void testDeleteExistCluster() throws PulsarAdminException {
        String cluster = "test-exist-cluster-" + UUID.randomUUID();

        admin.clusters()
                .createCluster(cluster, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.clusters().getCluster(cluster)));

        admin.clusters().deleteCluster(cluster);
    }

    @Test
    public void testDeleteNonExistentFailureDomain() {
        assertThrows(PulsarAdminException.NotFoundException.class,
                () -> admin.clusters().deleteFailureDomain(CLUSTER, "non-existent-failure-domain"));
    }

    @Test
    public void testDeleteNonExistentFailureDomainInNonExistCluster() {
        assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> admin.clusters().deleteFailureDomain(CLUSTER + UUID.randomUUID(),
                        "non-existent-failure-domain"));
    }

    @Test
    public void testDeleteExistFailureDomain() throws PulsarAdminException {
        String domainName = CLUSTER + "-failure-domain";
        FailureDomain domain = FailureDomain.builder()
                .brokers(Set.of("b1", "b2", "b3"))
                .build();
        admin.clusters().createFailureDomain(CLUSTER, domainName, domain);
        Awaitility.await().untilAsserted(() -> admin.clusters().getFailureDomain(CLUSTER, domainName));

        admin.clusters().deleteFailureDomain(CLUSTER, domainName);
    }
}
