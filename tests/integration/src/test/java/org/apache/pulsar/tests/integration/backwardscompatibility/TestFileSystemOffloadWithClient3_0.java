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
package org.apache.pulsar.tests.integration.backwardscompatibility;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.offload.TestBaseOffload;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class TestFileSystemOffloadWithClient3_0 extends TestBaseOffload {
    @Override
    protected Map<String, String> getEnv() {
        Map<String, String> result = new HashMap<>();
        result.put("managedLedgerMaxEntriesPerLedger", String.valueOf(getNumEntriesPerLedger()));
        result.put("managedLedgerMinLedgerRolloverTimeMinutes", "0");
        result.put("managedLedgerOffloadDriver", "filesystem");
        result.put("fileSystemURI", "file:///tmp");

        return result;
    }

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();
        pulsarCluster.getProxy().setDockerImageName(PulsarContainer.PULSAR_3_0_IMAGE_NAME);
    }

    @Test(dataProvider = "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeDeletionLag(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        final String tenant = "offload-test-deletion-lag-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        pulsarCluster.runAdminCommandOnAnyBroker("tenants",
                "create", "--allowed-clusters", pulsarCluster.getClusterName(),
                "--admin-roles", "offload-admin", tenant);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "create", "--clusters", pulsarCluster.getClusterName(), namespace);

        // set threshold to offload runs immediately after role
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-offload-threshold", "--size", "0", namespace);

        String output = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-offload-deletion-lag", namespace).getStdout();
        Assert.assertTrue(output.contains("Unset for namespace"));

        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl.get()).build();

        long offloadedLedger = writeAndWaitForOffload(serviceUrl.get(), adminUrl.get(), topic);
        // give it up to 5 seconds to delete, it shouldn't
        // so we wait this every time
        Thread.sleep(5000);
        Assert.assertTrue(ledgerExistsInBookKeeper(offloadedLedger));

        long finalOffloadedLedger1 = offloadedLedger;
        ManagedLedgerInternalStats.LedgerInfo offloadedLedgerInfo =
                admin.topics().getInternalStats(topic).ledgers.stream()
                        .filter((x) -> x.ledgerId == finalOffloadedLedger1).findFirst().get();
        Assert.assertTrue(offloadedLedgerInfo.offloaded);
        Assert.assertFalse(offloadedLedgerInfo.bookkeeperDeleted);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "set-offload-deletion-lag", namespace,
                "--lag", "0m");
        output = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-offload-deletion-lag", namespace).getStdout();
        Assert.assertTrue(output.contains("0 minute(s)"));

        offloadedLedger = writeAndWaitForOffload(serviceUrl.get(), adminUrl.get(), topic);
        // wait up to 10 seconds for ledger to be deleted
        for (int i = 0; i < 10 && ledgerExistsInBookKeeper(offloadedLedger); i++) {
            writeAndWaitForOffload(serviceUrl.get(), adminUrl.get(), topic);
            Thread.sleep(1000);
        }
        Assert.assertFalse(ledgerExistsInBookKeeper(offloadedLedger));

        long finalOffloadedLedger2 = offloadedLedger;
        offloadedLedgerInfo = admin.topics().getInternalStats(topic).ledgers.stream()
                .filter((x) -> x.ledgerId == finalOffloadedLedger2).findFirst().get();
        Assert.assertTrue(offloadedLedgerInfo.offloaded);

        // old version (3.0.0) server, new version client
        // the new field `bookkeeperDeleted` should always be false
        Assert.assertFalse(offloadedLedgerInfo.bookkeeperDeleted);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "clear-offload-deletion-lag", namespace);

        Thread.sleep(5); // wait 5 seconds to allow broker to see update

        output = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-offload-deletion-lag", namespace).getStdout();
        Assert.assertTrue(output.contains("Unset for namespace"));

        offloadedLedger = writeAndWaitForOffload(serviceUrl.get(), adminUrl.get(), topic);

        // give it up to 5 seconds to delete, it shouldn't
        // so we wait this every time
        Thread.sleep(5000);
        Assert.assertTrue(ledgerExistsInBookKeeper(offloadedLedger));

        long finalOffloadedLedger3 = offloadedLedger;
        offloadedLedgerInfo = admin.topics().getInternalStats(topic).ledgers.stream()
                .filter((x) -> x.ledgerId == finalOffloadedLedger3).findFirst().get();
        Assert.assertTrue(offloadedLedgerInfo.offloaded);
        Assert.assertFalse(offloadedLedgerInfo.bookkeeperDeleted);

        admin.close();
    }
}
