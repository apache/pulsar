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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;

import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AdminApiOffloadTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setManagedLedgerMaxEntriesPerLedger(10);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);

        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/ns1", Sets.newHashSet("test"));
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    private void testOffload(String topicName, String mlName) throws Exception {
        LedgerOffloader offloader = mock(LedgerOffloader.class);
        when(offloader.getOffloadDriverName()).thenReturn("mock");

        doReturn(offloader).when(pulsar).getManagedLedgerOffloader(any(), any());

        CompletableFuture<Void> promise = new CompletableFuture<>();
        doReturn(promise).when(offloader).offload(any(), any(), any());

        MessageId currentId = MessageId.latest;
        try (Producer<byte[]> p = pulsarClient.newProducer().topic(topicName).enableBatching(false).create()) {
            for (int i = 0; i < 15; i++) {
                currentId = p.send("Foobar".getBytes());
            }
        }

        ManagedLedgerInfo info = pulsar.getManagedLedgerFactory().getManagedLedgerInfo(mlName);
        Assert.assertEquals(info.ledgers.size(), 2);

        Assert.assertEquals(admin.topics().offloadStatus(topicName).status,
                            LongRunningProcessStatus.Status.NOT_RUN);

        admin.topics().triggerOffload(topicName, currentId);

        Assert.assertEquals(admin.topics().offloadStatus(topicName).status,
                            LongRunningProcessStatus.Status.RUNNING);

        try {
            admin.topics().triggerOffload(topicName, currentId);
            Assert.fail("Should have failed");
        } catch (ConflictException e) {
            // expected
        }

        // fail first time
        promise.completeExceptionally(new Exception("Some random failure"));

        Assert.assertEquals(admin.topics().offloadStatus(topicName).status,
                            LongRunningProcessStatus.Status.ERROR);
        Assert.assertTrue(admin.topics().offloadStatus(topicName).lastError.contains("Some random failure"));

        // Try again
        doReturn(CompletableFuture.completedFuture(null))
            .when(offloader).offload(any(), any(), any());

        admin.topics().triggerOffload(topicName, currentId);

        Assert.assertEquals(admin.topics().offloadStatus(topicName).status,
                            LongRunningProcessStatus.Status.SUCCESS);
        MessageIdImpl firstUnoffloaded = admin.topics().offloadStatus(topicName).firstUnoffloadedMessage;
        // First unoffloaded is the first entry of current ledger
        Assert.assertEquals(firstUnoffloaded.getLedgerId(), info.ledgers.get(1).ledgerId);
        Assert.assertEquals(firstUnoffloaded.getEntryId(), 0);

        verify(offloader, times(2)).offload(any(), any(), any());
    }


    @Test
    public void testOffloadV2() throws Exception {
        String topicName = "persistent://prop-xyz/ns1/topic1";
        String mlName = "prop-xyz/ns1/persistent/topic1";
        testOffload(topicName, mlName);
    }

    @Test
    public void testOffloadV1() throws Exception {
        String topicName = "persistent://prop-xyz/test/ns1/topic2";
        String mlName = "prop-xyz/test/ns1/persistent/topic2";
        testOffload(topicName, mlName);
    }

    @Test
    public void testOffloadPolicies() throws Exception {
        String namespaceName = "prop-xyz/ns1";
        String driver = "aws-s3";
        String region = "test-region";
        String bucket = "test-bucket";
        String endpoint = "test-endpoint";

        OffloadPolicies offload1 = OffloadPolicies.create(
                driver, region, bucket, endpoint, 100, 100, -1, null);
        admin.namespaces().setOffloadPolicies(namespaceName, offload1);
        OffloadPolicies offload2 = admin.namespaces().getOffloadPolicies(namespaceName);
        Assert.assertEquals(offload1, offload2);
    }

}
