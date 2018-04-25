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

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Sets;

import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class AdminApiOffloadTest extends MockedPulsarServiceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(AdminApiOffloadTest.class);

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setManagedLedgerMaxEntriesPerLedger(10);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);

        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster("test", new ClusterData("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/ns1", Sets.newHashSet("test"));
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    interface OffloadMethod {
        public MessageId runOffload(String topicName, MessageId currentId) throws Exception;
    }

    private void setupAndOffload(String topicName, String mlName,
                                 OffloadMethod offloadMethod) throws Exception {
        LedgerOffloader offloader = mock(LedgerOffloader.class);
        doReturn(offloader).when(pulsar).getManagedLedgerOffloader();
        doReturn(CompletableFuture.completedFuture(null))
            .when(offloader).offload(anyObject(), anyObject(), anyObject());

        MessageId currentId = MessageId.latest;
        try (Producer p = pulsarClient.newProducer().topic(topicName).enableBatching(false).create()) {
            for (int i = 0; i < 15; i++) {
                currentId = p.send("Foobar".getBytes());
            }
        }

        ManagedLedgerInfo info = pulsar.getManagedLedgerFactory().getManagedLedgerInfo(mlName);
        Assert.assertEquals(info.ledgers.size(), 2);

        MessageId firstUnoffloaded = offloadMethod.runOffload(topicName, currentId);

        // First unoffloaded is the first entry of current ledger
        Assert.assertEquals(((MessageIdImpl)firstUnoffloaded).getLedgerId(),
                            info.ledgers.get(1).ledgerId);
        Assert.assertEquals(((MessageIdImpl)firstUnoffloaded).getEntryId(), 0);

        verify(offloader, times(1)).offload(anyObject(), anyObject(), anyObject());
    }

    @Test
    public void testOffloadV2() throws Exception {
        String topicName = "persistent://prop-xyz/ns1/topic1";
        String mlName = "prop-xyz/ns1/persistent/topic1";

        setupAndOffload(topicName, mlName,
                        (topicName2, currentId) -> admin.persistentTopics().offloadPrefix(topicName2, currentId));
    }

    @Test
    public void testOffloadAsyncV2() throws Exception {
        String topicName = "persistent://prop-xyz/ns1/topic2";
        String mlName = "prop-xyz/ns1/persistent/topic2";

        setupAndOffload(topicName, mlName,
                        (topicName2, currentId)
                        -> admin.persistentTopics().offloadPrefixAsync(topicName2, currentId).get());
    }

    @Test
    public void testOffloadV1() throws Exception {
        String topicName = "persistent://prop-xyz/test/ns1/topic3";
        String mlName = "prop-xyz/test/ns1/persistent/topic3";

        setupAndOffload(topicName, mlName,
                        (topicName2, currentId) -> admin.persistentTopics().offloadPrefix(topicName2, currentId));
    }

    @Test
    public void testOffloadAsyncV1() throws Exception {
        String topicName = "persistent://prop-xyz/test/ns1/topic4";
        String mlName = "prop-xyz/test/ns1/persistent/topic4";

        setupAndOffload(topicName, mlName,
                        (topicName2, currentId)
                        -> admin.persistentTopics().offloadPrefixAsync(topicName2, currentId).get());
    }

}
