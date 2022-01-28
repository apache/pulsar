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

import com.google.common.collect.Sets;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "flaky")
public class TopicMessageTTLTest extends MockedPulsarServiceBaseTest {

    private final String testTenant = "my-tenant";
    private final String testCluster = "test";
    private final String testNamespace = "my-namespace";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/test-topic-message-ttl";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        resetConfig();
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(true);
        this.conf.setTtlDurationDefaultInSeconds(3600);
        super.internalSetup();

        admin.clusters().createCluster(testCluster, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet(testCluster));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet(testCluster));
        admin.topics().createPartitionedTopic(testTopic, 2);
        Producer producer = pulsarClient.newProducer().topic(testTenant + "/" + testNamespace + "/" + "dummy-topic").create();
        producer.close();
        waitForZooKeeperWatchers();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "isV1")
    public Object[][] isV1() {
        return new Object[][] { { true }, { false } };
    }

    @Test
    public void testSetThenRemoveMessageTTL() throws Exception {
        admin.topics().setMessageTTL(testTopic, 100);
        log.info("Message TTL set success on topic: {}", testTopic);

        waitForZooKeeperWatchers();
        Integer messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info("Message TTL {} get on topic: {}", testTopic, messageTTL);
        Assert.assertEquals(messageTTL.intValue(), 100);

        waitForZooKeeperWatchers();
        admin.topics().removeMessageTTL(testTopic);
        messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info("Message TTL {} get on topic: {}", testTopic, messageTTL);
        Assert.assertNull(messageTTL);
    }

    @Test
    public void testSetInvalidMessageTTL() throws Exception {
        try {
            admin.topics().setMessageTTL(testTopic, -100);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.PRECONDITION_FAILED_412);
        }

        try {
            admin.topics().setMessageTTL(testTopic, (int)2147483650L);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.PRECONDITION_FAILED_412);
        }
    }

    @Test
    public void testGetMessageTTL() throws Exception {
        // Check default topic level message TTL.
        Integer messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info("Message TTL {} get on topic: {}", testTopic, messageTTL);
        Assert.assertNull(messageTTL);

        admin.topics().setMessageTTL(testTopic, 200);
        log.info("Message TTL set success on topic: {}", testTopic);

        waitForZooKeeperWatchers();
        messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info("Message TTL {} get on topic: {}", testTopic, messageTTL);
        Assert.assertEquals(messageTTL.intValue(), 200);
    }

    @Test
    public void testTopicPolicyDisabled() throws Exception {
        super.internalCleanup();
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(false);
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet("test"));
        admin.topics().createPartitionedTopic(testTopic, 2);

        try {
            admin.topics().getMessageTTL(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().setMessageTTL(testTopic, 200);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @Test(timeOut = 20000)
    public void testDifferentLevelPolicyPriority() throws Exception {
        final String topicName = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();

        Integer namespaceMessageTTL = admin.namespaces().getNamespaceMessageTTL(myNamespace);
        Assert.assertNull(namespaceMessageTTL);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                (int) persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), 3600));

        admin.namespaces().setNamespaceMessageTTL(myNamespace, 10);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.namespaces().getNamespaceMessageTTL(myNamespace).intValue(), 10));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                (int) persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), 10));

        admin.namespaces().setNamespaceMessageTTL(myNamespace, 0);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.namespaces().getNamespaceMessageTTL(myNamespace).intValue(), 0));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                (int) persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), 0));

        admin.namespaces().removeNamespaceMessageTTL(myNamespace);
        Awaitility.await().untilAsserted(()
                -> Assert.assertNull(admin.namespaces().getNamespaceMessageTTL(myNamespace)));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                (int) persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), 3600));
    }

    @Test(dataProvider = "isV1")
    public void testNamespaceTTL(boolean isV1) throws Exception {
        String myNamespace = testTenant + "/" + (isV1 ? testCluster + "/" : "") + "n1"+isV1;
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet(testCluster));

        admin.namespaces().setNamespaceMessageTTL(myNamespace, 10);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.namespaces().getNamespaceMessageTTL(myNamespace).intValue(), 10));

        admin.namespaces().removeNamespaceMessageTTL(myNamespace);
        Awaitility.await().untilAsserted(()
                -> Assert.assertNull(admin.namespaces().getNamespaceMessageTTL(myNamespace)));
    }

    @Test(timeOut = 20000)
    public void testDifferentLevelPolicyApplied() throws Exception {
        final String topicName = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        //namespace-level default value is null
        Integer namespaceMessageTTL = admin.namespaces().getNamespaceMessageTTL(myNamespace);
        Assert.assertNull(namespaceMessageTTL);
        //topic-level default value is null
        Integer topicMessageTTL = admin.topics().getMessageTTL(topicName);
        Assert.assertNull(topicMessageTTL);
        //use broker-level by default
        int topicMessageTTLApplied = admin.topics().getMessageTTL(topicName, true);
        Assert.assertEquals(topicMessageTTLApplied, 3600);

        admin.namespaces().setNamespaceMessageTTL(myNamespace, 10);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.namespaces().getNamespaceMessageTTL(myNamespace).intValue(), 10));
        topicMessageTTLApplied = admin.topics().getMessageTTL(topicName, true);
        Assert.assertEquals(topicMessageTTLApplied, 10);

        admin.namespaces().setNamespaceMessageTTL(myNamespace, 0);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.namespaces().getNamespaceMessageTTL(myNamespace).intValue(), 0));
        topicMessageTTLApplied = admin.topics().getMessageTTL(topicName, true);
        Assert.assertEquals(topicMessageTTLApplied, 0);

        admin.topics().setMessageTTL(topicName, 20);
        Awaitility.await().untilAsserted(()
                -> Assert.assertNotNull(admin.topics().getMessageTTL(topicName)));
        topicMessageTTLApplied = admin.topics().getMessageTTL(topicName, true);
        Assert.assertEquals(topicMessageTTLApplied, 20);

        admin.namespaces().removeNamespaceMessageTTL(myNamespace);
        admin.topics().removeMessageTTL(topicName);
        Awaitility.await().untilAsserted(()
                -> Assert.assertEquals(admin.topics().getMessageTTL(topicName, true).intValue(), 3600));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                (int) persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().get(), 3600));
    }

}
