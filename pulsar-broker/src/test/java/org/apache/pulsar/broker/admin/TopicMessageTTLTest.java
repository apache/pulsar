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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class TopicMessageTTLTest extends MockedPulsarServiceBaseTest {

    private final String testTenant = "my-tenant";

    private final String testNamespace = "my-namespace";

    private final String myNamespace = testTenant + "/" + testNamespace;

    private final String testTopic = "persistent://" + myNamespace + "/test-topic-message-ttl";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(true);
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet("test"));
        admin.topics().createPartitionedTopic(testTopic, 2);
        Producer producer = pulsarClient.newProducer().topic(testTenant + "/" + testNamespace + "/" + "dummy-topic").create();
        producer.close();
        Thread.sleep(3000);
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSetThenRemoveMessageTTL() throws Exception {
        admin.topics().setMessageTTL(testTopic, 100);
        log.info("Message TTL set success on topic: {}", testTopic);

        Thread.sleep(3000);
        Integer messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info("Message TTL {} get on topic: {}", testTopic, messageTTL);
        Assert.assertEquals(messageTTL.intValue(), 100);

        Thread.sleep(3000);
        admin.topics().removeMessageTTL(testTopic);
        messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info("Message TTL {} get on topic: {}", testTopic, messageTTL);
        Assert.assertEquals(messageTTL.intValue(), 0);
    }

    @Test
    public void testSetInvalidMessageTTL() throws Exception {
        try {
            admin.topics().setMessageTTL(testTopic, -100);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        try {
            admin.topics().setMessageTTL(testTopic, (int)2147483650L);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }
    }

    @Test
    public void testGetMessageTTL() throws Exception {
        // Check default topic level message TTL.
        Integer messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info("Message TTL {} get on topic: {}", testTopic, messageTTL);
        Assert.assertEquals(messageTTL.intValue(), 0);

        admin.topics().setMessageTTL(testTopic, 200);
        log.info("Message TTL set success on topic: {}", testTopic);

        Thread.sleep(3000);
        messageTTL = admin.topics().getMessageTTL(testTopic);
        log.info("Message TTL {} get on topic: {}", testTopic, messageTTL);
        Assert.assertEquals(messageTTL.intValue(), 200);
    }

    @Test
    public void testTopicPolicyDisabled() throws Exception {
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(false);
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet("test"));
        admin.topics().createPartitionedTopic(testTopic, 2);

        try {
            admin.topics().getMessageTTL(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 405);
        }

        try {
            admin.topics().setMessageTTL(testTopic, 200);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 405);
        }
    }

}
