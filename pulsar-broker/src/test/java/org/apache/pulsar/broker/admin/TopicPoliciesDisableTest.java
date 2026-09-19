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

import java.util.Set;
import lombok.CustomLog;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-admin")
public class TopicPoliciesDisableTest extends MockedPulsarServiceBaseTest {

    private final String testTenant = "my-tenant";

    private final String testNamespace = "my-namespace";

    private final String myNamespace = testTenant + "/" + testNamespace;

    private final String testTopic = "persistent://" + myNamespace + "/test-set-backlog-quota";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setTopicLevelPoliciesEnabled(false);
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Set.of("test"));
        admin.topics().createPartitionedTopic(testTopic, 2);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testBacklogQuotaDisabled() {
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info().attr("backlogQuota", backlogQuota).attr("topic", testTopic)
                .log("Backlog quota will set to the topic");

        try {
            admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().removeBacklogQuota(testTopic, BacklogQuota.BacklogQuotaType.destination_storage);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().getBacklogQuotaMap(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testRetentionDisabled() {
        RetentionPolicies retention = new RetentionPolicies();
        log.info().attr("retention", retention).attr("topic", testTopic)
                .log("Retention will set to the topic");

        try {
            admin.topics().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().getRetention(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPersistenceDisabled() {
        PersistencePolicies persistencePolicies = new PersistencePolicies();
        log.info().attr("persistencePolicies", persistencePolicies).attr("topic", testTopic)
                .log("PersistencePolicies will set to the topic");

        try {
            admin.topics().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().getPersistence(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDispatchRateDisabled() throws Exception {
        DispatchRate dispatchRate = DispatchRate.builder().build();
        log.info().attr("dispatchRate", dispatchRate).attr("topic", testTopic)
                .log("Dispatch Rate will set to the topic");

        try {
            admin.topics().setDispatchRate(testTopic, dispatchRate);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().getDispatchRate(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSubscriptionDispatchRateDisabled() throws Exception {
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1000)
                .dispatchThrottlingRateInMsg(1020 * 1024)
                .ratePeriodInSecond(1)
                .build();
        log.info().attr("dispatchRate", dispatchRate).attr("topic", testTopic)
                .log("Dispatch Rate will set to the topic");

        try {
            admin.topics().setSubscriptionDispatchRate(testTopic, dispatchRate);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 405);
        }

        try {
            admin.topics().getSubscriptionDispatchRate(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCompactionThresholdDisabled() {
        Long compactionThreshold = 10000L;
        log.info().attr("compactionThreshold", compactionThreshold).attr("topic", testTopic)
                .log("Compaction threshold will set to the topic");

        try {
            admin.topics().setCompactionThreshold(testTopic, compactionThreshold);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().getCompactionThreshold(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMaxConsumersPerSubscription() throws Exception {
        int maxConsumersPerSubscription = 10;
        log.info().attr("maxConsumersPerSubscription", maxConsumersPerSubscription)
                .attr("topic", testTopic)
                .log("MaxConsumersPerSubscription will set to the topic");

        try {
            admin.topics().setMaxConsumersPerSubscription(testTopic, maxConsumersPerSubscription);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().getMaxConsumersPerSubscription(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().removeMaxConsumersPerSubscription(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSubscriptionExpirationTimeDisabled() throws Exception {
        int subscriptionExpirationTime = 10;
        log.info().attr("subscriptionExpirationTime", subscriptionExpirationTime)
                .attr("topic", testTopic)
                .log("SubscriptionExpirationTime will set to the topic");

        try {
            admin.topicPolicies().setSubscriptionExpirationTime(testTopic, subscriptionExpirationTime);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topicPolicies().getSubscriptionExpirationTime(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topicPolicies().removeSubscriptionExpirationTime(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPublishRateDisabled() throws Exception {
        PublishRate publishRate = new PublishRate(10000, 1024 * 1024 * 5);
        log.info().attr("publishRate", publishRate).attr("topic", testTopic)
                .log("Publish Rate will set to the topic");

        try {
            admin.topics().setPublishRate(testTopic, publishRate);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().getPublishRate(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMaxProducersDisabled() {
        log.info().attr("topic", testTopic).log("MaxProducers will set to the topic");
        try {
            admin.topics().setMaxProducers(testTopic, 2);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().getMaxProducers(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMaxConsumersDisabled() {
        log.info().attr("topic", testTopic).log("MaxConsumers will set to the topic");
        try {
            admin.topics().setMaxConsumers(testTopic, 2);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }

        try {
            admin.topics().getMaxConsumers(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSubscribeRateDisabled() throws Exception {
        SubscribeRate subscribeRate = new SubscribeRate(10, 30);
        log.info().attr("subscribeRate", subscribeRate).attr("topic", testTopic)
                .log("Subscribe Rate will set to the topic");

        try {
            admin.topics().setSubscribeRate(testTopic, subscribeRate);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 405);
        }

        try {
            admin.topics().getSubscribeRate(testTopic);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 405);
        }
    }
}
