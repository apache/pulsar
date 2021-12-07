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

@Slf4j
@Test(groups = "broker")
public class TopicPoliciesDisableTest extends MockedPulsarServiceBaseTest {

    private final String testTenant = "my-tenant";

    private final String testNamespace = "my-namespace";

    private final String myNamespace = testTenant + "/" + testNamespace;

    private final String testTopic = "persistent://" + myNamespace + "/test-set-backlog-quota";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(false);
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet("test"));
        admin.topics().createPartitionedTopic(testTopic, 2);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testBacklogQuotaDisabled() {
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);

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

    @Test
    public void testRetentionDisabled() {
        RetentionPolicies retention = new RetentionPolicies();
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);

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

    @Test
    public void testPersistenceDisabled() {
        PersistencePolicies persistencePolicies = new PersistencePolicies();
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);

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

    @Test
    public void testDispatchRateDisabled() throws Exception {
        DispatchRate dispatchRate = DispatchRate.builder().build();
        log.info("Dispatch Rate: {} will set to the topic: {}", dispatchRate, testTopic);

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

    @Test
    public void testSubscriptionDispatchRateDisabled() throws Exception {
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1000)
                .dispatchThrottlingRateInMsg(1020*1024)
                .ratePeriodInSecond(1)
                .build();
        log.info("Dispatch Rate: {} will set to the topic: {}", dispatchRate, testTopic);

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

    @Test
    public void testCompactionThresholdDisabled() {
        Long compactionThreshold = 10000L;
        log.info("Compaction threshold: {} will set to the topic: {}", compactionThreshold, testTopic);

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

    @Test
    public void testMaxConsumersPerSubscription() throws Exception {
        int maxConsumersPerSubscription = 10;
        log.info("MaxConsumersPerSubscription: {} will set to the topic: {}", maxConsumersPerSubscription, testTopic);

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

    @Test
    public void testPublishRateDisabled() throws Exception {
        PublishRate publishRate = new PublishRate(10000, 1024 * 1024 * 5);
        log.info("Publish Rate: {} will set to the topic: {}", publishRate, testTopic);

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

    @Test
    public void testMaxProducersDisabled() {
        log.info("MaxProducers will set to the topic: {}", testTopic);
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

    @Test
    public void testMaxConsumersDisabled() {
        log.info("MaxConsumers will set to the topic: {}", testTopic);
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

    @Test
    public void testSubscribeRateDisabled() throws Exception {
        SubscribeRate subscribeRate = new SubscribeRate(10, 30);
        log.info("Subscribe Rate: {} will set to the topic: {}", subscribeRate, testTopic);

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
