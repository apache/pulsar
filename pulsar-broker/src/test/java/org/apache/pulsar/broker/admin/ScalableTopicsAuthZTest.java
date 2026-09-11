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

import java.util.List;
import java.util.UUID;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class ScalableTopicsAuthZTest extends AuthZTest {

    private static final String NAMESPACE = "public/default";

    @SneakyThrows
    @BeforeClass(alwaysRun = true)
    public void setup() {
        configureTokenAuthentication();
        configureDefaultAuthorization();
        start();
        this.superUserAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
        final TenantInfo tenantInfo = superUserAdmin.tenants().getTenantInfo("public");
        tenantInfo.getAdminRoles().add(TENANT_ADMIN_SUBJECT);
        superUserAdmin.tenants().updateTenant("public", tenantInfo);
        this.tenantManagerAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(TENANT_ADMIN_TOKEN))
                .build();
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void cleanup() {
        close();
    }

    private PulsarAdmin nobodyAdmin() throws Exception {
        return PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(NOBODY_TOKEN))
                .build();
    }

    private static String randomTopic() {
        return NAMESPACE + "/" + UUID.randomUUID();
    }

    /** A segment URL is segment://tenant/namespace/topic/descriptor. */
    private static String segmentTopicOf(String topic) {
        return "segment://" + topic + "/0000-7fff-1";
    }

    /** Asserts that calling {@code call} throws {@link PulsarAdminException.NotAuthorizedException}. */
    private static void assertNotAuthorized(ThrowingRunnable call) {
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, call::run);
    }

    /**
     * Asserts that {@code call} either succeeds or fails with anything other than NotAuthorized.
     * Used to verify that the role is allowed past the authz check, without depending on the
     * scalable-topic happy path being fully wired up in the test harness.
     */
    private static void assertAuthorized(ThrowingRunnable call) {
        try {
            call.run();
        } catch (PulsarAdminException.NotAuthorizedException e) {
            Assert.fail("Operation should have been authorized, got NotAuthorized instead", e);
        } catch (Throwable ignored) {
            // Any other failure is fine — we only care about the authz contract.
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Throwable;
    }

    // -------- ScalableTopics: tenant-admin-allowed endpoints --------

    @Test
    public void testListScalableTopicsAuthZ() throws Exception {
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().listScalableTopics(NAMESPACE));
        assertAuthorized(() -> tenantManagerAdmin.scalableTopics().listScalableTopics(NAMESPACE));
        assertAuthorized(() -> superUserAdmin.scalableTopics().listScalableTopics(NAMESPACE));
    }

    @Test
    public void testCreateScalableTopicAuthZ() throws Exception {
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().createScalableTopic(randomTopic(), 1));
        assertAuthorized(() -> tenantManagerAdmin.scalableTopics().createScalableTopic(randomTopic(), 1));
        assertAuthorized(() -> superUserAdmin.scalableTopics().createScalableTopic(randomTopic(), 1));
    }

    @Test
    public void testGetScalableTopicMetadataAuthZ() throws Exception {
        final String topic = randomTopic();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().getMetadata(topic));
        assertAuthorized(() -> tenantManagerAdmin.scalableTopics().getMetadata(topic));
        assertAuthorized(() -> superUserAdmin.scalableTopics().getMetadata(topic));
    }

    @Test
    public void testDeleteScalableTopicAuthZ() throws Exception {
        final String topic = randomTopic();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().deleteScalableTopic(topic, true));
        // tenant admin allowed; we let it succeed (or get NotFound on a second attempt).
        assertAuthorized(() -> tenantManagerAdmin.scalableTopics().deleteScalableTopic(topic, true));
        assertAuthorized(() -> superUserAdmin.scalableTopics().deleteScalableTopic(randomTopic(), true));
    }

    @Test
    public void testGetStatsAuthZ() throws Exception {
        final String topic = randomTopic();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().getStats(topic));
        assertAuthorized(() -> tenantManagerAdmin.scalableTopics().getStats(topic));
        assertAuthorized(() -> superUserAdmin.scalableTopics().getStats(topic));
    }

    @Test
    public void testCreateSubscriptionAuthZ() throws Exception {
        final String topic = randomTopic();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics()
                .createSubscription(topic, "sub-nobody", ScalableSubscriptionType.STREAM));
        assertAuthorized(() -> tenantManagerAdmin.scalableTopics()
                .createSubscription(topic, "sub-tenant", ScalableSubscriptionType.STREAM));
        assertAuthorized(() -> superUserAdmin.scalableTopics()
                .createSubscription(topic, "sub-super", ScalableSubscriptionType.STREAM));
    }

    @Test
    public void testDeleteSubscriptionAuthZ() throws Exception {
        final String topic = randomTopic();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().deleteSubscription(topic, "sub-x"));
        assertAuthorized(() -> tenantManagerAdmin.scalableTopics().deleteSubscription(topic, "sub-x"));
        assertAuthorized(() -> superUserAdmin.scalableTopics().deleteSubscription(topic, "sub-x"));
    }

    // -------- ScalableTopics: super-user-only endpoints --------

    @Test
    public void testSplitSegmentAuthZ() throws Exception {
        final String topic = randomTopic();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().splitSegment(topic, 1L));
        assertNotAuthorized(() -> tenantManagerAdmin.scalableTopics().splitSegment(topic, 1L));
        assertAuthorized(() -> superUserAdmin.scalableTopics().splitSegment(topic, 1L));
    }

    @Test
    public void testMergeSegmentsAuthZ() throws Exception {
        final String topic = randomTopic();
        superUserAdmin.scalableTopics().createScalableTopic(topic, 1);
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().mergeSegments(topic, 1L, 2L));
        assertNotAuthorized(() -> tenantManagerAdmin.scalableTopics().mergeSegments(topic, 1L, 2L));
        assertAuthorized(() -> superUserAdmin.scalableTopics().mergeSegments(topic, 1L, 2L));
    }

    // -------- Segments resource: super-user only on all endpoints --------

    @Test
    public void testCreateSegmentAuthZ() throws Exception {
        final String segment = segmentTopicOf(randomTopic());
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().createSegment(segment, List.of()));
        assertNotAuthorized(() -> tenantManagerAdmin.scalableTopics().createSegment(segment, List.of()));
        assertAuthorized(() -> superUserAdmin.scalableTopics().createSegment(segment, List.of()));
    }

    @Test
    public void testTerminateSegmentAuthZ() throws Exception {
        final String segment = segmentTopicOf(randomTopic());
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().terminateSegment(segment));
        assertNotAuthorized(() -> tenantManagerAdmin.scalableTopics().terminateSegment(segment));
        assertAuthorized(() -> superUserAdmin.scalableTopics().terminateSegment(segment));
    }

    @Test
    public void testDeleteSegmentAuthZ() throws Exception {
        final String segment = segmentTopicOf(randomTopic());
        @Cleanup PulsarAdmin nobody = nobodyAdmin();
        assertNotAuthorized(() -> nobody.scalableTopics().deleteSegment(segment, true));
        assertNotAuthorized(() -> tenantManagerAdmin.scalableTopics().deleteSegment(segment, true));
        assertAuthorized(() -> superUserAdmin.scalableTopics().deleteSegment(segment, true));
    }
}
