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
package org.apache.pulsar.broker.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import jakarta.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.logging.log4j.Level;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.testng.annotations.Test;

public class PulsarAuthorizationProviderTest {

    private static final String MISSING_TENANT = "non-existent-tenant";
    private static final String ROLE = "some-role";
    private static final TopicName SEGMENT = TopicName.get("segment://tenant/ns/orders/0000-7fff-1");

    private static PulsarAuthorizationProvider providerReturning(
            CompletableFuture<Optional<TenantInfo>> tenantLookup) throws Exception {
        TenantResources tenantResources = mock(TenantResources.class);
        when(tenantResources.getTenantAsync(MISSING_TENANT)).thenReturn(tenantLookup);
        PulsarResources pulsarResources = mock(PulsarResources.class);
        when(pulsarResources.getTenantResources()).thenReturn(tenantResources);

        PulsarAuthorizationProvider provider = new PulsarAuthorizationProvider();
        provider.initialize(new ServiceConfiguration(), pulsarResources);
        return provider;
    }

    /** A tenant that was never created: {@code getTenantAsync} succeeds with an empty Optional. */
    private static PulsarAuthorizationProvider providerWithMissingTenant() throws Exception {
        return providerReturning(CompletableFuture.completedFuture(Optional.empty()));
    }

    private static void assertNotFound(ExecutionException ee) {
        RestException restException = (RestException) ee.getCause();
        assertEquals(restException.getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    /**
     * A client asking about a tenant that does not exist is a client-side 404, not a broker fault, so
     * it must not be reported at ERROR. Any client can trigger it at will simply by mistyping a tenant.
     */
    @Test
    public void testMissingTenantIsNotLoggedAtErrorByValidateTenantAdminAccess() throws Exception {
        PulsarAuthorizationProvider provider = providerWithMissingTenant();

        try (LogCapture logs = LogCapture.attach(PulsarAuthorizationProvider.class)) {
            CompletableFuture<Boolean> future =
                    provider.validateTenantAdminAccess(MISSING_TENANT, ROLE, null);

            assertNotFound(expectThrows(ExecutionException.class, future::get));
            assertEquals(logs.messagesAt(Level.ERROR), List.of(),
                    "A tenant that does not exist must not be logged at ERROR");
        }
    }

    /**
     * The path an actual LOOKUP takes: ServerCnx -> AuthorizationService -> allowTopicOperationAsync.
     * One ERROR line per lookup here is what floods a broker when a misconfigured client hammers a
     * cluster that does not host its tenant.
     */
    @Test
    public void testMissingTenantIsNotLoggedAtErrorByLookupAuthorization() throws Exception {
        PulsarAuthorizationProvider provider = providerWithMissingTenant();
        TopicName topicName = TopicName.get("persistent://" + MISSING_TENANT + "/ns/topic");

        try (LogCapture logs = LogCapture.attach(PulsarAuthorizationProvider.class)) {
            CompletableFuture<Boolean> future =
                    provider.allowTopicOperationAsync(topicName, ROLE, TopicOperation.LOOKUP, null);

            assertNotFound(expectThrows(ExecutionException.class, future::get));
            assertEquals(logs.messagesAt(Level.ERROR), List.of(),
                    "A tenant that does not exist must not be logged at ERROR");
        }
    }

    /**
     * Guards the fix from over-reaching: a genuine metadata store failure is a broker-side fault and
     * must still be reported at ERROR.
     */
    @Test
    public void testMetadataStoreFailureIsStillLoggedAtError() throws Exception {
        PulsarAuthorizationProvider provider = providerReturning(CompletableFuture.failedFuture(
                new MetadataStoreException("simulated metadata store failure")));

        try (LogCapture logs = LogCapture.attach(PulsarAuthorizationProvider.class)) {
            CompletableFuture<Boolean> future =
                    provider.validateTenantAdminAccess(MISSING_TENANT, ROLE, null);

            expectThrows(ExecutionException.class, future::get);
            assertFalse(logs.messagesAt(Level.ERROR).isEmpty(),
                    "A metadata store failure must still be logged at ERROR");
        }
    }

    private static PulsarAuthorizationProvider providerWithPolicies(Policies policies, ServiceConfiguration conf)
            throws Exception {
        NamespaceResources namespaceResources = mock(NamespaceResources.class);
        when(namespaceResources.getPoliciesAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(policies)));
        PulsarResources pulsarResources = mock(PulsarResources.class);
        when(pulsarResources.getNamespaceResources()).thenReturn(namespaceResources);

        PulsarAuthorizationProvider provider = new PulsarAuthorizationProvider();
        provider.initialize(conf, pulsarResources);
        return provider;
    }

    private static Policies policiesGrantingOnTopic(String topic, String role, AuthAction action) {
        Policies policies = new Policies();
        policies.auth_policies.getTopicAuthentication()
                .computeIfAbsent(topic, __ -> new HashMap<>())
                .put(role, Set.of(action));
        return policies;
    }

    /** A segment of a scalable topic is authorized with the permissions granted on its parent topic://. */
    @Test
    public void testSegmentIsAuthorizedWithParentTopicPermissions() throws Exception {
        PulsarAuthorizationProvider provider = providerWithPolicies(
                policiesGrantingOnTopic("topic://tenant/ns/orders", ROLE, AuthAction.produce),
                new ServiceConfiguration());

        assertTrue(provider.canProduceAsync(SEGMENT, ROLE, null).get());
        assertTrue(provider.canLookupAsync(SEGMENT, ROLE, null).get());
        assertFalse(provider.canConsumeAsync(SEGMENT, ROLE, null, null).get(),
                "Only produce was granted on the parent topic");
        assertFalse(provider.canProduceAsync(SEGMENT, "other-role", null).get());
        assertFalse(provider.canProduceAsync(
                TopicName.get("segment://tenant/ns/payments/0000-7fff-1"), ROLE, null).get(),
                "A segment of another scalable topic must not get the permissions of this one");
    }

    @Test
    public void testSegmentIsAuthorizedWithNamespacePermissions() throws Exception {
        Policies policies = new Policies();
        policies.auth_policies.getNamespaceAuthentication().put(ROLE, Set.of(AuthAction.consume));
        PulsarAuthorizationProvider provider = providerWithPolicies(policies, new ServiceConfiguration());

        assertTrue(provider.canConsumeAsync(SEGMENT, ROLE, null, null).get());
        assertFalse(provider.canProduceAsync(SEGMENT, ROLE, null).get());
    }

    @Test
    public void testSegmentIsAuthorizedWithParentTopicWildcardPermissions() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setAuthorizationAllowWildcardsMatching(true);
        PulsarAuthorizationProvider provider = providerWithPolicies(
                policiesGrantingOnTopic("topic://tenant/ns/orders", "app-*", AuthAction.produce), conf);

        assertTrue(provider.canProduceAsync(SEGMENT, "app-1", null).get());
        assertFalse(provider.canProduceAsync(SEGMENT, "other-role", null).get());
    }

    /**
     * A scalable topic has no partitions: unlike a partition of a partitioned topic, a scalable topic whose name
     * ends with "-partition-N" is a topic of its own, and the permissions of the name without the suffix don't
     * apply to it or to its segments.
     */
    @Test
    public void testScalableTopicNameWithPartitionSuffixIsNotAPartition() throws Exception {
        Policies policies = policiesGrantingOnTopic("topic://tenant/ns/orders", ROLE, AuthAction.produce);
        policies.auth_policies.getTopicAuthentication()
                .put("persistent://tenant/ns/orders", Map.of(ROLE, Set.of(AuthAction.produce)));
        PulsarAuthorizationProvider provider = providerWithPolicies(policies, new ServiceConfiguration());

        assertFalse(provider.canProduceAsync(TopicName.get("topic://tenant/ns/orders-partition-1"), ROLE, null).get());
        assertFalse(provider.canProduceAsync(
                TopicName.get("segment://tenant/ns/orders-partition-1/0000-ffff-0"), ROLE, null).get());
        assertTrue(provider.canProduceAsync(TopicName.get("persistent://tenant/ns/orders-partition-1"), ROLE, null)
                .get(), "A partition of a partitioned topic still gets the permissions of the partitioned topic");
    }

    /** A provider whose updates of the namespace policies are applied to {@code policies}. */
    private static PulsarAuthorizationProvider providerUpdatingPolicies(Policies policies, boolean readOnly)
            throws Exception {
        return providerUpdatingPolicies(policies, policies, readOnly);
    }

    /**
     * A provider that reads {@code readPolicies} from the metadata store, and whose updates of the namespace policies
     * are applied to {@code updatedPolicies}, as when another broker changed them in between and the update is
     * retried on their latest version. Its cache hasn't caught up with the store: it has no permissions at all.
     */
    private static PulsarAuthorizationProvider providerUpdatingPolicies(Policies readPolicies,
                                                                       Policies updatedPolicies, boolean readOnly)
            throws Exception {
        NamespaceResources namespaceResources = mock(NamespaceResources.class);
        when(namespaceResources.getPoliciesAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(new Policies())));
        when(namespaceResources.refreshAndGetPoliciesAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(readPolicies)));
        when(namespaceResources.getPoliciesReadOnlyAsync()).thenReturn(CompletableFuture.completedFuture(readOnly));
        when(namespaceResources.setPoliciesAsync(any(), any())).thenAnswer(invocation -> {
            Function<Policies, Policies> update = invocation.getArgument(1);
            update.apply(updatedPolicies);
            return CompletableFuture.completedFuture(null);
        });
        PulsarResources pulsarResources = mock(PulsarResources.class);
        when(pulsarResources.getNamespaceResources()).thenReturn(namespaceResources);
        return new PulsarAuthorizationProvider(new ServiceConfiguration(), pulsarResources);
    }

    /**
     * Copying the permissions of a regular topic to the scalable topic it is migrated to adds, for each role, the
     * permissions granted on the regular topic itself to those already granted on the scalable topic.
     */
    @Test
    public void testCopyPermissionsAddsThePermissionsOfTheSourceTopic() throws Exception {
        String source = "persistent://tenant/ns/orders";
        String target = "topic://tenant/ns/orders";
        Policies policies = new Policies();
        Map<String, Map<String, Set<AuthAction>>> topicPermissions = policies.auth_policies.getTopicAuthentication();
        topicPermissions.put(source, new HashMap<>(Map.of(
                "producer", Set.of(AuthAction.produce),
                "consumer", Set.of(AuthAction.consume))));
        topicPermissions.put(source + "-partition-0",
                new HashMap<>(Map.of("partition-reader", Set.of(AuthAction.consume))));
        topicPermissions.put(target, new HashMap<>(Map.of(
                "consumer", Set.of(AuthAction.produce),
                "scalable-reader", Set.of(AuthAction.consume))));
        policies.auth_policies.getNamespaceAuthentication().put("namespace-reader", Set.of(AuthAction.consume));
        PulsarAuthorizationProvider provider = providerUpdatingPolicies(policies, false);

        provider.copyPermissionsAsync(TopicName.get(source), TopicName.get(target)).get();

        assertEquals(topicPermissions.get(target), Map.of(
                "producer", Set.of(AuthAction.produce),
                "consumer", Set.of(AuthAction.produce, AuthAction.consume),
                "scalable-reader", Set.of(AuthAction.consume)));
        assertEquals(topicPermissions.get(source), Map.of(
                "producer", Set.of(AuthAction.produce),
                "consumer", Set.of(AuthAction.consume)));
    }

    /**
     * The permissions are copied from the version of the policies being updated: a permission revoked on the source
     * topic after the policies were read, by another broker, is not restored on the target topic.
     */
    @Test
    public void testCopyPermissionsDoesNotRestoreAPermissionRevokedMeanwhile() throws Exception {
        String source = "persistent://tenant/ns/orders";
        String target = "topic://tenant/ns/orders";
        Policies readPolicies = new Policies();
        readPolicies.auth_policies.getTopicAuthentication().put(source, new HashMap<>(Map.of(
                "revoked", Set.of(AuthAction.produce),
                "kept", Set.of(AuthAction.consume))));
        Policies updatedPolicies = new Policies();
        updatedPolicies.auth_policies.getTopicAuthentication()
                .put(source, new HashMap<>(Map.of("kept", Set.of(AuthAction.consume))));
        PulsarAuthorizationProvider provider = providerUpdatingPolicies(readPolicies, updatedPolicies, false);

        provider.copyPermissionsAsync(TopicName.get(source), TopicName.get(target)).get();

        assertEquals(updatedPolicies.auth_policies.getTopicAuthentication().get(target),
                Map.of("kept", Set.of(AuthAction.consume)));
    }

    /** A topic without permissions of its own has nothing to copy: the policies are left alone, even read-only. */
    @Test
    public void testCopyPermissionsOfTopicWithoutPermissions() throws Exception {
        Policies policies = policiesGrantingOnTopic("persistent://tenant/ns/payments", ROLE, AuthAction.produce);
        PulsarAuthorizationProvider provider = providerUpdatingPolicies(policies, true);

        provider.copyPermissionsAsync(TopicName.get("persistent://tenant/ns/orders"),
                TopicName.get("topic://tenant/ns/orders")).get();

        assertFalse(policies.auth_policies.getTopicAuthentication().containsKey("topic://tenant/ns/orders"));
    }

    /** Permissions that can't be copied fail the copy, rather than letting their roles lose access. */
    @Test
    public void testCopyPermissionsFailsWithReadOnlyPolicies() throws Exception {
        PulsarAuthorizationProvider provider = providerUpdatingPolicies(
                policiesGrantingOnTopic("persistent://tenant/ns/orders", ROLE, AuthAction.produce), true);

        ExecutionException e = expectThrows(ExecutionException.class, () -> provider.copyPermissionsAsync(
                TopicName.get("persistent://tenant/ns/orders"), TopicName.get("topic://tenant/ns/orders")).get());
        assertTrue(e.getCause() instanceof IllegalStateException, "Unexpected failure: " + e.getCause());
    }
}
