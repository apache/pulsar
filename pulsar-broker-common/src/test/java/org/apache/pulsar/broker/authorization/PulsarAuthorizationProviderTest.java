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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.Level;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.testng.annotations.Test;

public class PulsarAuthorizationProviderTest {

    private static final String MISSING_TENANT = "non-existent-tenant";
    private static final String ROLE = "some-role";

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
}
