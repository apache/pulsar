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
package org.apache.pulsar.functions.worker.rest.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.HashSet;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.worker.LeaderService;
import org.apache.pulsar.functions.worker.MetricsGenerator;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test of {@link WorkerImpl} to ensure that all methods properly fail if the {@link AuthenticationParameters}
 * do not represent a superuser.
 */
public class WorkerImplTest {
    WorkerImpl worker;

    @DataProvider(name = "authParamsForNonSuperusers")
    public Object[][] partitionedTopicProvider() {
        return new Object[][] {
                { AuthenticationParameters.builder().build() }, // all fields null should fail
                { AuthenticationParameters.builder().clientRole("user").build() }, // Not superuser, no proxy
                { AuthenticationParameters.builder()
                        .clientRole("proxySuperuser").build() }, // Proxy role needs original principal
                { AuthenticationParameters.builder()
                        .clientRole("proxyNotSuperuser").build() }, // Proxy role needs original principal
                { AuthenticationParameters.builder().clientRole("proxyNotSuperuser")
                        .originalPrincipal("superuser").build() }, // Proxy is not superuser
                { AuthenticationParameters.builder().clientRole("proxyNotSuperuser")
                        .originalPrincipal("user").build() }, // Neither proxy nor original principal is superuser
                { AuthenticationParameters.builder().clientRole("proxySuperuser")
                        .originalPrincipal("user").build() }, // Original principal is not superuser
                { AuthenticationParameters.builder().clientRole("user")
                        .originalPrincipal("superuser").build() }, // User is not a proxyRole
        };
    }

    @BeforeClass
    void setup() throws Exception {
        WorkerConfig config = new WorkerConfig();
        config.setPulsarFunctionsCluster("testThrowIfNotSuperUserFailures");
        config.setAuthorizationEnabled(true);
        HashSet<String> proxyRoles = new HashSet<>();
        proxyRoles.add("proxySuperuser");
        proxyRoles.add("proxyNotSuperuser");
        HashSet<String> superUserRoles = new HashSet<>();
        superUserRoles.add("superuser");
        superUserRoles.add("superuser2");
        superUserRoles.add("proxySuperuser");
        config.setSuperUserRoles(superUserRoles);
        config.setProxyRoles(proxyRoles);
        AuthorizationService authorizationService = new AuthorizationService(
                PulsarConfigurationLoader.convertFrom(config), mock(PulsarResources.class));
        PulsarWorkerService pulsarWorkerService = mock(PulsarWorkerService.class);
        when(pulsarWorkerService.getWorkerConfig()).thenReturn(config);
        when(pulsarWorkerService.getAuthorizationService()).thenReturn(authorizationService);
        when(pulsarWorkerService.isInitialized()).thenReturn(true);
        when(pulsarWorkerService.getMetricsGenerator()).thenReturn(mock(MetricsGenerator.class));
        LeaderService leaderService = mock(LeaderService.class);
        when(leaderService.isLeader()).thenReturn(true);
        when(pulsarWorkerService.getLeaderService()).thenReturn(leaderService);
        worker = new WorkerImpl(() -> pulsarWorkerService);
    }

    @Test(dataProvider = "authParamsForNonSuperusers")
    public void ensureNonSuperuserCombinationsFailAuthorization(AuthenticationParameters authParams) throws Throwable {
        assertThrowsRestException401(() -> worker.getCluster(authParams));
        assertThrowsRestException401(() -> worker.getClusterLeader(authParams));
        assertThrowsRestException401(() -> worker.getAssignments(authParams));
        assertThrowsRestException401(() -> worker.getWorkerMetrics(authParams));
        assertThrowsRestException401(() -> worker.getFunctionsMetrics(authParams));
        assertThrowsRestException401(() -> worker.getListOfConnectors(authParams));
        assertThrowsRestException401(() -> worker.rebalance(null, authParams));
        assertThrowsRestException401(() -> worker.drain(null, "test", authParams, false));
        assertThrowsRestException401(() -> worker.getDrainStatus(null, "test", authParams, false));
        // This endpoint is not protected
        assertTrue(worker.isLeaderReady(authParams));
    }

    private void assertThrowsRestException401(Assert.ThrowingRunnable runnable) throws Throwable {
        try {
            runnable.run();
            fail("Should have thrown RestException");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), 401);
        }
    }
}
