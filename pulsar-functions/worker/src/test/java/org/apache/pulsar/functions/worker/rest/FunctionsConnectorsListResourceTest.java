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
package org.apache.pulsar.functions.worker.rest;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImplV2;
import org.apache.pulsar.functions.worker.rest.api.SinksImpl;
import org.apache.pulsar.functions.worker.rest.api.SourcesImpl;
import org.apache.pulsar.functions.worker.rest.api.WorkerImpl;
import org.apache.pulsar.functions.worker.rest.api.v2.FunctionsApiV2Resource;
import org.apache.pulsar.functions.worker.rest.api.v3.FunctionsApiV3Resource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Verifies that the functions connectors list endpoints apply the same checks as the worker connectors endpoint.
 */
@SuppressWarnings("deprecation")
public class FunctionsConnectorsListResourceTest {

    private static final String SUPER_USER = "superuser";
    private static final String PROXY_ROLE = "proxy";

    private WorkerConfig workerConfig;
    private PulsarWorkerService workerService;
    private List<ConnectorDefinition> connectorDefinitions;

    @BeforeMethod
    public void setup() throws Exception {
        workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsCluster("test");
        workerConfig.setAuthorizationEnabled(true);
        workerConfig.setSuperUserRoles(Set.of(SUPER_USER, PROXY_ROLE));
        workerConfig.setProxyRoles(Set.of(PROXY_ROLE));
        AuthorizationService authorizationService = new AuthorizationService(
                PulsarConfigurationLoader.convertFrom(workerConfig), mock(PulsarResources.class));

        ConnectorDefinition source = new ConnectorDefinition();
        source.setName("test-source");
        source.setSourceClass("org.example.TestSource");
        ConnectorDefinition sink = new ConnectorDefinition();
        sink.setName("test-sink");
        sink.setSinkClass("org.example.TestSink");
        connectorDefinitions = List.of(source, sink);
        ConnectorsManager connectorsManager = mock(ConnectorsManager.class);
        when(connectorsManager.getConnectorDefinitions()).thenReturn(connectorDefinitions);

        workerService = mock(PulsarWorkerService.class);
        when(workerService.isInitialized()).thenReturn(true);
        when(workerService.getWorkerConfig()).thenReturn(workerConfig);
        when(workerService.getAuthorizationService()).thenReturn(authorizationService);
        when(workerService.getConnectorsManager()).thenReturn(connectorsManager);
        Supplier<PulsarWorkerService> supplier = () -> workerService;
        FunctionsImpl functions = new FunctionsImpl(supplier);
        doReturn(functions).when(workerService).getFunctions();
        doReturn(new FunctionsImplV2(functions)).when(workerService).getFunctionsV2();
        doReturn(new WorkerImpl(supplier)).when(workerService).getWorkers();
    }

    @DataProvider(name = "resources")
    public Object[][] resources() {
        return new Object[][] {{"v2"}, {"v3"}};
    }

    private List<ConnectorDefinition> getConnectorsList(String version, String role, String originalPrincipal)
            throws Exception {
        ServletContext servletContext = mock(ServletContext.class);
        when(servletContext.getAttribute(FunctionApiResource.ATTRIBUTE_FUNCTION_WORKER)).thenReturn(workerService);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName)).thenReturn(role);
        when(request.getHeader(FunctionApiResource.ORIGINAL_PRINCIPAL_HEADER)).thenReturn(originalPrincipal);
        if ("v2".equals(version)) {
            FunctionsApiV2Resource resource = new FunctionsApiV2Resource();
            resource.servletContext = servletContext;
            resource.httpRequest = request;
            return resource.getConnectorsList();
        } else {
            FunctionsApiV3Resource resource = new FunctionsApiV3Resource();
            resource.servletContext = servletContext;
            resource.httpRequest = request;
            return resource.getConnectorsList();
        }
    }

    private void assertStatus401(String version, String role, String originalPrincipal) {
        RestException e = expectThrows(RestException.class,
                () -> getConnectorsList(version, role, originalPrincipal));
        assertEquals(e.getResponse().getStatus(), 401);
    }

    @Test(dataProvider = "resources")
    public void testConnectorsListRequiresSuperUser(String version) throws Exception {
        assertStatus401(version, null, null);
        assertStatus401(version, "user", null);
        assertEquals(getConnectorsList(version, SUPER_USER, null), connectorDefinitions);
    }

    @Test(dataProvider = "resources")
    public void testConnectorsListChecksOriginalPrincipal(String version) throws Exception {
        assertStatus401(version, PROXY_ROLE, "user");
        assertStatus401(version, PROXY_ROLE, null);
        assertEquals(getConnectorsList(version, PROXY_ROLE, SUPER_USER), connectorDefinitions);
    }

    @Test(dataProvider = "resources")
    public void testConnectorsListWithoutAuthorization(String version) throws Exception {
        workerConfig.setAuthorizationEnabled(false);
        assertEquals(getConnectorsList(version, "user", null), connectorDefinitions);
    }

    @Test
    public void testBuiltinSourcesAndSinksListsUnchanged() {
        Supplier<PulsarWorkerService> supplier = () -> workerService;
        assertEquals(new SourcesImpl(supplier).getSourceList(), List.of(connectorDefinitions.get(0)));
        assertEquals(new SinksImpl(supplier).getSinkList(), List.of(connectorDefinitions.get(1)));
    }
}
