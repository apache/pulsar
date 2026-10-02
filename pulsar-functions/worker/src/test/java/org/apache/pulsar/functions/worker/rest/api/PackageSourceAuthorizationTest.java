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
package org.apache.pulsar.functions.worker.rest.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.utils.ValidatableFunctionPackage;
import org.apache.pulsar.functions.utils.io.Connector;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PackageSourceAuthorizationTest {
    private static final String PACKAGE = "function://other-tenant/private/code@v1";
    private static final NamespaceName PACKAGE_NAMESPACE = NamespaceName.get("other-tenant/private");
    private PulsarWorkerService worker;
    private PulsarAdmin admin;
    private AuthorizationService authorization;
    private AuthenticationParameters authParams;
    private WorkerConfig config;

    @BeforeMethod
    public void setup() {
        worker = mock(PulsarWorkerService.class);
        admin = mock(PulsarAdmin.class);
        authorization = mock(AuthorizationService.class);
        config = new WorkerConfig();
        config.setAuthorizationEnabled(true);
        config.setFunctionsWorkerEnablePackageManagement(true);
        config.setMetadataStoreOperationTimeoutSeconds(5);
        authParams = AuthenticationParameters.builder().clientRole("proxy").originalPrincipal("caller")
                .clientAuthenticationDataSource(mock(AuthenticationDataSource.class)).build();
        when(worker.isInitialized()).thenReturn(true);
        when(worker.getWorkerConfig()).thenReturn(config);
        when(worker.getBrokerAdmin()).thenReturn(admin);
        when(worker.getAuthorizationService()).thenReturn(authorization);
        when(authorization.allowFunctionOpsAsync(any(), any(AuthenticationParameters.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(authorization.allowSinkOpsAsync(any(), any(AuthenticationParameters.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(authorization.allowSourceOpsAsync(any(), any(AuthenticationParameters.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
    }

    private void permission(CompletableFuture<Boolean> result) {
        when(authorization.allowNamespaceOperationAsync(PACKAGE_NAMESPACE, NamespaceOperation.PACKAGES,
                "caller", "proxy", authParams.getClientAuthenticationDataSource())).thenReturn(result);
    }

    @DataProvider
    public Object[][] requests() {
        return new Object[][] {
                {"function", false}, {"function", true}, {"source", false}, {"source", true},
                {"sink", false}, {"sink", true}, {"transform", false}, {"transform", true}
        };
    }

    private void request(String kind, boolean update, String packageUrl) {
        switch (kind) {
            case "function":
                FunctionsImpl functions = new FunctionsImpl(() -> worker);
                if (update) {
                    functions.updateFunction("tenant", "ns", "name", null, null, packageUrl,
                            new FunctionConfig(), authParams, null);
                } else {
                    functions.registerFunction("tenant", "ns", "name", null, null, packageUrl,
                            new FunctionConfig(), authParams);
                }
                break;
            case "source":
                SourcesImpl sources = new SourcesImpl(() -> worker);
                if (update) {
                    sources.updateSource("tenant", "ns", "name", null, null, packageUrl,
                            new SourceConfig(), authParams, null);
                } else {
                    sources.registerSource("tenant", "ns", "name", null, null, packageUrl,
                            new SourceConfig(), authParams);
                }
                break;
            default:
                SinksImpl sinks = new SinksImpl(() -> worker);
                SinkConfig sinkConfig = new SinkConfig();
                if (kind.equals("transform")) {
                    sinkConfig.setTransformFunction(packageUrl);
                    sinkConfig.setArchive("builtin://example");
                    packageUrl = null;
                }
                if (update) {
                    sinks.updateSink("tenant", "ns", "name", null, null, packageUrl,
                            sinkConfig, authParams, null);
                } else {
                    sinks.registerSink("tenant", "ns", "name", null, null, packageUrl,
                            sinkConfig, authParams);
                }
        }
    }

    @Test(dataProvider = "requests")
    public void deniedPackageIsNotDownloaded(String kind, boolean update) {
        permission(CompletableFuture.completedFuture(false));
        assertThatThrownBy(() -> request(kind, update, PACKAGE)).isInstanceOfSatisfying(RestException.class,
                e -> assertThat(e.getResponse().getStatus()).isEqualTo(401));
        verifyNoInteractions(admin);
    }

    @Test(dataProvider = "requests")
    public void invalidPackageUrlIsNotDownloaded(String kind, boolean update) {
        assertThatThrownBy(() -> request(kind, update, "function://invalid"))
                .isInstanceOfSatisfying(RestException.class,
                        e -> assertThat(e.getResponse().getStatus()).isEqualTo(400));
        verifyNoInteractions(admin);
    }

    @Test(dataProvider = "requests")
    public void authorizationFailureSkipsDownload(String kind, boolean update) {
        permission(CompletableFuture.failedFuture(new IllegalStateException("authorization unavailable")));
        assertThatThrownBy(() -> request(kind, update, PACKAGE)).isInstanceOfSatisfying(RestException.class,
                e -> assertThat(e.getResponse().getStatus()).isEqualTo(500));
        verifyNoInteractions(admin);
    }

    @DataProvider
    public Object[][] downloads() {
        return new Object[][] {
                {"function", true}, {"function", false}, {"source", true}, {"source", false},
                {"sink", true}, {"sink", false}, {"transform", true}, {"transform", false}
        };
    }

    @Test(dataProvider = "downloads")
    public void packagePermissionControlsDownload(String kind, boolean allowed) throws Exception {
        permission(CompletableFuture.completedFuture(allowed));
        when(admin.tenants()).thenReturn(mock(Tenants.class));
        Namespaces namespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(namespaces);
        when(namespaces.getNamespaces("tenant")).thenReturn(List.of("tenant/ns"));
        when(worker.getFunctionMetaDataManager()).thenReturn(mock(FunctionMetaDataManager.class));
        if (kind.equals("transform")) {
            ConnectorsManager connectors = mock(ConnectorsManager.class);
            Connector connector = mock(Connector.class);
            when(worker.getConnectorsManager()).thenReturn(connectors);
            when(connectors.getConnector("example")).thenReturn(connector);
            when(connector.getConnectorFunctionPackage()).thenReturn(mock(ValidatableFunctionPackage.class));
        }
        Packages packages = mock(Packages.class);
        when(admin.packages()).thenReturn(packages);
        // Stop the request at the package download.
        doThrow(new PulsarAdminException("download reached")).when(packages).download(any(), any());
        PulsarFunctionTestTemporaryDirectory directory =
                PulsarFunctionTestTemporaryDirectory.create("package-source-test");
        try {
            directory.useTemporaryDirectoriesForWorkerConfig(config);
            if (allowed) {
                assertThatThrownBy(() -> request(kind, false, PACKAGE)).hasMessageContaining("download reached");
                verify(packages).download(eq(PACKAGE), any());
            } else {
                assertThatThrownBy(() -> request(kind, false, PACKAGE))
                        .isInstanceOfSatisfying(RestException.class,
                                e -> assertThat(e.getResponse().getStatus()).isEqualTo(401));
                verifyNoInteractions(packages);
            }
        } finally {
            directory.delete();
        }
    }

    @Test
    public void permissionTimeoutSkipsDownload() {
        config.setMetadataStoreOperationTimeoutSeconds(0);
        permission(new CompletableFuture<>());
        assertThatThrownBy(() -> request("function", false, PACKAGE)).isInstanceOfSatisfying(RestException.class,
                e -> assertThat(e.getResponse().getStatus()).isEqualTo(500));
        verifyNoInteractions(admin);
    }

    @Test
    public void synchronousAuthorizationFailureSkipsDownload() {
        when(authorization.allowNamespaceOperationAsync(PACKAGE_NAMESPACE, NamespaceOperation.PACKAGES,
                "caller", "proxy", authParams.getClientAuthenticationDataSource()))
                .thenThrow(new IllegalStateException("authorization unavailable"));
        assertThatThrownBy(() -> request("function", false, PACKAGE)).isInstanceOfSatisfying(RestException.class,
                e -> assertThat(e.getResponse().getStatus()).isEqualTo(500));
        verifyNoInteractions(admin);
    }

    @Test
    public void interruptedPermissionCheckPreservesInterrupt() {
        permission(new CompletableFuture<>());
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> request("function", false, PACKAGE)).isInstanceOfSatisfying(RestException.class,
                    e -> assertThat(e.getResponse().getStatus()).isEqualTo(500));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verifyNoInteractions(admin);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void waitsForPermissionBeforeContinuing() throws Exception {
        CompletableFuture<Boolean> decision = new CompletableFuture<>();
        CompletableFuture<Void> invoked = new CompletableFuture<>();
        when(authorization.allowNamespaceOperationAsync(PACKAGE_NAMESPACE, NamespaceOperation.PACKAGES,
                "caller", "proxy", authParams.getClientAuthenticationDataSource())).thenAnswer(invocation -> {
                    invoked.complete(null);
                    return decision;
                });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<Void> result = CompletableFuture.runAsync(
                    () -> request("function", false, PACKAGE), executor);
            invoked.get(5, TimeUnit.SECONDS);
            assertThat(result).isNotDone();
            verifyNoInteractions(admin);
            decision.complete(false);
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(RestException.class);
            verifyNoInteractions(admin);
        } finally {
            decision.complete(false);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @DataProvider
    public Object[][] principals() {
        return new Object[][] {
                {"caller", null, Set.of("caller"), true},
                {"caller", null, Set.of("other"), false},
                {"proxy", "caller", Set.of("proxy", "caller"), true},
                {"proxy", "caller", Set.of("proxy"), false},
                {"proxy", "caller", Set.of("caller"), false},
                {"proxy", null, Set.of("proxy"), false},
                {"caller", "other", Set.of("caller", "other"), false}
        };
    }

    @Test(dataProvider = "principals")
    public void packagePermissionChecksProxyAndOriginalPrincipal(String role, String original,
                                                                 Set<String> allowedRoles, boolean allowed)
            throws Exception {
        config.setProxyRoles(Set.of("proxy"));
        PulsarResources resources = mock(PulsarResources.class);
        TenantResources tenants = mock(TenantResources.class);
        NamespaceResources namespaces = mock(NamespaceResources.class);
        when(resources.getTenantResources()).thenReturn(tenants);
        when(resources.getNamespaceResources()).thenReturn(namespaces);
        when(tenants.getTenantAsync(PACKAGE_NAMESPACE.getTenant()))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(TenantInfo.builder().build())));
        Policies policies = new Policies();
        allowedRoles.forEach(r -> policies.auth_policies.getNamespaceAuthentication()
                .put(r, Set.of(AuthAction.packages)));
        when(namespaces.getPoliciesAsync(PACKAGE_NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(policies)));
        AuthorizationService service =
                new AuthorizationService(PulsarConfigurationLoader.convertFrom(config), resources);
        when(worker.getAuthorizationService()).thenReturn(service);
        AuthenticationParameters parameters = AuthenticationParameters.builder().clientRole(role)
                .originalPrincipal(original)
                .clientAuthenticationDataSource(authParams.getClientAuthenticationDataSource())
                .build();
        FunctionsImpl functions = new FunctionsImpl(() -> worker);
        if (allowed) {
            functions.checkPackageSourcePermission(PACKAGE, parameters);
        } else {
            assertThatThrownBy(() -> functions.checkPackageSourcePermission(PACKAGE, parameters))
                    .isInstanceOfSatisfying(RestException.class,
                            e -> assertThat(e.getResponse().getStatus()).isEqualTo(401));
        }
    }

    @Test
    public void authorizationDisabledAndNonPackageUrlsKeepExistingBehavior() {
        FunctionsImpl functions = new FunctionsImpl(() -> worker);
        config.setAuthorizationEnabled(false);
        functions.checkPackageSourcePermission(PACKAGE, null);
        config.setAuthorizationEnabled(true);
        for (String url : new String[] {null, "", "builtin://example", "https://example.test/code.jar"}) {
            functions.checkPackageSourcePermission(url, null);
        }
        verifyNoInteractions(authorization, admin);
    }
}
