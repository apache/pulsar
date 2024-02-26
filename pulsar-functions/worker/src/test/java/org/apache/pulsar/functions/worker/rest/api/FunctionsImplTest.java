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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.JavaInstanceRunnable;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeInfo;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FunctionsImplTest {

    private static final class TestFunction implements org.apache.pulsar.functions.api.Function<String, String> {

        @Override
        public String process(String input, Context context) {
            return input;
        }
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";
    private static final String outputTopic = "test-output-topic";
    private static final String outputSerdeClassName = TopicSchema.DEFAULT_SERDE;
    private static final String className = TestFunction.class.getName();
    private Function.SubscriptionType subscriptionType = Function.SubscriptionType.FAILOVER;
    private static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    static {
        topicsToSerDeClassName.put("persistent://sample/standalone/ns1/test_src", TopicSchema.DEFAULT_SERDE);
    }
    private static final int parallelism = 1;
    private static final String workerId = "worker-0";
    private static final String superUser = "superUser";
    private static final String proxyUser = "proxyUser";

    private PulsarWorkerService mockedWorkerService;
    private PulsarAdmin mockedPulsarAdmin;
    private Tenants mockedTenants;
    private Namespaces mockedNamespaces;
    private TenantInfo mockedTenantInfo;
    private List<String> namespaceList = new LinkedList<>();
    private FunctionMetaDataManager mockedManager;
    private FunctionRuntimeManager mockedFunctionRunTimeManager;
    private RuntimeFactory mockedRuntimeFactory;
    private Namespace mockedNamespace;
    private FunctionsImpl resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;
    private Function.FunctionMetaData mockedFunctionMetadata;
    private PulsarFunctionTestTemporaryDirectory tempDirectory;

    @BeforeMethod
    public void setup() throws Exception {
        this.mockedManager = mock(FunctionMetaDataManager.class);
        this.mockedFunctionRunTimeManager = mock(FunctionRuntimeManager.class);
        this.mockedTenantInfo = mock(TenantInfo.class);
        this.mockedRuntimeFactory = mock(RuntimeFactory.class);
        this.mockedInputStream = mock(InputStream.class);
        this.mockedNamespace = mock(Namespace.class);
        this.mockedFormData = mock(FormDataContentDisposition.class);
        when(mockedFormData.getFileName()).thenReturn("test");
        this.mockedPulsarAdmin = mock(PulsarAdmin.class);
        this.mockedTenants = mock(Tenants.class);
        this.mockedNamespaces = mock(Namespaces.class);
        this.mockedFunctionMetadata = Function.FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        namespaceList.add(tenant + "/" + namespace);

        this.mockedWorkerService = mock(PulsarWorkerService.class);
        when(mockedWorkerService.getFunctionMetaDataManager()).thenReturn(mockedManager);
        when(mockedWorkerService.getFunctionRuntimeManager()).thenReturn(mockedFunctionRunTimeManager);
        when(mockedFunctionRunTimeManager.getRuntimeFactory()).thenReturn(mockedRuntimeFactory);
        when(mockedWorkerService.getDlogNamespace()).thenReturn(mockedNamespace);
        when(mockedWorkerService.isInitialized()).thenReturn(true);
        when(mockedWorkerService.getBrokerAdmin()).thenReturn(mockedPulsarAdmin);
        when(mockedPulsarAdmin.tenants()).thenReturn(mockedTenants);
        when(mockedPulsarAdmin.namespaces()).thenReturn(mockedNamespaces);
        when(mockedTenants.getTenantInfo(any())).thenReturn(mockedTenantInfo);
        when(mockedNamespaces.getNamespaces(any())).thenReturn(namespaceList);
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetadata);
        when(mockedManager.containsFunction(tenant, namespace, function)).thenReturn(true);
        when(mockedFunctionRunTimeManager.findFunctionAssignment(eq(tenant), eq(namespace), eq(function), anyInt()))
                .thenReturn(Function.Assignment.newBuilder()
                        .setWorkerId(workerId)
                        .build());

        Function.FunctionDetails.Builder functionDetailsBuilder =  createDefaultFunctionDetails().toBuilder();
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetailsBuilder.build());
        instanceConfig.setMaxBufferedTuples(1024);

        JavaInstanceRunnable javaInstanceRunnable = new JavaInstanceRunnable(
                instanceConfig, null, null, null, null, null, null, null, null);
        CompletableFuture<InstanceCommunication.MetricsData> metricsDataCompletableFuture = new CompletableFuture<InstanceCommunication.MetricsData>();
        metricsDataCompletableFuture.complete(javaInstanceRunnable.getMetrics());
        Runtime runtime = mock(Runtime.class);
        doReturn(metricsDataCompletableFuture).when(runtime).getMetrics(anyInt());

        CompletableFuture<InstanceCommunication.FunctionStatus> functionStatusCompletableFuture = new CompletableFuture<>();
        functionStatusCompletableFuture.complete(javaInstanceRunnable.getFunctionStatus().build());

        RuntimeSpawner runtimeSpawner = mock(RuntimeSpawner.class);
        when(runtimeSpawner.getFunctionStatus(anyInt())).thenReturn(functionStatusCompletableFuture);
        doReturn(runtime).when(runtimeSpawner).getRuntime();

        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(runtimeSpawner).when(functionRuntimeInfo).getRuntimeSpawner();

        when(mockedFunctionRunTimeManager.getFunctionRuntimeInfo(any())).thenReturn(functionRuntimeInfo);

        // worker config
        WorkerConfig workerConfig = new WorkerConfig()
                .setWorkerId(workerId)
                .setWorkerPort(8080)
                .setFunctionMetadataTopicName("pulsar/functions")
                .setNumFunctionPackageReplicas(3)
                .setPulsarServiceUrl("pulsar://localhost:6650/");
        tempDirectory = PulsarFunctionTestTemporaryDirectory.create(getClass().getSimpleName());
        tempDirectory.useTemporaryDirectoriesForWorkerConfig(workerConfig);
        when(mockedWorkerService.getWorkerConfig()).thenReturn(workerConfig);

        this.resource = spy(new FunctionsImpl(() -> mockedWorkerService));

    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (tempDirectory != null) {
            tempDirectory.delete();
        }
    }

    @Test
    public void testStatusEmpty() {
        assertNotNull(this.resource.getFunctionInstanceStatus(tenant, namespace, function, "0", null, null));
    }

    @Test
    public void testMetricsEmpty() throws PulsarClientException  {
        Function.FunctionDetails.Builder functionDetailsBuilder =  createDefaultFunctionDetails().toBuilder();
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetailsBuilder.build());
        instanceConfig.setMaxBufferedTuples(1024);

        JavaInstanceRunnable javaInstanceRunnable = new JavaInstanceRunnable(
                instanceConfig, null, null, null, null, null, null, null, null);
        CompletableFuture<InstanceCommunication.MetricsData> completableFuture = new CompletableFuture<InstanceCommunication.MetricsData>();
        completableFuture.complete(javaInstanceRunnable.getMetrics());
        Runtime runtime = mock(Runtime.class);
        doReturn(completableFuture).when(runtime).getMetrics(anyInt());
        RuntimeSpawner runtimeSpawner = mock(RuntimeSpawner.class);
        doReturn(runtime).when(runtimeSpawner).getRuntime();

        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(runtimeSpawner).when(functionRuntimeInfo).getRuntimeSpawner();

        FunctionInstanceStatsImpl instanceStats1 = WorkerUtils
                .getFunctionInstanceStats("public/default/test", functionRuntimeInfo, 0);
        FunctionInstanceStatsImpl instanceStats2 = WorkerUtils
                .getFunctionInstanceStats("public/default/test", functionRuntimeInfo, 1);

        FunctionStatsImpl functionStats = new FunctionStatsImpl();
        functionStats.addInstance(instanceStats1);
        functionStats.addInstance(instanceStats2);

        assertNotNull(functionStats.calculateOverall());
    }

    // Suppress the deprecation warnings until we actually remove the deprecated method
    @SuppressWarnings("deprecation")
    @Test
    public void testIsAuthorizedRole() throws Exception {

        AuthenticationDataSource authenticationDataSource = mock(AuthenticationDataSource.class);
        FunctionsImpl functionImpl = spy(new FunctionsImpl(() -> mockedWorkerService));
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setAuthorizationEnabled(true);
        HashSet<String> superUsers = new HashSet<>();
        superUsers.add(superUser);
        superUsers.add(proxyUser);
        workerConfig.setSuperUserRoles(superUsers);
        workerConfig.setProxyRoles(Collections.singleton(proxyUser));
        // TODO remove mocking by relying on TestPulsarResources. Can't do now because this commit needs to be
        //  cherry picked back.
        PulsarResources pulsarResources = mock(PulsarResources.class);
        TenantResources tenantResources = mock(TenantResources.class);
        when(pulsarResources.getTenantResources()).thenReturn(tenantResources);
        TenantInfo tenantInfo = TenantInfo.builder().adminRoles(Collections.singleton("tenant-admin")).build();
        when(tenantResources.getTenantAsync("test-tenant"))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(tenantInfo)));
        NamespaceResources namespaceResources = mock(NamespaceResources.class);
        when(pulsarResources.getNamespaceResources()).thenReturn(namespaceResources);
        Policies p = new Policies();
        p.auth_policies.getNamespaceAuthentication().put("test-function-user", Collections.singleton(AuthAction.functions));
        when(namespaceResources.getPoliciesAsync(NamespaceName.get("test-tenant/test-ns")))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(p)));

        AuthorizationService authorizationService = new AuthorizationService(
                PulsarConfigurationLoader.convertFrom(workerConfig), pulsarResources);
        doReturn(workerConfig).when(mockedWorkerService).getWorkerConfig();
        doReturn(authorizationService).when(mockedWorkerService).getAuthorizationService();

        // test super user
        assertTrue(functionImpl.isAuthorizedRole("test-tenant", "test-ns", superUser,
                authenticationDataSource));
        assertTrue(functionImpl.isSuperUser(superUser, null));

        // test normal user with no permissions
        assertFalse(functionImpl.isAuthorizedRole("test-tenant", "test-ns", "test-non-admin-user",
                authenticationDataSource));

        // if user is tenant admin
        assertTrue(functionImpl.isAuthorizedRole("test-tenant", "test-ns", "tenant-admin",
                authenticationDataSource));

        // test user allow function action
        assertTrue(functionImpl.isAuthorizedRole("test-tenant", "test-ns", "test-function-user",
                authenticationDataSource));

        // test role is null
        assertThrows(RestException.class, () -> functionImpl.isAuthorizedRole("test-tenant",
                "test-ns", null, authenticationDataSource));

        // test proxy user with no original principal
        assertFalse(functionImpl.isAuthorizedRole("test-tenant", "test-ns",
                AuthenticationParameters.builder().clientRole(proxyUser).build()));

        // test proxy user with tenant admin original principal
        assertTrue(functionImpl.isAuthorizedRole("test-tenant", "test-ns",
                AuthenticationParameters.builder().clientRole(proxyUser).originalPrincipal("tenant-admin").build()));

        // test proxy user with non admin user
        assertFalse(functionImpl.isAuthorizedRole("test-tenant", "test-ns",
                AuthenticationParameters.builder().clientRole(proxyUser).originalPrincipal("test-non-admin-user").build()));

        // test proxy user with allow function action
        assertTrue(functionImpl.isAuthorizedRole("test-tenant", "test-ns",
                AuthenticationParameters.builder().clientRole(proxyUser).originalPrincipal("test-function-user").build()));

        // test non-proxy user passing original principal
        assertFalse(functionImpl.isAuthorizedRole("test-tenant", "test-ns",
                AuthenticationParameters.builder().clientRole("nobody").originalPrincipal("test-non-admin-user").build()));
    }

    @Test
    public void testIsSuperUser() throws PulsarAdminException {

        FunctionsImpl functionImpl = spy(new FunctionsImpl(() -> mockedWorkerService));
        AuthorizationService authorizationService = mock(AuthorizationService.class);
        doReturn(authorizationService).when(mockedWorkerService).getAuthorizationService();
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setAuthorizationEnabled(true);
        workerConfig.setSuperUserRoles(Collections.singleton(superUser));
        doReturn(workerConfig).when(mockedWorkerService).getWorkerConfig();
        when(authorizationService.isSuperUser(any(AuthenticationParameters.class)))
            .thenAnswer((invocationOnMock) -> {
                String role = invocationOnMock.getArgument(0, AuthenticationParameters.class).getClientRole();
                return CompletableFuture.completedFuture(superUser.equals(role));
            });

        assertTrue(functionImpl.isSuperUser(superUser, null));

        assertFalse(functionImpl.isSuperUser("normal-user", null));
        assertFalse(functionImpl.isSuperUser( null, null));

        // test super roles is null and it's not a pulsar super user
        when(authorizationService.isSuperUser(AuthenticationParameters.builder().clientRole(superUser).build()))
                .thenReturn(CompletableFuture.completedFuture(false));
        functionImpl = spy(new FunctionsImpl(() -> mockedWorkerService));
        workerConfig = new WorkerConfig();
        workerConfig.setAuthorizationEnabled(true);
        doReturn(workerConfig).when(mockedWorkerService).getWorkerConfig();
        assertFalse(functionImpl.isSuperUser(superUser, null));

        // test super role is null but the auth datasource contains superuser
        when(authorizationService.isSuperUser(any(AuthenticationParameters.class)))
            .thenAnswer((invocationOnMock -> {
                AuthenticationParameters authData = invocationOnMock.getArgument(0, AuthenticationParameters.class);
                String user = authData.getClientAuthenticationDataSource().getHttpHeader("mockedUser");
                return CompletableFuture.completedFuture(superUser.equals(user));
            }));
        AuthenticationDataSource authData = mock(AuthenticationDataSource.class);
        when(authData.getHttpHeader("mockedUser")).thenReturn(superUser);
        assertTrue(functionImpl.isSuperUser("non-superuser", authData));

        AuthenticationDataSource nonSuperuserAuthData = mock(AuthenticationDataSource.class);
        when(nonSuperuserAuthData.getHttpHeader("mockedUser")).thenReturn("non-superuser");
        assertFalse(functionImpl.isSuperUser("non-superuser", nonSuperuserAuthData));
    }

    public static FunctionConfig createDefaultFunctionConfig() {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        return functionConfig;
    }

    public static Function.FunctionDetails createDefaultFunctionDetails() {
        FunctionConfig functionConfig = createDefaultFunctionConfig();
        return FunctionConfigUtils.convert(functionConfig);
    }
}
