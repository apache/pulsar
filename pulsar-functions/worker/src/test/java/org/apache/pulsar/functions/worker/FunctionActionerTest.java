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
package org.apache.pulsar.functions.worker;

import static org.apache.pulsar.common.functions.Utils.FILE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.fail;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.auth.FunctionAuthProvider;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.apache.pulsar.functions.proto.Instance;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionActioner}.
 */
public class FunctionActionerTest {

    /**
     * Validates FunctionActioner tries to download file from bk.
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testStartFunctionWithDLNamespace() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        Namespace dlogNamespace = mock(Namespace.class);
        // throw exception when dlogNamespace is accessed by actioner and verify it
        final String exceptionMsg = "dl namespace not-found";
        doThrow(new IllegalArgumentException(exceptionMsg)).when(dlogNamespace).openLog(any());

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace,
                new ConnectorsManager(workerConfig), new FunctionsManager(workerConfig), mock(PulsarAdmin.class),
                mock(PackageUrlValidator.class));
        FunctionMetaData function1 = new FunctionMetaData();
        function1.setFunctionDetails().setTenant("test-tenant")
                .setNamespace("test-namespace").setName("func-1");
        Instance instance = new Instance();
        instance.setFunctionMetaData().copyFrom(function1);
        instance.setInstanceId(0);
        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();
        doThrow(new IllegalStateException("StartupException")).when(functionRuntimeInfo).setStartupException(any());

        // actioner should try to download file from bk-dlogNamespace and fails with exception
        try {
            actioner.startFunction(functionRuntimeInfo);
            fail();
        } catch (IllegalStateException ex) {
            assertEquals(ex.getMessage(), "StartupException");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStartFunctionWithPkgUrl() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");
        workerConfig.setAdditionalEnabledFunctionsUrlPatterns(List.of("file:///user/.*", "http://invalid/.*"));
        workerConfig.setAdditionalEnabledConnectorUrlPatterns(List.of("file:///user/.*", "http://invalid/.*"));
        String downloadDir = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        workerConfig.setDownloadDirectory(downloadDir);

        RuntimeFactory factory = mock(RuntimeFactory.class);
        Runtime runtime = mock(Runtime.class);
        doReturn(runtime).when(factory).createContainer(any(), any(), any(), any(), any(), any());
        doNothing().when(runtime).start();
        Namespace dlogNamespace = mock(Namespace.class);
        final String exceptionMsg = "dl namespace not-found";
        doThrow(new IllegalArgumentException(exceptionMsg)).when(dlogNamespace).openLog(any());

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace,
                new ConnectorsManager(workerConfig), new FunctionsManager(workerConfig), mock(PulsarAdmin.class),
                new PackageUrlValidator(workerConfig));

        // (1) test with file url. functionActioner should be able to consider file-url and it should be able to call
        // RuntimeSpawner
        String pkgPathLocation = FILE + ":///user/my-file.jar";
        startFunction(actioner, pkgPathLocation, pkgPathLocation);
        verify(runtime, times(1)).start();

        // (2) test with http-url, downloading file from http should fail with UnknownHostException due to invalid url
        String invalidPkgPathLocation = "http://invalid/my-file.jar";

        try {
            startFunction(actioner, invalidPkgPathLocation, pkgPathLocation);
            fail();
        } catch (IllegalStateException ex) {
            assertEquals(ex.getMessage(), "StartupException");
        }

        try {
            startFunction(actioner, pkgPathLocation, invalidPkgPathLocation);
            fail();
        } catch (IllegalStateException ex) {
            assertEquals(ex.getMessage(), "StartupException");
        }
    }

    private void startFunction(FunctionActioner actioner, String pkgPathLocation, String extraPkgPathLocation) {
        FunctionMetaData function = new FunctionMetaData();
        function.setFunctionDetails().setTenant("test-tenant")
                .setNamespace("test-namespace").setName("func-1");
        function.setPackageLocation().setPackagePath(pkgPathLocation);
        function.setTransformFunctionPackageLocation().setPackagePath(extraPkgPathLocation);
        Instance instance = new Instance();
        instance.setFunctionMetaData().copyFrom(function);
        instance.setInstanceId(0);
        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();
        doThrow(new IllegalStateException("StartupException")).when(functionRuntimeInfo).setStartupException(any());

        actioner.startFunction(functionRuntimeInfo);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFunctionAuthDisabled() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");
        String downloadDir = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        workerConfig.setDownloadDirectory(downloadDir);

        RuntimeFactory factory = mock(RuntimeFactory.class);
        Runtime runtime = mock(Runtime.class);
        doReturn(runtime).when(factory).createContainer(any(), any(), any(), any(), any(), any());
        doNothing().when(runtime).start();
        Namespace dlogNamespace = mock(Namespace.class);
        final String exceptionMsg = "dl namespace not-found";
        doThrow(new IllegalArgumentException(exceptionMsg)).when(dlogNamespace).openLog(any());

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace,
                new ConnectorsManager(workerConfig), new FunctionsManager(workerConfig), mock(PulsarAdmin.class),
                mock(PackageUrlValidator.class));


        String pkgPathLocation = "http://invalid/my-file.jar";
        FunctionMetaData functionMeta = new FunctionMetaData();
        functionMeta.setFunctionDetails().setTenant("test-tenant")
                .setNamespace("test-namespace").setName("func-1");
        functionMeta.setPackageLocation().setPackagePath(pkgPathLocation);

        Instance instance = new Instance();
        instance.setFunctionMetaData().copyFrom(functionMeta);

        RuntimeSpawner runtimeSpawner = spy(actioner.getRuntimeSpawner(instance, "foo", "bar"));

        assertNull(runtimeSpawner.getInstanceConfig().getFunctionAuthenticationSpec());

        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);

        RuntimeFactory runtimeFactory = mock(RuntimeFactory.class);

        Optional<FunctionAuthProvider> functionAuthProvider = Optional.of(mock(FunctionAuthProvider.class));
        doReturn(functionAuthProvider).when(runtimeFactory).getAuthProvider();

        doReturn(runtimeFactory).when(runtimeSpawner).getRuntimeFactory();
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();
        doReturn(runtimeSpawner).when(functionRuntimeInfo).getRuntimeSpawner();

        actioner.terminateFunction(functionRuntimeInfo);

        // make sure cache
        verify(functionAuthProvider.get(), times(0)).cleanUpAuthData(any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStartFunctionWithPackageUrl() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");
        String downloadDir = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        workerConfig.setDownloadDirectory(downloadDir);

        RuntimeFactory factory = mock(RuntimeFactory.class);
        Runtime runtime = mock(Runtime.class);
        doReturn(runtime).when(factory).createContainer(any(), any(), any(), any(), any(), any());
        doNothing().when(runtime).start();
        Namespace dlogNamespace = mock(Namespace.class);
        final String exceptionMsg = "dl namespace not-found";
        doThrow(new IllegalArgumentException(exceptionMsg)).when(dlogNamespace).openLog(any());
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        Packages packages = mock(Packages.class);
        doReturn(packages).when(pulsarAdmin).packages();
        doNothing().when(packages).download(any(), any());

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace,
                new ConnectorsManager(workerConfig), new FunctionsManager(workerConfig), pulsarAdmin,
                mock(PackageUrlValidator.class));

        // (1) test with file url. functionActioner should be able to consider file-url and it should be able to call
        // RuntimeSpawner
        String pkgPathLocation = "function://public/default/test-function@latest";
        startFunction(actioner, pkgPathLocation, pkgPathLocation);
        verify(runtime, times(1)).start();
    }

}
