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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.fail;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.functions.MemoryLimit;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.auth.FunctionAuthProvider;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.apache.pulsar.functions.proto.Instance;
import org.apache.pulsar.functions.proto.PackageLocationMetaData;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.testng.annotations.DataProvider;
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

    @DataProvider(name = "originalFileNames")
    public Object[][] originalFileNames() {
        return new Object[][]{
                {"", "", ""},
                {"package.jar", "package.jar", ""},
                // Each original file name below is the name of the directory the other package is downloaded to.
                // These rows pass without the fix too; they check that one package's file cannot take the
                // directory of the other package.
                {"transform-function-package", "transform-function.jar", ""},
                {"sink.jar", "package", ""},
                // An older worker downloaded packages into the instance directory itself, so a stale package file
                // may sit where a download directory is created now.
                {"", "", "package"},
                {"", "", "transform-function-package"},
        };
    }

    /**
     * A sink and its transform function, both given as package URLs, must be downloaded to different files,
     * otherwise the transform function package replaces the sink package before the runtime is created.
     */
    @Test(dataProvider = "originalFileNames")
    public void testStartSinkWithTransformFunctionPackageUrl(String sinkOriginalFileName,
                                                             String transformFunctionOriginalFileName,
                                                             String stalePackageFileName)
            throws Exception {
        File downloadDirectory = Files.createTempDirectory("function-actioner-test").toFile();
        try {
            if (!stalePackageFileName.isEmpty()) {
                Path instanceDirectory = Files.createDirectories(downloadDirectory.toPath()
                        .resolve(Path.of("test-tenant", "test-namespace", "test-sink", "0")));
                Files.writeString(instanceDirectory.resolve(stalePackageFileName), "stale package");
            }
            WorkerConfig workerConfig = new WorkerConfig();
            workerConfig.setWorkerId("worker-1");
            workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
            workerConfig.setDownloadDirectory(downloadDirectory.getAbsolutePath());

            String sinkPackageUrl = "sink://public/default/test-sink@latest";
            String transformFunctionPackageUrl = "function://public/default/test-transform-function@latest";
            PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
            Packages packages = mock(Packages.class);
            doReturn(packages).when(pulsarAdmin).packages();
            doAnswer(invocation -> {
                Files.writeString(Path.of(invocation.getArgument(1, String.class)),
                        "content of " + invocation.getArgument(0, String.class));
                return null;
            }).when(packages).download(any(), any());

            record PackageFile(String path, String content) {
            }
            List<PackageFile> packageFilesAtContainerCreation = new ArrayList<>();
            RuntimeFactory factory = mock(RuntimeFactory.class);
            Runtime runtime = mock(Runtime.class);
            doAnswer(invocation -> {
                for (int index : new int[]{1, 3}) {
                    String path = invocation.getArgument(index, String.class);
                    packageFilesAtContainerCreation.add(new PackageFile(path, Files.readString(Path.of(path))));
                }
                return runtime;
            }).when(factory).createContainer(any(), any(), any(), any(), any(), any());

            @SuppressWarnings("resource")
            FunctionActioner actioner = new FunctionActioner(workerConfig, factory, mock(Namespace.class),
                    new ConnectorsManager(workerConfig), new FunctionsManager(workerConfig), pulsarAdmin,
                    mock(PackageUrlValidator.class));

            FunctionMetaData function = new FunctionMetaData();
            function.setFunctionDetails().setTenant("test-tenant")
                    .setNamespace("test-namespace").setName("test-sink")
                    .setComponentType(FunctionDetails.ComponentType.SINK)
                    .setRuntime(FunctionDetails.Runtime.JAVA)
                    .setClassName("org.apache.pulsar.functions.api.utils.IdentityFunction")
                    .setSink().setClassName("org.example.TestSink");
            PackageLocationMetaData sinkPackageLocation =
                    function.setPackageLocation().setPackagePath(sinkPackageUrl);
            PackageLocationMetaData transformFunctionPackageLocation =
                    function.setTransformFunctionPackageLocation().setPackagePath(transformFunctionPackageUrl);
            if (!sinkOriginalFileName.isEmpty()) {
                sinkPackageLocation.setOriginalFileName(sinkOriginalFileName);
            }
            if (!transformFunctionOriginalFileName.isEmpty()) {
                transformFunctionPackageLocation.setOriginalFileName(transformFunctionOriginalFileName);
            }
            Instance instance = new Instance();
            instance.setFunctionMetaData().copyFrom(function);
            instance.setInstanceId(0);
            FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
            doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();

            actioner.startFunction(functionRuntimeInfo);

            verify(functionRuntimeInfo, never()).setStartupException(any());
            verify(runtime).start();
            assertThat(packageFilesAtContainerCreation)
                    .as("package files passed to the runtime factory")
                    .hasSize(2);
            PackageFile sinkPackageFile = packageFilesAtContainerCreation.get(0);
            PackageFile transformFunctionPackageFile = packageFilesAtContainerCreation.get(1);
            assertThat(sinkPackageFile.content())
                    .as("content of the sink package file %s when the runtime is created", sinkPackageFile.path())
                    .isEqualTo("content of " + sinkPackageUrl);
            assertThat(transformFunctionPackageFile.content())
                    .as("content of the transform function package file %s when the runtime is created",
                            transformFunctionPackageFile.path())
                    .isEqualTo("content of " + transformFunctionPackageUrl);
            assertThat(transformFunctionPackageFile.path())
                    .as("transform function package file")
                    .isNotEqualTo(sinkPackageFile.path());

            actioner.stopFunction(functionRuntimeInfo);

            assertThat(new File(sinkPackageFile.path()))
                    .as("sink package file after the function is stopped")
                    .doesNotExist();
            assertThat(new File(transformFunctionPackageFile.path()))
                    .as("transform function package file after the function is stopped")
                    .doesNotExist();
        } finally {
            FileUtils.deleteDirectory(downloadDirectory);
        }
    }

    @Test
    public void testCreateInstanceConfigPulsarClientMemoryLimit() {
        WorkerConfig workerConfig = new WorkerConfig();
        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, mock(RuntimeFactory.class),
                mock(Namespace.class), mock(ConnectorsManager.class), mock(FunctionsManager.class),
                mock(PulsarAdmin.class), mock(PackageUrlValidator.class));

        // neither the worker nor the function sets a limit
        InstanceConfig instanceConfig = actioner.createInstanceConfig(new FunctionDetails(), null, 0, "c");
        assertNull(instanceConfig.getPulsarClientMemoryLimit());

        // the worker default applies to a function without its own limit
        workerConfig.setPulsarClientMemoryLimit(new MemoryLimit().setPercentOfMaxDirectMemory(50.0));
        instanceConfig = actioner.createInstanceConfig(new FunctionDetails(), null, 0, "c");
        assertEquals(instanceConfig.getPulsarClientMemoryLimit(),
                new MemoryLimit().setPercentOfMaxDirectMemory(50.0));

        // the function's own limit overrides the worker default
        FunctionDetails functionDetails = new FunctionDetails();
        functionDetails.setPulsarClientMemoryLimit().setLimitInBytes(1024);
        instanceConfig = actioner.createInstanceConfig(functionDetails, null, 0, "c");
        assertEquals(instanceConfig.getPulsarClientMemoryLimit(), new MemoryLimit().setLimitInBytes(1024L));
    }
}
