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
package org.apache.pulsar.functions.worker.rest.api.v2;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.RestException;
import org.apache.pulsar.functions.worker.rest.api.ComponentImpl;
import org.apache.pulsar.functions.worker.rest.api.SourceImpl;
import org.apache.pulsar.io.twitter.TwitterFireHose;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link SourceApiV2Resource}.
 */
@PrepareForTest({Utils.class, ConnectorUtils.class, org.apache.pulsar.functions.utils.Utils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.*" })
@Slf4j
public class SourceApiV2ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String source = "test-source";
    private static final String outputTopic = "test-output-topic";
    private static final String outputSerdeClassName = TopicSchema.DEFAULT_SERDE;
    private static final String className = TwitterFireHose.class.getName();
    private static final int parallelism = 1;
    private static final String JAR_FILE_NAME = "pulsar-io-twitter.nar";
    private static final String INVALID_JAR_FILE_NAME = "pulsar-io-cassandra.nar";
    private String JAR_FILE_PATH;
    private String INVALID_JAR_FILE_PATH;

    private WorkerService mockedWorkerService;
    private PulsarAdmin mockedPulsarAdmin;
    private Tenants mockedTenants;
    private Namespaces mockedNamespaces;
    private TenantInfo mockedTenantInfo;
    private List<String> namespaceList = new LinkedList<>();
    private FunctionMetaDataManager mockedManager;
    private FunctionRuntimeManager mockedFunctionRunTimeManager;
    private RuntimeFactory mockedRuntimeFactory;
    private Namespace mockedNamespace;
    private SourceImpl resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;
    private FunctionMetaData mockedFunctionMetaData;

    @BeforeMethod
    public void setup() throws Exception {
        this.mockedManager = mock(FunctionMetaDataManager.class);
        this.mockedFunctionRunTimeManager = mock(FunctionRuntimeManager.class);
        this.mockedRuntimeFactory = mock(RuntimeFactory.class);
        this.mockedInputStream = mock(InputStream.class);
        this.mockedNamespace = mock(Namespace.class);
        this.mockedFormData = mock(FormDataContentDisposition.class);
        when(mockedFormData.getFileName()).thenReturn("test");
        this.mockedTenantInfo = mock(TenantInfo.class);
        this.mockedPulsarAdmin = mock(PulsarAdmin.class);
        this.mockedTenants = mock(Tenants.class);
        this.mockedNamespaces = mock(Namespaces.class);
        namespaceList.add(tenant + "/" + namespace);

        this.mockedWorkerService = mock(WorkerService.class);
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

        URL file = Thread.currentThread().getContextClassLoader().getResource(JAR_FILE_NAME);
        if (file == null)  {
            throw new RuntimeException("Failed to file required test archive: " + JAR_FILE_NAME);
        }
        JAR_FILE_PATH = file.getFile();
        INVALID_JAR_FILE_PATH = Thread.currentThread().getContextClassLoader().getResource(INVALID_JAR_FILE_NAME).getFile();

        // worker config
        WorkerConfig workerConfig = new WorkerConfig()
            .setWorkerId("test")
            .setWorkerPort(8080)
            .setDownloadDirectory("/tmp/pulsar/functions")
            .setFunctionMetadataTopicName("pulsar/functions")
            .setNumFunctionPackageReplicas(3)
            .setPulsarServiceUrl("pulsar://localhost:6650/");
        when(mockedWorkerService.getWorkerConfig()).thenReturn(workerConfig);

        this.resource = spy(new SourceImpl(() -> mockedWorkerService));
        Mockito.doReturn(ComponentImpl.ComponentType.SOURCE).when(this.resource).calculateSubjectType(any());
    }

    //
    // Register Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testRegisterSourceMissingTenant() throws IOException {
        testRegisterSourceMissingArguments(
            null,
            namespace,
                source,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                null,
                "Tenant is not provided");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testRegisterSourceMissingNamespace() throws IOException {
        testRegisterSourceMissingArguments(
            tenant,
            null,
                source,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                null,
                "Namespace is not provided");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Name is not provided")
    public void testRegisterSourceMissingSourceName() throws IOException {
        testRegisterSourceMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                null,
                "Source Name is not provided");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Package is not provided")
    public void testRegisterSourceMissingPackage() throws IOException {
        testRegisterSourceMissingArguments(
            tenant,
            namespace,
                source,
            null,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            null,
            parallelism,
                null,
                "Source Package is not provided");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Package is not provided")
    public void testRegisterSourceMissingPackageDetails() throws IOException {
        testRegisterSourceMissingArguments(
            tenant,
            namespace,
                source,
            mockedInputStream,
            null,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                null,
                "Source Package is not provided");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Failed to extract source class from archive")
    public void testRegisterSourceInvalidJarWithNoSource() throws IOException {
        FileInputStream inputStream = new FileInputStream(INVALID_JAR_FILE_PATH);
        testRegisterSourceMissingArguments(
                tenant,
                namespace,
                source,
                inputStream,
                null,
                outputTopic,
                outputSerdeClassName,
                null,
                parallelism,
                null,
                "Failed to extract source class from archive");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Topic name cannot be null")
    public void testRegisterSourceNoOutputTopic() throws IOException {
        FileInputStream inputStream = new FileInputStream(JAR_FILE_PATH);
        testRegisterSourceMissingArguments(
                tenant,
                namespace,
                source,
                inputStream,
                mockedFormData,
                null,
                outputSerdeClassName,
                className,
                parallelism,
                null,
                "Topic name cannot be null");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Invalid Source Jar")
    public void testRegisterSourceHttpUrl() throws IOException {
        testRegisterSourceMissingArguments(
                tenant,
                namespace,
                source,
                null,
                null,
                outputTopic,
                outputSerdeClassName,
                className,
                parallelism,
                "http://localhost:1234/test",
                "Invalid Source Jar");
    }

    private void testRegisterSourceMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            FormDataContentDisposition details,
            String outputTopic,
            String outputSerdeClassName,
            String className,
            Integer parallelism,
            String pkgUrl,
            String errorExpected) {
        SourceConfig sourceConfig = new SourceConfig();
        if (tenant != null) {
            sourceConfig.setTenant(tenant);
        }
        if (namespace != null) {
            sourceConfig.setNamespace(namespace);
        }
        if (function != null) {
            sourceConfig.setName(function);
        }
        if (outputTopic != null) {
            sourceConfig.setTopicName(outputTopic);
        }
        if (outputSerdeClassName != null) {
            sourceConfig.setSerdeClassName(outputSerdeClassName);
        }
        if (className != null) {
            sourceConfig.setClassName(className);
        }
        if (parallelism != null) {
            sourceConfig.setParallelism(parallelism);
        }

        resource.registerFunction(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                pkgUrl,
                null,
                new Gson().toJson(sourceConfig),
                null);

    }

    private void registerDefaultSource() throws IOException {
        SourceConfig sourceConfig = createDefaultSourceConfig();
        resource.registerFunction(
            tenant,
            namespace,
                source,
            new FileInputStream(JAR_FILE_PATH),
            mockedFormData,
            null,
            null,
            new Gson().toJson(sourceConfig),
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source already exists")
    public void testRegisterExistedSource() throws IOException {
        Configurator.setRootLevel(Level.DEBUG);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        registerDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testRegisterSourceUploadFailure() throws Exception {
        mockStatic(Utils.class);
        doThrow(new IOException("upload failure")).when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

        registerDefaultSource();
    }

    public void testRegisterSourceSuccess() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        registerDefaultSource();
    }

    @Test
    public void testRegisterSourceConflictingFields() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));
        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(className);
        sourceConfig.setParallelism(parallelism);
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);
        resource.registerFunction(
                actualTenant,
                actualNamespace,
                actualName,
                new FileInputStream(JAR_FILE_PATH),
                mockedFormData,
                null,
                null,
                new Gson().toJson(sourceConfig),
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to register")
    public void testRegisterSourceFailure() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("source failed to register");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        registerDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registeration interrupted")
    public void testRegisterSourceInterrupted() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function registeration interrupted"));
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        registerDefaultSource();
    }

    //
    // Update Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testUpdateSourceMissingTenant() throws IOException {
        testUpdateSourceMissingArguments(
            null,
            namespace,
                source,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Tenant is not provided");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testUpdateSourceMissingNamespace() throws IOException {
        testUpdateSourceMissingArguments(
            tenant,
            null,
                source,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Namespace is not provided");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Name is not provided")
    public void testUpdateSourceMissingFunctionName() throws IOException {
        testUpdateSourceMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Source Name is not provided");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateSourceMissingPackage() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSourceMissingArguments(
            tenant,
            namespace,
                source,
                null,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            null,
            parallelism,
                "Update contains no change");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateSourceMissingTopicName() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSourceMissingArguments(
                tenant,
                namespace,
                source,
                null,
                mockedFormData,
                null,
                outputSerdeClassName,
                null,
                parallelism,
                "Update contains no change");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism should positive number")
    public void testUpdateSourceNegativeParallelism() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSourceMissingArguments(
                tenant,
                namespace,
                source,
                null,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                className,
                -2,
                "Source parallelism should positive number");
    }

    @Test
    public void testUpdateSourceChangedParallelism() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSourceMissingArguments(
                tenant,
                namespace,
                source,
                null,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                className,
                parallelism + 1,
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Destination topics differ")
    public void testUpdateSourceChangedTopic() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSourceMissingArguments(
                tenant,
                namespace,
                source,
                null,
                mockedFormData,
                "DifferentTopic",
                outputSerdeClassName,
                className,
                parallelism,
                "Destination topics differ");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism should positive number")
    public void testUpdateSourceZeroParallelism() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSourceMissingArguments(
                tenant,
                namespace,
                source,
                mockedInputStream,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                className,
                0,
                "Source parallelism should positive number");
    }

    private void testUpdateSourceMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            FormDataContentDisposition details,
            String outputTopic,
            String outputSerdeClassName,
            String className,
            Integer parallelism,
            String expectedError) throws IOException {

        mockStatic(ConnectorUtils.class);
        doReturn(TwitterFireHose.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSourceType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        SourceConfig sourceConfig = new SourceConfig();
        if (tenant != null) {
            sourceConfig.setTenant(tenant);
        }
        if (namespace != null) {
            sourceConfig.setNamespace(namespace);
        }
        if (function != null) {
            sourceConfig.setName(function);
        }
        if (outputTopic != null) {
            sourceConfig.setTopicName(outputTopic);
        }
        if (outputSerdeClassName != null) {
            sourceConfig.setSerdeClassName(outputSerdeClassName);
        }
        if (className != null) {
            sourceConfig.setClassName(className);
        }
        if (parallelism != null) {
            sourceConfig.setParallelism(parallelism);
        }

        if (expectedError == null) {
            RequestResult rr = new RequestResult()
                    .setSuccess(true)
                    .setMessage("source registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
        }

        resource.updateFunction(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            null,
            null,
            new Gson().toJson(sourceConfig),
                null);

    }

    private void updateDefaultSource() throws IOException {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(className);
        sourceConfig.setParallelism(parallelism);
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);

        mockStatic(ConnectorUtils.class);
        doReturn(TwitterFireHose.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSourceType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        resource.updateFunction(
            tenant,
            namespace,
                source,
                new FileInputStream(JAR_FILE_PATH),
            mockedFormData,
            null,
            null,
            new Gson().toJson(sourceConfig),
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source doesn't exist")
    public void testUpdateNotExistedSource() throws IOException {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

        updateDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testUpdateSourceUploadFailure() throws Exception {
        mockStatic(Utils.class);
        doThrow(new IOException("upload failure")).when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        updateDefaultSource();
    }

    @Test
    public void testUpdateSourceSuccess() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        updateDefaultSource();
    }

    @Test
    public void testUpdateSourceWithUrl() throws IOException {
        Configurator.setRootLevel(Level.DEBUG);

        String filePackageUrl = "file://" + JAR_FILE_PATH;

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(className);
        sourceConfig.setParallelism(parallelism);

        mockStatic(ConnectorUtils.class);
        doReturn(TwitterFireHose.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSourceType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);


        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        resource.updateFunction(
            tenant,
            namespace,
                source,
            null,
            null,
            filePackageUrl,
            null,
            new Gson().toJson(sourceConfig),
                null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to register")
    public void testUpdateSourceFailure() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("source failed to register");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        updateDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registeration interrupted")
    public void testUpdateSourceInterrupted() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function registeration interrupted"));
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        updateDefaultSource();
    }

    //
    // deregister source
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testDeregisterSourceMissingTenant() throws Exception {
        testDeregisterSourceMissingArguments(
            null,
            namespace,
                source,
            "Tenant");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testDeregisterSourceMissingNamespace() throws Exception {
        testDeregisterSourceMissingArguments(
            tenant,
            null,
                source,
            "Namespace");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Name is not provided")
    public void testDeregisterSourceMissingFunctionName() throws Exception {
        testDeregisterSourceMissingArguments(
            tenant,
            namespace,
            null,
            "Source Name");
    }

    private void testDeregisterSourceMissingArguments(
        String tenant,
        String namespace,
        String function,
        String missingFieldName
    ) {
        resource.deregisterFunction(
            tenant,
            namespace,
            function,
                null);

    }

    private void deregisterDefaultSource() {
        resource.deregisterFunction(
            tenant,
            namespace,
                source,
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp= "Source test-source doesn't exist")
    public void testDeregisterNotExistedSource() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
        deregisterDefaultSource();
    }

    @Test
    public void testDeregisterSourceSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source deregistered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(requestResult);

        deregisterDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to deregister")
    public void testDeregisterSourceFailure() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("source failed to deregister");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(requestResult);

        deregisterDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function deregisteration interrupted")
    public void testDeregisterSourceInterrupted() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function deregisteration interrupted"));
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(requestResult);

        deregisterDefaultSource();
    }

    //
    // Get Source Info
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testGetSourceMissingTenant() throws Exception {
        testGetSourceMissingArguments(
            null,
            namespace,
                source,
            "Tenant");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testGetSourceMissingNamespace() throws Exception {
        testGetSourceMissingArguments(
            tenant,
            null,
                source,
            "Namespace");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Name is not provided")
    public void testGetSourceMissingFunctionName() throws Exception {
        testGetSourceMissingArguments(
            tenant,
            namespace,
            null,
            "Source Name");
    }

    private void testGetSourceMissingArguments(
        String tenant,
        String namespace,
        String source,
        String missingFieldName
    ) throws IOException {
        resource.getFunctionInfo(
            tenant,
            namespace,
            source
        );
    }

    private SourceConfig getDefaultSourceInfo() throws IOException {
        return resource.getSourceInfo(
            tenant,
            namespace,
                source
        );
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source doesn't exist")
    public void testGetNotExistedSource() throws IOException {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
        getDefaultSourceInfo();
    }

    @Test
    public void testGetSourceSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        SourceSpec sourceSpec = SourceSpec.newBuilder().setBuiltin("jdbc").build();
        SinkSpec sinkSpec = SinkSpec.newBuilder()
                .setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setClassName(IdentityFunction.class.getName())
                .setSink(sinkSpec)
                .setName(source)
                .setNamespace(namespace)
                .setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE)
                .setRuntime(FunctionDetails.Runtime.JAVA)
                .setAutoAck(true)
                .setTenant(tenant)
                .setParallelism(parallelism)
                .setSource(sourceSpec).build();
        FunctionMetaData metaData = FunctionMetaData.newBuilder()
            .setCreateTime(System.currentTimeMillis())
            .setFunctionDetails(functionDetails)
            .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath("/path/to/package"))
            .setVersion(1234)
            .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source))).thenReturn(metaData);

        SourceConfig config = getDefaultSourceInfo();
        assertEquals(SourceConfigUtils.convertFromDetails(functionDetails), config);
    }

    //
    // List Sources
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testListSourcesMissingTenant() throws Exception {
        testListSourcesMissingArguments(
            null,
            namespace,
            "Tenant");
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testListSourcesMissingNamespace() throws Exception {
        testListSourcesMissingArguments(
            tenant,
            null,
            "Namespace");
    }

    private void testListSourcesMissingArguments(
        String tenant,
        String namespace,
        String missingFieldName
    ) {
        resource.listFunctions(
            tenant,
            namespace
        );
    }

    private List<String> listDefaultSources() {
        return resource.listFunctions(
            tenant,
            namespace);
    }

    @Test
    public void testListSourcesSuccess() throws Exception {
        final List<String> functions = Lists.newArrayList("test-1", "test-2");
        final List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()
        ).build());
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()
        ).build());
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functionMetaDataList);

        List<String> sourceList = listDefaultSources();
        assertEquals(functions, sourceList);
    }

    @Test
    public void testOnlyGetSources() throws Exception {
        final List<String> functions = Lists.newArrayList("test-1");
        final List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        FunctionMetaData f1 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()).build();
        functionMetaDataList.add(f1);
        FunctionMetaData f2 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()).build();
        functionMetaDataList.add(f2);
        FunctionMetaData f3 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-3").build()).build();
        functionMetaDataList.add(f3);
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functionMetaDataList);
        doReturn(ComponentImpl.ComponentType.SOURCE).when(this.resource).calculateSubjectType(f1);
        doReturn(ComponentImpl.ComponentType.FUNCTION).when(this.resource).calculateSubjectType(f2);
        doReturn(ComponentImpl.ComponentType.SINK).when(this.resource).calculateSubjectType(f3);

        List<String> sourceList = listDefaultSources();
        assertEquals(functions, sourceList);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace does not exist")
    public void testRegisterFunctionNonexistantNamespace() throws Exception {
        this.namespaceList.clear();
        registerDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant does not exist")
    public void testRegisterFunctionNonexistantTenant() throws Exception {
        when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
        registerDefaultSource();
    }

    private SourceConfig createDefaultSourceConfig() {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(className);
        sourceConfig.setParallelism(parallelism);
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);
        return sourceConfig;
    }

    private FunctionDetails createDefaultFunctionDetails() {
        return SourceConfigUtils.convert(createDefaultSourceConfig(), new SourceConfigUtils.ExtractedSourceDetails(null, null));
    }
}
