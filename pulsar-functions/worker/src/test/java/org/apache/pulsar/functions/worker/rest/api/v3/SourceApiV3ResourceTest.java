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
package org.apache.pulsar.functions.worker.rest.api.v3;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.core.Response;

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
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.api.SourcesImpl;
import org.apache.pulsar.io.twitter.TwitterFireHose;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link SourcesApiV3Resource}.
 */
@PrepareForTest({WorkerUtils.class, ConnectorUtils.class, FunctionCommon.class, InstanceUtils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.*" })
public class SourceApiV3ResourceTest {

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
    private SourcesImpl resource;
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

        this.resource = spy(new SourcesImpl(() -> mockedWorkerService));
        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType(any())).thenReturn(FunctionDetails.ComponentType.SOURCE);
    }

    //
    // Register Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testRegisterSourceMissingTenant() {
        try {
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
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testRegisterSourceMissingNamespace() {
        try {
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
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source name is not provided")
    public void testRegisterSourceMissingSourceName() {
        try {
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
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source class UnknownClass must be in class path")
    public void testRegisterSourceWrongClassName() {
        try {
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    mockedInputStream,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    "UnknownClass",
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source package is not provided")
    public void testRegisterSourceMissingPackage() {
        try {
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
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Package is not provided")
    public void testRegisterSourceMissingPackageDetails() {
        try {
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
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Failed to extract source class from archive")
    public void testRegisterSourceInvalidJarWithNoSource() throws IOException {
        try {
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
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Topic name cannot be null")
    public void testRegisterSourceNoOutputTopic() throws IOException {
        try {
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
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Encountered error .*. when getting Source package from .*")
    public void testRegisterSourceHttpUrl() {
        try {
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
                    "http://localhost:1234/test"
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
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
            String pkgUrl) {
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

        resource.registerSource(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                pkgUrl,
                sourceConfig,
                null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source config is not provided")
    public void testMissingSinkConfig() {
        resource.registerSource(
                tenant,
                namespace,
                source,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source config is not provided")
    public void testUpdateMissingSinkConfig() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        resource.updateSource(
                tenant,
                namespace,
                source,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null, null);
    }

    private void registerDefaultSource() throws IOException {
        SourceConfig sourceConfig = createDefaultSourceConfig();
        resource.registerSource(
            tenant,
            namespace,
                source,
            new FileInputStream(JAR_FILE_PATH),
            mockedFormData,
            null,
            sourceConfig,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source already exists")
    public void testRegisterExistedSource() throws IOException {
        try {
            Configurator.setRootLevel(Level.DEBUG);

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testRegisterSourceUploadFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testRegisterSourceSuccess() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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

        mockStatic(WorkerUtils.class);
        PowerMockito.doNothing().when(WorkerUtils.class, "uploadFileToBookkeeper", anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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
        resource.registerSource(
                actualTenant,
                actualNamespace,
                actualName,
                new FileInputStream(JAR_FILE_PATH),
                mockedFormData,
                null,
                sourceConfig,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to register")
    public void testRegisterSourceFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

            RequestResult rr = new RequestResult()
                .setSuccess(false)
                .setMessage("source failed to register");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registration interrupted")
    public void testRegisterSourceInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                new IOException("Function registration interrupted"));
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Update Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testUpdateSourceMissingTenant() throws Exception {
        try {
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
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testUpdateSourceMissingNamespace() throws Exception {
        try {
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
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source name is not provided")
    public void testUpdateSourceMissingFunctionName() throws Exception {
        try {
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
                    "Source name is not provided");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateSourceMissingPackage() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

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
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateSourceMissingTopicName() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

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
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism must be a positive number")
    public void testUpdateSourceNegativeParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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
                    "Source parallelism must be a positive number");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateSourceChangedParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateSourceChangedTopic() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

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
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism must be a positive number")
    public void testUpdateSourceZeroParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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
                    "Source parallelism must be a positive number");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
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
            String expectedError) throws Exception {

        mockStatic(ConnectorUtils.class);
        doReturn(TwitterFireHose.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

        mockStatic(FunctionCommon.class);
        PowerMockito.when(FunctionCommon.class, "createPkgTempFile").thenCallRealMethod();
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any());

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

        resource.updateSource(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            null,
            sourceConfig,
                null, null, null);

    }

    private void updateDefaultSource() throws Exception {
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

        mockStatic(FunctionCommon.class);
        PowerMockito.when(FunctionCommon.class, "createPkgTempFile").thenCallRealMethod();
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any(File.class));

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        resource.updateSource(
            tenant,
            namespace,
                source,
                new FileInputStream(JAR_FILE_PATH),
            mockedFormData,
            null,
            sourceConfig,
                null, null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source doesn't exist")
    public void testUpdateNotExistedSource() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testUpdateSourceUploadFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testUpdateSourceSuccess() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        updateDefaultSource();
    }

    @Test
    public void testUpdateSourceWithUrl() throws Exception {
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

        mockStatic(FunctionCommon.class);
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(anyString(), any(NarClassLoader.class));
        PowerMockito.when(FunctionCommon.class, "extractFileFromPkgURL", any()).thenCallRealMethod();

        doReturn(mock(NarClassLoader.class)).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any());

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);


        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        resource.updateSource(
            tenant,
            namespace,
                source,
            null,
            null,
            filePackageUrl,
            sourceConfig,
                null, null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to register")
    public void testUpdateSourceFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            RequestResult rr = new RequestResult()
                    .setSuccess(false)
                    .setMessage("source failed to register");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registration interrupted")
    public void testUpdateSourceInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                new IOException("Function registration interrupted"));
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // deregister source
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testDeregisterSourceMissingTenant() {
        try {
            testDeregisterSourceMissingArguments(
                null,
                namespace,
                    source
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testDeregisterSourceMissingNamespace() {
        try {
            testDeregisterSourceMissingArguments(
                tenant,
                null,
                source
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source name is not provided")
    public void testDeregisterSourceMissingFunctionName() {
        try {
            testDeregisterSourceMissingArguments(
                tenant,
                namespace,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testDeregisterSourceMissingArguments(
            String tenant,
            String namespace,
            String function
    ) {
        resource.deregisterFunction(
            tenant,
            namespace,
            function,
                null, null);

    }

    private void deregisterDefaultSource() {
        resource.deregisterFunction(
            tenant,
            namespace,
                source,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp= "Source test-source doesn't exist")
    public void testDeregisterNotExistedSource() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            deregisterDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testDeregisterSourceSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source))).thenReturn(FunctionMetaData.newBuilder().build());

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source deregistered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(requestResult);

        deregisterDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to deregister")
    public void testDeregisterSourceFailure() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source))).thenReturn(FunctionMetaData.newBuilder().build());

            RequestResult rr = new RequestResult()
                .setSuccess(false)
                .setMessage("source failed to deregister");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(requestResult);

            deregisterDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function deregistration interrupted")
    public void testDeregisterSourceInterrupted() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source))).thenReturn(FunctionMetaData.newBuilder().build());

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                new IOException("Function deregistration interrupted"));
            when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(requestResult);

            deregisterDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Get Source Info
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testGetSourceMissingTenant() {
        try {
            testGetSourceMissingArguments(
                    null,
                    namespace,
                    source
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testGetSourceMissingNamespace() {
        try {
            testGetSourceMissingArguments(
                tenant,
                null,
                    source
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source name is not provided")
    public void testGetSourceMissingFunctionName() {
        try {
            testGetSourceMissingArguments(
                tenant,
                namespace,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testGetSourceMissingArguments(
            String tenant,
            String namespace,
            String source
    ) {
        resource.getFunctionInfo(
            tenant,
            namespace,
            source, null, null
        );
    }

    private SourceConfig getDefaultSourceInfo() {
        return resource.getSourceInfo(
            tenant,
            namespace,
                source
        );
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source doesn't exist")
    public void testGetNotExistedSource() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            getDefaultSourceInfo();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testGetSourceSuccess() {
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
    public void testListSourcesMissingTenant() {
        try {
            testListSourcesMissingArguments(
                null,
                namespace
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testListSourcesMissingNamespace() {
        try {
            testListSourcesMissingArguments(
                tenant,
                null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testListSourcesMissingArguments(
            String tenant,
            String namespace
    ) {
        resource.listFunctions(
            tenant,
            namespace, null, null
        );
    }

    private List<String> listDefaultSources() {
        return resource.listFunctions(
            tenant,
            namespace, null, null);
    }

    @Test
    public void testListSourcesSuccess() {
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
    public void testOnlyGetSources() {
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
        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f1.getFunctionDetails())).thenReturn(FunctionDetails.ComponentType.SOURCE);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f2.getFunctionDetails())).thenReturn(FunctionDetails.ComponentType.FUNCTION);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f3.getFunctionDetails())).thenReturn(FunctionDetails.ComponentType.SINK);

        List<String> sourceList = listDefaultSources();
        assertEquals(functions, sourceList);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace does not exist")
    public void testRegisterFunctionNonExistingNamespace() throws Exception {
        try {
            this.namespaceList.clear();
            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant does not exist")
    public void testRegisterFunctionNonExistingTenant() throws Exception {
        try {
            when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
            registerDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
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
        return SourceConfigUtils.convert(createDefaultSourceConfig(),
                new SourceConfigUtils.ExtractedSourceDetails(null, null));
    }
}
