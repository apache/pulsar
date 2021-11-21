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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.Function.SubscriptionType;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.LeaderService;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.apache.pulsar.functions.worker.rest.api.PulsarFunctionTestTemporaryDirectory;
import org.apache.pulsar.functions.worker.rest.api.v2.FunctionsApiV2Resource;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionsApiV2Resource}.
 */
@PrepareForTest({WorkerUtils.class, InstanceUtils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.functions.api.*" })
public class FunctionApiV3ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final class TestFunction implements Function<String, String> {

        @Override
        public String process(String input, Context context) {
            return input;
        }
    }

    private static final class WrongFunction implements Consumer<String> {
        @Override
        public void accept(String s) {

        }
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";
    private static final String outputTopic = "test-output-topic";
    private static final String outputSerdeClassName = TopicSchema.DEFAULT_SERDE;
    private static final String className = TestFunction.class.getName();
    private SubscriptionType subscriptionType = SubscriptionType.FAILOVER;
    private static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    static {
        topicsToSerDeClassName.put("persistent://sample/standalone/ns1/test_src", TopicSchema.DEFAULT_SERDE);
    }
    private static final int parallelism = 1;

    private PulsarWorkerService mockedWorkerService;
    private PulsarAdmin mockedPulsarAdmin;
    private Tenants mockedTenants;
    private Namespaces mockedNamespaces;
    private Functions mockedFunctions;
    private TenantInfoImpl mockedTenantInfo;
    private List<String> namespaceList = new LinkedList<>();
    private FunctionMetaDataManager mockedManager;
    private FunctionRuntimeManager mockedFunctionRunTimeManager;
    private RuntimeFactory mockedRuntimeFactory;
    private Namespace mockedNamespace;
    private FunctionsImpl resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;
    private FunctionMetaData mockedFunctionMetadata;
    private LeaderService mockedLeaderService;
    private Packages mockedPackages;
    private PulsarFunctionTestTemporaryDirectory tempDirectory;

    @BeforeMethod
    public void setup() throws Exception {
        this.mockedManager = mock(FunctionMetaDataManager.class);
        this.mockedFunctionRunTimeManager = mock(FunctionRuntimeManager.class);
        this.mockedTenantInfo = mock(TenantInfoImpl.class);
        this.mockedRuntimeFactory = mock(RuntimeFactory.class);
        this.mockedInputStream = mock(InputStream.class);
        this.mockedNamespace = mock(Namespace.class);
        this.mockedFormData = mock(FormDataContentDisposition.class);
        when(mockedFormData.getFileName()).thenReturn("test");
        this.mockedPulsarAdmin = mock(PulsarAdmin.class);
        this.mockedTenants = mock(Tenants.class);
        this.mockedNamespaces = mock(Namespaces.class);
        this.mockedFunctions = mock(Functions.class);
        this.mockedPackages = mock(Packages.class);
        this.mockedLeaderService = mock(LeaderService.class);
        this.mockedFunctionMetadata = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        namespaceList.add(tenant + "/" + namespace);

        this.mockedWorkerService = mock(PulsarWorkerService.class);
        when(mockedWorkerService.getFunctionMetaDataManager()).thenReturn(mockedManager);
        when(mockedWorkerService.getFunctionRuntimeManager()).thenReturn(mockedFunctionRunTimeManager);
        when(mockedWorkerService.getLeaderService()).thenReturn(mockedLeaderService);
        when(mockedFunctionRunTimeManager.getRuntimeFactory()).thenReturn(mockedRuntimeFactory);
        when(mockedWorkerService.getDlogNamespace()).thenReturn(mockedNamespace);
        when(mockedWorkerService.isInitialized()).thenReturn(true);
        when(mockedWorkerService.getBrokerAdmin()).thenReturn(mockedPulsarAdmin);
        when(mockedWorkerService.getFunctionAdmin()).thenReturn(mockedPulsarAdmin);
        when(mockedPulsarAdmin.tenants()).thenReturn(mockedTenants);
        when(mockedPulsarAdmin.namespaces()).thenReturn(mockedNamespaces);
        when(mockedPulsarAdmin.functions()).thenReturn(mockedFunctions);
        when(mockedPulsarAdmin.packages()).thenReturn(mockedPackages);
        when(mockedTenants.getTenantInfo(any())).thenReturn(mockedTenantInfo);
        when(mockedNamespaces.getNamespaces(any())).thenReturn(namespaceList);
        when(mockedLeaderService.isLeader()).thenReturn(true);
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetadata);
        doNothing().when(mockedPackages).download(anyString(), anyString());

        // worker config
        WorkerConfig workerConfig = new WorkerConfig()
            .setWorkerId("test")
            .setWorkerPort(8080)
            .setFunctionMetadataTopicName("pulsar/functions")
            .setNumFunctionPackageReplicas(3)
            .setPulsarServiceUrl("pulsar://localhost:6650/");
        tempDirectory = PulsarFunctionTestTemporaryDirectory.create(getClass().getSimpleName());
        tempDirectory.useTemporaryDirectoriesForWorkerConfig(workerConfig);
        when(mockedWorkerService.getWorkerConfig()).thenReturn(workerConfig);

        this.resource = spy(new FunctionsImpl(() -> mockedWorkerService));
        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType(any())).thenReturn(FunctionDetails.ComponentType.FUNCTION);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (tempDirectory != null) {
            tempDirectory.delete();
        }
    }

    //
    // Register Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testRegisterFunctionMissingTenant() {
        try {
            testRegisterFunctionMissingArguments(
                    null,
                    namespace,
                    function,
                    mockedInputStream,
                    topicsToSerDeClassName,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testRegisterFunctionMissingNamespace() {
        try {
            testRegisterFunctionMissingArguments(
                tenant,
                null,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function name is not provided")
    public void testRegisterFunctionMissingFunctionName() {
        try {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            topicsToSerDeClassName,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                null);
    } catch (RestException re){
        assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
        throw re;
    }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function Package is not provided")
    public void testRegisterFunctionMissingPackage() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    null,
                    topicsToSerDeClassName,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "No input topic\\(s\\) specified for the function")
    public void testRegisterFunctionMissingInputTopics() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    null,
                    null,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function Package is not provided")
    public void testRegisterFunctionMissingPackageDetails() {
        try {
            testRegisterFunctionMissingArguments(
                tenant,
                namespace,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                null,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function classname cannot be null")
    public void testRegisterFunctionMissingClassName() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    mockedInputStream,
                    topicsToSerDeClassName,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    null,
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function class UnknownClass must be in class path")
    public void testRegisterFunctionWrongClassName() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    mockedInputStream,
                    topicsToSerDeClassName,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    "UnknownClass",
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function parallelism must be a positive number")
    public void testRegisterFunctionWrongParallelism() {
        try {
            testRegisterFunctionMissingArguments(
                tenant,
                namespace,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                className,
                -2,
                null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class,
            expectedExceptionsMessageRegExp = "Output topic persistent://sample/standalone/ns1/test_src is also being used as an input topic \\(topics must be one or the other\\)")
    public void testRegisterFunctionSameInputOutput() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    mockedInputStream,
                    topicsToSerDeClassName,
                    mockedFormData,
                    topicsToSerDeClassName.keySet().iterator().next(),
                    outputSerdeClassName,
                    className,
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Output topic " + function + "-output-topic/test:" + " is invalid")
    public void testRegisterFunctionWrongOutputTopic() {
        try {
            testRegisterFunctionMissingArguments(
                tenant,
                namespace,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                function + "-output-topic/test:",
                outputSerdeClassName,
                className,
                parallelism,
                null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Encountered error .*. when getting Function package from .*")
    public void testRegisterFunctionHttpUrl() {
        try {
            testRegisterFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                topicsToSerDeClassName,
                null,
                outputTopic,
                outputSerdeClassName,
                className,
                parallelism,
                "http://localhost:1234/test");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function class .*. does not implement the correct interface")
    public void testRegisterFunctionImplementWrongInterface() {
        try {
            testRegisterFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    mockedInputStream,
                    topicsToSerDeClassName,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    WrongFunction.class.getName(),
                    parallelism,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testRegisterFunctionMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            Map<String, String> topicsToSerDeClassName,
            FormDataContentDisposition details,
            String outputTopic,
            String outputSerdeClassName,
            String className,
            Integer parallelism,
            String functionPkgUrl) {
        FunctionConfig functionConfig = new FunctionConfig();
        if (tenant != null) {
            functionConfig.setTenant(tenant);
        }
        if (namespace != null) {
            functionConfig.setNamespace(namespace);
        }
        if (function != null) {
            functionConfig.setName(function);
        }
        if (topicsToSerDeClassName != null) {
            functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        }
        if (outputTopic != null) {
            functionConfig.setOutput(outputTopic);
        }
        if (outputSerdeClassName != null) {
            functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        }
        if (className != null) {
            functionConfig.setClassName(className);
        }
        if (parallelism != null) {
            functionConfig.setParallelism(parallelism);
        }
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);

        resource.registerFunction(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                functionPkgUrl,
                functionConfig,
                null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function config is not provided")
    public void testMissingFunctionConfig() {
        resource.registerFunction(
                tenant,
                namespace,
                function,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function config is not provided")
    public void testUpdateMissingFunctionConfig() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        resource.updateFunction(
                tenant,
                namespace,
                function,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null, null);
    }

    private void registerDefaultFunction() {
        registerDefaultFunctionWithPackageUrl(null);
    }

    private void registerDefaultFunctionWithPackageUrl(String packageUrl) {
        FunctionConfig functionConfig = createDefaultFunctionConfig();
        resource.registerFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            packageUrl,
            functionConfig,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function already exists")
    public void testRegisterExistedFunction() {
        try {
            Configurator.setRootLevel(Level.DEBUG);
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);
            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }


    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testRegisterFunctionUploadFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);

            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                any(File.class),
                any(Namespace.class));
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testRegisterFunctionSuccess() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadToBookKeeper(
                any(Namespace.class),
                any(InputStream.class),
                anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(timeOut = 20000)
    public void testRegisterFunctionSuccessWithPackageName() {
        registerDefaultFunctionWithPackageUrl("function://public/default/test@v1");
    }

    @Test(timeOut = 20000)
    public void testRegisterFunctionFailedWithWrongPackageName() throws PulsarAdminException {
        try {
            doThrow(new PulsarAdminException("package name is invalid"))
                .when(mockedPackages).download(anyString(), anyString());
            registerDefaultFunctionWithPackageUrl("function://");
        } catch (RestException e) {
            // expected exception
            assertEquals(e.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace does not exist")
    public void testRegisterFunctionNonExistingNamespace() {
        try {
            this.namespaceList.clear();
            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant does not exist")
    public void testRegisterFunctionNonexistantTenant() throws Exception {
        try {
            when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "function failed to register")
    public void testRegisterFunctionFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadToBookKeeper(
                any(Namespace.class),
                any(InputStream.class),
                anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

            doThrow(new IllegalArgumentException("function failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration interrupted")
    public void testRegisterFunctionInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadToBookKeeper(
                any(Namespace.class),
                any(InputStream.class),
                anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

            doThrow(new IllegalStateException("Function registration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Update Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testUpdateFunctionMissingTenant() throws Exception {
        try {
            testUpdateFunctionMissingArguments(
                null,
                namespace,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    "Tenant is not provided");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testUpdateFunctionMissingNamespace() throws Exception {
        try {
            testUpdateFunctionMissingArguments(
                tenant,
                null,
                function,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    "Namespace is not provided");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function name is not provided")
    public void testUpdateFunctionMissingFunctionName() throws Exception {
        try {
            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                null,
                mockedInputStream,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    "Function name is not provided");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateFunctionMissingPackage() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();
            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                className,
                parallelism,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateFunctionMissingInputTopic() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            testUpdateFunctionMissingArguments(
                    tenant,
                    namespace,
                    function,
                    null,
                    null,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    className,
                    parallelism,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateFunctionMissingClassName() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                    outputSerdeClassName,
                null,
                parallelism,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateFunctionChangedParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                topicsToSerDeClassName,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                null,
                parallelism + 1,
                null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateFunctionChangedInputs() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            null,
            topicsToSerDeClassName,
            mockedFormData,
            "DifferentOutput",
            outputSerdeClassName,
            null,
            parallelism,
            null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Input Topics cannot be altered")
    public void testUpdateFunctionChangedOutput() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            Map<String, String> someOtherInput = new HashMap<>();
            someOtherInput.put("DifferentTopic", TopicSchema.DEFAULT_SERDE);
            testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                null,
                someOtherInput,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                null,
                parallelism,
                "Input Topics cannot be altered");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testUpdateFunctionMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            Map<String, String> topicsToSerDeClassName,
            FormDataContentDisposition details,
            String outputTopic,
            String outputSerdeClassName,
            String className,
            Integer parallelism,
            String expectedError) throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        FunctionConfig functionConfig = new FunctionConfig();
        if (tenant != null) {
            functionConfig.setTenant(tenant);
        }
        if (namespace != null) {
            functionConfig.setNamespace(namespace);
        }
        if (function != null) {
            functionConfig.setName(function);
        }
        if (topicsToSerDeClassName != null) {
            functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        }
        if (outputTopic != null) {
            functionConfig.setOutput(outputTopic);
        }
        if (outputSerdeClassName != null) {
            functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        }
        if (className != null) {
            functionConfig.setClassName(className);
        }
        if (parallelism != null) {
            functionConfig.setParallelism(parallelism);
        }
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);

        if (expectedError != null) {
            doThrow(new IllegalArgumentException(expectedError))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
        }

        resource.updateFunction(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            null,
            functionConfig,
                null, null, null);

    }

    private void updateDefaultFunction() {
        updateDefaultFunctionWithPackageUrl(null);
    }

    private void updateDefaultFunctionWithPackageUrl(String packageUrl) {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);

        resource.updateFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            packageUrl,
            functionConfig,
                null, null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function doesn't exist")
    public void testUpdateNotExistedFunction() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);
            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testUpdateFunctionUploadFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                any(File.class),
                any(Namespace.class));
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testUpdateFunctionSuccess() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadToBookKeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());
        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        updateDefaultFunction();
    }

    @Test
    public void testUpdateFunctionWithUrl() {
        Configurator.setRootLevel(Level.DEBUG);

        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String filePackageUrl = "file://" + fileLocation;

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        resource.updateFunction(
            tenant,
            namespace,
            function,
            null,
            null,
            filePackageUrl,
            functionConfig,
                null, null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "function failed to register")
    public void testUpdateFunctionFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadToBookKeeper(
                any(Namespace.class),
                any(InputStream.class),
                anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            doThrow(new IllegalArgumentException("function failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registeration interrupted")
    public void testUpdateFunctionInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadToBookKeeper(
                any(Namespace.class),
                any(InputStream.class),
                anyString());
            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            doThrow(new IllegalStateException("Function registeration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }


    @Test(timeOut = 20000)
    public void testUpdateFunctionSuccessWithPackageName() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);
        updateDefaultFunctionWithPackageUrl("function://public/default/test@v1");
    }

    @Test(timeOut = 20000)
    public void testUpdateFunctionFailedWithWrongPackageName() throws PulsarAdminException {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);
        try {
            doThrow(new PulsarAdminException("package name is invalid"))
                .when(mockedPackages).download(anyString(), anyString());
            registerDefaultFunctionWithPackageUrl("function://");
        } catch (RestException e) {
            // expected exception
            assertEquals(e.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
        }
    }

    //
    // deregister function
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testDeregisterFunctionMissingTenant() {
        try {

            testDeregisterFunctionMissingArguments(
                null,
                namespace,
                function
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testDeregisterFunctionMissingNamespace() {
        try {
            testDeregisterFunctionMissingArguments(
                tenant,
                null,
                function
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function name is not provided")
    public void testDeregisterFunctionMissingFunctionName() {
        try {
             testDeregisterFunctionMissingArguments(
                tenant,
                namespace,
                null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testDeregisterFunctionMissingArguments(
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

    private void deregisterDefaultFunction() {
        resource.deregisterFunction(
            tenant,
            namespace,
            function,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function doesn't exist")
    public void testDeregisterNotExistedFunction() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);
            deregisterDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testDeregisterFunctionSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        deregisterDefaultFunction();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "function failed to deregister")
    public void testDeregisterFunctionFailure() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            doThrow(new IllegalArgumentException("function failed to deregister"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            deregisterDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function deregisteration interrupted")
    public void testDeregisterFunctionInterrupted() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

            doThrow(new IllegalStateException("Function deregisteration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            deregisterDefaultFunction();
        }
        catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Get Function Info
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testGetFunctionMissingTenant() {
        try {
            testGetFunctionMissingArguments(
                null,
                namespace,
                function
            );
        }
        catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testGetFunctionMissingNamespace() {
        try {
            testGetFunctionMissingArguments(
                tenant,
                null,
                function
            );
        }
        catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function name is not provided")
    public void testGetFunctionMissingFunctionName() {
        try {
            testGetFunctionMissingArguments(
                tenant,
                namespace,
                null
            );
        }
        catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testGetFunctionMissingArguments(
            String tenant,
            String namespace,
            String function
    ) {
        resource.getFunctionInfo(
            tenant,
            namespace,
            function,null,null
        );

    }

    private FunctionConfig getDefaultFunctionInfo() {
        return resource.getFunctionInfo(
            tenant,
            namespace,
            function,
                null,
                null
        );
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function doesn't exist")
    public void testGetNotExistedFunction() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);
            getDefaultFunctionInfo();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testGetFunctionSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        SinkSpec sinkSpec = SinkSpec.newBuilder()
                .setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setClassName(className)
                .setSink(sinkSpec)
                .setName(function)
                .setNamespace(namespace)
                .setProcessingGuarantees(ProcessingGuarantees.ATMOST_ONCE)
                .setTenant(tenant)
                .setParallelism(parallelism)
                .setSource(SourceSpec.newBuilder().setSubscriptionType(subscriptionType)
                        .putAllTopicsToSerDeClassName(topicsToSerDeClassName)).build();
        FunctionMetaData metaData = FunctionMetaData.newBuilder()
            .setCreateTime(System.currentTimeMillis())
            .setFunctionDetails(functionDetails)
            .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath("/path/to/package"))
            .setVersion(1234)
            .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(function))).thenReturn(metaData);

        FunctionConfig functionConfig = getDefaultFunctionInfo();
        assertEquals(
                FunctionConfigUtils.convertFromDetails(functionDetails),
                functionConfig);
    }

    //
    // List Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testListFunctionsMissingTenant() {
        try {
            testListFunctionsMissingArguments(
                null,
                namespace
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testListFunctionsMissingNamespace() {
        try {
            testListFunctionsMissingArguments(
                tenant,
                null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testListFunctionsMissingArguments(
            String tenant,
            String namespace
    ) {
        resource.listFunctions(
            tenant,
            namespace,null,null
        );

    }

    private List<String> listDefaultFunctions() {
        return resource.listFunctions(
            tenant,
            namespace,null,null
        );
    }

    @Test
    public void testListFunctionsSuccess() {
        final List<String> functions = Lists.newArrayList("test-1", "test-2");
        final List<FunctionMetaData> metaDataList = new LinkedList<>();
        FunctionMetaData functionMetaData1 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()
        ).build();
        FunctionMetaData functionMetaData2 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()
        ).build();
        metaDataList.add(functionMetaData1);
        metaDataList.add(functionMetaData2);
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(metaDataList);

        List<String> functionList = listDefaultFunctions();
        assertEquals(functions, functionList);
    }

    @Test
    public void testOnlyGetSources() {
        List<String> functions = Lists.newArrayList("test-2");
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
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

        List<String> functionList = listDefaultFunctions();
        assertEquals(functions, functionList);
    }

    @Test
    public void testDownloadFunctionHttpUrl() throws Exception {
        String jarHttpUrl = "https://repo1.maven.org/maven2/org/apache/pulsar/pulsar-common/2.4.2/pulsar-common-2.4.2.jar";
        String testDir = FunctionApiV3ResourceTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        PulsarWorkerService worker = mock(PulsarWorkerService.class);
        doReturn(true).when(worker).isInitialized();
        WorkerConfig config = mock(WorkerConfig.class);
        when(config.isAuthorizationEnabled()).thenReturn(false);
        when(worker.getWorkerConfig()).thenReturn(config);
        FunctionsImpl function = new FunctionsImpl(()-> worker);
        StreamingOutput streamOutput = function.downloadFunction(jarHttpUrl, null, null);
        File pkgFile = new File(testDir, UUID.randomUUID().toString());
        OutputStream output = new FileOutputStream(pkgFile);
        streamOutput.write(output);
        Assert.assertTrue(pkgFile.exists());
        if (pkgFile.exists()) {
            pkgFile.delete();
        }
    }

    @Test
    public void testDownloadFunctionFile() throws Exception {
        URL fileUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        File file = Paths.get(fileUrl.toURI()).toFile();
        String fileLocation = file.getAbsolutePath().replace('\\', '/');
        String testDir = FunctionApiV3ResourceTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        PulsarWorkerService worker = mock(PulsarWorkerService.class);
        doReturn(true).when(worker).isInitialized();
        WorkerConfig config = mock(WorkerConfig.class);
        when(config.isAuthorizationEnabled()).thenReturn(false);
        when(worker.getWorkerConfig()).thenReturn(config);
        FunctionsImpl function = new FunctionsImpl(() -> worker);
        StreamingOutput streamOutput = function.downloadFunction("file:///" + fileLocation, null, null);
        File pkgFile = new File(testDir, UUID.randomUUID().toString());
        OutputStream output = new FileOutputStream(pkgFile);
        streamOutput.write(output);
        Assert.assertTrue(pkgFile.exists());
        if (pkgFile.exists()) {
            pkgFile.delete();
        }
    }

    @Test
    public void testRegisterFunctionFileUrlWithValidSinkClass() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);

        URL fileUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        File file = Paths.get(fileUrl.toURI()).toFile();
        String fileLocation = file.getAbsolutePath().replace('\\', '/');
        String filePackageUrl = "file:///" + fileLocation;
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        resource.registerFunction(tenant, namespace, function, null, null, filePackageUrl, functionConfig, null, null);

    }

    @Test
    public void testRegisterFunctionWithConflictingFields() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);
        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

        URL fileUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        File file = Paths.get(fileUrl.toURI()).toFile();
        String fileLocation = file.getAbsolutePath().replace('\\', '/');
        String filePackageUrl = "file:///" + fileLocation;
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        resource.registerFunction(actualTenant, actualNamespace, actualName, null, null, filePackageUrl, functionConfig, null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function language runtime is either not set or cannot be determined")
    public void testCreateFunctionWithoutSettingRuntime() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);

        URL fileUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        File file = Paths.get(fileUrl.toURI()).toFile();
        String fileLocation = file.getAbsolutePath().replace('\\', '/');
        String filePackageUrl = "file:///" + fileLocation;
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(function);
        functionConfig.setClassName(className);
        functionConfig.setParallelism(parallelism);
        functionConfig.setCustomSerdeInputs(topicsToSerDeClassName);
        functionConfig.setOutput(outputTopic);
        functionConfig.setOutputSerdeClassName(outputSerdeClassName);
        resource.registerFunction(tenant, namespace, function, null, null, filePackageUrl, functionConfig, null, null);

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

    public static FunctionDetails createDefaultFunctionDetails() {
        FunctionConfig functionConfig = createDefaultFunctionConfig();
        return FunctionConfigUtils.convert(functionConfig, null);
    }
}
