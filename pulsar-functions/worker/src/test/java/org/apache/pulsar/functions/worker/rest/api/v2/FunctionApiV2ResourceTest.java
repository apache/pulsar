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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.Function.SubscriptionType;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.worker.*;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.apache.pulsar.functions.worker.rest.api.ComponentImpl;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionApiV2Resource}.
 */
@PrepareForTest(Utils.class)
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*" })
@Slf4j
public class FunctionApiV2ResourceTest {

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
    private FunctionsImpl resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;
    private FunctionMetaData mockedFunctionMetadata;

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
        this.mockedFunctionMetadata = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
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
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetadata);

        // worker config
        WorkerConfig workerConfig = new WorkerConfig()
            .setWorkerId("test")
            .setWorkerPort(8080)
            .setDownloadDirectory("/tmp/pulsar/functions")
            .setFunctionMetadataTopicName("pulsar/functions")
            .setNumFunctionPackageReplicas(3)
            .setPulsarServiceUrl("pulsar://localhost:6650/");
        when(mockedWorkerService.getWorkerConfig()).thenReturn(workerConfig);

        this.resource = spy(new FunctionsImpl(() -> mockedWorkerService));
        doReturn(ComponentImpl.ComponentType.FUNCTION).when(this.resource).calculateSubjectType(any());
    }

    //
    // Register Functions
    //

    @Test
    public void testRegisterFunctionMissingTenant() throws IOException {
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
                null,
                "Tenant is not provided");
    }

    @Test
    public void testRegisterFunctionMissingNamespace() throws IOException {
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
                null,
                "Namespace is not provided");
    }

    @Test
    public void testRegisterFunctionMissingFunctionName() throws IOException {
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
                null,
                "Function Name is not provided");
    }

    @Test
    public void testRegisterFunctionMissingPackage() throws IOException {
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
                null,
                "Function Package is not provided");
    }

    @Test
    public void testRegisterFunctionMissingInputTopics() throws IOException {
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
                null,
                "No input topic(s) specified for the function");
    }

    @Test
    public void testRegisterFunctionMissingPackageDetails() throws IOException {
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
                null,
                "Function Package is not provided");
    }

    @Test
    public void testRegisterFunctionMissingClassName() throws IOException {
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
                null,
                "Function classname cannot be null");
    }

    @Test
    public void testRegisterFunctionWrongClassName() throws IOException {
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
                null,
                "User class must be in class path");
    }

    @Test
    public void testRegisterFunctionWrongParallelism() throws IOException {
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
                null,
                "Function parallelism should positive number");
    }

    @Test
    public void testRegisterFunctionSameInputOutput() throws IOException {
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
                null,
                "Output topic " + topicsToSerDeClassName.keySet().iterator().next()
                        + " is also being used as an input topic (topics must be one or the other)");
    }

    @Test
    public void testRegisterFunctionWrongOutputTopic() throws IOException {
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
                null,
                "Output topic " + function + "-output-topic/test:" + " is invalid");
    }

    @Test
    public void testRegisterFunctionHttpUrl() throws IOException {
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
                "http://localhost:1234/test",
                "Corrupted Jar File");
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
            String functionPkgUrl,
            String errorExpected) throws IOException {
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

        Response response = resource.registerFunction(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                functionPkgUrl,
                null,
                new Gson().toJson(functionConfig),
                null);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        Assert.assertEquals(((ErrorData) response.getEntity()).reason, new ErrorData(errorExpected).reason);
    }

    private Response registerDefaultFunction() {
        FunctionConfig functionConfig = createDefaultFunctionConfig();
        return resource.registerFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            null,
            null,
            new Gson().toJson(functionConfig),
                null);
    }

    @Test
    public void testRegisterExistedFunction() throws IOException {
        Configurator.setRootLevel(Level.DEBUG);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        Response response = registerDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function " + function + " already exists").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testRegisterFunctionUploadFailure() throws Exception {
        mockStatic(Utils.class);
        doThrow(new IOException("upload failure")).when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
            any(File.class),
            any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = registerDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("upload failure").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testRegisterFunctionSuccess() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("function registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = registerDefaultFunction();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRegisterFunctionNonexistantNamespace() throws Exception {
        this.namespaceList.clear();
        Response response = registerDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Namespace does not exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testRegisterFunctionNonexistantTenant() throws Exception {
        when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
        Response response = registerDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Tenant does not exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testRegisterFunctionFailure() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("function failed to register");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = registerDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(rr.getMessage()).reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testRegisterFunctionInterrupted() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function registeration interrupted"));
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = registerDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function registeration interrupted").reason, ((ErrorData) response.getEntity()).reason);
    }

    //
    // Update Functions
    //

    @Test
    public void testUpdateFunctionMissingTenant() throws IOException {
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
    }

    @Test
    public void testUpdateFunctionMissingNamespace() throws IOException {
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
    }

    @Test
    public void testUpdateFunctionMissingFunctionName() throws IOException {
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
                "Function Name is not provided");
    }

    @Test
    public void testUpdateFunctionMissingPackage() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
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
    }

    @Test
    public void testUpdateFunctionMissingInputTopic() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
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
    }

    @Test
    public void testUpdateFunctionMissingClassName() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
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
    }

    @Test
    public void testUpdateFunctionChangedParallelism() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
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
    }

    @Test
    public void testUpdateFunctionChangedInputs() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
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
                "Output topics differ");
    }

    @Test
    public void testUpdateFunctionChangedOutput() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());
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
            String expectedError) throws IOException {
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

        if (expectedError == null) {
            RequestResult rr = new RequestResult()
                    .setSuccess(true)
                    .setMessage("function registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
        }

        Response response = resource.updateFunction(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            null,
            null,
            new Gson().toJson(functionConfig),
                null);

        if (expectedError == null) {
            assertEquals(Status.OK.getStatusCode(), response.getStatus());
        } else {
            assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
            Assert.assertEquals(((ErrorData) response.getEntity()).reason, new ErrorData(expectedError).reason);
        }
    }

    private Response updateDefaultFunction() throws IOException {
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

        return resource.updateFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            null,
            null,
            new Gson().toJson(functionConfig),
                null);
    }

    @Test
    public void testUpdateNotExistedFunction() throws IOException {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = updateDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function " + function + " doesn't exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testUpdateFunctionUploadFailure() throws Exception {
        mockStatic(Utils.class);
        doThrow(new IOException("upload failure")).when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
            any(File.class),
            any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        Response response = updateDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("upload failure").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testUpdateFunctionSuccess() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("function registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = updateDefaultFunction();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testUpdateFunctionWithUrl() throws IOException {
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
        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("function registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = resource.updateFunction(
            tenant,
            namespace,
            function,
            null,
            null,
            filePackageUrl,
            null,
            new Gson().toJson(functionConfig),
                null);

        assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testUpdateFunctionFailure() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("function failed to register");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = updateDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(rr.getMessage()).reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testUpdateFunctionInterrupted() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function registeration interrupted"));
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = updateDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function registeration interrupted").reason, ((ErrorData) response.getEntity()).reason);
    }

    //
    // deregister function
    //

    @Test
    public void testDeregisterFunctionMissingTenant() throws Exception {
        testDeregisterFunctionMissingArguments(
            null,
            namespace,
            function,
            "Tenant");
    }

    @Test
    public void testDeregisterFunctionMissingNamespace() throws Exception {
        testDeregisterFunctionMissingArguments(
            tenant,
            null,
            function,
            "Namespace");
    }

    @Test
    public void testDeregisterFunctionMissingFunctionName() throws Exception {
        testDeregisterFunctionMissingArguments(
            tenant,
            namespace,
            null,
            "Function Name");
    }

    private void testDeregisterFunctionMissingArguments(
        String tenant,
        String namespace,
        String function,
        String missingFieldName
    ) {
        Response response = resource.deregisterFunction(
            tenant,
            namespace,
            function,
                null);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
    }

    private Response deregisterDefaultFunction() {
        return resource.deregisterFunction(
            tenant,
            namespace,
            function,
                null);
    }

    @Test
    public void testDeregisterNotExistedFunction() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = deregisterDefaultFunction();
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function " + function + " doesn't exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testDeregisterFunctionSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("function deregistered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(requestResult);

        Response response = deregisterDefaultFunction();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(rr.toJson(), response.getEntity());
    }

    @Test
    public void testDeregisterFunctionFailure() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("function failed to deregister");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(requestResult);

        Response response = deregisterDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(rr.getMessage()).reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testDeregisterFunctionInterrupted() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function deregisteration interrupted"));
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(requestResult);

        Response response = deregisterDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function deregisteration interrupted").reason, ((ErrorData) response.getEntity()).reason);
    }

    //
    // Get Function Info
    //

    @Test
    public void testGetFunctionMissingTenant() throws Exception {
        testGetFunctionMissingArguments(
            null,
            namespace,
            function,
            "Tenant");
    }

    @Test
    public void testGetFunctionMissingNamespace() throws Exception {
        testGetFunctionMissingArguments(
            tenant,
            null,
            function,
            "Namespace");
    }

    @Test
    public void testGetFunctionMissingFunctionName() throws Exception {
        testGetFunctionMissingArguments(
            tenant,
            namespace,
            null,
            "Function Name");
    }

    private void testGetFunctionMissingArguments(
        String tenant,
        String namespace,
        String function,
        String missingFieldName
    ) throws IOException {
        Response response = resource.getFunctionInfo(
            tenant,
            namespace,
            function
        );

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
    }

    private Response getDefaultFunctionInfo() throws IOException {
        return resource.getFunctionInfo(
            tenant,
            namespace,
            function
        );
    }

    @Test
    public void testGetNotExistedFunction() throws IOException {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = getDefaultFunctionInfo();
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function " + function + " doesn't exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testGetFunctionSuccess() throws Exception {
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

        Response response = getDefaultFunctionInfo();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(
                new Gson().toJson(FunctionConfigUtils.convertFromDetails(functionDetails)),
                response.getEntity());
    }

    //
    // List Functions
    //

    @Test
    public void testListFunctionsMissingTenant() throws Exception {
        testListFunctionsMissingArguments(
            null,
            namespace,
            "Tenant");
    }

    @Test
    public void testListFunctionsMissingNamespace() throws Exception {
        testListFunctionsMissingArguments(
            tenant,
            null,
            "Namespace");
    }

    private void testListFunctionsMissingArguments(
        String tenant,
        String namespace,
        String missingFieldName
    ) {
        Response response = resource.listFunctions(
            tenant,
            namespace
        );

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
    }

    private Response listDefaultFunctions() {
        return resource.listFunctions(
            tenant,
            namespace
        );
    }

    @Test
    public void testListFunctionsSuccess() throws Exception {
        List<String> functions = Lists.newArrayList("test-1", "test-2");
        List<FunctionMetaData> metaDataList = new LinkedList<>();
        FunctionMetaData functionMetaData1 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()
        ).build();
        FunctionMetaData functionMetaData2 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()
        ).build();
        metaDataList.add(functionMetaData1);
        metaDataList.add(functionMetaData2);
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(metaDataList);

        Response response = listDefaultFunctions();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(new Gson().toJson(functions), response.getEntity());
    }

    @Test
    public void testOnlyGetSources() throws Exception {
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
        doReturn(ComponentImpl.ComponentType.SOURCE).when(this.resource).calculateSubjectType(f1);
        doReturn(ComponentImpl.ComponentType.FUNCTION).when(this.resource).calculateSubjectType(f2);
        doReturn(ComponentImpl.ComponentType.SINK).when(this.resource).calculateSubjectType(f3);

        Response response = listDefaultFunctions();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(new Gson().toJson(functions), response.getEntity());
    }

    @Test
    public void testDownloadFunctionHttpUrl() throws Exception {
        String jarHttpUrl = "http://central.maven.org/maven2/org/apache/pulsar/pulsar-common/1.22.0-incubating/pulsar-common-1.22.0-incubating.jar";
        String testDir = FunctionApiV2ResourceTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionsImpl function = new FunctionsImpl(null);
        Response response = function.downloadFunction(jarHttpUrl);
        StreamingOutput streamOutput = (StreamingOutput) response.getEntity();
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
        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String testDir = FunctionApiV2ResourceTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionsImpl function = new FunctionsImpl(null);
        Response response = function.downloadFunction("file://"+fileLocation);
        StreamingOutput streamOutput = (StreamingOutput) response.getEntity();
        File pkgFile = new File(testDir, UUID.randomUUID().toString());
        OutputStream output = new FileOutputStream(pkgFile);
        streamOutput.write(output);
        Assert.assertTrue(pkgFile.exists());
        if (pkgFile.exists()) {
            pkgFile.delete();
        }
    }

    @Test
    public void testRegisterFunctionFileUrlWithValidSinkClass() throws IOException {
        Configurator.setRootLevel(Level.DEBUG);

        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String filePackageUrl = "file://" + fileLocation;
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        RequestResult rr = new RequestResult().setSuccess(true).setMessage("function registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

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
        Response response = resource.registerFunction(tenant, namespace, function, null, null, filePackageUrl,
                null, new Gson().toJson(functionConfig), null);

        assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRegisterFunctionWithConflictingFields() throws IOException {
        Configurator.setRootLevel(Level.DEBUG);
        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String filePackageUrl = "file://" + fileLocation;
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

        RequestResult rr = new RequestResult().setSuccess(true).setMessage("function registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

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
        Response response = resource.registerFunction(actualTenant, actualNamespace, actualName, null, null, filePackageUrl,
                null, new Gson().toJson(functionConfig), null);

        assertEquals(Status.OK.getStatusCode(), response.getStatus());
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
