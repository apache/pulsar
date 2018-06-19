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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.Function.SubscriptionType;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.apache.pulsar.io.core.RecordContext;
import org.apache.pulsar.io.core.Sink;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

/**
 * Unit test of {@link FunctionApiV2Resource}.
 */
@PrepareForTest(Utils.class)
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*" })
public class FunctionApiV2ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final class TestFunction implements Function<String, String> {

        public String process(String input, Context context) throws Exception {
            return input;
        }
    }
    
    public static final class TestSink implements Sink<byte[]> {

        @Override
        public void close() throws Exception {
        }

        @Override
        public void open(Map config) throws Exception {
        }

        @Override
        public void write(RecordContext inputRecordContext, byte[] value) throws Exception {
        }
    }


    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";
    private static final String outputTopic = "test-output-topic";
    private static final String outputSerdeClassName = DefaultSerDe.class.getName();
    private static final String className = TestFunction.class.getName();
    private SubscriptionType subscriptionType = SubscriptionType.FAILOVER;
    private static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    static {
        topicsToSerDeClassName.put("persistent://sample/standalone/ns1/test_src", DefaultSerDe.class.getName());
    }
    private static final int parallelism = 1;

    private WorkerService mockedWorkerService;
    private FunctionMetaDataManager mockedManager;
    private Namespace mockedNamespace;
    private FunctionsImpl resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;

    @BeforeMethod
    public void setup() {
        this.mockedManager = mock(FunctionMetaDataManager.class);
        this.mockedInputStream = mock(InputStream.class);
        this.mockedNamespace = mock(Namespace.class);
        this.mockedFormData = mock(FormDataContentDisposition.class);
        when(mockedFormData.getFileName()).thenReturn("test");

        this.mockedWorkerService = mock(WorkerService.class);
        when(mockedWorkerService.getFunctionMetaDataManager()).thenReturn(mockedManager);
        when(mockedWorkerService.getDlogNamespace()).thenReturn(mockedNamespace);
        when(mockedWorkerService.isInitialized()).thenReturn(true);

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
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Tenant");
    }

    @Test
    public void testRegisterFunctionMissingNamespace() throws IOException {
        testRegisterFunctionMissingArguments(
            tenant,
            null,
            function,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Namespace");
    }

    @Test
    public void testRegisterFunctionMissingFunctionName() throws IOException {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Function Name");
    }

    @Test
    public void testRegisterFunctionMissingPackage() throws IOException {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            null,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Function Package");
    }

    @Test
    public void testRegisterFunctionMissingPackageDetails() throws IOException {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            null,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Function Package");
    }

    @Test
    public void testRegisterFunctionMissingClassName() throws IOException {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            null,
            parallelism,
                "ClassName");
    }

    @Test
    public void testRegisterFunctionMissingParallelism() throws IOException {
        testRegisterFunctionMissingArguments(
                tenant,
                namespace,
                function,
                mockedInputStream,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                className,
                null,
                "parallelism");
    }

    private void testRegisterFunctionMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            FormDataContentDisposition details,
            String outputTopic,
            String outputSerdeClassName,
            String className,
            Integer parallelism,
            String missingFieldName) throws IOException {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        if (tenant != null) {
            functionDetailsBuilder.setTenant(tenant);
        }
        if (namespace != null) {
            functionDetailsBuilder.setNamespace(namespace);
        }
        if (function != null) {
            functionDetailsBuilder.setName(function);
        }
        SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
        if (outputTopic != null) {
            sinkSpecBuilder.setTopic(outputTopic);
        }
        if (outputSerdeClassName != null) {
            sinkSpecBuilder.setSerDeClassName(outputSerdeClassName);
        }
        functionDetailsBuilder.setSink(sinkSpecBuilder);
        if (className != null) {
            functionDetailsBuilder.setClassName(className);
        }
        if (parallelism != null) {
            functionDetailsBuilder.setParallelism(parallelism);
        }

        FunctionDetails functionDetails = functionDetailsBuilder.build();
        Response response = resource.registerFunction(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                null,
                org.apache.pulsar.functions.utils.Utils.printJson(functionDetails));

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        if (missingFieldName.equals("parallelism")) {
            Assert.assertEquals(new ErrorData("Parallelism needs to be set to a positive number").reason, ((ErrorData) response.getEntity()).reason);
        } else {
            Assert.assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
        }
    }

    private Response registerDefaultFunction() throws IOException {
        SinkSpec sinkSpec = SinkSpec.newBuilder()
                .setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setTenant(tenant).setNamespace(namespace).setName(function)
                .setSink(sinkSpec)
                .setClassName(className)
                .setParallelism(parallelism)
                .setSource(SourceSpec.newBuilder().setSubscriptionType(subscriptionType)
                        .putAllTopicsToSerDeClassName(topicsToSerDeClassName)).build();
        return resource.registerFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            null,
            org.apache.pulsar.functions.utils.Utils.printJson(functionDetails));
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
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

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
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Tenant");
    }

    @Test
    public void testUpdateFunctionMissingNamespace() throws IOException {
        testUpdateFunctionMissingArguments(
            tenant,
            null,
            function,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Namespace");
    }

    @Test
    public void testUpdateFunctionMissingFunctionName() throws IOException {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Function Name");
    }

    @Test
    public void testUpdateFunctionMissingPackage() throws IOException {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            null,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Function Package");
    }

    @Test
    public void testUpdateFunctionMissingPackageDetails() throws IOException {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            null,
            outputTopic,
                outputSerdeClassName,
            className,
            parallelism,
                "Function Package");
    }

    @Test
    public void testUpdateFunctionMissingClassName() throws IOException {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            outputTopic,
                outputSerdeClassName,
            null,
            parallelism,
                "ClassName");
    }
    @Test
    public void testUpdateFunctionMissingParallelism() throws IOException {
        testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                mockedInputStream,
                mockedFormData,
                outputTopic,
                outputSerdeClassName,
                className,
                null,
                "parallelism");
    }

    private void testUpdateFunctionMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            FormDataContentDisposition details,
            String outputTopic,
            String outputSerdeClassName,
            String className,
            Integer parallelism,
            String missingFieldName) throws IOException {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        if (tenant != null) {
            functionDetailsBuilder.setTenant(tenant);
        }
        if (namespace != null) {
            functionDetailsBuilder.setNamespace(namespace);
        }
        if (function != null) {
            functionDetailsBuilder.setName(function);
        }
        SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
        if (outputTopic != null) {
            sinkSpecBuilder.setTopic(outputTopic);
        }
        if (outputSerdeClassName != null) {
            sinkSpecBuilder.setSerDeClassName(outputSerdeClassName);
        }
        functionDetailsBuilder.setSink(sinkSpecBuilder);
        if (className != null) {
            functionDetailsBuilder.setClassName(className);
        }
        if (parallelism != null) {
            functionDetailsBuilder.setParallelism(parallelism);
        }

        FunctionDetails functionDetails = functionDetailsBuilder.build();
        Response response = resource.updateFunction(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            null,
            org.apache.pulsar.functions.utils.Utils.printJson(functionDetails));

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        if (missingFieldName.equals("parallelism")) {
            Assert.assertEquals(new ErrorData("Parallelism needs to be set to a positive number").reason, ((ErrorData) response.getEntity()).reason);
        } else {
            Assert.assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
        }
    }

    private Response updateDefaultFunction() throws IOException {
        SinkSpec sinkSpec = SinkSpec.newBuilder()
                .setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setTenant(tenant).setNamespace(namespace).setName(function)
                .setSink(sinkSpec)
                .setClassName(className)
                .setParallelism(parallelism)
                .setSource(SourceSpec.newBuilder().setSubscriptionType(subscriptionType)
                        .putAllTopicsToSerDeClassName(topicsToSerDeClassName)).build();
        return resource.updateFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            null,
            org.apache.pulsar.functions.utils.Utils.printJson(functionDetails));
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
        Utils.uploadToBookeeper(
            any(Namespace.class),
            any(InputStream.class),
            anyString());

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
        
        SinkSpec sinkSpec = SinkSpec.newBuilder()
                .setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setTenant(tenant).setNamespace(namespace).setName(function)
                .setSink(sinkSpec)
                .setClassName(className)
                .setParallelism(parallelism)
                .setSource(SourceSpec.newBuilder().setSubscriptionType(subscriptionType)
                        .putAllTopicsToSerDeClassName(topicsToSerDeClassName)).build();
        
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
            org.apache.pulsar.functions.utils.Utils.printJson(functionDetails));
        
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
            function);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
    }

    private Response deregisterDefaultFunction() {
        return resource.deregisterFunction(
            tenant,
            namespace,
            function);
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
            function);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
    }

    private Response getDefaultFunctionInfo() throws IOException {
        return resource.getFunctionInfo(
            tenant,
            namespace,
            function);
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
            org.apache.pulsar.functions.utils.Utils.printJson(functionDetails),
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
            namespace);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
    }

    private Response listDefaultFunctions() {
        return resource.listFunctions(
            tenant,
            namespace);
    }

    @Test
    public void testListFunctionsSuccess() throws Exception {
        List<String> functions = Lists.newArrayList("test-1", "test-2");
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functions);

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

        SinkSpec sinkSpec = SinkSpec.newBuilder().setClassName(TestSink.class.getName()).setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        FunctionDetails functionDetails = FunctionDetails
                .newBuilder().setTenant(tenant).setNamespace(namespace).setName(function).setSink(sinkSpec)
                .setClassName(className).setParallelism(parallelism).setSource(SourceSpec.newBuilder()
                        .setSubscriptionType(subscriptionType).putAllTopicsToSerDeClassName(topicsToSerDeClassName))
                .build();
        Response response = resource.registerFunction(tenant, namespace, function, null, null, filePackageUrl,
                org.apache.pulsar.functions.utils.Utils.printJson(functionDetails));

        assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRegisterFunctionFileUrlWithInValidSinkClass() throws IOException {
        Configurator.setRootLevel(Level.DEBUG);

        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String filePackageUrl = "file://" + fileLocation;
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        RequestResult rr = new RequestResult().setSuccess(true).setMessage("function registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        SinkSpec sinkSpec = SinkSpec.newBuilder().setClassName(className).setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        FunctionDetails functionDetails = FunctionDetails
                .newBuilder().setTenant(tenant).setNamespace(namespace).setName(function).setSink(sinkSpec)
                .setClassName(className).setParallelism(parallelism).setSource(SourceSpec.newBuilder()
                        .setSubscriptionType(subscriptionType).putAllTopicsToSerDeClassName(topicsToSerDeClassName))
                .build();
        Response response = resource.registerFunction(tenant, namespace, function, null, null, filePackageUrl,
                org.apache.pulsar.functions.utils.Utils.printJson(functionDetails));

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }
}
