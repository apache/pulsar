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

import com.google.gson.Gson;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
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
public class FunctionApiV2ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final class TestFunction implements PulsarFunction<String, String> {

        public String process(String input, Context context) throws Exception {
            return input;
        }
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";
    private static final String sinkTopic = "test-sink-topic";
    private static final String sourceTopic = "test-source-topic";
    private static final String inputSerdeClassName = DefaultSerDe.class.getName();
    private static final String outputSerdeClassName = DefaultSerDe.class.getName();
    private static final String className = TestFunction.class.getName();
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
        this.mockedFormData = mock(FormDataContentDisposition.class);
        this.mockedNamespace = mock(Namespace.class);

        this.mockedWorkerService = mock(WorkerService.class);
        when(mockedWorkerService.getFunctionMetaDataManager()).thenReturn(mockedManager);
        when(mockedWorkerService.getDlogNamespace()).thenReturn(mockedNamespace);

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
    public void testRegisterFunctionMissingTenant() throws InvalidProtocolBufferException {
        testRegisterFunctionMissingArguments(
            null,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Tenant");
    }

    @Test
    public void testRegisterFunctionMissingNamespace() throws InvalidProtocolBufferException {
        testRegisterFunctionMissingArguments(
            tenant,
            null,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Namespace");
    }

    @Test
    public void testRegisterFunctionMissingFunctionName() throws InvalidProtocolBufferException {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Function Name");
    }

    @Test
    public void testRegisterFunctionMissingPackage() throws InvalidProtocolBufferException {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            null,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Function Package");
    }

    @Test
    public void testRegisterFunctionMissingPackageDetails() throws InvalidProtocolBufferException {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            null,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Function Package");
    }

    @Test
    public void testRegisterFunctionMissingSourceTopic() throws InvalidProtocolBufferException {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            null,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Input");
    }

    @Test
    public void testRegisterFunctionMissingInputSerde() throws InvalidProtocolBufferException {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            null,
            outputSerdeClassName,
            className,
            parallelism,
            "Input");
    }

    @Test
    public void testRegisterFunctionMissingClassName() throws InvalidProtocolBufferException {
        testRegisterFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            null,
            parallelism,
            "ClassName");
    }

    @Test
    public void testRegisterFunctionMissingParallelism() throws InvalidProtocolBufferException {
        testRegisterFunctionMissingArguments(
                tenant,
                namespace,
                function,
                mockedInputStream,
                mockedFormData,
                sinkTopic,
                sourceTopic,
                inputSerdeClassName,
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
        String sinkTopic,
        String sourceTopic,
        String inputSerdeClassName,
        String outputSerdeClassName,
        String className,
        Integer parallelism,
        String missingFieldName
    ) throws InvalidProtocolBufferException {
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        if (tenant != null) {
            functionConfigBuilder.setTenant(tenant);
        }
        if (namespace != null) {
            functionConfigBuilder.setNamespace(namespace);
        }
        if (function != null) {
            functionConfigBuilder.setName(function);
        }
        if (sinkTopic != null) {
            functionConfigBuilder.setOutput(sinkTopic);
        }
        if (sourceTopic != null && inputSerdeClassName != null) {
            functionConfigBuilder.putCustomSerdeInputs(sourceTopic, inputSerdeClassName);
        }
        if (outputSerdeClassName != null) {
            functionConfigBuilder.setOutputSerdeClassName(outputSerdeClassName);
        }
        if (className != null) {
            functionConfigBuilder.setClassName(className);
        }
        if (parallelism != null) {
            functionConfigBuilder.setParallelism(parallelism);
        }

        FunctionConfig functionConfig = functionConfigBuilder.build();
        Response response = resource.registerFunction(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                JsonFormat.printer().print(functionConfig));

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        if (missingFieldName.equals("parallelism")) {
            Assert.assertEquals(new ErrorData("Parallelism needs to be set to a positive number").reason, ((ErrorData) response.getEntity()).reason);
        } else {
            Assert.assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
        }
    }

    private Response registerDefaultFunction() throws InvalidProtocolBufferException {
        FunctionConfig functionConfig = FunctionConfig.newBuilder()
                .setTenant(tenant).setNamespace(namespace).setName(function)
                .setOutput(sinkTopic).putCustomSerdeInputs(sourceTopic, inputSerdeClassName)
                .setOutputSerdeClassName(outputSerdeClassName)
                .setClassName(className)
                .setParallelism(parallelism).build();
        return resource.registerFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData, JsonFormat.printer().print(functionConfig));
    }

    @Test
    public void testRegisterExistedFunction() throws InvalidProtocolBufferException {
        Configurator.setRootLevel(Level.DEBUG);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        Response response = registerDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function " + function + " already exist").reason, ((ErrorData) response.getEntity()).reason);
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
    public void testUpdateFunctionMissingTenant() throws InvalidProtocolBufferException {
        testUpdateFunctionMissingArguments(
            null,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Tenant");
    }

    @Test
    public void testUpdateFunctionMissingNamespace() throws InvalidProtocolBufferException {
        testUpdateFunctionMissingArguments(
            tenant,
            null,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Namespace");
    }

    @Test
    public void testUpdateFunctionMissingFunctionName() throws InvalidProtocolBufferException {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Function Name");
    }

    @Test
    public void testUpdateFunctionMissingPackage() throws InvalidProtocolBufferException {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            null,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Function Package");
    }

    @Test
    public void testUpdateFunctionMissingPackageDetails() throws InvalidProtocolBufferException {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            null,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Function Package");
    }

    @Test
    public void testUpdateFunctionMissingSourceTopic() throws InvalidProtocolBufferException {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            null,
            inputSerdeClassName,
            outputSerdeClassName,
            className,
            parallelism,
            "Input");
    }

    @Test
    public void testUpdateFunctionMissingInputSerde() throws InvalidProtocolBufferException {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            null,
            outputSerdeClassName,
            className,
            parallelism,
            "Input");
    }

    @Test
    public void testUpdateFunctionMissingClassName() throws InvalidProtocolBufferException {
        testUpdateFunctionMissingArguments(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData,
            sinkTopic,
            sourceTopic,
            inputSerdeClassName,
            outputSerdeClassName,
            null,
            parallelism,
            "ClassName");
    }
    @Test
    public void testUpdateFunctionMissingParallelism() throws InvalidProtocolBufferException {
        testUpdateFunctionMissingArguments(
                tenant,
                namespace,
                function,
                mockedInputStream,
                mockedFormData,
                sinkTopic,
                sourceTopic,
                inputSerdeClassName,
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
        String sinkTopic,
        String sourceTopic,
        String inputSerdeClassName,
        String outputSerdeClassName,
        String className,
        Integer parallelism,
        String missingFieldName
    ) throws InvalidProtocolBufferException {
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        if (tenant != null) {
            functionConfigBuilder.setTenant(tenant);
        }
        if (namespace != null) {
            functionConfigBuilder.setNamespace(namespace);
        }
        if (function != null) {
            functionConfigBuilder.setName(function);
        }
        if (sinkTopic != null) {
            functionConfigBuilder.setOutput(sinkTopic);
        }
        if (sourceTopic != null && inputSerdeClassName != null) {
            functionConfigBuilder.putCustomSerdeInputs(sourceTopic, inputSerdeClassName);
        }
        if (outputSerdeClassName != null) {
            functionConfigBuilder.setOutputSerdeClassName(outputSerdeClassName);
        }
        if (className != null) {
            functionConfigBuilder.setClassName(className);
        }
        if (parallelism != null) {
            functionConfigBuilder.setParallelism(parallelism);
        }

        FunctionConfig functionConfig = functionConfigBuilder.build();
        Response response = resource.updateFunction(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            JsonFormat.printer().print(functionConfig));

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        if (missingFieldName.equals("parallelism")) {
            Assert.assertEquals(new ErrorData("Parallelism needs to be set to a positive number").reason, ((ErrorData) response.getEntity()).reason);
        } else {
            Assert.assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
        }
    }

    private Response updateDefaultFunction() throws InvalidProtocolBufferException {
        FunctionConfig functionConfig = FunctionConfig.newBuilder()
                .setTenant(tenant).setNamespace(namespace).setName(function)
                .setOutput(sinkTopic).putCustomSerdeInputs(sourceTopic, inputSerdeClassName)
                .setOutputSerdeClassName(outputSerdeClassName)
                .setClassName(className)
                .setParallelism(parallelism).build();
        return resource.updateFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData, JsonFormat.printer().print(functionConfig));
    }

    @Test
    public void testUpdateNotExistedFunction() throws InvalidProtocolBufferException {
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
    ) throws InvalidProtocolBufferException {
        Response response = resource.getFunctionInfo(
            tenant,
            namespace,
            function);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
    }

    private Response getDefaultFunctionInfo() throws InvalidProtocolBufferException {
        return resource.getFunctionInfo(
            tenant,
            namespace,
            function);
    }

    @Test
    public void testGetNotExistedFunction() throws InvalidProtocolBufferException {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = getDefaultFunctionInfo();
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function " + function + " doesn't exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testGetFunctionSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        FunctionConfig functionConfig = FunctionConfig.newBuilder()
                .setClassName(className)
                .putCustomSerdeInputs(sourceTopic, inputSerdeClassName)
                .setOutputSerdeClassName(outputSerdeClassName)
                .setName(function)
                .setNamespace(namespace)
                .setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATMOST_ONCE)
                .setOutput(sinkTopic)
                .setTenant(tenant)
                .setParallelism(parallelism).build();
        FunctionMetaData metaData = FunctionMetaData.newBuilder()
            .setCreateTime(System.currentTimeMillis())
            .setFunctionConfig(functionConfig)
            .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath("/path/to/package"))
            .setVersion(1234)
            .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(function))).thenReturn(metaData);

        Response response = getDefaultFunctionInfo();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(JsonFormat.printer().print(functionConfig), response.getEntity());
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
}
