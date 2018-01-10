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
package org.apache.pulsar.functions.worker.rest.api.v1;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

import com.google.gson.Gson;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.RequestHandler;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.fs.FunctionConfig.ProcessingGuarantees;
import org.apache.pulsar.functions.runtime.serde.Utf8StringSerDe;
import org.apache.pulsar.functions.fs.LimitsConfig;
import org.apache.pulsar.functions.worker.FunctionMetaData;
import org.apache.pulsar.functions.worker.FunctionRuntimeInfo;
import org.apache.pulsar.functions.worker.PackageLocationMetaData;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.RestUtils;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link ApiV1Resource}.
 */
@PrepareForTest(Utils.class)
@PowerMockIgnore("javax.management.*")
public class ApiV1ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final class TestFunction implements RequestHandler<String, String> {

        public String handleRequest(String input, Context context) throws Exception {
            return input;
        }
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";
    private static final String sinkTopic = "test-sink-topic";
    private static final String sourceTopic = "test-source-topic";
    private static final String inputSerdeClassName = Utf8StringSerDe.class.getName();
    private static final String outputSerdeClassName = Utf8StringSerDe.class.getName();
    private static final String className = TestFunction.class.getName();
    private static final LimitsConfig limitsConfig = new LimitsConfig()
        .setMaxTimeMs(1234)
        .setMaxCpuCores(2345)
        .setMaxMemoryMb(3456)
        .setMaxBufferedTuples(5678);

    private FunctionRuntimeManager mockedManager;
    private Namespace mockedNamespace;
    private ApiV1Resource resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;

    @BeforeMethod
    public void setup() {
        this.resource = spy(new ApiV1Resource());
        this.mockedManager = mock(FunctionRuntimeManager.class);
        this.mockedInputStream = mock(InputStream.class);
        this.mockedFormData = mock(FormDataContentDisposition.class);
        this.mockedNamespace = mock(Namespace.class);
        doReturn(mockedManager).when(resource).getWorkerFunctionStateManager();
        doReturn(mockedNamespace).when(resource).getDlogNamespace();

        // worker config
        WorkerConfig workerConfig = new WorkerConfig()
            .setWorkerId("test")
            .setWorkerPort(8080)
            .setLimitsConfig(limitsConfig)
            .setDownloadDirectory("/tmp/pulsar/functions")
            .setFunctionMetadataTopic("pulsar/functions")
            .setNumFunctionPackageReplicas(3)
            .setPulsarServiceUrl("pulsar://localhost:6650/")
            .setZookeeperServers("localhost:2181");
        doReturn(workerConfig).when(resource).getWorkerConfig();
    }

    //
    // Register Functions
    //

    @Test
    public void testRegisterFunctionMissingTenant() {
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
            "Tenant");
    }

    @Test
    public void testRegisterFunctionMissingNamespace() {
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
            "Namespace");
    }

    @Test
    public void testRegisterFunctionMissingFunctionName() {
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
            "Function Name");
    }

    @Test
    public void testRegisterFunctionMissingPackage() {
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
            "Function Package");
    }

    @Test
    public void testRegisterFunctionMissingPackageDetails() {
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
            "Function Package");
    }

    @Test
    public void testRegisterFunctionMissingSourceTopic() {
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
            "SourceTopic");
    }

    @Test
    public void testRegisterFunctionMissingInputSerde() {
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
            "InputSerdeClassName");
    }

    @Test
    public void testRegisterFunctionMissingClassName() {
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
            "ClassName");
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
        String missingFieldName
    ) {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant).setNamespace(namespace).setName(function)
                .setSinkTopic(sinkTopic).setSourceTopic(sourceTopic)
                .setInputSerdeClassName(inputSerdeClassName).setOutputSerdeClassName(outputSerdeClassName)
                .setClassName(className);
        Response response = resource.registerFunction(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            new Gson().toJson(functionConfig));

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        Assert.assertEquals(RestUtils.createMessage(missingFieldName + " is not provided"), response.getEntity());
    }

    private Response registerDefaultFunction() {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant).setNamespace(namespace).setName(function)
                .setSinkTopic(sinkTopic).setSourceTopic(sourceTopic)
                .setInputSerdeClassName(inputSerdeClassName).setOutputSerdeClassName(outputSerdeClassName)
                .setClassName(className);
        return resource.registerFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData, new Gson().toJson(functionConfig));
    }

    @Test
    public void testRegisterExistedFunction() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        Response response = registerDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(RestUtils.createMessage("Function " + function + " already exist"), response.getEntity());
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
        assertEquals(RestUtils.createMessage("upload failure"), response.getEntity());
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
        assertEquals(rr.toJson(), response.getEntity());
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
        assertEquals(rr.toJson(), response.getEntity());
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
        assertEquals(RestUtils.createMessage("java.io.IOException: Function registeration interrupted"), response.getEntity());
    }

    //
    // Update Functions
    //

    @Test
    public void testUpdateFunctionMissingTenant() {
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
            "Tenant");
    }

    @Test
    public void testUpdateFunctionMissingNamespace() {
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
            "Namespace");
    }

    @Test
    public void testUpdateFunctionMissingFunctionName() {
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
            "Function Name");
    }

    @Test
    public void testUpdateFunctionMissingPackage() {
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
            "Function Package");
    }

    @Test
    public void testUpdateFunctionMissingPackageDetails() {
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
            "Function Package");
    }

    @Test
    public void testUpdateFunctionMissingSourceTopic() {
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
            "SourceTopic");
    }

    @Test
    public void testUpdateFunctionMissingInputSerde() {
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
            "InputSerdeClassName");
    }

    @Test
    public void testUpdateFunctionMissingClassName() {
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
            "ClassName");
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
        String missingFieldName
    ) {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant).setNamespace(namespace).setName(function)
                .setSinkTopic(sinkTopic).setSourceTopic(sourceTopic)
                .setInputSerdeClassName(inputSerdeClassName).setOutputSerdeClassName(outputSerdeClassName)
                .setClassName(className);
        Response response = resource.updateFunction(
            tenant,
            namespace,
            function,
            inputStream,
            details,
            new Gson().toJson(functionConfig));

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(RestUtils.createMessage(missingFieldName + " is not provided"), response.getEntity());
    }

    private Response updateDefaultFunction() {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant).setNamespace(namespace).setName(function)
                .setSinkTopic(sinkTopic).setSourceTopic(sourceTopic)
                .setInputSerdeClassName(inputSerdeClassName).setOutputSerdeClassName(outputSerdeClassName)
                .setClassName(className);
        return resource.updateFunction(
            tenant,
            namespace,
            function,
            mockedInputStream,
            mockedFormData, new Gson().toJson(functionConfig));
    }

    @Test
    public void testUpdateNotExistedFunction() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = updateDefaultFunction();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(RestUtils.createMessage("Function " + function + " doesn't exist"), response.getEntity());
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
        assertEquals(RestUtils.createMessage("upload failure"), response.getEntity());
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
        assertEquals(rr.toJson(), response.getEntity());
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
        assertEquals(rr.toJson(), response.getEntity());
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
        assertEquals(RestUtils.createMessage("java.io.IOException: Function registeration interrupted"), response.getEntity());
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
        assertEquals(RestUtils.createMessage(missingFieldName + " is not provided"), response.getEntity());
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
        assertEquals(RestUtils.createMessage("Function " + function + " doesn't exist"), response.getEntity());
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
        assertEquals(rr.toJson(), response.getEntity());
    }

    @Test
    public void testDeregisterFunctionInterrupted() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function deregisteration interrupted"));
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(requestResult);

        Response response = deregisterDefaultFunction();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(RestUtils.createMessage("java.io.IOException: Function deregisteration interrupted"), response.getEntity());
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
    ) {
        Response response = resource.getFunctionInfo(
            tenant,
            namespace,
            function);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(RestUtils.createMessage(missingFieldName + " is not provided"), response.getEntity());
    }

    private Response getDefaultFunctionInfo() {
        return resource.getFunctionInfo(
            tenant,
            namespace,
            function);
    }

    @Test
    public void testGetNotExistedFunction() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        Response response = getDefaultFunctionInfo();
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
        assertEquals(RestUtils.createMessage("Function " + function + " doesn't exist"), response.getEntity());
    }

    @Test
    public void testGetFunctionSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        FunctionConfig config = new FunctionConfig()
            .setClassName(className)
            .setInputSerdeClassName(inputSerdeClassName)
            .setOutputSerdeClassName(outputSerdeClassName)
            .setName(function)
            .setNamespace(namespace)
            .setProcessingGuarantees(ProcessingGuarantees.ATMOST_ONCE)
            .setSinkTopic(sinkTopic)
            .setSourceTopic(sourceTopic)
            .setTenant(tenant);
        FunctionMetaData metaData = new FunctionMetaData()
            .setCreateTime(System.currentTimeMillis())
            .setFunctionConfig(config)
            .setPackageLocation(new PackageLocationMetaData().setPackagePath("/path/to/package"))
            .setRuntime("test")
            .setVersion(1234)
            .setWorkerId("test-worker");
        FunctionRuntimeInfo functionRuntimeInfo = new FunctionRuntimeInfo()
                .setFunctionMetaData(metaData);
        when(mockedManager.getFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(functionRuntimeInfo);

        Response response = getDefaultFunctionInfo();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(new Gson().toJson(config), response.getEntity());
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
        assertEquals(RestUtils.createMessage(missingFieldName + " is not provided"), response.getEntity());
    }

    private Response listDefaultFunctions() {
        return resource.listFunctions(
            tenant,
            namespace);
    }

    @Test
    public void testListFunctionsSuccess() throws Exception {
        List<String> functions = Lists.newArrayList("test-1", "test-2");
        when(mockedManager.listFunction(eq(tenant), eq(namespace))).thenReturn(functions);

        Response response = listDefaultFunctions();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(new Gson().toJson(functions), response.getEntity());
    }
}
