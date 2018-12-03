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
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.*;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.api.ComponentImpl;
import org.apache.pulsar.functions.worker.rest.api.SinkImpl;
import org.apache.pulsar.io.cassandra.CassandraStringSink;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.pulsar.functions.proto.Function.ProcessingGuarantees.ATLEAST_ONCE;
import static org.apache.pulsar.functions.source.TopicSchema.DEFAULT_SERDE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link SinkApiV2Resource}.
 */
@PrepareForTest({Utils.class, SinkConfigUtils.class, ConnectorUtils.class, org.apache.pulsar.functions.utils.Utils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.*" })
@Slf4j
public class SinkApiV2ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String sink = "test-sink";
    private static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    static {
        topicsToSerDeClassName.put("persistent://sample/standalone/ns1/test_src", DEFAULT_SERDE);
    }
    private static final String subscriptionName = "test-subscription";
    private static final String className = CassandraStringSink.class.getName();
    private static final int parallelism = 1;
    private static final String JAR_FILE_NAME = "pulsar-io-cassandra.nar";
    private static final String INVALID_JAR_FILE_NAME = "pulsar-io-twitter.nar";
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
    private SinkImpl resource;
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

        this.resource = spy(new SinkImpl(() -> mockedWorkerService));
        Mockito.doReturn(ComponentImpl.ComponentType.SINK).when(this.resource).calculateSubjectType(any());
    }

    //
    // Register Functions
    //

    @Test
    public void testRegisterSinkMissingTenant() throws IOException {
        testRegisterSinkMissingArguments(
            null,
            namespace,
                sink,
            mockedInputStream,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
                null,
                "Tenant is not provided");
    }

    @Test
    public void testRegisterSinkMissingNamespace() throws IOException {
        testRegisterSinkMissingArguments(
            tenant,
            null,
                sink,
            mockedInputStream,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
                null,
                "Namespace is not provided");
    }

    @Test
    public void testRegisterSinkMissingFunctionName() throws IOException {
        testRegisterSinkMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
                null,
                "Sink Name is not provided");
    }

    @Test
    public void testRegisterSinkMissingPackage() throws IOException {
        testRegisterSinkMissingArguments(
            tenant,
            namespace,
                sink,
            null,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
                null,
                "Sink Package is not provided");
    }

    @Test
    public void testRegisterSinkMissingPackageDetails() throws IOException {
        testRegisterSinkMissingArguments(
            tenant,
            namespace,
                sink,
            mockedInputStream,
            null,
            topicsToSerDeClassName,
            className,
            parallelism,
                null,
                "zip file is empty");
    }

    @Test
    public void testRegisterSinkInvalidJarNoSink() throws IOException {
        FileInputStream inputStream = new FileInputStream(INVALID_JAR_FILE_PATH);
        testRegisterSinkMissingArguments(
                tenant,
                namespace,
                sink,
                inputStream,
                null,
                topicsToSerDeClassName,
                className,
                parallelism,
                null,
                "Failed to extract sink class from archive");
    }

    @Test
    public void testRegisterSinkNoInput() throws IOException {
        testRegisterSinkMissingArguments(
                tenant,
                namespace,
                sink,
                mockedInputStream,
                mockedFormData,
                null,
                className,
                parallelism,
                null,
                "Must specify at least one topic of input via topicToSerdeClassName, topicsPattern, topicToSchemaType or inputSpecs");
    }

    @Test
    public void testRegisterSinkNegativeParallelism() throws IOException {
        testRegisterSinkMissingArguments(
                tenant,
                namespace,
                sink,
                mockedInputStream,
                mockedFormData,
                topicsToSerDeClassName,
                className,
                -2,
                null,
                "Sink parallelism should positive number");
    }

    @Test
    public void testRegisterSinkZeroParallelism() throws IOException {
        testRegisterSinkMissingArguments(
                tenant,
                namespace,
                sink,
                mockedInputStream,
                mockedFormData,
                topicsToSerDeClassName,
                className,
                0,
                null,
                "Sink parallelism should positive number");
    }

    @Test
    public void testRegisterSinkHttpUrl() throws IOException {
        testRegisterSinkMissingArguments(
                tenant,
                namespace,
                sink,
                null,
                null,
                topicsToSerDeClassName,
                className,
                parallelism,
                "http://localhost:1234/test",
                "Corrupt User PackageFile " + "http://localhost:1234/test with error Connection refused (Connection refused)");
    }

    private void testRegisterSinkMissingArguments(
            String tenant,
            String namespace,
            String sink,
            InputStream inputStream,
            FormDataContentDisposition details,
            Map<String, String> inputTopicMap,
            String className,
            Integer parallelism,
            String pkgUrl,
            String errorExpected) throws IOException {
        SinkConfig sinkConfig = new SinkConfig();
        if (tenant != null) {
            sinkConfig.setTenant(tenant);
        }
        if (namespace != null) {
            sinkConfig.setNamespace(namespace);
        }
        if (sink != null) {
            sinkConfig.setName(sink);
        }
        if (inputTopicMap != null) {
            sinkConfig.setTopicToSerdeClassName(inputTopicMap);
        }
        if (className != null) {
            sinkConfig.setClassName(className);
        }
        if (parallelism != null) {
            sinkConfig.setParallelism(parallelism);
        }

        Response response = resource.registerFunction(
                tenant,
                namespace,
                sink,
                inputStream,
                details,
                pkgUrl,
                null,
                new Gson().toJson(sinkConfig),
                null);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        Assert.assertEquals(((ErrorData) response.getEntity()).reason, new ErrorData(errorExpected).reason);
    }

    private Response registerDefaultSink() throws IOException {
        SinkConfig sinkConfig = createDefaultSinkConfig();
        return resource.registerFunction(
            tenant,
            namespace,
                sink,
                new FileInputStream(JAR_FILE_PATH),
            mockedFormData,
            null,
            null,
            new Gson().toJson(sinkConfig),
                null);
    }

    @Test
    public void testRegisterExistedSink() throws IOException {
        Configurator.setRootLevel(Level.DEBUG);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        Response response = registerDefaultSink();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Sink " + sink + " already exists").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testRegisterSinkUploadFailure() throws Exception {
        mockStatic(Utils.class);
        doThrow(new IOException("upload failure")).when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
            any(File.class),
            any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        Response response = registerDefaultSink();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("upload failure").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testRegisterSinkSuccess() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = registerDefaultSink();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRegisterSinkConflictingFields() throws Exception {
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

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sink);
        sinkConfig.setClassName(className);
        sinkConfig.setParallelism(parallelism);
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);
        Response response = resource.registerFunction(
                actualTenant,
                actualNamespace,
                actualName,
                new FileInputStream(JAR_FILE_PATH),
                mockedFormData,
                null,
                null,
                new Gson().toJson(sinkConfig),
                null);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testRegisterSinkFailure() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("source failed to register");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = registerDefaultSink();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(rr.getMessage()).reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testRegisterSinkInterrupted() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function registeration interrupted"));
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = registerDefaultSink();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function registeration interrupted").reason, ((ErrorData) response.getEntity()).reason);
    }

    //
    // Update Functions
    //

    @Test
    public void testUpdateSinkMissingTenant() throws IOException {
        testUpdateSinkMissingArguments(
            null,
            namespace,
                sink,
            mockedInputStream,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
                "Tenant is not provided");
    }

    @Test
    public void testUpdateSinkMissingNamespace() throws IOException {
        testUpdateSinkMissingArguments(
            tenant,
            null,
                sink,
            mockedInputStream,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
                "Namespace is not provided");
    }

    @Test
    public void testUpdateSinkMissingFunctionName() throws IOException {
        testUpdateSinkMissingArguments(
            tenant,
            namespace,
            null,
            mockedInputStream,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
                "Sink Name is not provided");
    }

    @Test
    public void testUpdateSinkMissingPackage() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSinkMissingArguments(
            tenant,
            namespace,
                sink,
            null,
            mockedFormData,
            topicsToSerDeClassName,
            className,
            parallelism,
                "Update contains no change");
    }

    @Test
    public void testUpdateSinkMissingInputs() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSinkMissingArguments(
                tenant,
                namespace,
                sink,
                null,
                mockedFormData,
                null,
                className,
                parallelism,
                "Update contains no change");
    }

    @Test
    public void testUpdateSinkDifferentInputs() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        Map<String, String> inputTopics = new HashMap<>();
        inputTopics.put("DifferntTopic", DEFAULT_SERDE);
        testUpdateSinkMissingArguments(
                tenant,
                namespace,
                sink,
                null,
                mockedFormData,
                inputTopics,
                className,
                parallelism,
                "Input Topics cannot be altered");
    }

    @Test
    public void testUpdateSinkDifferentParallelism() throws IOException {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSinkMissingArguments(
                tenant,
                namespace,
                sink,
                null,
                mockedFormData,
                topicsToSerDeClassName,
                className,
                parallelism + 1,
                null);
    }

    private void testUpdateSinkMissingArguments(
            String tenant,
            String namespace,
            String sink,
            InputStream inputStream,
            FormDataContentDisposition details,
            Map<String, String> inputTopicsMap,
            String className,
            Integer parallelism,
            String expectedError) throws IOException {
        mockStatic(ConnectorUtils.class);
        doReturn(CassandraStringSink.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSinkClass(any(NarClassLoader.class));

        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSinkType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));

        doReturn(ATLEAST_ONCE).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);


        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        SinkConfig sinkConfig = new SinkConfig();
        if (tenant != null) {
            sinkConfig.setTenant(tenant);
        }
        if (namespace != null) {
            sinkConfig.setNamespace(namespace);
        }
        if (sink != null) {
            sinkConfig.setName(sink);
        }
        if (inputTopicsMap != null) {
            sinkConfig.setTopicToSerdeClassName(inputTopicsMap);
        }
        if (className != null) {
            sinkConfig.setClassName(className);
        }
        if (parallelism != null) {
            sinkConfig.setParallelism(parallelism);
        }

        if (expectedError == null) {
            RequestResult rr = new RequestResult()
                    .setSuccess(true)
                    .setMessage("source registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
        }

        Response response = resource.updateFunction(
            tenant,
            namespace,
            sink,
            inputStream,
            details,
            null,
            null,
            new Gson().toJson(sinkConfig),
                null);

        if (expectedError == null) {
            assertEquals(Status.OK.getStatusCode(), response.getStatus());
        } else {
            assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
            Assert.assertEquals(((ErrorData) response.getEntity()).reason, new ErrorData(expectedError).reason);
        }
    }

    private Response updateDefaultSink() throws IOException {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sink);
        sinkConfig.setClassName(className);
        sinkConfig.setParallelism(parallelism);
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);

        mockStatic(ConnectorUtils.class);
        doReturn(CassandraStringSink.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSinkClass(any(NarClassLoader.class));

        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSinkType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));

        doReturn(ATLEAST_ONCE).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);


        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        return resource.updateFunction(
            tenant,
            namespace,
                sink,
                new FileInputStream(JAR_FILE_PATH),
            mockedFormData,
            null,
            null,
            new Gson().toJson(sinkConfig),
                null);
    }

    @Test
    public void testUpdateNotExistedSink() throws IOException {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        Response response = updateDefaultSink();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Sink " + sink + " doesn't exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testUpdateSinkUploadFailure() throws Exception {
        mockStatic(Utils.class);
        doThrow(new IOException("upload failure")).when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        Response response = updateDefaultSink();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("upload failure").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testUpdateSinkSuccess() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = updateDefaultSink();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testUpdateSinkWithUrl() throws IOException {
        Configurator.setRootLevel(Level.DEBUG);

        String filePackageUrl = "file://" + JAR_FILE_PATH;

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sink);
        sinkConfig.setClassName(className);
        sinkConfig.setParallelism(parallelism);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
        mockStatic(ConnectorUtils.class);
        doReturn(CassandraStringSink.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSinkClass(any(NarClassLoader.class));

        mockStatic(org.apache.pulsar.functions.utils.Utils.class);
        doReturn(String.class).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.getSinkType(anyString(), any(NarClassLoader.class));

        doReturn(mock(NarClassLoader.class)).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.extractNarClassLoader(any(Path.class), anyString(), any(File.class));

        doReturn(ATLEAST_ONCE).when(org.apache.pulsar.functions.utils.Utils.class);
        org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);


        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = resource.updateFunction(
            tenant,
            namespace,
                sink,
            null,
            null,
            filePackageUrl,
            null,
            new Gson().toJson(sinkConfig),
                null);

        assertEquals(Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void testUpdateSinkFailure() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("source failed to register");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = updateDefaultSink();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(rr.getMessage()).reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testUpdateSinkInterrupted() throws Exception {
        mockStatic(Utils.class);
        doNothing().when(Utils.class);
        Utils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function registeration interrupted"));
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        Response response = updateDefaultSink();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function registeration interrupted").reason, ((ErrorData) response.getEntity()).reason);
    }

    //
    // deregister source
    //

    @Test
    public void testDeregisterSinkMissingTenant() throws Exception {
        testDeregisterSinkMissingArguments(
            null,
            namespace,
                sink,
            "Tenant");
    }

    @Test
    public void testDeregisterSinkMissingNamespace() throws Exception {
        testDeregisterSinkMissingArguments(
            tenant,
            null,
                sink,
            "Namespace");
    }

    @Test
    public void testDeregisterSinkMissingFunctionName() throws Exception {
        testDeregisterSinkMissingArguments(
            tenant,
            namespace,
            null,
            "Sink Name");
    }

    private void testDeregisterSinkMissingArguments(
        String tenant,
        String namespace,
        String sink,
        String missingFieldName
    ) {
        Response response = resource.deregisterFunction(
            tenant,
            namespace,
            sink,
                null);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
    }

    private Response deregisterDefaultSink() {
        return resource.deregisterFunction(
            tenant,
            namespace,
                sink,
                null);
    }

    @Test
    public void testDeregisterNotExistedSink() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        Response response = deregisterDefaultSink();
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Sink " + sink + " doesn't exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testDeregisterSinkSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(true)
            .setMessage("source deregistered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(requestResult);

        Response response = deregisterDefaultSink();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(rr.toJson(), response.getEntity());
    }

    @Test
    public void testDeregisterSinkFailure() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        RequestResult rr = new RequestResult()
            .setSuccess(false)
            .setMessage("source failed to deregister");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(requestResult);

        Response response = deregisterDefaultSink();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(rr.getMessage()).reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testDeregisterSinkInterrupted() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
            new IOException("Function deregisteration interrupted"));
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(requestResult);

        Response response = deregisterDefaultSink();
        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Function deregisteration interrupted").reason, ((ErrorData) response.getEntity()).reason);
    }

    //
    // Get Sink Info
    //

    @Test
    public void testGetSinkMissingTenant() throws Exception {
        testGetSinkMissingArguments(
            null,
            namespace,
                sink,
            "Tenant");
    }

    @Test
    public void testGetSinkMissingNamespace() throws Exception {
        testGetSinkMissingArguments(
            tenant,
            null,
                sink,
            "Namespace");
    }

    @Test
    public void testGetSinkMissingFunctionName() throws Exception {
        testGetSinkMissingArguments(
            tenant,
            namespace,
            null,
            "Sink Name");
    }

    private void testGetSinkMissingArguments(
        String tenant,
        String namespace,
        String sink,
        String missingFieldName
    ) throws IOException {
        Response response = resource.getFunctionInfo(
            tenant,
            namespace,
            sink
        );

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData(missingFieldName + " is not provided").reason, ((ErrorData) response.getEntity()).reason);
    }

    private Response getDefaultSinkInfo() throws IOException {
        return resource.getFunctionInfo(
            tenant,
            namespace,
                sink
        );
    }

    @Test
    public void testGetNotExistedSink() throws IOException {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        Response response = getDefaultSinkInfo();
        assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Sink " + sink + " doesn't exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testGetSinkSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        Function.SourceSpec sourceSpec = Function.SourceSpec.newBuilder()
                .setSubscriptionType(Function.SubscriptionType.SHARED)
                .setSubscriptionName(subscriptionName)
                .putInputSpecs("input", Function.ConsumerSpec.newBuilder()
                .setSerdeClassName(DEFAULT_SERDE)
                .setIsRegexPattern(false)
                .build()).build();
        Function.SinkSpec sinkSpec = Function.SinkSpec.newBuilder()
                .setBuiltin("jdbc")
                .build();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setClassName(IdentityFunction.class.getName())
                .setSink(sinkSpec)
                .setName(sink)
                .setNamespace(namespace)
                .setProcessingGuarantees(ATLEAST_ONCE)
                .setTenant(tenant)
                .setParallelism(parallelism)
                .setRuntime(FunctionDetails.Runtime.JAVA)
                .setSource(sourceSpec).build();
        FunctionMetaData metaData = FunctionMetaData.newBuilder()
            .setCreateTime(System.currentTimeMillis())
            .setFunctionDetails(functionDetails)
            .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("/path/to/package"))
            .setVersion(1234)
            .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink))).thenReturn(metaData);

        Response response = getDefaultSinkInfo();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(
            new Gson().toJson(SinkConfigUtils.convertFromDetails(functionDetails)),
            response.getEntity());
    }

    //
    // List Sinks
    //

    @Test
    public void testListSinksMissingTenant() throws Exception {
        testListSinksMissingArguments(
            null,
            namespace,
            "Tenant");
    }

    @Test
    public void testListFunctionsMissingNamespace() throws Exception {
        testListSinksMissingArguments(
            tenant,
            null,
            "Namespace");
    }

    private void testListSinksMissingArguments(
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

    private Response listDefaultSinks() {
        return resource.listFunctions(
            tenant,
            namespace
        );
    }

    @Test
    public void testListSinksSuccess() throws Exception {
        List<String> functions = Lists.newArrayList("test-1", "test-2");
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()
        ).build());
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()
        ).build());
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functionMetaDataList);

        Response response = listDefaultSinks();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(new Gson().toJson(functions), response.getEntity());
    }

    @Test
    public void testOnlyGetSinks() throws Exception {
        List<String> functions = Lists.newArrayList("test-3");
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

        Response response = listDefaultSinks();
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertEquals(new Gson().toJson(functions), response.getEntity());
    }

    @Test
    public void testRegisterFunctionNonexistantNamespace() throws Exception {
        this.namespaceList.clear();
        Response response = registerDefaultSink();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Namespace does not exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    @Test
    public void testRegisterFunctionNonexistantTenant() throws Exception {
        when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
        Response response = registerDefaultSink();
        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(new ErrorData("Tenant does not exist").reason, ((ErrorData) response.getEntity()).reason);
    }

    private SinkConfig createDefaultSinkConfig() {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sink);
        sinkConfig.setClassName(className);
        sinkConfig.setParallelism(parallelism);
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);
        return sinkConfig;
    }

    private FunctionDetails createDefaultFunctionDetails() throws IOException {
        return SinkConfigUtils.convert(createDefaultSinkConfig(), null);
    }
}
