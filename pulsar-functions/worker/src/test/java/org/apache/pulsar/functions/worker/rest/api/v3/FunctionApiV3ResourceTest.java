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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FunctionApiV3ResourceTest extends AbstractFunctionApiResourceTest {
    private FunctionsImpl resource;
    @Override
    protected void doSetup() {
        super.doSetup();
        this.resource = spy(new FunctionsImpl(() -> mockedWorkerService));
    }

    protected void registerFunction(String tenant, String namespace, String function, InputStream inputStream,
                                    FormDataContentDisposition details, String functionPkgUrl, FunctionConfig functionConfig) {
        resource.registerFunction(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                functionPkgUrl,
                functionConfig,
                null);
    }
    protected void updateFunction(String tenant,
                                  String namespace,
                                  String functionName,
                                  InputStream uploadedInputStream,
                                  FormDataContentDisposition fileDetail,
                                  String functionPkgUrl,
                                  FunctionConfig functionConfig,
                                  AuthenticationParameters authParams,
                                  UpdateOptionsImpl updateOptions) {
        resource.updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail, functionPkgUrl,
                functionConfig, authParams, updateOptions);
    }

    protected StreamingOutput downloadFunction(String tenant, String namespace, String componentName,
                                               AuthenticationParameters authParams) {
        return resource.downloadFunction(tenant, namespace, componentName, authParams);
    }

    protected File downloadFunction(final String path, final AuthenticationParameters authParams) throws IOException {
        StreamingOutput streamingOutput = resource.downloadFunction(path, authParams);
        File pkgFile = File.createTempFile("testpkg", "nar");
        try(OutputStream output = new FileOutputStream(pkgFile)) {
            streamingOutput.write(output);
        }
        return pkgFile;
    }

    protected void testDeregisterFunctionMissingArguments(
            String tenant,
            String namespace,
            String function
    ) {
        resource.deregisterFunction(
                tenant,
                namespace,
                function,
                null);
    }

    protected void deregisterDefaultFunction() {
        resource.deregisterFunction(
                tenant,
                namespace,
                function,
                null);
    }

    protected void testGetFunctionMissingArguments(
            String tenant,
            String namespace,
            String function
    ) {
        resource.getFunctionInfo(
                tenant,
                namespace,
                function, null
        );

    }

    protected FunctionConfig getDefaultFunctionInfo() {
        return resource.getFunctionInfo(
                tenant,
                namespace,
                function,
                null
        );
    }

    protected void testListFunctionsMissingArguments(
            String tenant,
            String namespace
    ) {
        resource.listFunctions(
                tenant,
                namespace, null
        );

    }

    protected List<String> listDefaultFunctions() {
        return resource.listFunctions(
                tenant,
                namespace, null
        );
    }

    @Test
    public void testDownloadFunctionBuiltinConnectorByName() throws Exception {
        File file = getPulsarApiExamplesNar();
        WorkerConfig config = new WorkerConfig()
                .setUploadBuiltinSinksSources(false);
        when(mockedWorkerService.getWorkerConfig()).thenReturn(config);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        Function.FunctionMetaData metaData = Function.FunctionMetaData.newBuilder()
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("builtin://cassandra"))
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setComponentType(Function.FunctionDetails.ComponentType.SINK))
                .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(function))).thenReturn(metaData);

        registerBuiltinConnector("cassandra", file);

        StreamingOutput streamOutput = downloadFunction(tenant, namespace, function,
                AuthenticationParameters.builder().build());
        File pkgFile = File.createTempFile("testpkg", "nar");
        OutputStream output = new FileOutputStream(pkgFile);
        streamOutput.write(output);
        Assert.assertTrue(pkgFile.exists());
        Assert.assertEquals(file.length(), pkgFile.length());
        pkgFile.delete();
    }

    @Test
    public void testDownloadFunctionBuiltinFunctionByName() throws Exception {
        File file = getPulsarApiExamplesNar();
        WorkerConfig config = new WorkerConfig()
                .setUploadBuiltinSinksSources(false);
        when(mockedWorkerService.getWorkerConfig()).thenReturn(config);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        Function.FunctionMetaData metaData = Function.FunctionMetaData.newBuilder()
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("builtin://exclamation"))
                .setFunctionDetails(
                        Function.FunctionDetails.newBuilder().setComponentType(Function.FunctionDetails.ComponentType.FUNCTION))
                .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(function))).thenReturn(metaData);

        registerBuiltinFunction("exclamation", file);

        StreamingOutput streamOutput = downloadFunction(tenant, namespace, function,
                AuthenticationParameters.builder().build());
        File pkgFile = File.createTempFile("testpkg", "nar");
        OutputStream output = new FileOutputStream(pkgFile);
        streamOutput.write(output);
        Assert.assertTrue(pkgFile.exists());
        Assert.assertEquals(file.length(), pkgFile.length());
        pkgFile.delete();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function doesn't"
            + " exist")
    public void testGetNotExistedFunction() throws IOException {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);
            getDefaultFunctionInfo();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testGetFunctionSuccess() throws IOException {
        mockInstanceUtils();
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        Function.SinkSpec sinkSpec = Function.SinkSpec.newBuilder()
                .setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        Function.FunctionDetails functionDetails = Function.FunctionDetails.newBuilder()
                .setClassName(className)
                .setSink(sinkSpec)
                .setAutoAck(true)
                .setName(function)
                .setNamespace(namespace)
                .setProcessingGuarantees(Function.ProcessingGuarantees.ATMOST_ONCE)
                .setTenant(tenant)
                .setParallelism(parallelism)
                .setSource(Function.SourceSpec.newBuilder().setSubscriptionType(subscriptionType)
                        .putAllTopicsToSerDeClassName(topicsToSerDeClassName)).build();
        Function.FunctionMetaData metaData = Function.FunctionMetaData.newBuilder()
                .setCreateTime(System.currentTimeMillis())
                .setFunctionDetails(functionDetails)
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("/path/to/package"))
                .setVersion(1234)
                .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(function))).thenReturn(metaData);

        FunctionConfig functionConfig = getDefaultFunctionInfo();
        assertEquals(
                FunctionConfigUtils.convertFromDetails(functionDetails),
                functionConfig);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function language runtime is "
            + "either not set or cannot be determined")
    public void testCreateFunctionWithoutSettingRuntime() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);

        File file = getPulsarApiExamplesNar();
        String filePackageUrl = file.toURI().toString();
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
        registerFunction(tenant, namespace, function, null, null, filePackageUrl, functionConfig);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function config is not provided")
    public void testMissingFunctionConfig() throws IOException {
        registerFunction(tenant, namespace, function, mockedInputStream, mockedFormData, null, null);
    }

    /*
        Externally managed runtime,
        uploadBuiltinSinksSources == false
        Make sure uploadFileToBookkeeper is not called
        */
    @Test
    public void testRegisterFunctionSuccessK8sNoUpload() throws Exception {
        mockedWorkerService.getWorkerConfig().setUploadBuiltinSinksSources(false);

        mockStatic(WorkerUtils.class, ctx -> {
            ctx.when(() -> WorkerUtils.uploadFileToBookkeeper(
                            anyString(),
                            any(File.class),
                            any(Namespace.class)))
                    .thenThrow(new RuntimeException("uploadFileToBookkeeper triggered"));

        });

        registerBuiltinFunction("exclamation", getPulsarApiExamplesNar());
        when(mockedRuntimeFactory.externallyManaged()).thenReturn(true);
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        FunctionConfig functionConfig = createDefaultFunctionConfig();
        functionConfig.setJar("builtin://exclamation");

        registerFunction(tenant, namespace, function, null, mockedFormData, null, functionConfig);
    }

    /*
        Externally managed runtime,
        uploadBuiltinSinksSources == true
        Make sure uploadFileToBookkeeper is called
        */
    @Test
    public void testRegisterFunctionSuccessK8sWithUpload() throws Exception {
        final String injectedErrMsg = "uploadFileToBookkeeper triggered";
        mockedWorkerService.getWorkerConfig().setUploadBuiltinSinksSources(true);

        mockStatic(WorkerUtils.class, ctx -> {
            ctx.when(() -> WorkerUtils.uploadFileToBookkeeper(
                            anyString(),
                            any(File.class),
                            any(Namespace.class)))
                    .thenThrow(new RuntimeException(injectedErrMsg));

        });

        registerBuiltinFunction("exclamation", getPulsarApiExamplesNar());
        when(mockedRuntimeFactory.externallyManaged()).thenReturn(true);
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);

        FunctionConfig functionConfig = createDefaultFunctionConfig();
        functionConfig.setJar("builtin://exclamation");

        try {
            registerFunction(tenant, namespace, function, null, mockedFormData, null, functionConfig);
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertEquals(e.getMessage(), injectedErrMsg);
        }
    }

}
