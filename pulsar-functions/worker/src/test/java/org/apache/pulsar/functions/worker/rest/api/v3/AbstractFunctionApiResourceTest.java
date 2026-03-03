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

package org.apache.pulsar.functions.worker.rest.api.v3;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.ws.rs.core.Response;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.SubscriptionType;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public abstract class AbstractFunctionApiResourceTest extends AbstractFunctionsResourceTest {
    @Override
    protected void customizeWorkerConfig(WorkerConfig workerConfig, Method method) {
        if (method.getName().contains("Upload")) {
            workerConfig.setFunctionsWorkerEnablePackageManagement(false);
        }
    }

    @Test
    public void testListFunctionsSuccess() {
        mockInstanceUtils();
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
        when(mockedManager.listFunctions(eq(TENANT), eq(NAMESPACE))).thenReturn(metaDataList);

        List<String> functionList = listDefaultFunctions();
        assertEquals(functions, functionList);
    }

    @Test
    public void testOnlyGetSources() {
        List<String> functions = Lists.newArrayList("test-2");
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        FunctionMetaData f1 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder()
                        .setName("test-1")
                        .setComponentType(FunctionDetails.ComponentType.SOURCE)
                        .build()).build();
        functionMetaDataList.add(f1);
        FunctionMetaData f2 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder()
                        .setName("test-2")
                        .setComponentType(FunctionDetails.ComponentType.FUNCTION)
                        .build()).build();
        functionMetaDataList.add(f2);
        FunctionMetaData f3 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder()
                        .setName("test-3")
                        .setComponentType(FunctionDetails.ComponentType.SINK)
                        .build()).build();
        functionMetaDataList.add(f3);
        when(mockedManager.listFunctions(eq(TENANT), eq(NAMESPACE))).thenReturn(functionMetaDataList);

        List<String> functionList = listDefaultFunctions();
        assertEquals(functions, functionList);
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

    protected static final String FUNCTION = "test-function";
    protected static final String OUTPUT_TOPIC = "test-output-topic";
    protected static final String OUTPUT_SERDE_CLASS_NAME = TopicSchema.DEFAULT_SERDE;
    protected static final String CLASS_NAME = TestFunction.class.getName();
    protected SubscriptionType subscriptionType = SubscriptionType.FAILOVER;
    protected FunctionMetaData mockedFunctionMetadata;


    @Override
    protected void doSetup() {
        this.mockedFunctionMetadata =
                FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetadata);
    }

    @Override
    protected FunctionDetails.ComponentType getComponentType() {
        return FunctionDetails.ComponentType.FUNCTION;
    }


    protected abstract void registerFunction(String tenant, String namespace, String function, InputStream inputStream,
                              FormDataContentDisposition details, String functionPkgUrl, FunctionConfig functionConfig)
            throws IOException;
    protected abstract void updateFunction(String tenant,
                                  String namespace,
                                  String functionName,
                                  InputStream uploadedInputStream,
                                  FormDataContentDisposition fileDetail,
                                  String functionPkgUrl,
                                  FunctionConfig functionConfig,
                                  AuthenticationParameters authParams,
                                  UpdateOptionsImpl updateOptions) throws IOException;

    protected abstract File downloadFunction(String path, AuthenticationParameters authParams)
            throws IOException;

    protected abstract void testDeregisterFunctionMissingArguments(
            String tenant,
            String namespace,
            String function
    );

    protected abstract void deregisterDefaultFunction();

    protected abstract void testGetFunctionMissingArguments(
            String tenant,
            String namespace,
            String function
    ) throws IOException;

    protected abstract void testListFunctionsMissingArguments(
            String tenant,
            String namespace
    );

    protected abstract List<String> listDefaultFunctions();

    //
    // Register Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testRegisterFunctionMissingTenant() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    null,
                    NAMESPACE,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testRegisterFunctionMissingNamespace() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    null,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function name is not provided")
    public void testRegisterFunctionMissingFunctionName() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    null,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function package is not "
            + "provided")
    public void testRegisterFunctionMissingPackage() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    null,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "No input topic\\(s\\) "
            + "specified for the function")
    public void testRegisterFunctionMissingInputTopics() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    mockedInputStream,
                    null,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function Package is not "
            + "provided")
    public void testRegisterFunctionMissingPackageDetails() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    null,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class,
            expectedExceptionsMessageRegExp = "Function class name is not provided.")
    public void testRegisterFunctionMissingClassName() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    null,
                    PARALLELISM,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function class UnknownClass "
            + "must be in class path")
    public void testRegisterFunctionWrongClassName() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    "UnknownClass",
                    PARALLELISM,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function parallelism must be a"
            + " positive number")
    public void testRegisterFunctionWrongParallelism() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    -2,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class,
            expectedExceptionsMessageRegExp = "Output topic persistent://public/default/test_src is also being used "
                    + "as an input topic \\(topics must be one or the other\\)")
    public void testRegisterFunctionSameInputOutput() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    TOPICS_TO_SER_DE_CLASS_NAME.keySet().iterator().next(),
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Output topic " + FUNCTION
            + "-output-topic/test:" + " is invalid")
    public void testRegisterFunctionWrongOutputTopic() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    FUNCTION + "-output-topic/test:",
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Encountered error .*. when "
            + "getting Function package from .*")
    public void testRegisterFunctionHttpUrl() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    null,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    null,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    "http://localhost:1234/test");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function class .*. does not "
            + "implement the correct interface")
    public void testRegisterFunctionImplementWrongInterface() throws IOException {
        try {
            testRegisterFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    WrongFunction.class.getName(),
                    PARALLELISM,
                    null);
        } catch (RestException re) {
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
            String functionPkgUrl) throws IOException {
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

        registerFunction(tenant, namespace, function, inputStream, details, functionPkgUrl, functionConfig);

    }

    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = "Function config is not provided")
    public void testUpdateMissingFunctionConfig() throws IOException {
        when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);

        updateFunction(
                TENANT,
                NAMESPACE,
                FUNCTION,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null);
    }


    private void registerDefaultFunction() throws IOException {
        registerDefaultFunctionWithPackageUrl(null);
    }

    private void registerDefaultFunctionWithPackageUrl(String packageUrl) throws IOException {
        FunctionConfig functionConfig = createDefaultFunctionConfig();
        registerFunction(TENANT, NAMESPACE, FUNCTION, mockedInputStream, mockedFormData, packageUrl, functionConfig);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function already"
            + " exists")
    public void testRegisterExistedFunction() throws IOException {
        try {
            Configurator.setRootLevel(Level.DEBUG);
            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);
            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }


    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testRegisterFunctionUploadFailure() throws IOException {
        try {
            mockWorkerUtils(ctx -> {
                ctx.when(() -> {
                            WorkerUtils.uploadFileToBookkeeper(
                                    anyString(),
                                    any(File.class),
                                    any(Namespace.class));
                        }
                ).thenThrow(new IOException("upload failure"));
            });

            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(false);

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testRegisterFunctionSuccess() throws IOException {
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(false);

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(timeOut = 20000)
    public void testRegisterFunctionSuccessWithPackageName() throws IOException {
        registerDefaultFunctionWithPackageUrl("function://public/default/test@v1");
    }

    @Test(timeOut = 20000)
    public void testRegisterFunctionFailedWithWrongPackageName() throws PulsarAdminException, IOException {
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
    public void testRegisterFunctionNonExistingNamespace() throws IOException {
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
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(false);

            doThrow(new IllegalArgumentException("function failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            registerDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration "
            + "interrupted")
    public void testRegisterFunctionInterrupted() throws Exception {
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(false);

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
                    NAMESPACE,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
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
                    TENANT,
                    null,
                    FUNCTION,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
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
                    TENANT,
                    NAMESPACE,
                    null,
                    mockedInputStream,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    "Function name is not provided");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateFunctionMissingPackage() throws Exception {
        try {
            mockWorkerUtils();
            testUpdateFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    null,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateFunctionMissingInputTopic() throws Exception {
        try {
            mockWorkerUtils();

            testUpdateFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    null,
                    null,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    CLASS_NAME,
                    PARALLELISM,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateFunctionMissingClassName() throws Exception {
        try {
            mockWorkerUtils();

            testUpdateFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    null,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    null,
                    PARALLELISM,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateFunctionChangedParallelism() throws Exception {
        try {
            mockWorkerUtils();

            testUpdateFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    null,
                    TOPICS_TO_SER_DE_CLASS_NAME,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    null,
                    PARALLELISM + 1,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateFunctionChangedInputs() throws Exception {
        mockWorkerUtils();

        testUpdateFunctionMissingArguments(
                TENANT,
                NAMESPACE,
                FUNCTION,
                null,
                TOPICS_TO_SER_DE_CLASS_NAME,
                mockedFormData,
                "DifferentOutput",
                OUTPUT_SERDE_CLASS_NAME,
                null,
                PARALLELISM,
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Input Topics cannot be altered")
    public void testUpdateFunctionChangedOutput() throws Exception {
        try {
            mockWorkerUtils();

            Map<String, String> someOtherInput = new HashMap<>();
            someOtherInput.put("DifferentTopic", TopicSchema.DEFAULT_SERDE);
            testUpdateFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    FUNCTION,
                    null,
                    someOtherInput,
                    mockedFormData,
                    OUTPUT_TOPIC,
                    OUTPUT_SERDE_CLASS_NAME,
                    null,
                    PARALLELISM,
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

        updateFunction(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                null,
                functionConfig,
                null, null);

    }

    private void updateDefaultFunction() throws IOException {
        updateDefaultFunctionWithPackageUrl(null);
    }

    private void updateDefaultFunctionWithPackageUrl(String packageUrl) throws IOException {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(TENANT);
        functionConfig.setNamespace(NAMESPACE);
        functionConfig.setName(FUNCTION);
        functionConfig.setClassName(CLASS_NAME);
        functionConfig.setParallelism(PARALLELISM);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(TOPICS_TO_SER_DE_CLASS_NAME);
        functionConfig.setOutput(OUTPUT_TOPIC);
        functionConfig.setOutputSerdeClassName(OUTPUT_SERDE_CLASS_NAME);

        updateFunction(
                TENANT,
                NAMESPACE,
                FUNCTION,
                mockedInputStream,
                mockedFormData,
                packageUrl,
                functionConfig,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function doesn't"
            + " exist")
    public void testUpdateNotExistedFunction() throws IOException {
        try {
            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(false);
            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testUpdateFunctionUploadFailure() throws Exception {
        try {
            mockWorkerUtils(ctx -> {
                ctx.when(() -> {
                    WorkerUtils.uploadFileToBookkeeper(
                            anyString(),
                            any(File.class),
                            any(Namespace.class));

                }).thenThrow(new IOException("upload failure"));
            });

            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);

            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testUpdateFunctionSuccess() throws Exception {
        mockWorkerUtils();

        when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);

        updateDefaultFunction();
    }

    @Test
    public void testUpdateFunctionWithUrl() throws IOException {
        Configurator.setRootLevel(Level.DEBUG);

        String fileLocation = FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String filePackageUrl = "file://" + fileLocation;

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setOutput(OUTPUT_TOPIC);
        functionConfig.setOutputSerdeClassName(OUTPUT_SERDE_CLASS_NAME);
        functionConfig.setTenant(TENANT);
        functionConfig.setNamespace(NAMESPACE);
        functionConfig.setName(FUNCTION);
        functionConfig.setClassName(CLASS_NAME);
        functionConfig.setParallelism(PARALLELISM);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(TOPICS_TO_SER_DE_CLASS_NAME);

        when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);

        updateFunction(
                TENANT,
                NAMESPACE,
                FUNCTION,
                null,
                null,
                filePackageUrl,
                functionConfig,
                null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "function failed to register")
    public void testUpdateFunctionFailure() throws Exception {
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);

            doThrow(new IllegalArgumentException("function failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registeration "
            + "interrupted")
    public void testUpdateFunctionInterrupted() throws Exception {
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);

            doThrow(new IllegalStateException("Function registeration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            updateDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }


    @Test(timeOut = 20000)
    public void testUpdateFunctionSuccessWithPackageName() throws IOException {
        when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);
        updateDefaultFunctionWithPackageUrl("function://public/default/test@v1");
    }

    @Test(timeOut = 20000)
    public void testUpdateFunctionFailedWithWrongPackageName() throws PulsarAdminException, IOException {
        when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);
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
                    NAMESPACE,
                    FUNCTION
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
                    TENANT,
                    null,
                    FUNCTION
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
                    TENANT,
                    NAMESPACE,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function doesn't"
            + " exist")
    public void testDeregisterNotExistedFunction() {
        try {
            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(false);
            deregisterDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testDeregisterFunctionSuccess() {
        when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);

        deregisterDefaultFunction();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "function failed to deregister")
    public void testDeregisterFunctionFailure() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);

            doThrow(new IllegalArgumentException("function failed to deregister"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            deregisterDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function deregisteration "
            + "interrupted")
    public void testDeregisterFunctionInterrupted() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);

            doThrow(new IllegalStateException("Function deregisteration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            deregisterDefaultFunction();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Get Function Info
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testGetFunctionMissingTenant() throws IOException {
        try {
            testGetFunctionMissingArguments(
                    null,
                    NAMESPACE,
                    FUNCTION
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testGetFunctionMissingNamespace() throws IOException {
        try {
            testGetFunctionMissingArguments(
                    TENANT,
                    null,
                    FUNCTION
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function name is not provided")
    public void testGetFunctionMissingFunctionName() throws IOException {
        try {
            testGetFunctionMissingArguments(
                    TENANT,
                    NAMESPACE,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    //
    // List Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testListFunctionsMissingTenant() {
        try {
            testListFunctionsMissingArguments(
                    null,
                    NAMESPACE
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
                    TENANT,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testDownloadFunctionHttpUrl() throws Exception {
        String jarHttpUrl =
                "https://repo1.maven.org/maven2/org/apache/pulsar/pulsar-common/2.4.2/pulsar-common-2.4.2.jar";
        File pkgFile = downloadFunction(jarHttpUrl, null);
        pkgFile.delete();
    }

    @Test
    public void testDownloadFunctionFile() throws Exception {
        File file = getPulsarApiExamplesNar();
        File pkgFile = downloadFunction(file.toURI().toString(), null);
        Assert.assertTrue(pkgFile.exists());
        Assert.assertEquals(file.length(), pkgFile.length());
        pkgFile.delete();
    }

    @Test
    public void testDownloadFunctionBuiltinConnector() throws Exception {
        File file = getPulsarApiExamplesNar();

        WorkerConfig config = new WorkerConfig()
                .setUploadBuiltinSinksSources(false);
        when(mockedWorkerService.getWorkerConfig()).thenReturn(config);

        registerBuiltinConnector("cassandra", file);

        File pkgFile = downloadFunction("builtin://cassandra", null);
        Assert.assertTrue(pkgFile.exists());
        Assert.assertEquals(file.length(), pkgFile.length());
        pkgFile.delete();
    }

    @Test
    public void testDownloadFunctionBuiltinFunction() throws Exception {
        File file = getPulsarApiExamplesNar();

        WorkerConfig config = new WorkerConfig()
                .setUploadBuiltinSinksSources(false);
        when(mockedWorkerService.getWorkerConfig()).thenReturn(config);

        registerBuiltinFunction("exclamation", file);

        File pkgFile = downloadFunction("builtin://exclamation", null);
        Assert.assertTrue(pkgFile.exists());
        Assert.assertEquals(file.length(), pkgFile.length());
        pkgFile.delete();
    }

    @Test
    public void testRegisterFunctionFileUrlWithValidSinkClass() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);

        File file = getPulsarApiExamplesNar();
        String filePackageUrl = file.toURI().toString();
        when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(false);

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(TENANT);
        functionConfig.setNamespace(NAMESPACE);
        functionConfig.setName(FUNCTION);
        functionConfig.setClassName(CLASS_NAME);
        functionConfig.setParallelism(PARALLELISM);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(TOPICS_TO_SER_DE_CLASS_NAME);
        functionConfig.setOutput(OUTPUT_TOPIC);
        functionConfig.setOutputSerdeClassName(OUTPUT_SERDE_CLASS_NAME);
        registerFunction(TENANT, NAMESPACE, FUNCTION, null, null, filePackageUrl, functionConfig);

    }

    @Test
    public void testRegisterFunctionWithConflictingFields() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);
        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

        File file = getPulsarApiExamplesNar();
        String filePackageUrl = file.toURI().toString();
        when(mockedManager.containsFunction(eq(TENANT), eq(NAMESPACE), eq(FUNCTION))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(TENANT);
        functionConfig.setNamespace(NAMESPACE);
        functionConfig.setName(FUNCTION);
        functionConfig.setClassName(CLASS_NAME);
        functionConfig.setParallelism(PARALLELISM);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setCustomSerdeInputs(TOPICS_TO_SER_DE_CLASS_NAME);
        functionConfig.setOutput(OUTPUT_TOPIC);
        functionConfig.setOutputSerdeClassName(OUTPUT_SERDE_CLASS_NAME);
        registerFunction(actualTenant, actualNamespace, actualName, null, null, filePackageUrl, functionConfig);
    }

    public static FunctionConfig createDefaultFunctionConfig() {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(TENANT);
        functionConfig.setNamespace(NAMESPACE);
        functionConfig.setName(FUNCTION);
        functionConfig.setClassName(CLASS_NAME);
        functionConfig.setParallelism(PARALLELISM);
        functionConfig.setCustomSerdeInputs(TOPICS_TO_SER_DE_CLASS_NAME);
        functionConfig.setOutput(OUTPUT_TOPIC);
        functionConfig.setOutputSerdeClassName(OUTPUT_SERDE_CLASS_NAME);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        return functionConfig;
    }

    public static FunctionDetails createDefaultFunctionDetails() {
        FunctionConfig functionConfig = createDefaultFunctionConfig();
        return FunctionConfigUtils.convert(functionConfig);
    }
}
