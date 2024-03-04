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

import static org.apache.pulsar.functions.proto.Function.ProcessingGuarantees.ATLEAST_ONCE;
import static org.apache.pulsar.functions.source.TopicSchema.DEFAULT_SERDE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.rest.api.SinksImpl;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test of {@link SinksApiV3Resource}.
 */
public class SinkApiV3ResourceTest extends AbstractFunctionsResourceTest {

    private static final String sink = "test-sink";

    private SinksImpl resource;

    @Override
    protected void doSetup() {
        this.resource = spy(new SinksImpl(() -> mockedWorkerService));
    }

    @Override
    protected Function.FunctionDetails.ComponentType getComponentType() {
        return Function.FunctionDetails.ComponentType.SINK;
    }

    @Override
    protected File getDefaultNarFile() {
        return getPulsarIOCassandraNar();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testRegisterSinkMissingTenant() {
        try {
            testRegisterSinkMissingArguments(
                    null,
                    namespace,
                    sink,
                    mockedInputStream,
                    mockedFormData,
                    topicsToSerDeClassName,
                    CASSANDRA_STRING_SINK,
                    parallelism,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testRegisterSinkMissingNamespace() {
        try {
            testRegisterSinkMissingArguments(
                    tenant,
                    null,
                    sink,
                    mockedInputStream,
                    mockedFormData,
                    topicsToSerDeClassName,
                    CASSANDRA_STRING_SINK,
                    parallelism,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink name is not provided")
    public void testRegisterSinkMissingSinkName() {
        try {
            testRegisterSinkMissingArguments(
                    tenant,
                    namespace,
                    null,
                    mockedInputStream,
                    mockedFormData,
                    topicsToSerDeClassName,
                    CASSANDRA_STRING_SINK,
                    parallelism,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink package is not provided")
    public void testRegisterSinkMissingPackage() {
        try {
            testRegisterSinkMissingArguments(
                    tenant,
                    namespace,
                    sink,
                    null,
                    mockedFormData,
                    topicsToSerDeClassName,
                    null,
                    parallelism,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink class UnknownClass not "
            + "found")
    public void testRegisterSinkWrongClassName() {
        mockInstanceUtils();
        try {
            testRegisterSinkMissingArguments(
                    tenant,
                    namespace,
                    sink,
                    mockedInputStream,
                    mockedFormData,
                    topicsToSerDeClassName,
                    "UnknownClass",
                    parallelism,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink package doesn't contain "
            + "the META-INF/services/pulsar-io.yaml file.")
    public void testRegisterSinkMissingPackageDetails() {
        mockInstanceUtils();
        try {
            testRegisterSinkMissingArguments(
                    tenant,
                    namespace,
                    sink,
                    mockedInputStream,
                    null,
                    topicsToSerDeClassName,
                    null,
                    parallelism,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Failed to extract sink class "
            + "from archive")
    public void testRegisterSinkInvalidJarNoSink() throws IOException {
        mockInstanceUtils();
        try {
            try (FileInputStream inputStream = new FileInputStream(getPulsarIOTwitterNar())) {
                testRegisterSinkMissingArguments(
                        tenant,
                        namespace,
                        sink,
                        inputStream,
                        mockedFormData,
                        topicsToSerDeClassName,
                        null,
                        parallelism,
                        null
                );
            }
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Must specify at least one "
            + "topic of input via topicToSerdeClassName, topicsPattern, topicToSchemaType or inputSpecs")
    public void testRegisterSinkNoInput() throws IOException {
        mockInstanceUtils();
        try {
            try (FileInputStream inputStream = new FileInputStream(getPulsarIOCassandraNar())) {
                testRegisterSinkMissingArguments(
                        tenant,
                        namespace,
                        sink,
                        inputStream,
                        mockedFormData,
                        null,
                        CASSANDRA_STRING_SINK,
                        parallelism,
                        null
                );
            }
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink parallelism must be a "
            + "positive number")
    public void testRegisterSinkNegativeParallelism() throws IOException {
        mockInstanceUtils();
        try {
            try (FileInputStream inputStream = new FileInputStream(getPulsarIOCassandraNar())) {
                testRegisterSinkMissingArguments(
                        tenant,
                        namespace,
                        sink,
                        inputStream,
                        mockedFormData,
                        topicsToSerDeClassName,
                        CASSANDRA_STRING_SINK,
                        -2,
                        null
                );
            }
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink parallelism must be a "
            + "positive number")
    public void testRegisterSinkZeroParallelism() throws IOException {
        mockInstanceUtils();
        try {
            try (FileInputStream inputStream = new FileInputStream(getPulsarIOCassandraNar())) {
                testRegisterSinkMissingArguments(
                        tenant,
                        namespace,
                        sink,
                        inputStream,
                        mockedFormData,
                        topicsToSerDeClassName,
                        CASSANDRA_STRING_SINK,
                        0,
                        null
                );
            }
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Encountered error .*. when "
            + "getting Sink package from .*")
    public void testRegisterSinkHttpUrl() {
        try {
            testRegisterSinkMissingArguments(
                    tenant,
                    namespace,
                    sink,
                    null,
                    null,
                    topicsToSerDeClassName,
                    CASSANDRA_STRING_SINK,
                    parallelism,
                    "http://localhost:1234/test"
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
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
            String pkgUrl) {
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

        resource.registerSink(
                tenant,
                namespace,
                sink,
                inputStream,
                details,
                pkgUrl,
                sinkConfig,
                null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink config is not provided")
    public void testMissingSinkConfig() {
        resource.registerSink(
                tenant,
                namespace,
                sink,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink config is not provided")
    public void testUpdateMissingSinkConfig() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
        resource.updateSink(
                tenant,
                namespace,
                sink,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null);
    }

    private void registerDefaultSink() throws IOException {
        registerDefaultSinkWithPackageUrl(null);
    }

    private void registerDefaultSinkWithPackageUrl(String packageUrl) throws IOException {
        SinkConfig sinkConfig = createDefaultSinkConfig();
        try (FileInputStream inputStream = new FileInputStream(getPulsarIOCassandraNar())) {
            resource.registerSink(
                    tenant,
                    namespace,
                    sink,
                    inputStream,
                    mockedFormData,
                    packageUrl,
                    sinkConfig,
                    null);
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink test-sink already exists")
    public void testRegisterExistedSink() throws IOException {
        try {
            Configurator.setRootLevel(Level.DEBUG);

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            registerDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testRegisterSinkUploadFailure() throws Exception {
        mockInstanceUtils();
        try {
            mockWorkerUtils(ctx -> {
                    ctx.when(() -> WorkerUtils.uploadFileToBookkeeper(
                            anyString(),
                            any(File.class),
                            any(Namespace.class)))
                            .thenThrow(new IOException("upload failure"));
            });

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

            registerDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testRegisterSinkSuccess() throws Exception {
        mockInstanceUtils();
        mockWorkerUtils();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        registerDefaultSink();
    }

    @Test
    public void testRegisterSinkConflictingFields() throws Exception {
        mockInstanceUtils();
        mockWorkerUtils();

        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

        SinkConfig sinkConfig = createDefaultSinkConfig();
        try (FileInputStream inputStream = new FileInputStream(getPulsarIOCassandraNar())) {
            resource.registerSink(
                    actualTenant,
                    actualNamespace,
                    actualName,
                    inputStream,
                    mockedFormData,
                    null,
                    sinkConfig,
                    null);
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "sink failed to register")
    public void testRegisterSinkFailure() throws Exception {
        mockInstanceUtils();
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

            doThrow(new IllegalArgumentException("sink failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            registerDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration "
            + "interrupted")
    public void testRegisterSinkInterrupted() throws Exception {
        mockInstanceUtils();
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

            doThrow(new IllegalStateException("Function registration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            registerDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test(timeOut = 20000)
    public void testRegisterSinkSuccessWithPackageName() throws IOException {
        mockInstanceUtils();
        registerDefaultSinkWithPackageUrl("sink://public/default/test@v1");
    }

    @Test(timeOut = 20000)
    public void testRegisterSinkFailedWithWrongPackageName() throws PulsarAdminException, IOException {
        try {
            doThrow(new PulsarAdminException("package name is invalid"))
                    .when(mockedPackages).download(anyString(), anyString());
            registerDefaultSinkWithPackageUrl("function://");
        } catch (RestException e) {
            // expected exception
            assertEquals(e.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
        }
    }

    //
    // Update Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testUpdateSinkMissingTenant() throws Exception {
        try {
            testUpdateSinkMissingArguments(
                    null,
                    namespace,
                    sink,
                    mockedInputStream,
                    mockedFormData,
                    topicsToSerDeClassName,
                    CASSANDRA_STRING_SINK,
                    parallelism,
                    "Tenant is not provided");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testUpdateSinkMissingNamespace() throws Exception {
        try {
            testUpdateSinkMissingArguments(
                    tenant,
                    null,
                    sink,
                    mockedInputStream,
                    mockedFormData,
                    topicsToSerDeClassName,
                    CASSANDRA_STRING_SINK,
                    parallelism,
                    "Namespace is not provided");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink name is not provided")
    public void testUpdateSinkMissingFunctionName() throws Exception {
        try {
            testUpdateSinkMissingArguments(
                    tenant,
                    namespace,
                    null,
                    mockedInputStream,
                    mockedFormData,
                    topicsToSerDeClassName,
                    CASSANDRA_STRING_SINK,
                    parallelism,
                    "Sink name is not provided");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateSinkMissingPackage() throws Exception {
        try {
            mockWorkerUtils();

            testUpdateSinkMissingArguments(
                    tenant,
                    namespace,
                    sink,
                    null,
                    mockedFormData,
                    topicsToSerDeClassName,
                    null,
                    parallelism,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateSinkMissingInputs() throws Exception {
        try {
            mockWorkerUtils();

            testUpdateSinkMissingArguments(
                    tenant,
                    namespace,
                    sink,
                    null,
                    mockedFormData,
                    null,
                    null,
                    parallelism,
                    "Update contains no change");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Input Topics cannot be altered")
    public void testUpdateSinkDifferentInputs() throws Exception {
        try {
            mockWorkerUtils();

            Map<String, String> inputTopics = new HashMap<>();
            inputTopics.put("DifferentTopic", DEFAULT_SERDE);
            testUpdateSinkMissingArguments(
                    tenant,
                    namespace,
                    sink,
                    null,
                    mockedFormData,
                    inputTopics,
                    CASSANDRA_STRING_SINK,
                    parallelism,
                    "Input Topics cannot be altered");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateSinkDifferentParallelism() throws Exception {
        mockWorkerUtils();

        try (FileInputStream inputStream = new FileInputStream(getPulsarIOCassandraNar())) {
            testUpdateSinkMissingArguments(
                    tenant,
                    namespace,
                    sink,
                    inputStream,
                    mockedFormData,
                    topicsToSerDeClassName,
                    CASSANDRA_STRING_SINK,
                    parallelism + 1,
                    null);
        }
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
            String expectedError) throws Exception {
        mockFunctionCommon(tenant, namespace, sink);

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

        if (expectedError != null) {
            doThrow(new IllegalArgumentException(expectedError))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
        }

        resource.updateSink(
                tenant,
                namespace,
                sink,
                inputStream,
                details,
                null,
                sinkConfig,
                null, null);

    }

    private void updateDefaultSink() throws Exception {
        updateDefaultSinkWithPackageUrl(null);
    }

    private void updateDefaultSinkWithPackageUrl(String packageUrl) throws Exception {
        SinkConfig sinkConfig = createDefaultSinkConfig();

        this.mockedFunctionMetaData =
                FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        try (FileInputStream inputStream = new FileInputStream(getPulsarIOCassandraNar())) {
            resource.updateSink(
                    tenant,
                    namespace,
                    sink,
                    inputStream,
                    mockedFormData,
                    packageUrl,
                    sinkConfig,
                    null, null);
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink test-sink doesn't exist")
    public void testUpdateNotExistedSink() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);
            updateDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testUpdateSinkUploadFailure() throws Exception {
        try {
            mockWorkerUtils(ctx -> {
                ctx.when(() -> WorkerUtils.uploadFileToBookkeeper(
                                anyString(),
                                any(File.class),
                                any(Namespace.class)))
                        .thenThrow(new IOException("upload failure"));
            });

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            updateDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testUpdateSinkSuccess() throws Exception {
        mockWorkerUtils();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        updateDefaultSink();
    }

    @Test
    public void testUpdateSinkWithUrl() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);

        String filePackageUrl = getPulsarIOCassandraNar().toURI().toString();

        SinkConfig sinkConfig = createDefaultSinkConfig();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

        this.mockedFunctionMetaData =
                FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        resource.updateSink(
                tenant,
                namespace,
                sink,
                null,
                null,
                filePackageUrl,
                sinkConfig,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "sink failed to register")
    public void testUpdateSinkFailure() throws Exception {
        try {
            mockWorkerUtils();
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            doThrow(new IllegalArgumentException("sink failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            updateDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(timeOut = 20000)
    public void testUpdateSinkSuccessWithPackageName() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
        updateDefaultSinkWithPackageUrl("function://public/default/test@v1");
    }

    @Test(timeOut = 20000)
    public void testUpdateSinkFailedWithWrongPackageName() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
        try {
            doThrow(new PulsarAdminException("package name is invalid"))
                    .when(mockedPackages).download(anyString(), anyString());
            updateDefaultSinkWithPackageUrl("function://");
        } catch (RestException e) {
            // expected exception
            assertEquals(e.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration "
            + "interrupted")
    public void testUpdateSinkInterrupted() throws Exception {
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            doThrow(new IllegalStateException("Function registration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            updateDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // deregister sink
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testDeregisterSinkMissingTenant() {
        try {
            testDeregisterSinkMissingArguments(
                    null,
                    namespace,
                    sink
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testDeregisterSinkMissingNamespace() {
        try {
            testDeregisterSinkMissingArguments(
                    tenant,
                    null,
                    sink
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink name is not provided")
    public void testDeregisterSinkMissingFunctionName() {
        try {
            testDeregisterSinkMissingArguments(
                    tenant,
                    namespace,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testDeregisterSinkMissingArguments(
            String tenant,
            String namespace,
            String sink
    ) {
        resource.deregisterFunction(
                tenant,
                namespace,
                sink,
                null);

    }

    private void deregisterDefaultSink() {
        resource.deregisterFunction(
                tenant,
                namespace,
                sink,
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink test-sink doesn't exist")
    public void testDeregisterNotExistedSink() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);
            deregisterDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testDeregisterSinkSuccess() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "sink failed to deregister")
    public void testDeregisterSinkFailure() throws Exception {
        mockInstanceUtils();
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink)))
                    .thenReturn(FunctionMetaData.newBuilder().build());

            doThrow(new IllegalArgumentException("sink failed to deregister"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            deregisterDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function deregistration "
            + "interrupted")
    public void testDeregisterSinkInterrupted() throws Exception {
        mockInstanceUtils();
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink)))
                    .thenReturn(FunctionMetaData.newBuilder().build());

            doThrow(new IllegalStateException("Function deregistration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            deregisterDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testDeregisterSinkBKPackageCleanup() {
        mockInstanceUtils();
        try (final MockedStatic<WorkerUtils> ctx = Mockito.mockStatic(WorkerUtils.class)) {

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            String packagePath =
                    "public/default/test/591541f0-c7c5-40c0-983b-610c722f90b0-pulsar-io-batch-data-generator-2.7.0.nar";
            FunctionMetaData functionMetaData = FunctionMetaData.newBuilder()
                    .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath(packagePath))
                    .build();
            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink)))
                    .thenReturn(functionMetaData);

            deregisterDefaultSink();

            ctx.verify(() -> WorkerUtils.deleteFromBookkeeper(any(), eq(packagePath)), times(1));
        }
    }

    @Test
    public void testDeregisterBuiltinSinkBKPackageCleanup() {
        mockInstanceUtils();

        try (final MockedStatic<WorkerUtils> ctx = Mockito.mockStatic(WorkerUtils.class)) {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            String packagePath = String.format("%s://data-generator", Utils.BUILTIN);
            FunctionMetaData functionMetaData = FunctionMetaData.newBuilder()
                    .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath(packagePath))
                    .build();
            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink)))
                    .thenReturn(functionMetaData);

            deregisterDefaultSink();

            // if the sink is a builtin sink we shouldn't try to clean it up
            ctx.verify(() -> WorkerUtils.deleteFromBookkeeper(any(), anyString()), times(0));
        }
    }

    @Test
    public void testDeregisterHTTPSinkBKPackageCleanup() {
        mockInstanceUtils();

        try (final MockedStatic<WorkerUtils> ctx = Mockito.mockStatic(WorkerUtils.class)) {

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            String packagePath = "http://foo.com/connector.jar";
            FunctionMetaData functionMetaData = FunctionMetaData.newBuilder()
                    .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath(packagePath))
                    .build();

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink)))
                    .thenReturn(functionMetaData);

            deregisterDefaultSink();

            // if the sink is a is download from a http url, we shouldn't try to clean it up
            ctx.verify(() -> WorkerUtils.deleteFromBookkeeper(any(), anyString()), times(0));
        }
    }

    @Test
    public void testDeregisterFileSinkBKPackageCleanup() {
        mockInstanceUtils();

        try (final MockedStatic<WorkerUtils> ctx = Mockito.mockStatic(WorkerUtils.class)) {

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);

            String packagePath = "file://foo/connector.jar";
            FunctionMetaData functionMetaData = FunctionMetaData.newBuilder()
                    .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath(packagePath))
                    .build();

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink)))
                    .thenReturn(functionMetaData);

            deregisterDefaultSink();

            // if the sink package has a file url, we shouldn't try to clean it up
            ctx.verify(() -> WorkerUtils.deleteFromBookkeeper(any(), eq(packagePath)), times(0));
        }
    }

    //
    // Get Sink Info
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testGetSinkMissingTenant() {
        try {
            testGetSinkMissingArguments(
                    null,
                    namespace,
                    sink
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testGetSinkMissingNamespace() {
        try {
            testGetSinkMissingArguments(
                    tenant,
                    null,
                    sink
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink name is not provided")
    public void testGetSinkMissingFunctionName() {
        try {

            testGetSinkMissingArguments(
                    tenant,
                    namespace,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testGetSinkMissingArguments(
            String tenant,
            String namespace,
            String sink
    ) {
        resource.getFunctionInfo(
                tenant,
                namespace,
                sink,
                AuthenticationParameters.builder().build()
        );

    }

    private SinkConfig getDefaultSinkInfo() {
        return resource.getSinkInfo(
                tenant,
                namespace,
                sink,
                AuthenticationParameters.builder().build()
        );
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Sink test-sink doesn't exist")
    public void testGetNotExistedSink() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);
            getDefaultSinkInfo();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testGetSinkSuccess() {
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

        getDefaultSinkInfo();

        assertNotNull(getDefaultSinkInfo().getInputs());
        assertEquals(getDefaultSinkInfo().getInputs(), Collections.singleton("input"));

        assertEquals(
                SinkConfigUtils.convertFromDetails(functionDetails),
                getDefaultSinkInfo());
    }

    //
    // List Sinks
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testListSinksMissingTenant() {
        try {
            testListSinksMissingArguments(
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
            testListSinksMissingArguments(
                    tenant,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testListSinksMissingArguments(
            String tenant,
            String namespace
    ) {
        resource.listFunctions(
                tenant,
                namespace,
                AuthenticationParameters.builder().build()
        );

    }

    private List<String> listDefaultSinks() {
        return resource.listFunctions(
                tenant,
                namespace,
                AuthenticationParameters.builder().build()
        );
    }

    @Test
    public void testListSinksSuccess() {
        mockInstanceUtils();
        final List<String> functions = Lists.newArrayList("test-1", "test-2");
        final List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()
        ).build());
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()
        ).build());
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functionMetaDataList);

        List<String> sinkList = listDefaultSinks();
        assertEquals(functions, sinkList);
    }

    @Test
    public void testOnlyGetSinks() {
        final List<String> functions = Lists.newArrayList("test-3");
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

        mockStatic(InstanceUtils.class, ctx -> {
            ctx.when(() -> InstanceUtils.calculateSubjectType(eq(f1.getFunctionDetails())))
                    .thenReturn(FunctionDetails.ComponentType.SOURCE);
            ctx.when(() -> InstanceUtils.calculateSubjectType(eq(f2.getFunctionDetails())))
                    .thenReturn(FunctionDetails.ComponentType.FUNCTION);
            ctx.when(() -> InstanceUtils.calculateSubjectType(eq(f3.getFunctionDetails())))
                    .thenReturn(FunctionDetails.ComponentType.SINK);

        });

        List<String> sinkList = listDefaultSinks();
        assertEquals(functions, sinkList);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace does not exist")
    public void testregisterSinkNonExistingNamespace() throws Exception {
        try {
            this.namespaceList.clear();
            registerDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant does not exist")
    public void testregisterSinkNonExistingTenant() throws Exception {
        try {
            when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
            registerDefaultSink();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private SinkConfig createDefaultSinkConfig() {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sink);
        sinkConfig.setClassName(CASSANDRA_STRING_SINK);
        sinkConfig.setParallelism(parallelism);
        sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);
        return sinkConfig;
    }

    private void mockFunctionCommon(String tenant, String namespace, String sink) throws IOException {
        this.mockedFunctionMetaData =
                Function.FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(sink))).thenReturn(mockedFunctionMetaData);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(true);
    }

    private FunctionDetails createDefaultFunctionDetails() throws IOException {
        return SinkConfigUtils.convert(createDefaultSinkConfig(),
                new SinkConfigUtils.ExtractedSinkDetails(null, null));
    }

    /*
    Externally managed runtime,
    uploadBuiltinSinksSources == false
    Make sure uploadFileToBookkeeper is not called
    */
    @Test
    public void testRegisterSinkSuccessK8sNoUpload() throws Exception {
        mockedWorkerService.getWorkerConfig().setUploadBuiltinSinksSources(false);

        mockStatic(WorkerUtils.class, ctx -> {
            ctx.when(() -> WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class)))
                    .thenThrow(new RuntimeException("uploadFileToBookkeeper triggered"));

        });

        registerBuiltinConnector("cassandra", getPulsarIOCassandraNar());

        when(mockedRuntimeFactory.externallyManaged()).thenReturn(true);
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        SinkConfig sinkConfig = createDefaultSinkConfig();
        sinkConfig.setArchive("builtin://cassandra");

        resource.registerSink(
                tenant,
                namespace,
                sink,
                null,
                mockedFormData,
                null,
                sinkConfig,
                null);
    }

    /*
    Externally managed runtime,
    uploadBuiltinSinksSources == true
    Make sure uploadFileToBookkeeper is called
    */
    @Test
    public void testRegisterSinkSuccessK8sWithUpload() throws Exception {
        final String injectedErrMsg = "uploadFileToBookkeeper triggered";
        mockedWorkerService.getWorkerConfig().setUploadBuiltinSinksSources(true);

        mockStatic(WorkerUtils.class, ctx -> {
            ctx.when(() -> WorkerUtils.uploadFileToBookkeeper(
                            anyString(),
                            any(File.class),
                            any(Namespace.class)))
                    .thenThrow(new RuntimeException(injectedErrMsg));

        });

        registerBuiltinConnector("cassandra", getPulsarIOCassandraNar());

        when(mockedRuntimeFactory.externallyManaged()).thenReturn(true);
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(sink))).thenReturn(false);

        SinkConfig sinkConfig = createDefaultSinkConfig();
        sinkConfig.setArchive("builtin://cassandra");

        try {
            resource.registerSink(
                    tenant,
                    namespace,
                    sink,
                    null,
                    mockedFormData,
                    null,
                    sinkConfig,
                    null);
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertEquals(e.getMessage(), injectedErrMsg);
        }
    }

    @Test
    public void testUpdateSinkWithNoChange() throws IOException {
        mockWorkerUtils();

        // No change on config,
        SinkConfig sinkConfig = createDefaultSinkConfig();

        mockStatic(SinkConfigUtils.class, ctx -> {
            ctx.when(() -> SinkConfigUtils.convertFromDetails(any())).thenReturn(sinkConfig);
        });

        mockFunctionCommon(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName());

        // config has not changes and don't update auth, should fail
        try {
            resource.updateSink(
                    sinkConfig.getTenant(),
                    sinkConfig.getNamespace(),
                    sinkConfig.getName(),
                    null,
                    mockedFormData,
                    null,
                    sinkConfig,
                    null,
                    null);
            fail("Update without changes should fail");
        } catch (RestException e) {
            assertTrue(e.getMessage().contains("Update contains no change"));
        }

        try {
            UpdateOptionsImpl updateOptions = new UpdateOptionsImpl();
            updateOptions.setUpdateAuthData(false);
            resource.updateSink(
                    sinkConfig.getTenant(),
                    sinkConfig.getNamespace(),
                    sinkConfig.getName(),
                    null,
                    mockedFormData,
                    null,
                    sinkConfig,
                    null,
                    updateOptions);
            fail("Update without changes should fail");
        } catch (RestException e) {
            assertTrue(e.getMessage().contains("Update contains no change"));
        }

        // no changes but set the auth-update flag to true, should not fail
        UpdateOptionsImpl updateOptions = new UpdateOptionsImpl();
        updateOptions.setUpdateAuthData(true);
        try (FileInputStream inputStream = new FileInputStream(getPulsarIOCassandraNar())) {
            resource.updateSink(
                    sinkConfig.getTenant(),
                    sinkConfig.getNamespace(),
                    sinkConfig.getName(),
                    inputStream,
                    mockedFormData,
                    null,
                    sinkConfig,
                    null,
                    updateOptions);
        }
    }
}
