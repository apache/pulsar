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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.rest.api.SourcesImpl;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Unit test of {@link SourcesApiV3Resource}.
 */
public class SourceApiV3ResourceTest extends AbstractFunctionsResourceTest {

    private static final String source = "test-source";
    private static final String outputTopic = "test-output-topic";
    private static final String outputSerdeClassName = TopicSchema.DEFAULT_SERDE;
    private static final String TWITTER_FIRE_HOSE = "org.apache.pulsar.io.twitter.TwitterFireHose";
    private SourcesImpl resource;

    @Override
    protected void doSetup() {
        this.resource = spy(new SourcesImpl(() -> mockedWorkerService));
    }

    @Override
    protected FunctionDetails.ComponentType getComponentType() {
        return FunctionDetails.ComponentType.SOURCE;
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
                    TWITTER_FIRE_HOSE,
                    parallelism,
                    null
            );
        } catch (RestException re) {
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
                    TWITTER_FIRE_HOSE,
                    parallelism,
                    null
            );
        } catch (RestException re) {
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
                    TWITTER_FIRE_HOSE,
                    parallelism,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source class UnknownClass not "
            + "found in class loader")
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
        } catch (RestException re) {
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
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source Package is not provided")
    public void testRegisterSourceMissingPackageDetails() throws IOException {
        try (InputStream inputStream = new FileInputStream(getPulsarIOTwitterNar())) {
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    inputStream,
                    null,
                    outputTopic,
                    outputSerdeClassName,
                    TWITTER_FIRE_HOSE,
                    parallelism,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source package doesn't contain"
            + " the META-INF/services/pulsar-io.yaml file.")
    public void testRegisterSourceMissingPackageDetailsAndClassname() {
        try {
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    mockedInputStream,
                    null,
                    outputTopic,
                    outputSerdeClassName,
                    null,
                    parallelism,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source package doesn't contain"
            + " the META-INF/services/pulsar-io.yaml file.")
    public void testRegisterSourceInvalidJarWithNoSource() throws IOException {
        try (InputStream inputStream = new FileInputStream(getPulsarIOInvalidNar())) {
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    inputStream,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    null,
                    parallelism,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Topic name cannot be null")
    public void testRegisterSourceNoOutputTopic() throws IOException {
        try (InputStream inputStream = new FileInputStream(getPulsarIOTwitterNar())) {
            testRegisterSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    inputStream,
                    mockedFormData,
                    null,
                    outputSerdeClassName,
                    TWITTER_FIRE_HOSE,
                    parallelism,
                    null
            );
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Encountered error .*. when "
            + "getting Source package from .*")
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
                    TWITTER_FIRE_HOSE,
                    parallelism,
                    "http://localhost:1234/test"
            );
        } catch (RestException re) {
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
                null);

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
                null);
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
                null, null);
    }

    private void registerDefaultSource() throws IOException {
        registerDefaultSourceWithPackageUrl(getPulsarIOTwitterNar().toURI().toString());
    }

    private void registerDefaultSourceWithPackageUrl(String packageUrl) throws IOException {
        SourceConfig sourceConfig = createDefaultSourceConfig();
        resource.registerSource(
                tenant,
                namespace,
                source,
                null,
                null,
                packageUrl,
                sourceConfig,
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source already "
            + "exists")
    public void testRegisterExistedSource() throws IOException {
        try {
            Configurator.setRootLevel(Level.DEBUG);

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            registerDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testRegisterSourceUploadFailure() throws Exception {
        try {
            mockWorkerUtils(ctx -> {
                ctx.when(() ->
                                WorkerUtils.uploadFileToBookkeeper(
                                        anyString(),
                                        any(File.class),
                                        any(Namespace.class)))
                        .thenThrow(new IOException("upload failure"));
            });

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            when(mockedRuntimeFactory.externallyManaged()).thenReturn(true);

            registerDefaultSource();
            fail();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testRegisterSourceSuccess() throws Exception {
        mockWorkerUtils();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

        registerDefaultSource();
    }



    @Test(timeOut = 20000)
    public void testRegisterSourceSuccessWithPackageName() throws IOException {
        registerDefaultSource();
    }

    @Test(timeOut = 20000)
    public void testRegisterSourceFailedWithWrongPackageName() throws PulsarAdminException, IOException {
        try {
            doThrow(new PulsarAdminException("package name is invalid"))
                    .when(mockedPackages).download(anyString(), anyString());
            registerDefaultSourceWithPackageUrl("source://");
        } catch (RestException e) {
            // expected exception
            assertEquals(e.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
        }
    }

    @Test
    public void testRegisterSourceConflictingFields() throws Exception {

        mockWorkerUtils();

        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

        SourceConfig sourceConfig = createDefaultSourceConfig();
        try (InputStream inputStream = new FileInputStream(getPulsarIOTwitterNar())) {
            resource.registerSource(
                    actualTenant,
                    actualNamespace,
                    actualName,
                    inputStream,
                    mockedFormData,
                    null,
                    sourceConfig,
                    null);
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to register")
    public void testRegisterSourceFailure() throws Exception {
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

            doThrow(new IllegalArgumentException("source failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            registerDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration "
            + "interrupted")
    public void testRegisterSourceInterrupted() throws Exception {
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

            doThrow(new IllegalStateException("Function registration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            registerDefaultSource();
        } catch (RestException re) {
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
                    TWITTER_FIRE_HOSE,
                    parallelism,
                    "Tenant is not provided");
        } catch (RestException re) {
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
                    TWITTER_FIRE_HOSE,
                    parallelism,
                    "Namespace is not provided");
        } catch (RestException re) {
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
                    TWITTER_FIRE_HOSE,
                    parallelism,
                    "Source name is not provided");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateSourceMissingPackage() throws Exception {
        try {
            mockStatic(WorkerUtils.class, ctx -> {
            });

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
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateSourceMissingTopicName() throws Exception {
        try {
            mockStatic(WorkerUtils.class, ctx -> {
            });

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
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism must be a "
            + "positive number")
    public void testUpdateSourceNegativeParallelism() throws Exception {
        try {
            mockWorkerUtils();

            testUpdateSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    null,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    TWITTER_FIRE_HOSE,
                    -2,
                    "Source parallelism must be a positive number");
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateSourceChangedParallelism() throws Exception {
        try {
            mockWorkerUtils();

            try(FileInputStream inputStream = new FileInputStream(getPulsarIOTwitterNar())) {
                testUpdateSourceMissingArguments(
                        tenant,
                        namespace,
                        source,
                        inputStream,
                        mockedFormData,
                        outputTopic,
                        outputSerdeClassName,
                        TWITTER_FIRE_HOSE,
                        parallelism + 1,
                        null);
            }
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateSourceChangedTopic() throws Exception {
        mockWorkerUtils();

        try(FileInputStream inputStream = new FileInputStream(getPulsarIOTwitterNar())) {
            testUpdateSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    inputStream,
                    mockedFormData,
                    "DifferentTopic",
                    outputSerdeClassName,
                    TWITTER_FIRE_HOSE,
                    parallelism,
                    null);
        }
    }

    @Test
    public void testUpdateSourceWithNoChange() throws IOException {
        mockWorkerUtils();

        // No change on config,
        SourceConfig sourceConfig = createDefaultSourceConfig();
        mockStatic(SourceConfigUtils.class, ctx -> {
            ctx.when(() -> SourceConfigUtils.convertFromDetails(any())).thenReturn(sourceConfig);
        });

        mockFunctionCommon(sourceConfig.getTenant(), sourceConfig.getNamespace(), sourceConfig.getName());

        // config has not changes and don't update auth, should fail
        try {
            resource.updateSource(
                    sourceConfig.getTenant(),
                    sourceConfig.getNamespace(),
                    sourceConfig.getName(),
                    null,
                    mockedFormData,
                    null,
                    sourceConfig,
                    null,
                    null);
            fail("Update without changes should fail");
        } catch (RestException e) {
            assertTrue(e.getMessage().contains("Update contains no change"));
        }

        try {
            UpdateOptionsImpl updateOptions = new UpdateOptionsImpl();
            updateOptions.setUpdateAuthData(false);
            resource.updateSource(
                    sourceConfig.getTenant(),
                    sourceConfig.getNamespace(),
                    sourceConfig.getName(),
                    null,
                    mockedFormData,
                    null,
                    sourceConfig,
                    null,
                    updateOptions);
            fail("Update without changes should fail");
        } catch (RestException e) {
            assertTrue(e.getMessage().contains("Update contains no change"));
        }

        // no changes but set the auth-update flag to true, should not fail
        UpdateOptionsImpl updateOptions = new UpdateOptionsImpl();
        updateOptions.setUpdateAuthData(true);
        try (InputStream inputStream = new FileInputStream(getPulsarIOTwitterNar())) {
            resource.updateSource(
                    sourceConfig.getTenant(),
                    sourceConfig.getNamespace(),
                    sourceConfig.getName(),
                    inputStream,
                    mockedFormData,
                    null,
                    sourceConfig,
                    null,
                    updateOptions);
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism must be a "
            + "positive number")
    public void testUpdateSourceZeroParallelism() throws Exception {
        try {
            mockWorkerUtils();

            testUpdateSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    mockedInputStream,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    TWITTER_FIRE_HOSE,
                    0,
                    "Source parallelism must be a positive number");
        } catch (RestException re) {
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

        mockFunctionCommon(tenant, namespace, function);

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

        if (expectedError != null) {
            doThrow(new IllegalArgumentException(expectedError))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());
        }

        resource.updateSource(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                null,
                sourceConfig,
                null, null);

    }

    private void mockFunctionCommon(String tenant, String namespace, String function) {
        mockStatic(ConnectorUtils.class, c -> {
        });
        mockStatic(ClassLoaderUtils.class, c -> {
        });

        this.mockedFunctionMetaData =
                FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);
    }

    private void updateDefaultSource() throws Exception {
        updateDefaultSourceWithPackageUrl(getPulsarIOTwitterNar().toURI().toString());
    }

    private void updateDefaultSourceWithPackageUrl(String packageUrl) throws Exception {
        SourceConfig sourceConfig = createDefaultSourceConfig();

        this.mockedFunctionMetaData =
                FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        resource.updateSource(
                tenant,
                namespace,
                source,
                null,
                mockedFormData,
                packageUrl,
                sourceConfig,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source doesn't " +
            "exist")
    public void testUpdateNotExistedSource() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            updateDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testUpdateSourceUploadFailure() throws Exception {
        try {
            mockWorkerUtils(ctx -> {
                ctx.when(() -> WorkerUtils.uploadFileToBookkeeper(
                        anyString(),
                        any(File.class),
                        any(Namespace.class))).thenThrow(new IOException("upload failure"));
            });

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
            SourceConfig sourceConfig = createDefaultSourceConfig();
            this.mockedFunctionMetaData =
                    FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
            when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

            try(InputStream inputStream = new FileInputStream(getPulsarIOTwitterNar())) {
                resource.updateSource(
                        tenant,
                        namespace,
                        source,
                        inputStream,
                        mockedFormData,
                        null,
                        sourceConfig,
                        null, null);
            }
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testUpdateSourceSuccess() throws Exception {
        mockWorkerUtils();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        updateDefaultSource();
    }

    @Test
    public void testUpdateSourceWithUrl() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);

        String filePackageUrl = getPulsarIOTwitterNar().toURI().toString();

        SourceConfig sourceConfig = createDefaultSourceConfig();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        mockStatic(ConnectorUtils.class, c -> {
        });
        mockStatic(ClassLoaderUtils.class, c -> {
        });

        this.mockedFunctionMetaData =
                FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        resource.updateSource(
                tenant,
                namespace,
                source,
                null,
                null,
                filePackageUrl,
                sourceConfig,
                null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to register")
    public void testUpdateSourceFailure() throws Exception {
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            doThrow(new IllegalArgumentException("source failed to register"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            updateDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function registration " +
            "interrupted")
    public void testUpdateSourceInterrupted() throws Exception {
        try {
            mockWorkerUtils();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            doThrow(new IllegalStateException("Function registration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            updateDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test(timeOut = 20000)
    public void testUpdateSourceSuccessWithPackageName() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        updateDefaultSourceWithPackageUrl("source://public/default/test@v1");
    }

    @Test(timeOut = 20000)
    public void testUpdateSourceFailedWithWrongPackageName() throws Exception {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        try {
            doThrow(new PulsarAdminException("package name is invalid"))
                    .when(mockedPackages).download(anyString(), anyString());
            updateDefaultSourceWithPackageUrl("source://");
        } catch (RestException e) {
            // expected exception
            assertEquals(e.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
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
        } catch (RestException re) {
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
        } catch (RestException re) {
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
        } catch (RestException re) {
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
                null);

    }

    private void deregisterDefaultSource() {
        resource.deregisterFunction(
                tenant,
                namespace,
                source,
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source doesn't " +
            "exist")
    public void testDeregisterNotExistedSource() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            deregisterDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testDeregisterSourceSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                .thenReturn(FunctionMetaData.newBuilder().build());

        deregisterDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to deregister")
    public void testDeregisterSourceFailure() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                    .thenReturn(FunctionMetaData.newBuilder().build());

            doThrow(new IllegalArgumentException("source failed to deregister"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            deregisterDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function deregistration "
            + "interrupted")
    public void testDeregisterSourceInterrupted() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                    .thenReturn(FunctionMetaData.newBuilder().build());

            doThrow(new IllegalStateException("Function deregistration interrupted"))
                    .when(mockedManager).updateFunctionOnLeader(any(FunctionMetaData.class), Mockito.anyBoolean());

            deregisterDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testDeregisterSourceBKPackageCleanup() throws IOException {
        String packagePath =
                "public/default/test/591541f0-c7c5-40c0-983b-610c722f90b0-pulsar-io-batch-data-generator-2.7.0.nar";
        try (final MockedStatic<WorkerUtils> ctx = Mockito.mockStatic(WorkerUtils.class)) {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);


            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                    .thenReturn(FunctionMetaData.newBuilder().setPackageLocation(
                            PackageLocationMetaData.newBuilder().setPackagePath(packagePath).build()).build());

            deregisterDefaultSource();
            ctx.verify(() -> {
                WorkerUtils.deleteFromBookkeeper(any(), eq(packagePath));
            }, times(1));
        }
    }

    @Test
    public void testDeregisterBuiltinSourceBKPackageCleanup() throws IOException {

        String packagePath = String.format("%s://data-generator", Utils.BUILTIN);
        try (final MockedStatic<WorkerUtils> ctx = Mockito.mockStatic(WorkerUtils.class)) {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);


            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                    .thenReturn(FunctionMetaData.newBuilder().setPackageLocation(
                            PackageLocationMetaData.newBuilder().setPackagePath(packagePath).build()).build());

            deregisterDefaultSource();
            // if the source is a builtin source we shouldn't try to clean it up
            ctx.verify(() -> {
                WorkerUtils.deleteFromBookkeeper(any(), eq(packagePath));
            }, times(0));
        }
    }

    @Test
    public void testDeregisterHTTPSourceBKPackageCleanup() throws IOException {
        String packagePath = "http://foo.com/connector.jar";
        try (final MockedStatic<WorkerUtils> ctx = Mockito.mockStatic(WorkerUtils.class)) {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);


            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                    .thenReturn(FunctionMetaData.newBuilder().setPackageLocation(
                            PackageLocationMetaData.newBuilder().setPackagePath(packagePath).build()).build());

            deregisterDefaultSource();
            // if the source is a is download from a http url, we shouldn't try to clean it up
            ctx.verify(() -> {
                WorkerUtils.deleteFromBookkeeper(any(), eq(packagePath));
            }, times(0));
        }
    }

    @Test
    public void testDeregisterFileSourceBKPackageCleanup() throws IOException {

            String packagePath = "file://foo/connector.jar";

            try (final MockedStatic<WorkerUtils> ctx = Mockito.mockStatic(WorkerUtils.class)) {

                when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
                when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                        .thenReturn(FunctionMetaData.newBuilder().setPackageLocation(
                                PackageLocationMetaData.newBuilder().setPackagePath(packagePath).build()).build());

                deregisterDefaultSource();
                // if the source has a file url, we shouldn't try to clean it up
                ctx.verify(() -> {
                    WorkerUtils.deleteFromBookkeeper(any(), eq(packagePath));
                }, times(0));
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
        } catch (RestException re) {
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
        } catch (RestException re) {
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
        } catch (RestException re) {
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
                source,
                AuthenticationParameters.builder().build()
        );
    }

    private SourceConfig getDefaultSourceInfo() {
        return resource.getSourceInfo(
                tenant,
                namespace,
                source,
                AuthenticationParameters.builder().build()
        );
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source test-source doesn't "
            + "exist")
    public void testGetNotExistedSource() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            getDefaultSourceInfo();
        } catch (RestException re) {
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
        } catch (RestException re) {
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
        } catch (RestException re) {
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
                namespace,
                AuthenticationParameters.builder().build()
        );
    }

    private List<String> listDefaultSources() {
        return resource.listFunctions(
                tenant,
                namespace,
                AuthenticationParameters.builder().build()
        );
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
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functionMetaDataList);
        List<String> sourceList = listDefaultSources();
        assertEquals(functions, sourceList);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace does not exist")
    public void testRegisterFunctionNonExistingNamespace() throws Exception {
        try {
            this.namespaceList.clear();
            registerDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant does not exist")
    public void testRegisterFunctionNonExistingTenant() throws Exception {
        try {
            when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
            registerDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private SourceConfig createDefaultSourceConfig() {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(TWITTER_FIRE_HOSE);
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
