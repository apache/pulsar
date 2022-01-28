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

import static org.apache.pulsar.functions.worker.rest.api.v3.SinkApiV3ResourceTest.getPulsarIOCassandraNar;
import static org.apache.pulsar.functions.worker.rest.api.v3.SinkApiV3ResourceTest.getPulsarIOInvalidNar;
import static org.apache.pulsar.functions.worker.rest.api.v3.SinkApiV3ResourceTest.getPulsarIOTwitterNar;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.LeaderService;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.rest.api.PulsarFunctionTestTemporaryDirectory;
import org.apache.pulsar.functions.worker.rest.api.SourcesImpl;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link SourcesApiV3Resource}.
 */
@PrepareForTest({WorkerUtils.class, ConnectorUtils.class, FunctionCommon.class, ClassLoaderUtils.class,
        InstanceUtils.class})
@PowerMockIgnore({"javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.*"})
public class SourceApiV3ResourceTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String source = "test-source";
    private static final String outputTopic = "test-output-topic";
    private static final String outputSerdeClassName = TopicSchema.DEFAULT_SERDE;
    private static final String TWITTER_FIRE_HOSE = "org.apache.pulsar.io.twitter.TwitterFireHose";
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
    private SourcesImpl resource;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;
    private FunctionMetaData mockedFunctionMetaData;
    private LeaderService mockedLeaderService;
    private Packages mockedPackages;
    private PulsarFunctionTestTemporaryDirectory tempDirectory;

    private static NarClassLoader narClassLoader;

    @BeforeClass
    public void setupNarClassLoader() throws IOException {
        narClassLoader = NarClassLoader.getFromArchive(getPulsarIOTwitterNar(), Collections.emptySet());
    }

    @AfterClass(alwaysRun = true)
    public void cleanupNarClassLoader() throws IOException {
        if (narClassLoader != null) {
            narClassLoader.close();
            narClassLoader = null;
        }
    }

    @BeforeMethod
    public void setup() throws Exception {
        this.mockedManager = mock(FunctionMetaDataManager.class);
        this.mockedFunctionRunTimeManager = mock(FunctionRuntimeManager.class);
        this.mockedRuntimeFactory = mock(RuntimeFactory.class);
        this.mockedInputStream = mock(InputStream.class);
        this.mockedNamespace = mock(Namespace.class);
        this.mockedFormData = mock(FormDataContentDisposition.class);
        when(mockedFormData.getFileName()).thenReturn("test");
        this.mockedTenantInfo = mock(TenantInfoImpl.class);
        this.mockedPulsarAdmin = mock(PulsarAdmin.class);
        this.mockedTenants = mock(Tenants.class);
        this.mockedNamespaces = mock(Namespaces.class);
        this.mockedFunctions = mock(Functions.class);
        this.mockedLeaderService = mock(LeaderService.class);
        this.mockedPackages = mock(Packages.class);
        namespaceList.add(tenant + "/" + namespace);

        this.mockedWorkerService = mock(PulsarWorkerService.class);
        when(mockedWorkerService.getFunctionMetaDataManager()).thenReturn(mockedManager);
        when(mockedWorkerService.getLeaderService()).thenReturn(mockedLeaderService);
        when(mockedWorkerService.getFunctionRuntimeManager()).thenReturn(mockedFunctionRunTimeManager);
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
        doAnswer(invocationOnMock -> {
            Files.copy(getPulsarIOTwitterNar().toPath(), Paths.get(invocationOnMock.getArgument(1, String.class)),
                    StandardCopyOption.REPLACE_EXISTING);
            return null;
        }).when(mockedPackages).download(any(), any());

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

        this.resource = spy(new SourcesImpl(() -> mockedWorkerService));
        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType(any())).thenReturn(FunctionDetails.ComponentType.SOURCE);
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

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source class UnknownClass must"
            + " be in class path")
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

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source package does not have the"
            + " correct format. Pulsar cannot determine if the package is a NAR package"
            + " or JAR package. Source classname is not provided and attempts to load it as a NAR package "
            + "produced the following error.")
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

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source package does not have "
            + "the correct format.*")
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
                null, null);

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
                null, null);
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
                null, null, null);
    }

    private void registerDefaultSource() throws IOException {
        registerDefaultSourceWithPackageUrl("source://public/default/test@v1");
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
                null, null);
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
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);
            when(mockedRuntimeFactory.externallyManaged()).thenReturn(true);

            registerDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testRegisterSourceSuccess() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(false);

        registerDefaultSource();
    }

    @Test(timeOut = 20000)
    public void testRegisterSourceSuccessWithPackageName() throws IOException {
        registerDefaultSourceWithPackageUrl("source://public/default/test@v1");
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

        mockStatic(WorkerUtils.class);
        PowerMockito.doNothing().when(WorkerUtils.class, "uploadFileToBookkeeper", anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(TWITTER_FIRE_HOSE);
        sourceConfig.setParallelism(parallelism);
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);
        try (InputStream inputStream = new FileInputStream(getPulsarIOTwitterNar())) {
            resource.registerSource(
                    actualTenant,
                    actualNamespace,
                    actualName,
                    inputStream,
                    mockedFormData,
                    null,
                    sourceConfig,
                    null, null);
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to register")
    public void testRegisterSourceFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

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
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

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
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            testUpdateSourceMissingArguments(
                    tenant,
                    namespace,
                    source,
                    null,
                    mockedFormData,
                    outputTopic,
                    outputSerdeClassName,
                    TWITTER_FIRE_HOSE,
                    parallelism + 1,
                    null);
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateSourceChangedTopic() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

        testUpdateSourceMissingArguments(
                tenant,
                namespace,
                source,
                null,
                mockedFormData,
                "DifferentTopic",
                outputSerdeClassName,
                TWITTER_FIRE_HOSE,
                parallelism,
                null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source parallelism must be a "
            + "positive number")
    public void testUpdateSourceZeroParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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

        mockStatic(ConnectorUtils.class);
        doReturn(TWITTER_FIRE_HOSE).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        PowerMockito.when(FunctionCommon.class, "createPkgTempFile").thenCallRealMethod();
        PowerMockito.when(FunctionCommon.class, "getClassLoaderFromPackage", any(), any(), any(), any())
                .thenCallRealMethod();

        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(argThat(clazz -> clazz.getName().equals(TWITTER_FIRE_HOSE)));

        doReturn(narClassLoader).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any());

        this.mockedFunctionMetaData =
                FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

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
                null, null, null);

    }

    private void updateDefaultSource() throws Exception {
        updateDefaultSourceWithPackageUrl(null);
    }

    private void updateDefaultSourceWithPackageUrl(String packageUrl) throws Exception {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(TWITTER_FIRE_HOSE);
        sourceConfig.setParallelism(parallelism);
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);

        mockStatic(ConnectorUtils.class);
        doReturn(TWITTER_FIRE_HOSE).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        PowerMockito.when(FunctionCommon.class, "createPkgTempFile").thenCallRealMethod();
        PowerMockito.when(FunctionCommon.class, "getClassLoaderFromPackage", any(), any(), any(), any())
                .thenCallRealMethod();

        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(argThat(clazz -> clazz.getName().equals(TWITTER_FIRE_HOSE)));

        doReturn(narClassLoader).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(File.class), any());

        this.mockedFunctionMetaData =
                FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        try (InputStream inputStream = new FileInputStream(getPulsarIOCassandraNar())) {
            resource.updateSource(
                    tenant,
                    namespace,
                    source,
                    inputStream,
                    mockedFormData,
                    packageUrl,
                    sourceConfig,
                    null, null, null);
        }
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
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
            updateDefaultSource();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testUpdateSourceSuccess() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        updateDefaultSource();
    }

    @Test
    public void testUpdateSourceWithUrl() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);

        String filePackageUrl = getPulsarIOCassandraNar().toURI().toString();

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTopicName(outputTopic);
        sourceConfig.setSerdeClassName(outputSerdeClassName);
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(source);
        sourceConfig.setClassName(TWITTER_FIRE_HOSE);
        sourceConfig.setParallelism(parallelism);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);
        mockStatic(ConnectorUtils.class);
        doReturn(TWITTER_FIRE_HOSE).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(argThat(clazz -> clazz.getName().equals(TWITTER_FIRE_HOSE)));
        PowerMockito.when(FunctionCommon.class, "extractFileFromPkgURL", any()).thenCallRealMethod();
        PowerMockito.when(FunctionCommon.class, "getClassLoaderFromPackage", any(), any(), any(), any())
                .thenCallRealMethod();

        doReturn(narClassLoader).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any());

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
                null, null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to register")
    public void testUpdateSourceFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

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
                null, null);

    }

    private void deregisterDefaultSource() {
        resource.deregisterFunction(
                tenant,
                namespace,
                source,
                null, null);
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

        mockStatic(WorkerUtils.class);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        String packagePath =
                "public/default/test/591541f0-c7c5-40c0-983b-610c722f90b0-pulsar-io-batch-data-generator-2.7.0.nar";
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                .thenReturn(FunctionMetaData.newBuilder().setPackageLocation(
                        PackageLocationMetaData.newBuilder().setPackagePath(packagePath).build()).build());

        deregisterDefaultSource();

        PowerMockito.verifyStatic(WorkerUtils.class, times(1));
        WorkerUtils.deleteFromBookkeeper(any(), eq(packagePath));
    }

    @Test
    public void testDeregisterBuiltinSourceBKPackageCleanup() throws IOException {

        mockStatic(WorkerUtils.class);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        String packagePath = String.format("%s://data-generator", Utils.BUILTIN);
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                .thenReturn(FunctionMetaData.newBuilder().setPackageLocation(
                        PackageLocationMetaData.newBuilder().setPackagePath(packagePath).build()).build());

        deregisterDefaultSource();

        // if the source is a builtin source we shouldn't try to clean it up
        PowerMockito.verifyStatic(WorkerUtils.class, times(0));
        WorkerUtils.deleteFromBookkeeper(any(), eq(packagePath));
    }

    @Test
    public void testDeregisterHTTPSourceBKPackageCleanup() throws IOException {

        mockStatic(WorkerUtils.class);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        String packagePath = "http://foo.com/connector.jar";
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                .thenReturn(FunctionMetaData.newBuilder().setPackageLocation(
                        PackageLocationMetaData.newBuilder().setPackagePath(packagePath).build()).build());

        deregisterDefaultSource();

        // if the source is a is download from a http url, we shouldn't try to clean it up
        PowerMockito.verifyStatic(WorkerUtils.class, times(0));
        WorkerUtils.deleteFromBookkeeper(any(), eq(packagePath));
    }

    @Test
    public void testDeregisterFileSourceBKPackageCleanup() throws IOException {

        mockStatic(WorkerUtils.class);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(source))).thenReturn(true);

        String packagePath = "file://foo/connector.jar";
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(source)))
                .thenReturn(FunctionMetaData.newBuilder().setPackageLocation(
                        PackageLocationMetaData.newBuilder().setPackagePath(packagePath).build()).build());

        deregisterDefaultSource();

        // if the source has a file url, we shouldn't try to clean it up
        PowerMockito.verifyStatic(WorkerUtils.class, times(0));
        WorkerUtils.deleteFromBookkeeper(any(), eq(packagePath));
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
                source, null, null
        );
    }

    private SourceConfig getDefaultSourceInfo() {
        return resource.getSourceInfo(
                tenant,
                namespace,
                source
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
                namespace, null, null
        );
    }

    private List<String> listDefaultSources() {
        return resource.listFunctions(
                tenant,
                namespace, null, null);
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
        PowerMockito.when(InstanceUtils.calculateSubjectType(f1.getFunctionDetails()))
                .thenReturn(FunctionDetails.ComponentType.SOURCE);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f2.getFunctionDetails()))
                .thenReturn(FunctionDetails.ComponentType.FUNCTION);
        PowerMockito.when(InstanceUtils.calculateSubjectType(f3.getFunctionDetails()))
                .thenReturn(FunctionDetails.ComponentType.SINK);

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
