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

import static org.apache.pulsar.functions.source.TopicSchema.DEFAULT_SERDE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.LoadedFunctionPackage;
import org.apache.pulsar.functions.utils.ValidatableFunctionPackage;
import org.apache.pulsar.functions.utils.functions.FunctionArchive;
import org.apache.pulsar.functions.utils.functions.FunctionUtils;
import org.apache.pulsar.functions.utils.io.Connector;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.ConnectorsManager;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.FunctionsManager;
import org.apache.pulsar.functions.worker.LeaderService;
import org.apache.pulsar.functions.worker.PackageUrlValidator;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.rest.api.PulsarFunctionTestTemporaryDirectory;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.mockito.Answers;
import org.mockito.MockSettings;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class AbstractFunctionsResourceTest {

    protected static final String tenant = "test-tenant";
    protected static final String namespace = "test-namespace";
    protected static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    protected static final String subscriptionName = "test-subscription";
    protected static final String CASSANDRA_STRING_SINK = "org.apache.pulsar.io.cassandra.CassandraStringSink";
    protected static final int parallelism = 1;
    private static final String SYSTEM_PROPERTY_NAME_CASSANDRA_NAR_FILE_PATH = "pulsar-io-cassandra.nar.path";
    private static final String SYSTEM_PROPERTY_NAME_TWITTER_NAR_FILE_PATH = "pulsar-io-twitter.nar.path";
    private static final String SYSTEM_PROPERTY_NAME_INVALID_NAR_FILE_PATH = "pulsar-io-invalid.nar.path";
    private static final String SYSTEM_PROPERTY_NAME_FUNCTIONS_API_EXAMPLES_NAR_FILE_PATH =
            "pulsar-functions-api-examples.nar.path";
    protected static Map<String, MockedStatic> mockStaticContexts = new HashMap<>();

    static {
        topicsToSerDeClassName.put("test_src", DEFAULT_SERDE);
        topicsToSerDeClassName.put("persistent://public/default/test_src", TopicSchema.DEFAULT_SERDE);
    }

    protected PulsarWorkerService mockedWorkerService;
    protected PulsarAdmin mockedPulsarAdmin;
    protected Tenants mockedTenants;
    protected Namespaces mockedNamespaces;
    protected Functions mockedFunctions;
    protected TenantInfoImpl mockedTenantInfo;
    protected List<String> namespaceList = new LinkedList<>();
    protected FunctionMetaDataManager mockedManager;
    protected FunctionRuntimeManager mockedFunctionRunTimeManager;
    protected RuntimeFactory mockedRuntimeFactory;
    protected Namespace mockedNamespace;
    protected InputStream mockedInputStream;
    protected FormDataContentDisposition mockedFormData;
    protected Function.FunctionMetaData mockedFunctionMetaData;
    protected LeaderService mockedLeaderService;
    protected Packages mockedPackages;
    protected PulsarFunctionTestTemporaryDirectory tempDirectory;
    protected ConnectorsManager connectorsManager = new ConnectorsManager();
    protected FunctionsManager functionsManager = new FunctionsManager();

    public static File getPulsarIOCassandraNar() {
        return new File(Objects.requireNonNull(System.getProperty(SYSTEM_PROPERTY_NAME_CASSANDRA_NAR_FILE_PATH)
                , "pulsar-io-cassandra.nar file location must be specified with "
                        + SYSTEM_PROPERTY_NAME_CASSANDRA_NAR_FILE_PATH + " system property"));
    }

    public static File getPulsarIOTwitterNar() {
        return new File(Objects.requireNonNull(System.getProperty(SYSTEM_PROPERTY_NAME_TWITTER_NAR_FILE_PATH)
                , "pulsar-io-twitter.nar file location must be specified with "
                        + SYSTEM_PROPERTY_NAME_TWITTER_NAR_FILE_PATH + " system property"));
    }

    public static File getPulsarIOInvalidNar() {
        return new File(Objects.requireNonNull(System.getProperty(SYSTEM_PROPERTY_NAME_INVALID_NAR_FILE_PATH)
                , "invalid nar file location must be specified with "
                        + SYSTEM_PROPERTY_NAME_INVALID_NAR_FILE_PATH + " system property"));
    }

    public static File getPulsarApiExamplesNar() {
        return new File(Objects.requireNonNull(
                System.getProperty(SYSTEM_PROPERTY_NAME_FUNCTIONS_API_EXAMPLES_NAR_FILE_PATH)
                , "pulsar-functions-api-examples.nar file location must be specified with "
                        + SYSTEM_PROPERTY_NAME_FUNCTIONS_API_EXAMPLES_NAR_FILE_PATH + " system property"));
    }

    @BeforeMethod
    public final void setup(Method method) throws Exception {
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
            Files.copy(getDefaultNarFile().toPath(), Paths.get(invocationOnMock.getArgument(1, String.class)),
                    StandardCopyOption.REPLACE_EXISTING);
            return null;
        }).when(mockedPackages).download(any(), any());

        // worker config
        List<String> urlPatterns =
                Arrays.asList("http://localhost.*", "file:.*", "https://repo1.maven.org/maven2/org/apache/pulsar/.*");
        WorkerConfig workerConfig = new WorkerConfig()
                .setWorkerId("test")
                .setWorkerPort(8080)
                .setFunctionMetadataTopicName("pulsar/functions")
                .setNumFunctionPackageReplicas(3)
                .setPulsarServiceUrl("pulsar://localhost:6650/")
                .setAdditionalEnabledFunctionsUrlPatterns(urlPatterns)
                .setAdditionalEnabledConnectorUrlPatterns(urlPatterns);
        customizeWorkerConfig(workerConfig, method);
        tempDirectory = PulsarFunctionTestTemporaryDirectory.create(getClass().getSimpleName());
        tempDirectory.useTemporaryDirectoriesForWorkerConfig(workerConfig);
        when(mockedWorkerService.getWorkerConfig()).thenReturn(workerConfig);
        when(mockedWorkerService.getFunctionsManager()).thenReturn(functionsManager);
        when(mockedWorkerService.getConnectorsManager()).thenReturn(connectorsManager);
        PackageUrlValidator packageUrlValidator = new PackageUrlValidator(workerConfig);
        when(mockedWorkerService.getPackageUrlValidator()).thenReturn(packageUrlValidator);

        doSetup();
    }

    protected void customizeWorkerConfig(WorkerConfig workerConfig, Method method) {

    }

    protected File getDefaultNarFile() {
        return getPulsarIOTwitterNar();
    }

    protected void doSetup() throws Exception {

    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (tempDirectory != null) {
            tempDirectory.delete();
        }
        mockStaticContexts.values().forEach(MockedStatic::close);
        mockStaticContexts.clear();
    }

    protected <T> void mockStatic(Class<T> classStatic, Consumer<MockedStatic<T>> consumer) {
        mockStatic(classStatic, withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS), consumer);
    }

    private <T> void mockStatic(Class<T> classStatic, MockSettings mockSettings, Consumer<MockedStatic<T>> consumer) {
        final MockedStatic<T> mockedStatic = mockStaticContexts.computeIfAbsent(classStatic.getName(),
                name -> Mockito.mockStatic(classStatic, mockSettings));
        consumer.accept(mockedStatic);
    }

    protected void mockWorkerUtils() {
        mockWorkerUtils(null);
    }

    protected void mockWorkerUtils(Consumer<MockedStatic<WorkerUtils>> consumer) {
        mockStatic(WorkerUtils.class, withSettings(), ctx -> {
            // make dumpToTmpFile return the nar file copy
            ctx.when(() -> WorkerUtils.dumpToTmpFile(mockedInputStream))
                    .thenAnswer(invocation -> {
                        Path tempFile = Files.createTempFile("test", ".nar");
                        Files.copy(getPulsarApiExamplesNar().toPath(), tempFile,
                                StandardCopyOption.REPLACE_EXISTING);
                        return tempFile.toFile();
                    });
            ctx.when(() -> WorkerUtils.dumpToTmpFile(any()))
                    .thenAnswer(Answers.CALLS_REAL_METHODS);
            if (consumer != null) {
                consumer.accept(ctx);
            }
        });
    }

    protected void mockInstanceUtils() {
        mockStatic(InstanceUtils.class, ctx -> {
            ctx.when(() -> InstanceUtils.calculateSubjectType(any()))
                    .thenReturn(getComponentType());
        });
    }

    protected abstract Function.FunctionDetails.ComponentType getComponentType();

    public static class LoadedConnector extends Connector {
        public LoadedConnector(ConnectorDefinition connectorDefinition) {
            super(null, connectorDefinition, null, true);
        }

        @Override
        public ValidatableFunctionPackage getConnectorFunctionPackage() {
            return new LoadedFunctionPackage(getClass().getClassLoader(), ConnectorDefinition.class,
                    getConnectorDefinition());
        }
    }


    protected void registerBuiltinConnector(String connectorType, String className) {
        ConnectorDefinition connectorDefinition = null;
        if (className != null) {
            connectorDefinition = new ConnectorDefinition();
            // set source and sink class to the same to simplify the test
            connectorDefinition.setSinkClass(className);
            connectorDefinition.setSourceClass(className);
        }
        connectorsManager.addConnector(connectorType, new LoadedConnector(connectorDefinition));
    }

    protected void registerBuiltinConnector(String connectorType, File packageFile) {
        ConnectorDefinition cntDef;
        try {
            cntDef = ConnectorUtils.getConnectorDefinition(packageFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        connectorsManager.addConnector(connectorType,
                new Connector(packageFile.toPath(), cntDef, NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, true));
    }

    public static class LoadedFunctionArchive extends FunctionArchive {
        public LoadedFunctionArchive(FunctionDefinition functionDefinition) {
            super(null, functionDefinition, null, true);
        }

        @Override
        public ValidatableFunctionPackage getFunctionPackage() {
            return new LoadedFunctionPackage(getClass().getClassLoader(), FunctionDefinition.class,
                    getFunctionDefinition());
        }
    }

    protected void registerBuiltinFunction(String functionType, String className) {
        FunctionDefinition functionDefinition = null;
        if (className != null) {
            functionDefinition = new FunctionDefinition();
            functionDefinition.setFunctionClass(className);
        }
        functionsManager.addFunction(functionType, new LoadedFunctionArchive(functionDefinition));
    }

    protected void registerBuiltinFunction(String functionType, File packageFile) {
        FunctionDefinition cntDef;
        try {
            cntDef = FunctionUtils.getFunctionDefinition(packageFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        functionsManager.addFunction(functionType,
                new FunctionArchive(packageFile.toPath(), cntDef, NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR, true));
    }
}
