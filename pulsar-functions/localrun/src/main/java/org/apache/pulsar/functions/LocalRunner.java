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
package org.apache.pulsar.functions;

import static org.apache.pulsar.common.functions.Utils.inferMissingArguments;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.nar.FileUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.stats.FunctionCollectorRegistry;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.runtime.RuntimeUtils;
import org.apache.pulsar.functions.runtime.process.ProcessRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsprovider.EnvironmentBasedSecretsProvider;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.DefaultSecretsProviderConfigurator;
import org.apache.pulsar.functions.secretsproviderconfigurator.NameAndConfigBasedSecretsProviderConfigurator;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry;
import org.apache.pulsar.functions.utils.functions.FunctionUtils;
import org.apache.pulsar.functions.utils.functions.Functions;
import org.apache.pulsar.functions.utils.io.Connector;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.WorkerConfig;

@Slf4j
public class LocalRunner implements AutoCloseable {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<RuntimeSpawner> spawners = new LinkedList<>();
    private final String narExtractionDirectory;
    private final File narExtractionDirectoryCreated;
    private final String connectorsDir;
    private final Thread shutdownHook;
    private ClassLoader userCodeClassLoader;
    private boolean userCodeClassLoaderCreated;
    private RuntimeFactory runtimeFactory;
    private HTTPServer metricsServer;

    public enum RuntimeEnv {
        THREAD,
        PROCESS
    }

    public static class FunctionConfigConverter implements IStringConverter<FunctionConfig> {
        @Override
        public FunctionConfig convert(String value) {
            try {
                return ObjectMapperFactory.getThreadLocal().readValue(value, FunctionConfig.class);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse function config:", e);
            }
        }
    }

    public static class SourceConfigConverter implements IStringConverter<SourceConfig> {
        @Override
        public SourceConfig convert(String value) {
            try {
                return ObjectMapperFactory.getThreadLocal().readValue(value, SourceConfig.class);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse source config:", e);
            }
        }
    }

    public static class SinkConfigConverter implements IStringConverter<SinkConfig> {
        @Override
        public SinkConfig convert(String value) {
            try {
                return ObjectMapperFactory.getThreadLocal().readValue(value, SinkConfig.class);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse sink config:", e);
            }
        }
    }

    public static class RuntimeConverter implements IStringConverter<RuntimeEnv> {
        @Override
        public RuntimeEnv convert(String value) {
            return RuntimeEnv.valueOf(value);
        }
    }

    @Parameter(names = "--functionConfig", description = "The json representation of FunctionConfig", hidden = true, converter = FunctionConfigConverter.class)
    protected FunctionConfig functionConfig;
    @Parameter(names = "--sourceConfig", description = "The json representation of SourceConfig", hidden = true, converter = SourceConfigConverter.class)
    protected SourceConfig sourceConfig;
    @Parameter(names = "--sinkConfig", description = "The json representation of SinkConfig", hidden = true, converter = SinkConfigConverter.class)
    protected SinkConfig sinkConfig;
    @Parameter(names = "--stateStorageServiceUrl", description = "The URL for the state storage service (by default Apache BookKeeper)", hidden = true)
    protected String stateStorageServiceUrl;
    @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker", hidden = true)
    protected String brokerServiceUrl;
    @Parameter(names = "--webServiceUrl", description = "The URL for the Pulsar web service", hidden = true)
    protected String webServiceUrl = null;
    @Parameter(names = "--clientAuthPlugin", description = "Client authentication plugin using which function-process can connect to broker", hidden = true)
    protected String clientAuthPlugin;
    @Parameter(names = "--clientAuthParams", description = "Client authentication param", hidden = true)
    protected String clientAuthParams;
    @Parameter(names = "--useTls", description = "Use tls connection\n", hidden = true, arity = 1)
    protected boolean useTls;
    @Parameter(names = "--tlsAllowInsecureConnection", description = "Allow insecure tls connection\n", hidden = true, arity = 1)
    protected boolean tlsAllowInsecureConnection;
    @Parameter(names = "--tlsHostNameVerificationEnabled", description = "Enable hostname verification", hidden = true, arity = 1)
    protected boolean tlsHostNameVerificationEnabled;
    @Parameter(names = "--tlsTrustCertFilePath", description = "tls trust cert file path", hidden = true)
    protected String tlsTrustCertFilePath;
    @Parameter(names = "--instanceIdOffset", description = "Start the instanceIds from this offset", hidden = true)
    protected int instanceIdOffset = 0;
    @Parameter(names = "--runtime", description = "Function runtime to use (Thread/Process)", hidden = true, converter = RuntimeConverter.class)
    protected RuntimeEnv runtimeEnv;
    @Parameter(names = "--secretsProviderClassName", description = "Whats the classname of secrets provider", hidden = true)
    protected String secretsProviderClassName;
    @Parameter(names = "--secretsProviderConfig", description = "Whats the config for the secrets provider", hidden = true)
    protected String secretsProviderConfig;
    @Parameter(names = "--metricsPortStart", description = "The starting port range for metrics server. When running instances as threads, one metrics server is used to host the stats for all instances.", hidden = true)
    protected Integer metricsPortStart;

    private static final String DEFAULT_SERVICE_URL = "pulsar://localhost:6650";
    private static final String DEFAULT_WEB_SERVICE_URL = "http://localhost:8080";

    public static void main(String[] args) throws Exception {
        LocalRunner localRunner = LocalRunner.builder().build();
        JCommander jcommander = new JCommander(localRunner);
        jcommander.setProgramName("LocalRunner");

        // parse args by JCommander
        jcommander.parse(args);
        try {
            localRunner.start(true);
        } catch (Exception e) {
            log.error("Encountered error starting localrunner", e);
            localRunner.close();
        }
    }

    @Builder
    public LocalRunner(FunctionConfig functionConfig, SourceConfig sourceConfig, SinkConfig sinkConfig, String
            stateStorageServiceUrl, String brokerServiceUrl, String clientAuthPlugin, String clientAuthParams,
                       boolean useTls, boolean tlsAllowInsecureConnection, boolean tlsHostNameVerificationEnabled,
                       String tlsTrustCertFilePath, int instanceIdOffset, RuntimeEnv runtimeEnv,
                       String secretsProviderClassName, String secretsProviderConfig, String narExtractionDirectory,
                       String connectorsDirectory, Integer metricsPortStart) {
        this.functionConfig = functionConfig;
        this.sourceConfig = sourceConfig;
        this.sinkConfig = sinkConfig;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.brokerServiceUrl = brokerServiceUrl;
        this.clientAuthPlugin = clientAuthPlugin;
        this.clientAuthParams = clientAuthParams;
        this.useTls = useTls;
        this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
        this.tlsHostNameVerificationEnabled = tlsHostNameVerificationEnabled;
        this.tlsTrustCertFilePath = tlsTrustCertFilePath;
        this.instanceIdOffset = instanceIdOffset;
        this.runtimeEnv = runtimeEnv;
        this.secretsProviderClassName = secretsProviderClassName;
        this.secretsProviderConfig = secretsProviderConfig;
        if (narExtractionDirectory != null) {
            this.narExtractionDirectoryCreated = null;
            this.narExtractionDirectory = narExtractionDirectory;
        } else {
            this.narExtractionDirectoryCreated = createNarExtractionTempDirectory();
            this.narExtractionDirectory = this.narExtractionDirectoryCreated.getAbsolutePath();
        }
        if (connectorsDirectory != null) {
            this.connectorsDir = connectorsDirectory;
        } else {
            String pulsarHome = System.getenv("PULSAR_HOME");
            if (pulsarHome == null) {
                pulsarHome = Paths.get("").toAbsolutePath().toString();
            }
            this.connectorsDir = Paths.get(pulsarHome, "connectors").toString();
        }
        this.metricsPortStart = metricsPortStart;
        shutdownHook = new Thread(() -> {
            try {
                LocalRunner.this.close();
            } catch (Exception exception) {
                log.warn("Encountered exception when closing localrunner", exception);
            }
        });
    }

    private static File createNarExtractionTempDirectory() {
        try {
            return Files.createTempDirectory("pulsar_localrunner_nars_").toFile();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create temp directory", e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            stop();
        } finally {
            if (narExtractionDirectoryCreated != null) {
                FileUtils.deleteFile(narExtractionDirectoryCreated, true);
            }
        }
    }

    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException e) {
                // ignore possible "Shutdown in progress"
            }

            if (metricsServer != null) {
                metricsServer.stop();
            }

            for (RuntimeSpawner spawner : spawners) {
                spawner.close();
            }
            spawners.clear();

            if (runtimeFactory != null) {
                runtimeFactory.close();
                runtimeFactory = null;
            }

            if (userCodeClassLoaderCreated) {
                if (userCodeClassLoader instanceof Closeable) {
                    try {
                        ((Closeable) userCodeClassLoader).close();
                    } catch (IOException e) {
                        log.warn("Error closing classloader", e);
                    }
                }
                userCodeClassLoaderCreated = false;
                userCodeClassLoader = null;
            }
        }
    }

    public void start(boolean blocking) throws Exception {
        List<RuntimeSpawner> local = new LinkedList<>();
        synchronized (this) {
            if (!running.compareAndSet(false, true)) {
                throw new IllegalArgumentException("Pulsar Function local run already started!");
            }
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            Function.FunctionDetails functionDetails;
            String userCodeFile = null;
            int parallelism;
            if (functionConfig != null) {
                FunctionConfigUtils.inferMissingArguments(functionConfig, true);
                parallelism = functionConfig.getParallelism();
                if (functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA) {
                    userCodeFile = functionConfig.getJar();

                    boolean isBuiltin = !StringUtils.isEmpty(functionConfig.getJar())
                            && functionConfig.getJar().startsWith(Utils.BUILTIN);
                    if (isBuiltin){
                        WorkerConfig workerConfig = WorkerConfig.load(System.getenv("PULSAR_HOME") + "/conf/functions_worker.yml");
                        Functions functions = FunctionUtils.searchForFunctions(System.getenv("PULSAR_HOME") + workerConfig.getFunctionsDirectory().replaceFirst("^.", ""));
                        String functionType = functionConfig.getJar().replaceFirst("^builtin://", "");
                        userCodeFile = functions.getFunctions().get(functionType).toString();
                    }

                    if (Utils.isFunctionPackageUrlSupported(userCodeFile)) {
                        File file = FunctionCommon.extractFileFromPkgURL(userCodeFile);
                        userCodeClassLoader = FunctionConfigUtils.validate(functionConfig, file);
                        userCodeClassLoaderCreated = true;
                    } else if (userCodeFile != null) {
                        File file = new File(userCodeFile);
                        if (!file.exists()) {
                            throw new RuntimeException("User jar does not exist");
                        }
                        userCodeClassLoader = FunctionConfigUtils.validate(functionConfig, file);
                        userCodeClassLoaderCreated = true;
                    } else {
                        if (!(runtimeEnv == null || runtimeEnv == RuntimeEnv.THREAD)) {
                            throw new IllegalStateException("The jar property must be specified in FunctionConfig.");
                        }
                        FunctionConfigUtils.validateJavaFunction(functionConfig, Thread.currentThread()
                                .getContextClassLoader());
                    }
                } else if (functionConfig.getRuntime() == FunctionConfig.Runtime.GO) {
                    userCodeFile = functionConfig.getGo();
                } else if (functionConfig.getRuntime() == FunctionConfig.Runtime.PYTHON) {
                    userCodeFile = functionConfig.getPy();
                } else {
                    throw new UnsupportedOperationException();
                }

                functionDetails = FunctionConfigUtils.convert(functionConfig,
                        userCodeClassLoader != null ? userCodeClassLoader :
                                Thread.currentThread().getContextClassLoader());
            } else if (sourceConfig != null) {
                inferMissingArguments(sourceConfig);
                userCodeFile = sourceConfig.getArchive();
                parallelism = sourceConfig.getParallelism();

                ClassLoader builtInSourceClassLoader = userCodeFile != null ? isBuiltInSource(userCodeFile) : null;
                if (builtInSourceClassLoader != null) {
                    functionDetails = SourceConfigUtils.convert(
                            sourceConfig, SourceConfigUtils.validateAndExtractDetails(
                                    sourceConfig, builtInSourceClassLoader, true));
                    userCodeClassLoader = builtInSourceClassLoader;
                } else if (userCodeFile != null && Utils.isFunctionPackageUrlSupported(userCodeFile)) {
                    File file = FunctionCommon.extractFileFromPkgURL(userCodeFile);
                    ClassLoader sourceClassLoader = FunctionCommon.getClassLoaderFromPackage(
                            Function.FunctionDetails.ComponentType.SOURCE,
                            sourceConfig.getClassName(), file, narExtractionDirectory);
                    functionDetails = SourceConfigUtils.convert(
                            sourceConfig,
                            SourceConfigUtils.validateAndExtractDetails(sourceConfig, sourceClassLoader, true));
                    userCodeClassLoader = sourceClassLoader;
                    userCodeClassLoaderCreated = true;
                } else if (userCodeFile != null) {
                    File file = new File(userCodeFile);
                    if (!file.exists()) {
                        throw new RuntimeException("Source archive (" + userCodeFile + ") does not exist");
                    }
                    ClassLoader sourceClassLoader = FunctionCommon.getClassLoaderFromPackage(
                            Function.FunctionDetails.ComponentType.SOURCE,
                            sourceConfig.getClassName(), file, narExtractionDirectory);
                    functionDetails = SourceConfigUtils.convert(
                            sourceConfig, SourceConfigUtils.validateAndExtractDetails(sourceConfig, sourceClassLoader, true));
                    userCodeClassLoader = sourceClassLoader;
                    userCodeClassLoaderCreated = true;
                } else {
                    if (!(runtimeEnv == null || runtimeEnv == RuntimeEnv.THREAD)) {
                        throw new IllegalStateException("The archive property must be specified in SourceConfig.");
                    }
                    functionDetails = SourceConfigUtils.convert(
                            sourceConfig, SourceConfigUtils.validateAndExtractDetails(
                                    sourceConfig, Thread.currentThread().getContextClassLoader(), true));
                }
            } else if (sinkConfig != null) {
                inferMissingArguments(sinkConfig);
                userCodeFile = sinkConfig.getArchive();
                parallelism = sinkConfig.getParallelism();

                ClassLoader builtInSinkClassLoader = userCodeFile != null ? isBuiltInSink(userCodeFile) : null;
                if (builtInSinkClassLoader != null) {
                    functionDetails = SinkConfigUtils.convert(
                            sinkConfig, SinkConfigUtils.validateAndExtractDetails(
                                    sinkConfig, builtInSinkClassLoader, true));
                    userCodeClassLoader = builtInSinkClassLoader;
                } else if (Utils.isFunctionPackageUrlSupported(userCodeFile)) {
                    File file = FunctionCommon.extractFileFromPkgURL(userCodeFile);
                    ClassLoader sinkClassLoader = FunctionCommon.getClassLoaderFromPackage(
                            Function.FunctionDetails.ComponentType.SINK,
                            sinkConfig.getClassName(), file, narExtractionDirectory);
                    functionDetails = SinkConfigUtils.convert(
                            sinkConfig, SinkConfigUtils.validateAndExtractDetails(sinkConfig, sinkClassLoader, true));
                    userCodeClassLoader = sinkClassLoader;
                    userCodeClassLoaderCreated = true;
                } else if (userCodeFile != null) {
                    File file = new File(userCodeFile);
                    if (!file.exists()) {
                        throw new RuntimeException("Sink archive does not exist");
                    }
                    ClassLoader sinkClassLoader = FunctionCommon.getClassLoaderFromPackage(
                            Function.FunctionDetails.ComponentType.SINK,
                            sinkConfig.getClassName(), file, narExtractionDirectory);
                    functionDetails = SinkConfigUtils.convert(
                            sinkConfig, SinkConfigUtils.validateAndExtractDetails(sinkConfig, sinkClassLoader,  true));
                    userCodeClassLoader = sinkClassLoader;
                    userCodeClassLoaderCreated = true;
                } else {
                    if (!(runtimeEnv == null || runtimeEnv == RuntimeEnv.THREAD)) {
                        throw new IllegalStateException("The archive property must be specified in SourceConfig.");
                    }
                    functionDetails = SinkConfigUtils.convert(
                            sinkConfig, SinkConfigUtils.validateAndExtractDetails(
                                    sinkConfig, Thread.currentThread().getContextClassLoader(), true));
                }
            } else {
                throw new IllegalArgumentException("Must specify Function, Source or Sink config");
            }

            if (System.getProperty(FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY) == null) {
                System.setProperty(FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY,
                        LocalRunner.class.getProtectionDomain().getCodeSource().getLocation().getFile());
            }

            AuthenticationConfig authConfig = AuthenticationConfig.builder().clientAuthenticationPlugin
                    (clientAuthPlugin)
                    .clientAuthenticationParameters(clientAuthParams).useTls(useTls)
                    .tlsAllowInsecureConnection(tlsAllowInsecureConnection)
                    .tlsHostnameVerificationEnable(tlsHostNameVerificationEnabled)
                    .tlsTrustCertsFilePath(tlsTrustCertFilePath).build();

            String serviceUrl = DEFAULT_SERVICE_URL;
            if (brokerServiceUrl != null) {
                serviceUrl = brokerServiceUrl;
            }
            if (webServiceUrl == null) {
                webServiceUrl = DEFAULT_WEB_SERVICE_URL;
            }

            if ((sourceConfig != null || sinkConfig != null || functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA)
                    && (runtimeEnv == null || runtimeEnv == RuntimeEnv.THREAD)) {
                // By default run java functions as threads
                startThreadedMode(functionDetails, parallelism, instanceIdOffset, serviceUrl,
                        stateStorageServiceUrl, authConfig, userCodeFile);
            } else {
                startProcessMode(functionDetails, parallelism, instanceIdOffset, serviceUrl,
                        stateStorageServiceUrl, authConfig, userCodeFile);
            }
            local.addAll(spawners);
        }

        if (blocking) {
            for (RuntimeSpawner spawner : local) {
                spawner.join();
                log.info("RuntimeSpawner quit because of", spawner.getRuntime().getDeathException());
            }
        }
    }

    private void startProcessMode(org.apache.pulsar.functions.proto.Function.FunctionDetails functionDetails,
                                           int parallelism, int instanceIdOffset, String serviceUrl,
                                           String stateStorageServiceUrl, AuthenticationConfig authConfig,
                                           String userCodeFile) throws Exception {
        SecretsProviderConfigurator secretsProviderConfigurator = getSecretsProviderConfigurator();
        runtimeFactory = new ProcessRuntimeFactory(
                serviceUrl,
                webServiceUrl,
                stateStorageServiceUrl,
                authConfig,
                null, /* java instance jar file */
                null, /* python instance file */
                null, /* log directory */
                null, /* extra dependencies dir */
                narExtractionDirectory, /* nar extraction dir */
                secretsProviderConfigurator,
                false, Optional.empty(), Optional.empty());

        for (int i = 0; i < parallelism; ++i) {
            InstanceConfig instanceConfig = new InstanceConfig();
            instanceConfig.setFunctionDetails(functionDetails);
            // TODO: correctly implement function version and id
            instanceConfig.setFunctionVersion(UUID.randomUUID().toString());
            instanceConfig.setFunctionId(UUID.randomUUID().toString());
            instanceConfig.setInstanceId(i + instanceIdOffset);
            instanceConfig.setMaxBufferedTuples(1024);
            instanceConfig.setPort(FunctionCommon.findAvailablePort());

            if (metricsPortStart != null) {
                int metricsPort = metricsPortStart + i;
                if (metricsPortStart < 0 || metricsPortStart > 65535) {
                    throw new IllegalArgumentException("Metrics port need to be within the range of 0 and 65535");
                }
                instanceConfig.setMetricsPort(metricsPort);
            } else {
                instanceConfig.setMetricsPort(FunctionCommon.findAvailablePort());
            }
            instanceConfig.setClusterName("local");
            if (functionConfig != null) {
                instanceConfig.setMaxPendingAsyncRequests(functionConfig.getMaxPendingAsyncRequests());
                if (functionConfig.getExposePulsarAdminClientEnabled() != null) {
                    instanceConfig.setExposePulsarAdminClientEnabled(functionConfig.getExposePulsarAdminClientEnabled());
                }
            }
            RuntimeSpawner runtimeSpawner = new RuntimeSpawner(
                    instanceConfig,
                    userCodeFile,
                    null,
                    runtimeFactory,
                    30000);
            spawners.add(runtimeSpawner);
            runtimeSpawner.start();
        }
        Timer statusCheckTimer = new Timer();
        statusCheckTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                CompletableFuture<String>[] futures = new CompletableFuture[spawners.size()];
                int index = 0;
                for (RuntimeSpawner spawner : spawners) {
                    futures[index] = spawner.getFunctionStatusAsJson(index);
                    index++;
                }
                try {
                    CompletableFuture.allOf(futures).get(5, TimeUnit.SECONDS);
                    for (index = 0; index < futures.length; ++index) {
                        String json = futures[index].get();
                        Gson gson = new GsonBuilder().setPrettyPrinting().create();
                        log.info(gson.toJson(new JsonParser().parse(json)));
                    }
                } catch (TimeoutException | InterruptedException | ExecutionException e) {
                    log.error("Could not get status from all local instances");
                }
            }
        }, 30000, 30000);
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(() -> statusCheckTimer.cancel()));
    }


    private void startThreadedMode(org.apache.pulsar.functions.proto.Function.FunctionDetails functionDetails,
                                           int parallelism, int instanceIdOffset, String serviceUrl,
                                           String stateStorageServiceUrl, AuthenticationConfig authConfig,
                                           String userCodeFile) throws Exception {

        if (metricsPortStart != null) {
            if (metricsPortStart < 0 || metricsPortStart > 65535) {
                throw new IllegalArgumentException("Metrics port need to be within the range of 0 and 65535");
            }
        }

        SecretsProvider secretsProvider;
        if (secretsProviderClassName != null) {
            secretsProvider = (SecretsProvider) Reflections.createInstance(secretsProviderClassName, ClassLoader.getSystemClassLoader());
            Map<String, String> config = null;
            if (secretsProviderConfig != null) {
                config = (Map<String, String>)new Gson().fromJson(secretsProviderConfig, Map.class);
            }
            secretsProvider.init(config);
        } else {
            secretsProvider = new ClearTextSecretsProvider();
        }
        boolean exposePulsarAdminClientEnabled = false;
        if (functionConfig != null && functionConfig.getExposePulsarAdminClientEnabled() != null) {
            exposePulsarAdminClientEnabled = functionConfig.getExposePulsarAdminClientEnabled();
        }

        // Collector Registry for prometheus metrics
        FunctionCollectorRegistry collectorRegistry = FunctionCollectorRegistry.getDefaultImplementation();
        RuntimeUtils.registerDefaultCollectors(collectorRegistry);

        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            if (userCodeClassLoader != null) {
                Thread.currentThread().setContextClassLoader(userCodeClassLoader);
            }
            runtimeFactory = new ThreadRuntimeFactory("LocalRunnerThreadGroup",
                    serviceUrl,
                    stateStorageServiceUrl,
                    authConfig,
                    secretsProvider,
                    collectorRegistry, narExtractionDirectory,
                    null,
                    exposePulsarAdminClientEnabled, webServiceUrl);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
        for (int i = 0; i < parallelism; ++i) {
            InstanceConfig instanceConfig = new InstanceConfig();
            instanceConfig.setFunctionDetails(functionDetails);
            // TODO: correctly implement function version and id
            instanceConfig.setFunctionVersion(UUID.randomUUID().toString());
            instanceConfig.setFunctionId(UUID.randomUUID().toString());
            instanceConfig.setInstanceId(i + instanceIdOffset);
            instanceConfig.setMaxBufferedTuples(1024);
            if (metricsPortStart != null) {
                instanceConfig.setMetricsPort(metricsPortStart);
            }
            instanceConfig.setClusterName("local");
            if (functionConfig != null) {
                instanceConfig.setMaxPendingAsyncRequests(functionConfig.getMaxPendingAsyncRequests());
                if (functionConfig.getExposePulsarAdminClientEnabled() != null) {
                    instanceConfig.setExposePulsarAdminClientEnabled(functionConfig.getExposePulsarAdminClientEnabled());
                }
            }

            RuntimeSpawner runtimeSpawner = new RuntimeSpawner(
                    instanceConfig,
                    userCodeFile,
                    null,
                    runtimeFactory,
                    30000);
            spawners.add(runtimeSpawner);
            runtimeSpawner.start();
        }
        if (metricsPortStart != null) {
            // starting metrics server
            log.info("Starting metrics server on port {}", metricsPortStart);
            metricsServer = new HTTPServer(new InetSocketAddress(metricsPortStart), collectorRegistry, true);
        }
    }

    private ClassLoader isBuiltInSource(String sourceType) throws IOException {
        // Validate the connector type from the locally available connectors
        TreeMap<String, Connector> connectors = getConnectors();

        String source = sourceType.replaceFirst("^builtin://", "");
        Connector connector = connectors.get(source);
        if (connector != null && connector.getConnectorDefinition().getSourceClass() != null) {
            // Source type is a valid built-in connector type.
            return connector.getClassLoader();
        } else {
            return null;
        }
    }

    private ClassLoader isBuiltInSink(String sinkType) throws IOException {
        // Validate the connector type from the locally available connectors
        TreeMap<String, Connector> connectors = getConnectors();

        String sink = sinkType.replaceFirst("^builtin://", "");
        Connector connector = connectors.get(sink);
        if (connector != null && connector.getConnectorDefinition().getSinkClass() != null) {
            // Sink type is a valid built-in connector type
            return connector.getClassLoader();
        } else {
            return null;
        }
    }

    private TreeMap<String, Connector> getConnectors() throws IOException {
        return ConnectorUtils.searchForConnectors(connectorsDir, narExtractionDirectory);
    }

    private SecretsProviderConfigurator getSecretsProviderConfigurator() {
        SecretsProviderConfigurator secretsProviderConfigurator;
        if (secretsProviderClassName != null) {
            Map<String, String> config = null;
            if (secretsProviderConfig != null) {
                config = (Map<String, String>)new Gson().fromJson(secretsProviderConfig, Map.class);
            }
            secretsProviderConfigurator = new NameAndConfigBasedSecretsProviderConfigurator(secretsProviderClassName, config);
        } else {
            secretsProviderConfigurator = new DefaultSecretsProviderConfigurator();
        }
        return secretsProviderConfigurator;
    }
}
