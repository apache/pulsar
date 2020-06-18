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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.process.ProcessRuntimeFactory;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.DefaultSecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry;
import org.apache.pulsar.functions.utils.functions.FunctionUtils;
import org.apache.pulsar.functions.utils.functions.Functions;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.utils.io.Connectors;
import org.apache.pulsar.functions.worker.WorkerConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.pulsar.common.functions.Utils.inferMissingArguments;

@Slf4j
public class LocalRunner {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<RuntimeSpawner> spawners = new LinkedList<>();
    private String narExtractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;

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

    private static final String DEFAULT_SERVICE_URL = "pulsar://localhost:6650";

    public static void main(String[] args) throws Exception {
        LocalRunner localRunner = LocalRunner.builder().build();
        JCommander jcommander = new JCommander(localRunner);
        jcommander.setProgramName("LocalRunner");

        // parse args by JCommander
        jcommander.parse(args);
        localRunner.start(true);
    }

    @Builder
    public LocalRunner(FunctionConfig functionConfig, SourceConfig sourceConfig, SinkConfig sinkConfig, String
            stateStorageServiceUrl, String brokerServiceUrl, String clientAuthPlugin, String clientAuthParams,
                       boolean useTls, boolean tlsAllowInsecureConnection, boolean tlsHostNameVerificationEnabled,
                       String tlsTrustCertFilePath, int instanceIdOffset, RuntimeEnv runtimeEnv) {
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

        java.lang.Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LocalRunner.this.stop();
            }
        });
    }

    public synchronized void stop() {
        running.set(false);
        log.info("Shutting down the localrun runtimeSpawner ...");
        for (RuntimeSpawner spawner : spawners) {
            spawner.close();
        }
        spawners.clear();
    }

    public void start(boolean blocking) throws Exception {
        List<RuntimeSpawner> local = new LinkedList<>();
        synchronized (this) {
            if (running.get() == true) {
                throw new IllegalArgumentException("Pulsar Function local run already started!");
            }

            Function.FunctionDetails functionDetails;
            String userCodeFile;
            int parallelism;
            if (functionConfig != null) {
                FunctionConfigUtils.inferMissingArguments(functionConfig);
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                parallelism = functionConfig.getParallelism();
                if (functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA) {
                    userCodeFile = functionConfig.getJar();

                    // if code file not specified try to get location of the code based on class.
                    if (userCodeFile == null && functionConfig.getClassName() != null) {
                        userCodeFile = Thread.currentThread().getContextClassLoader()
                                .loadClass(functionConfig.getClassName())
                                .getProtectionDomain().getCodeSource().getLocation().getFile();
                    }

                    boolean isBuiltin = !org.apache.commons.lang3.StringUtils.isEmpty(functionConfig.getJar()) && functionConfig.getJar().startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN);
                    if(isBuiltin){
                        WorkerConfig workerConfig = WorkerConfig.load(System.getenv("PULSAR_HOME") + "/conf/functions_worker.yml");
                        Functions functions = FunctionUtils.searchForFunctions(System.getenv("PULSAR_HOME") + workerConfig.getFunctionsDirectory().replaceFirst("^.", ""));
                        String functionType = functionConfig.getJar().replaceFirst("^builtin://", "");
                        userCodeFile = functions.getFunctions().get(functionType).toString();
                    }
                     
                    if (org.apache.pulsar.common.functions.Utils.isFunctionPackageUrlSupported(userCodeFile)) {
                        File file = FunctionCommon.extractFileFromPkgURL(userCodeFile);
                        classLoader = FunctionConfigUtils.validate(functionConfig, file);
                    } else {
                        File file = new File(userCodeFile);
                        if (!file.exists()) {
                            throw new RuntimeException("User jar does not exist");
                        }
                        classLoader = FunctionConfigUtils.validate(functionConfig, file);
                    }

                } else if (functionConfig.getRuntime() == FunctionConfig.Runtime.GO) {
                    userCodeFile = functionConfig.getGo();
                } else if (functionConfig.getRuntime() == FunctionConfig.Runtime.PYTHON) {
                    userCodeFile = functionConfig.getPy();
                } else {
                    throw new UnsupportedOperationException();
                }

                functionDetails = FunctionConfigUtils.convert(functionConfig, classLoader);
            } else if (sourceConfig != null) {
                inferMissingArguments(sourceConfig);
                userCodeFile = sourceConfig.getArchive();

                // if code file not specified try to get location of the code based on class.
                if (userCodeFile == null && sourceConfig.getClassName() != null) {
                    userCodeFile = Thread.currentThread().getContextClassLoader()
                            .loadClass(sourceConfig.getClassName())
                            .getProtectionDomain().getCodeSource().getLocation().getFile();
                }

                if (userCodeFile == null) {
                    userCodeFile = Thread.currentThread().getContextClassLoader()
                            .loadClass(LocalRunner.class.getName())
                            .getProtectionDomain().getCodeSource().getLocation().getFile();
                }

                String builtInSource = isBuiltInSource(userCodeFile);
                if (builtInSource != null) {
                    sourceConfig.setArchive(builtInSource);
                }
                parallelism = sourceConfig.getParallelism();

                if (org.apache.pulsar.common.functions.Utils.isFunctionPackageUrlSupported(userCodeFile)) {
                    File file = FunctionCommon.extractFileFromPkgURL(userCodeFile);
                    functionDetails = SourceConfigUtils.convert(sourceConfig, SourceConfigUtils.validate(sourceConfig, null, file, narExtractionDirectory, true));

                } else {
                    File file = new File(userCodeFile);
                    if (!file.exists()) {
                        throw new RuntimeException("Source archive (" + userCodeFile + ") does not exist");
                    }
                    functionDetails = SourceConfigUtils.convert(sourceConfig, SourceConfigUtils.validate(sourceConfig, null, file, narExtractionDirectory, true));
                }
            } else if (sinkConfig != null) {
                inferMissingArguments(sinkConfig);
                userCodeFile = sinkConfig.getArchive();

                // if code file not specified try to get location of the code based on class.
                if (userCodeFile == null && sinkConfig.getClassName() != null) {
                    userCodeFile = Thread.currentThread().getContextClassLoader()
                            .loadClass(sinkConfig.getClassName())
                            .getProtectionDomain().getCodeSource().getLocation().getFile();
                }

                String builtInSink = isBuiltInSink(userCodeFile);
                if (builtInSink != null) {
                    sinkConfig.setArchive(builtInSink);
                }
                parallelism = sinkConfig.getParallelism();

                if (org.apache.pulsar.common.functions.Utils.isFunctionPackageUrlSupported(userCodeFile)) {
                    File file = FunctionCommon.extractFileFromPkgURL(userCodeFile);
                    functionDetails = SinkConfigUtils.convert(sinkConfig, SinkConfigUtils.validate(sinkConfig, null, file, narExtractionDirectory, true));
                } else {
                    File file = new File(userCodeFile);
                    if (!file.exists()) {
                        throw new RuntimeException("Sink archive does not exist");
                    }
                    functionDetails = SinkConfigUtils.convert(sinkConfig, SinkConfigUtils.validate(sinkConfig, null, file, narExtractionDirectory,  true));
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

        try (ProcessRuntimeFactory containerFactory = new ProcessRuntimeFactory(
                serviceUrl,
                stateStorageServiceUrl,
                authConfig,
                null, /* java instance jar file */
                null, /* python instance file */
                null, /* log directory */
                null, /* extra dependencies dir */
                narExtractionDirectory, /* nar extraction dir */
                new DefaultSecretsProviderConfigurator(), false, Optional.empty(), Optional.empty())) {

            for (int i = 0; i < parallelism; ++i) {
                InstanceConfig instanceConfig = new InstanceConfig();
                instanceConfig.setFunctionDetails(functionDetails);
                // TODO: correctly implement function version and id
                instanceConfig.setFunctionVersion(UUID.randomUUID().toString());
                instanceConfig.setFunctionId(UUID.randomUUID().toString());
                instanceConfig.setInstanceId(i + instanceIdOffset);
                instanceConfig.setMaxBufferedTuples(1024);
                instanceConfig.setPort(FunctionCommon.findAvailablePort());
                instanceConfig.setClusterName("local");
                if (functionConfig != null) {
                    instanceConfig.setMaxPendingAsyncRequests(functionConfig.getMaxPendingAsyncRequests());
                }
                RuntimeSpawner runtimeSpawner = new RuntimeSpawner(
                        instanceConfig,
                        userCodeFile,
                        null,
                        containerFactory,
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
                    } catch (Exception ex) {
                        log.error("Could not get status from all local instances");
                    }
                }
            }, 30000, 30000);
            java.lang.Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    statusCheckTimer.cancel();
                }
            });
        }
    }


    private void startThreadedMode(org.apache.pulsar.functions.proto.Function.FunctionDetails functionDetails,
                                           int parallelism, int instanceIdOffset, String serviceUrl,
                                           String stateStorageServiceUrl, AuthenticationConfig authConfig,
                                           String userCodeFile) throws Exception {
        ThreadRuntimeFactory threadRuntimeFactory = new ThreadRuntimeFactory("LocalRunnerThreadGroup",
                serviceUrl,
                stateStorageServiceUrl,
                authConfig,
                new ClearTextSecretsProvider(), null, narExtractionDirectory, null);
        for (int i = 0; i < parallelism; ++i) {
            InstanceConfig instanceConfig = new InstanceConfig();
            instanceConfig.setFunctionDetails(functionDetails);
            // TODO: correctly implement function version and id
            instanceConfig.setFunctionVersion(UUID.randomUUID().toString());
            instanceConfig.setFunctionId(UUID.randomUUID().toString());
            instanceConfig.setInstanceId(i + instanceIdOffset);
            instanceConfig.setMaxBufferedTuples(1024);
            instanceConfig.setPort(FunctionCommon.findAvailablePort());
            instanceConfig.setClusterName("local");
            if (functionConfig != null) {
                instanceConfig.setMaxPendingAsyncRequests(functionConfig.getMaxPendingAsyncRequests());
            }
            RuntimeSpawner runtimeSpawner = new RuntimeSpawner(
                    instanceConfig,
                    userCodeFile,
                    null,
                    threadRuntimeFactory,
                    30000);
            spawners.add(runtimeSpawner);
            runtimeSpawner.start();
        }
    }

    private String isBuiltInSource(String sourceType) throws IOException {
        // Validate the connector source type from the locally available connectors
        Connectors connectors = getConnectors();

        String source = sourceType.replaceFirst("^builtin://", "");
        if (connectors.getSources().containsKey(source)) {
            // Source type is a valid built-in connector type. For local-run we'll fill it up with its own archive path
            return connectors.getSources().get(source).toString();
        } else {
            return null;
        }
    }

    private String isBuiltInSink(String sinkType) throws IOException {
        // Validate the connector source type from the locally available connectors
        Connectors connectors = getConnectors();

        String sink = sinkType.replaceFirst("^builtin://", "");
        if (connectors.getSinks().containsKey(sink)) {
            // Source type is a valid built-in connector type. For local-run we'll fill it up with its own archive path
            return connectors.getSinks().get(sink).toString();
        } else {
            return null;
        }
    }

    private Connectors getConnectors() throws IOException {
        // Validate the connector source type from the locally available connectors
        String pulsarHome = System.getenv("PULSAR_HOME");
        if (pulsarHome == null) {
            pulsarHome = Paths.get("").toAbsolutePath().toString();
        }
        String connectorsDir = Paths.get(pulsarHome, "connectors").toString();
        return ConnectorUtils.searchForConnectors(connectorsDir, narExtractionDirectory);
    }
}
