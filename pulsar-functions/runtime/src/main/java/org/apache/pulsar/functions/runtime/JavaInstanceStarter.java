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
package org.apache.pulsar.functions.runtime;


import static java.util.Objects.requireNonNull;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSinkType;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSourceType;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameterized;
import com.beust.jcommander.Strings;
import com.beust.jcommander.converters.StringConverter;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.Empty;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.exporter.HTTPServer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceCache;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.stats.FunctionCollectorRegistry;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.utils.FunctionCommon;


@Slf4j
public class JavaInstanceStarter implements AutoCloseable {
    @Parameter(names = "--function_details", description = "Function details json\n")
    public String functionDetailsJsonString;
    @Parameter(
            names = "--jar",
            description = "Path to user function jar\n",
            listConverter = StringConverter.class)
    public String jarFile;

    @Parameter(
            names = "--transform_function_jar",
            description = "Path to the transform function jar\n",
            listConverter = StringConverter.class)
    public String transformFunctionJarFile;

    @Parameter(names = "--instance_id", description = "instanceId used to uniquely identify a function instance\n")
    public Integer instanceId;

    @Parameter(names = "--function_id", description = "functionId used to uniquely identify a function\n")
    public String functionId;

    @Parameter(names = "--function_version", description = "The version of the function\n")
    public String functionVersion;

    @Parameter(names = "--pulsar_serviceurl", description = "serviceUrl of the target Pulsar cluster\n")
    public String pulsarServiceUrl;

    @Parameter(names = "--transform_function_id", description = "functionId of the transform function\n")
    public String transformFunctionId;

    @Parameter(names = "--client_auth_plugin", description = "Client auth plugin full classname\n")
    public String clientAuthenticationPlugin;

    @Parameter(names = "--client_auth_params", description = "Client auth parameters\n")
    public String clientAuthenticationParameters;

    @Parameter(names = "--use_tls", description = "Use tls connection\n")
    public String useTls = Boolean.FALSE.toString();

    @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection\n")
    public String tlsAllowInsecureConnection = Boolean.TRUE.toString();

    @Parameter(names = "--hostname_verification_enabled", description = "Enable hostname verification")
    public String tlsHostNameVerificationEnabled = Boolean.FALSE.toString();

    @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path")
    public String tlsTrustCertFilePath;

    @Parameter(names = "--state_storage_impl_class", description = "State storage service"
            + "implementation classname\n")
    public String stateStorageImplClass;

    @Parameter(names = "--state_storage_serviceurl", description = "State storage service url\n")
    public String stateStorageServiceUrl;

    @Parameter(names = "--port", description = "Port to listen on\n")
    public Integer port;

    @Parameter(names = "--metrics_port", description = "Port metrics will be exposed on\n")
    public Integer metricsPort;

    @Parameter(names = "--max_buffered_tuples", description = "Maximum number of tuples to buffer\n")
    public Integer maxBufferedTuples;

    @Parameter(names = "--expected_healthcheck_interval", description = "Expected interval in "
            + "seconds between health checks")
    public Integer expectedHealthCheckInterval;

    @Parameter(names = "--secrets_provider", description = "The classname of the secrets provider")
    public String secretsProviderClassName;

    @Parameter(names = "--secrets_provider_config", description = "The config that needs to be "
            + "passed to secrets provider")
    public String secretsProviderConfig;

    @Parameter(names = "--cluster_name", description = "The name of the cluster this "
            + "instance is running on")
    public String clusterName;

    @Parameter(names = "--nar_extraction_directory", description = "The directory where "
            + "extraction of nar packages happen")
    public String narExtractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;

    @Parameter(names = "--pending_async_requests", description = "Max pending async requests per instance")
    public Integer maxPendingAsyncRequests = 1000;

    @Parameter(names = "--web_serviceurl", description = "Pulsar Web Service Url")
    public String webServiceUrl  = null;

    @Parameter(names = "--expose_pulsaradmin", description = "Whether the pulsar admin client "
            + "exposed to function context, default is disabled. Providing this flag set value to true")

    public Boolean exposePulsarAdminClientEnabled = false;

    @Parameter(names = "--config_file", description = "The config file for instance to use, default "
            + "use coomand line args")
    public String configFile;

    private Server server;
    private RuntimeSpawner runtimeSpawner;
    private ThreadRuntimeFactory containerFactory;
    private Long lastHealthCheckTs = null;
    private HTTPServer metricsServer;
    private ScheduledFuture healthCheckTimer;

    public JavaInstanceStarter() {
    }

    public void start(String[] args, ClassLoader functionInstanceClassLoader, ClassLoader rootClassLoader)
            throws Exception {
        Thread.currentThread().setContextClassLoader(functionInstanceClassLoader);

        setConfigs(args);

        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionId(functionId);
        instanceConfig.setTransformFunctionId(transformFunctionId);
        instanceConfig.setFunctionVersion(functionVersion);
        instanceConfig.setInstanceId(instanceId);
        instanceConfig.setMaxBufferedTuples(maxBufferedTuples);
        instanceConfig.setClusterName(clusterName);
        instanceConfig.setMaxPendingAsyncRequests(maxPendingAsyncRequests);
        instanceConfig.setExposePulsarAdminClientEnabled(exposePulsarAdminClientEnabled);
        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();
        if (functionDetailsJsonString.charAt(0) == '\'') {
            functionDetailsJsonString = functionDetailsJsonString.substring(1);
        }
        if (functionDetailsJsonString.charAt(functionDetailsJsonString.length() - 1) == '\'') {
            functionDetailsJsonString = functionDetailsJsonString.substring(0, functionDetailsJsonString.length() - 1);
        }
        JsonFormat.parser().merge(functionDetailsJsonString, functionDetailsBuilder);
        inferringMissingTypeClassName(functionDetailsBuilder, functionInstanceClassLoader);
        Function.FunctionDetails functionDetails = functionDetailsBuilder.build();
        instanceConfig.setFunctionDetails(functionDetails);
        instanceConfig.setPort(port);
        instanceConfig.setMetricsPort(metricsPort);
        instanceConfig.setConfigFile(configFile);

        Map<String, String> secretsProviderConfigMap = null;
        if (!StringUtils.isEmpty(secretsProviderConfig)) {
            if (secretsProviderConfig.charAt(0) == '\'') {
                secretsProviderConfig = secretsProviderConfig.substring(1);
            }
            if (secretsProviderConfig.charAt(secretsProviderConfig.length() - 1) == '\'') {
                secretsProviderConfig = secretsProviderConfig.substring(0, secretsProviderConfig.length() - 1);
            }
            Type type = new TypeToken<Map<String, String>>() {
            }.getType();
            secretsProviderConfigMap = new Gson().fromJson(secretsProviderConfig, type);
        }

        if (StringUtils.isEmpty(secretsProviderClassName)) {
            secretsProviderClassName = ClearTextSecretsProvider.class.getName();
        }

        SecretsProvider secretsProvider;
        try {
            secretsProvider =
                    (SecretsProvider) Reflections.createInstance(secretsProviderClassName, functionInstanceClassLoader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        secretsProvider.init(secretsProviderConfigMap);

        // Collector Registry for prometheus metrics
        FunctionCollectorRegistry collectorRegistry = FunctionCollectorRegistry.getDefaultImplementation();
        RuntimeUtils.registerDefaultCollectors(collectorRegistry);

        containerFactory = new ThreadRuntimeFactory("LocalRunnerThreadGroup", pulsarServiceUrl,
                stateStorageImplClass,
                stateStorageServiceUrl,
                AuthenticationConfig.builder().clientAuthenticationPlugin(clientAuthenticationPlugin)
                        .clientAuthenticationParameters(clientAuthenticationParameters).useTls(isTrue(useTls))
                        .tlsAllowInsecureConnection(isTrue(tlsAllowInsecureConnection))
                        .tlsHostnameVerificationEnable(isTrue(tlsHostNameVerificationEnabled))
                        .tlsTrustCertsFilePath(tlsTrustCertFilePath).build(),
                secretsProvider, collectorRegistry, narExtractionDirectory, rootClassLoader,
                exposePulsarAdminClientEnabled, webServiceUrl);
        runtimeSpawner = new RuntimeSpawner(
                instanceConfig,
                jarFile,
                null, // we really dont use this in thread container
                transformFunctionJarFile,
                null, // we really dont use this in thread container
                containerFactory,
                expectedHealthCheckInterval * 1000);

        server = ServerBuilder.forPort(port)
                .addService(new InstanceControlImpl(runtimeSpawner))
                .build()
                .start();
        log.info("JavaInstance Server started, listening on " + port);
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            try {
                close();
            } catch (Exception ex) {
                System.err.println(ex);
            }
        }));

        log.info("Starting runtimeSpawner");
        runtimeSpawner.start();

        // starting metrics server
        log.info("Starting metrics server on port {}", metricsPort);
        metricsServer = new HTTPServer(new InetSocketAddress(metricsPort), collectorRegistry, true);

        if (expectedHealthCheckInterval > 0) {
            healthCheckTimer =
                    InstanceCache.getInstanceCache().getScheduledExecutorService().scheduleAtFixedRate(() -> {
                        try {
                            if (System.currentTimeMillis() - lastHealthCheckTs
                                    > 3 * expectedHealthCheckInterval * 1000) {
                                log.info("Haven't received health check from spawner in a while. Stopping instance...");
                                close();
                            }
                        } catch (Exception e) {
                            log.error("Error occurred when checking for latest health check", e);
                        }
                    }, expectedHealthCheckInterval * 1000, expectedHealthCheckInterval * 1000, TimeUnit.MILLISECONDS);
        }

        runtimeSpawner.join();
        log.info("RuntimeSpawner quit, shutting down JavaInstance");
        close();
    }

    private static boolean isTrue(String param) {
        return Boolean.TRUE.toString().equals(param);
    }

    @Override
    public void close() {
        try {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            if (server != null) {
                server.shutdown();
            }
            if (runtimeSpawner != null) {
                runtimeSpawner.close();
            }
            if (healthCheckTimer != null) {
                healthCheckTimer.cancel(false);
            }
            if (containerFactory != null) {
                containerFactory.close();
            }
            if (metricsServer != null) {
                metricsServer.stop();
            }

            InstanceCache.shutdown();
        } catch (Exception ex) {
            System.err.println(ex);
        }
    }

    private void inferringMissingTypeClassName(Function.FunctionDetails.Builder functionDetailsBuilder,
                                               ClassLoader classLoader) throws ClassNotFoundException {
        switch (functionDetailsBuilder.getComponentType()) {
            case FUNCTION:
                if ((functionDetailsBuilder.hasSource()
                        && functionDetailsBuilder.getSource().getTypeClassName().isEmpty())
                        || (functionDetailsBuilder.hasSink()
                        && functionDetailsBuilder.getSink().getTypeClassName().isEmpty())) {
                    Map<String, Object> userConfigs = new Gson().fromJson(functionDetailsBuilder.getUserConfig(),
                            new TypeToken<Map<String, Object>>() {
                            }.getType());
                    boolean isWindowConfigPresent = userConfigs.containsKey(WindowConfig.WINDOW_CONFIG_KEY);
                    String className = functionDetailsBuilder.getClassName();
                    if (isWindowConfigPresent) {
                        WindowConfig windowConfig = new Gson().fromJson(
                                (new Gson().toJson(userConfigs.get(WindowConfig.WINDOW_CONFIG_KEY))),
                                WindowConfig.class);
                        className = windowConfig.getActualWindowFunctionClassName();
                    }

                    Class<?>[] typeArgs = FunctionCommon.getFunctionTypes(classLoader.loadClass(className),
                            isWindowConfigPresent);
                    if (functionDetailsBuilder.hasSource()
                            && functionDetailsBuilder.getSource().getTypeClassName().isEmpty()
                            && typeArgs[0] != null) {
                        Function.SourceSpec.Builder sourceBuilder = functionDetailsBuilder.getSource().toBuilder();
                        sourceBuilder.setTypeClassName(typeArgs[0].getName());
                        functionDetailsBuilder.setSource(sourceBuilder.build());
                    }

                    if (functionDetailsBuilder.hasSink()
                            && functionDetailsBuilder.getSink().getTypeClassName().isEmpty()
                            && typeArgs[1] != null) {
                        Function.SinkSpec.Builder sinkBuilder = functionDetailsBuilder.getSink().toBuilder();
                        sinkBuilder.setTypeClassName(typeArgs[1].getName());
                        functionDetailsBuilder.setSink(sinkBuilder.build());
                    }
                }
                break;
            case SINK:
                if ((functionDetailsBuilder.hasSink()
                        && functionDetailsBuilder.getSink().getTypeClassName().isEmpty())) {
                    String typeArg = getSinkType(functionDetailsBuilder.getClassName(), classLoader).getName();

                    Function.SinkSpec.Builder sinkBuilder =
                            Function.SinkSpec.newBuilder(functionDetailsBuilder.getSink());
                    sinkBuilder.setTypeClassName(typeArg);
                    functionDetailsBuilder.setSink(sinkBuilder);

                    Function.SourceSpec sourceSpec = functionDetailsBuilder.getSource();
                    if (null == sourceSpec || StringUtils.isEmpty(sourceSpec.getTypeClassName())) {
                        Function.SourceSpec.Builder sourceBuilder = Function.SourceSpec.newBuilder(sourceSpec);
                        sourceBuilder.setTypeClassName(typeArg);
                        functionDetailsBuilder.setSource(sourceBuilder);
                    }
                }
                break;
            case SOURCE:
                if ((functionDetailsBuilder.hasSource()
                        && functionDetailsBuilder.getSource().getTypeClassName().isEmpty())) {
                    String typeArg = getSourceType(functionDetailsBuilder.getClassName(), classLoader).getName();

                    Function.SourceSpec.Builder sourceBuilder =
                            Function.SourceSpec.newBuilder(functionDetailsBuilder.getSource());
                    sourceBuilder.setTypeClassName(typeArg);
                    functionDetailsBuilder.setSource(sourceBuilder);

                    Function.SinkSpec sinkSpec = functionDetailsBuilder.getSink();
                    if (null == sinkSpec || StringUtils.isEmpty(sinkSpec.getTypeClassName())) {
                        Function.SinkSpec.Builder sinkBuilder = Function.SinkSpec.newBuilder(sinkSpec);
                        sinkBuilder.setTypeClassName(typeArg);
                        functionDetailsBuilder.setSink(sinkBuilder);
                    }
                }
                break;
        }
    }


    class InstanceControlImpl extends InstanceControlGrpc.InstanceControlImplBase {
        private RuntimeSpawner runtimeSpawner;

        public InstanceControlImpl(RuntimeSpawner runtimeSpawner) {
            this.runtimeSpawner = runtimeSpawner;
            lastHealthCheckTs = System.currentTimeMillis();
        }

        @Override
        public void getFunctionStatus(Empty request,
                                      StreamObserver<InstanceCommunication.FunctionStatus> responseObserver) {
            try {
                InstanceCommunication.FunctionStatus response =
                        runtimeSpawner.getFunctionStatus(runtimeSpawner.getInstanceConfig().getInstanceId()).get();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("Exception in JavaInstance doing getFunctionStatus", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void getAndResetMetrics(com.google.protobuf.Empty request,
                   io.grpc.stub.StreamObserver<org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData>
                   responseObserver) {
            Runtime runtime = runtimeSpawner.getRuntime();
            if (runtime != null) {
                try {
                    InstanceCommunication.MetricsData metrics = runtime.getAndResetMetrics().get();
                    responseObserver.onNext(metrics);
                    responseObserver.onCompleted();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Exception in JavaInstance doing getAndResetMetrics", e);
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void getMetrics(com.google.protobuf.Empty request,
                   io.grpc.stub.StreamObserver<org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData>
                   responseObserver) {
            Runtime runtime = runtimeSpawner.getRuntime();
            if (runtime != null) {
                try {
                    InstanceCommunication.MetricsData metrics = runtime.getMetrics(instanceId).get();
                    responseObserver.onNext(metrics);
                    responseObserver.onCompleted();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Exception in JavaInstance doing getAndResetMetrics", e);
                    throw new RuntimeException(e);
                }
            }
        }

        public void resetMetrics(com.google.protobuf.Empty request,
                                 io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
            Runtime runtime = runtimeSpawner.getRuntime();
            if (runtime != null) {
                try {
                    runtime.resetMetrics().get();
                    responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Exception in JavaInstance doing resetMetrics", e);
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void healthCheck(com.google.protobuf.Empty request,
                io.grpc.stub.StreamObserver<org.apache.pulsar.functions.proto.InstanceCommunication.HealthCheckResult>
                responseObserver) {
            log.debug("Received health check request...");
            InstanceCommunication.HealthCheckResult healthCheckResult =
                    InstanceCommunication.HealthCheckResult.newBuilder().setSuccess(true).build();
            responseObserver.onNext(healthCheckResult);
            responseObserver.onCompleted();

            lastHealthCheckTs = System.currentTimeMillis();
        }
    }

    @VisibleForTesting
    protected void useConfigFromFileAndProvideDefaultValue() throws IOException {
        if (configFile == null) {
            // skip the file if not provided
            throw new ValidationException("Error: config file cannot be null");
        }

        JavaInstanceConfiguration instanceConfiguration;
        try {
            instanceConfiguration = PulsarConfigurationLoader.create(
                    configFile, JavaInstanceConfiguration.class);
        }  catch (FileNotFoundException e) {
            log.warn("The file {} is not found, using command line args only", configFile);
            throw e;
        }

        requireNonNull(instanceConfiguration);
        // optional String configs
        jarFile = instanceConfiguration.getJarFile();
        transformFunctionJarFile = instanceConfiguration.getTransformFunctionJarFile();
        transformFunctionId = instanceConfiguration.getTransformFunctionId();
        clientAuthenticationPlugin = instanceConfiguration.getClientAuthenticationPlugin();
        clientAuthenticationParameters = instanceConfiguration.getClientAuthenticationParameters();
        tlsTrustCertFilePath = instanceConfiguration.getTlsTrustCertFilePath();
        stateStorageImplClass = instanceConfiguration.getStateStorageImplClass();
        stateStorageServiceUrl = instanceConfiguration.getStateStorageServiceUrl();
        secretsProviderClassName = instanceConfiguration.getSecretsProviderClassName();
        secretsProviderConfig = instanceConfiguration.getSecretsProviderConfig();
        narExtractionDirectory = instanceConfiguration.getNarExtractionDirectory();
        webServiceUrl = instanceConfiguration.getWebServiceUrl();

        // optional Integer configs
        maxPendingAsyncRequests = instanceConfiguration.getMaxPendingAsyncRequests();

        // optional Boolean configs
        useTls = instanceConfiguration.getUseTls();
        tlsAllowInsecureConnection = instanceConfiguration.getTlsAllowInsecureConnection();
        tlsHostNameVerificationEnabled = instanceConfiguration.getTlsHostNameVerificationEnabled();

        // special arity=0 Boolean config
        exposePulsarAdminClientEnabled = instanceConfiguration.getExposePulsarAdminClientEnabled();

        // required String configs
        functionDetailsJsonString = instanceConfiguration.getFunctionDetailsJsonString();
        functionId = instanceConfiguration.getFunctionId();
        functionVersion = instanceConfiguration.getFunctionVersion();
        pulsarServiceUrl = instanceConfiguration.getPulsarServiceUrl();
        clusterName = instanceConfiguration.getClusterName();
        // required Integer configs
        instanceId = instanceConfiguration.getInstanceId();
        port = instanceConfiguration.getPort();
        metricsPort = instanceConfiguration.getMetricsPort();
        maxBufferedTuples = instanceConfiguration.getMaxBufferedTuples();
        expectedHealthCheckInterval = instanceConfiguration.getExpectedHealthCheckInterval();
    }

    private void validateRequiredConfigs() {
        try {
            // String configs
            requireNonNull(functionDetailsJsonString);
            requireNonNull(functionId);
            requireNonNull(functionVersion);
            requireNonNull(pulsarServiceUrl);
            requireNonNull(clusterName);
            // Integer configs
            requireNonNull(instanceId);
            requireNonNull(port);
            requireNonNull(metricsPort);
            requireNonNull(maxBufferedTuples);
            requireNonNull(expectedHealthCheckInterval);
        } catch (NullPointerException e) {
            throw new ParameterException(String.format("The following option is required: [%s]",
                    Strings.join(",", requiredConfigFieldNames().toArray())),
                    e);
        }
    }

    /**
     * If a config field is not provided in JCommander, then it will be initialized to null.
     *
     * <p>File config and Command line config should be exclusive. When --config_file is used,
     * users should not provide any other command line arguments. This design is to avoid possible confusion and
     * allow users to easy reason where a configuration value comes from.
     * @param args
     * @throws IOException
     */
    @VisibleForTesting
    protected void setConfigs(String[] args) throws IOException{
        JCommander jcommander = new JCommander(this);
        // parse args by JCommander
        jcommander.parse(args);
        if (configFile != null) {
            checkNoOtherConfigs(args);
            useConfigFromFileAndProvideDefaultValue();
        }
        validateRequiredConfigs();
    }

    protected static Set<String> requireConfigFieldsNames = new HashSet<>(){{
        // String
        add("functionDetailsJsonString");
        add("functionId");
        add("functionVersion");
        add("pulsarServiceUrl");
        add("clusterName");
        add("instanceId");
        add("port");
        add("metricsPort");
        add("maxBufferedTuples");
        add("expectedHealthCheckInterval");
    }};

    @VisibleForTesting
    protected Set<String> requiredConfigFieldNames() {
        return requireConfigFieldsNames;
    }

    @VisibleForTesting
    protected Set<String> optionalConfigFieldNames() {
        JCommander jc = new JCommander(this);
        return jc.getFields()
                .keySet()
                .stream()
                .map(Parameterized::getName)
                .filter(name -> !requireConfigFieldsNames.contains(name))
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    protected Set<String> requiredConfigLongestArgNames() {
        JCommander jc = new JCommander(this);
        return jc.getFields()
                .entrySet()
                .stream()
                .filter(entry -> requireConfigFieldsNames.contains(entry.getKey().getName()))
                .map(entry -> entry.getValue().getLongestName())
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    protected Set<String> optionalConfigLongestArgNames() {
        JCommander jc = new JCommander(this);
        return jc.getFields()
                .entrySet()
                .stream()
                .filter(entry -> !requireConfigFieldsNames.contains(entry.getKey().getName()))
                .map(entry -> entry.getValue().getLongestName())
                .collect(Collectors.toSet());
    }

    private void checkNoOtherConfigs(String[] args) {
        if (args.length != 2) {
            throw new ValidationException("When using file config， configs should not be specified with command line");
        }
    }
}
