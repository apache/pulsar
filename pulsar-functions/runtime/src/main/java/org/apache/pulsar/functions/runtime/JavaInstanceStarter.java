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

package org.apache.pulsar.functions.runtime;

import static org.apache.pulsar.functions.utils.FunctionCommon.getSinkType;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSourceType;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.StringConverter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.Empty;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.exporter.HTTPServer;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.pool.TypePool;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceCache;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.stats.FunctionCollectorRegistry;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntime;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManagerImpl;


@Slf4j
public class JavaInstanceStarter implements AutoCloseable {
    @Parameter(names = "--function_details", description = "Function details json\n", required = true)
    public String functionDetailsJsonString;
    @Parameter(
            names = "--jar",
            description = "Path to Jar\n",
            listConverter = StringConverter.class)
    public String jarFile;

    @Parameter(names = "--instance_id", description = "Instance Id\n", required = true)
    public int instanceId;

    @Parameter(names = "--function_id", description = "Function Id\n", required = true)
    public String functionId;

    @Parameter(names = "--function_version", description = "Function Version\n", required = true)
    public String functionVersion;

    @Parameter(names = "--pulsar_serviceurl", description = "Pulsar Service Url\n", required = true)
    public String pulsarServiceUrl;

    @Parameter(names = "--client_auth_plugin", description = "Client auth plugin name\n")
    public String clientAuthenticationPlugin;

    @Parameter(names = "--client_auth_params", description = "Client auth param\n")
    public String clientAuthenticationParameters;

    @Parameter(names = "--use_tls", description = "Use tls connection\n")
    public String useTls = Boolean.FALSE.toString();

    @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection\n")
    public String tlsAllowInsecureConnection = Boolean.TRUE.toString();

    @Parameter(names = "--hostname_verification_enabled", description = "Enable hostname verification")
    public String tlsHostNameVerificationEnabled = Boolean.FALSE.toString();

    @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path")
    public String tlsTrustCertFilePath;

    @Parameter(names = "--state_storage_impl_class", description = "State Storage Service Implementation class\n", required= false)
    public String stateStorageImplClass;

    @Parameter(names = "--state_storage_serviceurl", description = "State Storage Service Url\n", required= false)
    public String stateStorageServiceUrl;

    @Parameter(names = "--port", description = "Port to listen on\n", required = true)
    public int port;

    @Parameter(names = "--metrics_port", description = "Port metrics will be exposed on\n", required = true)
    public int metrics_port;

    @Parameter(names = "--max_buffered_tuples", description = "Maximum number of tuples to buffer\n", required = true)
    public int maxBufferedTuples;

    @Parameter(names = "--expected_healthcheck_interval", description = "Expected interval in seconds between healtchecks", required = true)
    public int expectedHealthCheckInterval;

    @Parameter(names = "--secrets_provider", description = "The classname of the secrets provider", required = false)
    public String secretsProviderClassName;

    @Parameter(names = "--secrets_provider_config", description = "The config that needs to be passed to secrets provider", required = false)
    public String secretsProviderConfig;

    @Parameter(names = "--cluster_name", description = "The name of the cluster this instance is running on", required = true)
    public String clusterName;

    @Parameter(names = "--nar_extraction_directory", description = "The directory where extraction of nar packages happen", required = false)
    public String narExtractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;

    @Parameter(names = "--pending_async_requests", description = "Max pending async requests per instance", required = false)
    public int maxPendingAsyncRequests = 1000;

    @Parameter(names = "--web_serviceurl", description = "Pulsar Web Service Url", required = false)
    public String webServiceUrl = null;

    @Parameter(names = "--expose_pulsaradmin", description = "Whether the pulsar admin client exposed to function context, default is disabled.", required = false)
    public Boolean exposePulsarAdminClientEnabled = false;

    private Server server;
    private RuntimeSpawner runtimeSpawner;
    private ThreadRuntimeFactory containerFactory;
    private Long lastHealthCheckTs = null;
    private HTTPServer metricsServer;
    private ScheduledFuture healthCheckTimer;

    public JavaInstanceStarter() { }

    public void start(String[] args, ClassLoader functionInstanceClassLoader, ClassLoader rootClassLoader) throws Exception {
        Thread.currentThread().setContextClassLoader(functionInstanceClassLoader);

        JCommander jcommander = new JCommander(this);
        // parse args by JCommander
        jcommander.parse(args);

        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionId(functionId);
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
        FunctionCacheManager fnCache = new FunctionCacheManagerImpl(rootClassLoader);
        ClassLoader functionClassLoader = ThreadRuntime.loadJars(jarFile, instanceConfig,
                functionDetailsBuilder.getName(), narExtractionDirectory, fnCache);
        inferringMissingTypeClassName(functionDetailsBuilder, functionClassLoader);
        Function.FunctionDetails functionDetails = functionDetailsBuilder.build();
        instanceConfig.setFunctionDetails(functionDetails);
        instanceConfig.setPort(port);
        instanceConfig.setMetricsPort(metrics_port);

        Map<String, String> secretsProviderConfigMap = null;
        if (!StringUtils.isEmpty(secretsProviderConfig)) {
            if (secretsProviderConfig.charAt(0) == '\'') {
                secretsProviderConfig = secretsProviderConfig.substring(1);
            }
            if (secretsProviderConfig.charAt(secretsProviderConfig.length() - 1) == '\'') {
                secretsProviderConfig = secretsProviderConfig.substring(0, secretsProviderConfig.length() - 1);
            }
            Type type = new TypeToken<Map<String, String>>() {}.getType();
            secretsProviderConfigMap = new Gson().fromJson(secretsProviderConfig, type);
        }

        if (StringUtils.isEmpty(secretsProviderClassName)) {
            secretsProviderClassName = ClearTextSecretsProvider.class.getName();
        }

        SecretsProvider secretsProvider;
        try {
            secretsProvider = (SecretsProvider) Reflections.createInstance(secretsProviderClassName, functionInstanceClassLoader);
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
                exposePulsarAdminClientEnabled, webServiceUrl, fnCache);
        runtimeSpawner = new RuntimeSpawner(
                instanceConfig,
                jarFile,
                null, // we really dont use this in thread container
                containerFactory,
                expectedHealthCheckInterval * 1000);

        server = ServerBuilder.forPort(port)
                .addService(new InstanceControlImpl(runtimeSpawner))
                .build()
                .start();
        log.info("JavaInstance Server started, listening on " + port);
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                try {
                    close();
                } catch (Exception ex) {
                    System.err.println(ex);
                }
            }
        });

        log.info("Starting runtimeSpawner");
        runtimeSpawner.start();

        // starting metrics server
        log.info("Starting metrics server on port {}", metrics_port);
        metricsServer = new HTTPServer(new InetSocketAddress(metrics_port), collectorRegistry, true);

        if (expectedHealthCheckInterval > 0) {
            healthCheckTimer = InstanceCache.getInstanceCache().getScheduledExecutorService().scheduleAtFixedRate(() -> {
                try {
                    if (System.currentTimeMillis() - lastHealthCheckTs > 3 * expectedHealthCheckInterval * 1000) {
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
                                               ClassLoader classLoader) {
        TypePool typePool = TypePool.Default.of(ClassFileLocator.ForClassLoader.of(classLoader));
        switch (functionDetailsBuilder.getComponentType()) {
            case FUNCTION:
                if ((functionDetailsBuilder.hasSource()
                        && functionDetailsBuilder.getSource().getTypeClassName().isEmpty())
                        || (functionDetailsBuilder.hasSink()
                        && functionDetailsBuilder.getSink().getTypeClassName().isEmpty())) {
                    Map<String, Object> userConfigs = new Gson().fromJson(functionDetailsBuilder.getUserConfig(),
                            new TypeToken<Map<String, Object>>() {
                            }.getType());
                    boolean isWindowConfigPresent =
                            userConfigs != null && userConfigs.containsKey(WindowConfig.WINDOW_CONFIG_KEY);
                    String className = functionDetailsBuilder.getClassName();
                    if (isWindowConfigPresent) {
                        WindowConfig windowConfig = new Gson().fromJson(
                                (new Gson().toJson(userConfigs.get(WindowConfig.WINDOW_CONFIG_KEY))),
                                WindowConfig.class);
                        className = windowConfig.getActualWindowFunctionClassName();
                    }
                    TypeDefinition[] typeArgs = FunctionCommon.getFunctionTypes(typePool.describe(className).resolve(),
                            isWindowConfigPresent);
                    if (functionDetailsBuilder.hasSource()
                            && functionDetailsBuilder.getSource().getTypeClassName().isEmpty()
                            && typeArgs[0] != null) {
                        Function.SourceSpec.Builder sourceBuilder = functionDetailsBuilder.getSource().toBuilder();
                        sourceBuilder.setTypeClassName(typeArgs[0].asErasure().getTypeName());
                        functionDetailsBuilder.setSource(sourceBuilder.build());
                    }

                    if (functionDetailsBuilder.hasSink()
                            && functionDetailsBuilder.getSink().getTypeClassName().isEmpty()
                            && typeArgs[1] != null) {
                        Function.SinkSpec.Builder sinkBuilder = functionDetailsBuilder.getSink().toBuilder();
                        sinkBuilder.setTypeClassName(typeArgs[1].asErasure().getTypeName());
                        functionDetailsBuilder.setSink(sinkBuilder.build());
                    }
                }
                break;
            case SINK:
                if ((functionDetailsBuilder.hasSink()
                        && functionDetailsBuilder.getSink().getTypeClassName().isEmpty())) {
                    String typeArg =
                            getSinkType(functionDetailsBuilder.getSink().getClassName(), typePool).asErasure()
                                    .getTypeName();

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
                    String typeArg =
                            getSourceType(functionDetailsBuilder.getSource().getClassName(), typePool).asErasure()
                                    .getTypeName();

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
        public void getFunctionStatus(Empty request, StreamObserver<InstanceCommunication.FunctionStatus> responseObserver) {
            try {
                InstanceCommunication.FunctionStatus response = runtimeSpawner.getFunctionStatus(runtimeSpawner.getInstanceConfig().getInstanceId()).get();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("Exception in JavaInstance doing getFunctionStatus", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void getAndResetMetrics(com.google.protobuf.Empty request,
                                       io.grpc.stub.StreamObserver<org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData> responseObserver) {
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
                               io.grpc.stub.StreamObserver<org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData> responseObserver) {
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
                                io.grpc.stub.StreamObserver<org.apache.pulsar.functions.proto.InstanceCommunication.HealthCheckResult> responseObserver) {
            log.debug("Received health check request...");
            InstanceCommunication.HealthCheckResult healthCheckResult
                    = InstanceCommunication.HealthCheckResult.newBuilder().setSuccess(true).build();
            responseObserver.onNext(healthCheckResult);
            responseObserver.onCompleted();

            lastHealthCheckTs = System.currentTimeMillis();
        }
    }
}
