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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.StringConverter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.Function.ConsumerSpec;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.utils.ConsumerConfig;
import org.inferred.freebuilder.shaded.org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceMain implements AutoCloseable {
    @Parameter(names = "--function_classname", description = "Function Class Name\n", required = true)
    protected String className;
    @Parameter(
            names = "--jar",
            description = "Path to Jar\n",
            listConverter = StringConverter.class)
    protected String jarFile;
    @Parameter(names = "--name", description = "Function Name\n", required = true)
    protected String functionName;
    @Parameter(names = "--tenant", description = "Tenant Name\n", required = true)
    protected String tenant;
    @Parameter(names = "--namespace", description = "Namespace Name\n", required = true)
    protected String namespace;
    @Parameter(names = "--log_topic", description = "Log Topic")
    protected String logTopic;

    @Parameter(names = "--processing_guarantees", description = "Processing Guarantees\n", required = true)
    protected ProcessingGuarantees processingGuarantees;

    @Parameter(names = "--instance_id", description = "Instance Id\n", required = true)
    protected String instanceId;

    @Parameter(names = "--function_id", description = "Function Id\n", required = true)
    protected String functionId;

    @Parameter(names = "--function_version", description = "Function Version\n", required = true)
    protected String functionVersion;

    @Parameter(names = "--pulsar_serviceurl", description = "Pulsar Service Url\n", required = true)
    protected String pulsarServiceUrl;

    @Parameter(names = "--client_auth_plugin", description = "Client auth plugin name\n")
    protected String clientAuthenticationPlugin;

    @Parameter(names = "--client_auth_params", description = "Client auth param\n")
    protected String clientAuthenticationParameters;

    @Parameter(names = "--use_tls", description = "Use tls connection\n")
    protected String useTls = Boolean.FALSE.toString();

    @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection\n")
    protected String tlsAllowInsecureConnection = Boolean.TRUE.toString();

    @Parameter(names = "--hostname_verification_enabled", description = "Enable hostname verification")
    protected String tlsHostNameVerificationEnabled = Boolean.FALSE.toString();

    @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path")
    protected String tlsTrustCertFilePath;

    @Parameter(names = "--state_storage_serviceurl", description = "State Storage Service Url\n", required= false)
    protected String stateStorageServiceUrl;

    @Parameter(names = "--port", description = "Port to listen on\n", required = true)
    protected int port;

    @Parameter(names = "--max_buffered_tuples", description = "Maximum number of tuples to buffer\n", required = true)
    protected int maxBufferedTuples;

    @Parameter(names = "--user_config", description = "UserConfig\n")
    protected String userConfig;

    @Parameter(names = "--auto_ack", description = "Enable Auto Acking?\n")
    protected String autoAck = Boolean.TRUE.toString();

    @Parameter(names = "--source_classname", description = "The source classname")
    protected String sourceClassname;

    @Parameter(names = "--source_configs", description = "The source configs")
    protected String sourceConfigs;

    @Parameter(names = "--source_type_classname", description = "The return type of the source", required = true)
    protected String sourceTypeClassName;

    @Parameter(names = "--source_subscription_type", description = "The source subscription type", required = true)
    protected String sourceSubscriptionType;

    @Parameter(names = "--source_topics_schema", description = "A map of topics to Schema for the source")
    protected String sourceTopicsSchemaString;

    @Parameter(names = "--source_timeout_ms", description = "Source message timeout in milliseconds")
    protected Long sourceTimeoutMs;

    @Parameter(names = "--sink_type_classname", description = "The injest type of the sink", required = true)
    protected String sinkTypeClassName;

    @Parameter(names = "--sink_configs", description = "The sink configs\n")
    protected String sinkConfigs;

    @Parameter(names = "--sink_classname", description = "The sink classname\n")
    protected String sinkClassname;

    @Parameter(names = "--sink_topic", description = "The sink Topic Name\n")
    protected String sinkTopic;

    @Parameter(names = "--sink_topic_schema_type", description = "The sink Topic schema\n")
    protected String sinkSchemaTypeOrClassName;

    @Parameter(names = "--sink_serde_classname", description = "Sink SerDe\n")
    protected String sinkSerdeClassName;

    private Server server;
    private RuntimeSpawner runtimeSpawner;
    private ThreadRuntimeFactory containerFactory;
    private Long lastHealthCheckTs = null;
    private ScheduledExecutorService timer;

    public JavaInstanceMain() { }


    public void start() throws Exception {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionId(functionId);
        instanceConfig.setFunctionVersion(functionVersion);
        instanceConfig.setInstanceId(instanceId);
        instanceConfig.setMaxBufferedTuples(maxBufferedTuples);
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setTenant(tenant);
        functionDetailsBuilder.setNamespace(namespace);
        functionDetailsBuilder.setName(functionName);
        functionDetailsBuilder.setClassName(className);

        if (logTopic != null) {
            functionDetailsBuilder.setLogTopic(logTopic);
        }
        functionDetailsBuilder.setProcessingGuarantees(processingGuarantees);
        functionDetailsBuilder.setAutoAck(isTrue(autoAck));
        if (userConfig != null && !userConfig.isEmpty()) {
            functionDetailsBuilder.setUserConfig(userConfig);
        }

        // Setup source
        SourceSpec.Builder sourceDetailsBuilder = SourceSpec.newBuilder();
        if (sourceClassname != null) {
            sourceDetailsBuilder.setClassName(sourceClassname);
        }
        if (sourceConfigs != null && !sourceConfigs.isEmpty()) {;
            sourceDetailsBuilder.setConfigs(sourceConfigs);
        }
        sourceDetailsBuilder.setSubscriptionType(Function.SubscriptionType.valueOf(sourceSubscriptionType));

        Type type = new TypeToken<Map<String, ConsumerSpec>>(){}.getType();

        Map<String, ConsumerSpec> topicsSchema = new Gson().fromJson(sourceTopicsSchemaString, type);

        sourceDetailsBuilder.putAllTopicsToSchema(topicsSchema);
        sourceDetailsBuilder.setTypeClassName(sourceTypeClassName);
        if (sourceTimeoutMs != null) {
            sourceDetailsBuilder.setTimeoutMs(sourceTimeoutMs);
        }
        functionDetailsBuilder.setSource(sourceDetailsBuilder);

        // Setup sink
        SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
        if (sinkClassname != null) {
            sinkSpecBuilder.setClassName(sinkClassname);
        }
        if (sinkConfigs != null) {
            sinkSpecBuilder.setConfigs(sinkConfigs);
        }
        if (sinkSerdeClassName != null) {
            sinkSpecBuilder.setSerDeClassName(sinkSerdeClassName);
        }
        sinkSpecBuilder.setTypeClassName(sinkTypeClassName);
        if (sinkTopic != null && !sinkTopic.isEmpty()) {
            sinkSpecBuilder.setTopic(sinkTopic);
        }

        if (!StringUtils.isEmpty(sinkSchemaTypeOrClassName)) {
            sinkSpecBuilder.setSchemaTypeOrClassName(sinkSchemaTypeOrClassName);
        }

        functionDetailsBuilder.setSink(sinkSpecBuilder);

        FunctionDetails functionDetails = functionDetailsBuilder.build();
        instanceConfig.setFunctionDetails(functionDetails);
        instanceConfig.setPort(port);

        containerFactory = new ThreadRuntimeFactory("LocalRunnerThreadGroup", pulsarServiceUrl,
                stateStorageServiceUrl,
                AuthenticationConfig.builder().clientAuthenticationPlugin(clientAuthenticationPlugin)
                        .clientAuthenticationParameters(clientAuthenticationParameters).useTls(isTrue(useTls))
                        .tlsAllowInsecureConnection(isTrue(tlsAllowInsecureConnection))
                        .tlsHostnameVerificationEnable(isTrue(tlsHostNameVerificationEnabled))
                        .tlsTrustCertsFilePath(tlsTrustCertFilePath).build());
        runtimeSpawner = new RuntimeSpawner(
                instanceConfig,
                jarFile,
                containerFactory,
                30000);

        server = ServerBuilder.forPort(port)
                .addService(new InstanceControlImpl(runtimeSpawner))
                .build()
                .start();
        log.info("JaveInstance Server started, listening on " + port);
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                try {
                    server.shutdown();
                    runtimeSpawner.close();
                } catch (Exception ex) {
                    System.err.println(ex);
                }
            }
        });
        log.info("Starting runtimeSpawner");
        runtimeSpawner.start();

        timer = Executors.newSingleThreadScheduledExecutor();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (System.currentTimeMillis() - lastHealthCheckTs > 90000) {
                        log.info("Haven't received health check from spawner in a while. Stopping instance...");
                        close();
                    }
                } catch (Exception e) {
                    log.error("Error occurred when checking for latest health check", e);
                }
            }
        }, 30000, 30000, TimeUnit.MILLISECONDS);

        runtimeSpawner.join();
        log.info("RuntimeSpawner quit, shutting down JavaInstance");
        close();
    }

    private static boolean isTrue(String param) {
        return Boolean.TRUE.toString().equals(param);
    }

    public static void main(String[] args) throws Exception {
        JavaInstanceMain javaInstanceMain = new JavaInstanceMain();
        JCommander jcommander = new JCommander(javaInstanceMain);
        jcommander.setProgramName("JavaInstanceMain");

        // parse args by JCommander
        jcommander.parse(args);
        javaInstanceMain.start();
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
            if (timer != null) {
                timer.shutdown();
            }
            if (containerFactory != null) {
                containerFactory.close();
            }
        } catch (Exception ex) {
            System.err.println(ex);
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
                InstanceCommunication.FunctionStatus response = runtimeSpawner.getFunctionStatus().get();
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
                    InstanceCommunication.MetricsData metrics = runtime.getMetrics().get();
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
            log.debug("Recieved health check request...");
            InstanceCommunication.HealthCheckResult healthCheckResult
                    = InstanceCommunication.HealthCheckResult.newBuilder().setSuccess(true).build();
            responseObserver.onNext(healthCheckResult);
            responseObserver.onCompleted();

            lastHealthCheckTs = System.currentTimeMillis();
        }
    }
}
