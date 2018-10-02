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
import com.google.protobuf.Empty;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;

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
    @Parameter(names = "--function_details", description = "Function details json\n", required = true)
    protected String functionDetailsJsonString;
    @Parameter(
            names = "--jar",
            description = "Path to Jar\n",
            listConverter = StringConverter.class)
    protected String jarFile;

    @Parameter(names = "--instance_id", description = "Instance Id\n", required = true)
    protected int instanceId;

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

    @Parameter(names = "--expected_healthcheck_interval", description = "Expected interval in seconds between healtchecks", required = true)
    protected int expectedHealthCheckInterval;

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
        if (functionDetailsJsonString.charAt(0) == '\'') {
            functionDetailsJsonString = functionDetailsJsonString.substring(1);
        }
        if (functionDetailsJsonString.charAt(functionDetailsJsonString.length() - 1) == '\'') {
            functionDetailsJsonString = functionDetailsJsonString.substring(0, functionDetailsJsonString.length() - 1);
        }
        JsonFormat.parser().merge(functionDetailsJsonString, functionDetailsBuilder);
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
                null, // we really dont use this in thread container
                containerFactory,
                expectedHealthCheckInterval * 1000);

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

        if (expectedHealthCheckInterval > 0) {
            timer = Executors.newSingleThreadScheduledExecutor();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        if (System.currentTimeMillis() - lastHealthCheckTs > 3 * expectedHealthCheckInterval * 1000) {
                            log.info("Haven't received health check from spawner in a while. Stopping instance...");
                            close();
                        }
                    } catch (Exception e) {
                        log.error("Error occurred when checking for latest health check", e);
                    }
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
