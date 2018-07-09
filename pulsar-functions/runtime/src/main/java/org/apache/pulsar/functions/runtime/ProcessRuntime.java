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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A function container implemented using java thread.
 */
@Slf4j
class ProcessRuntime implements Runtime {

    // The thread that invokes the function
    @Getter
    private Process process;
    @Getter
    private List<String> processArgs;
    private int instancePort;
    @Getter
    private Throwable deathException;
    private ManagedChannel channel;
    private InstanceControlGrpc.InstanceControlFutureStub stub;
    private ScheduledExecutorService timer;
    private InstanceConfig instanceConfig;

    ProcessRuntime(InstanceConfig instanceConfig,
                   String instanceFile,
                   String logDirectory,
                   String codeFile,
                   String pulsarServiceUrl,
                   String stateStorageServiceUrl,
                   AuthenticationConfig authConfig) {
        this.instanceConfig = instanceConfig;
        this.instancePort = instanceConfig.getPort();
        this.processArgs = composeArgs(instanceConfig, instanceFile, logDirectory, codeFile, pulsarServiceUrl, stateStorageServiceUrl,
                authConfig);
    }

    private List<String> composeArgs(InstanceConfig instanceConfig,
                                     String instanceFile,
                                     String logDirectory,
                                     String codeFile,
                                     String pulsarServiceUrl,
                                     String stateStorageServiceUrl,
                                     AuthenticationConfig authConfig) {
        List<String> args = new LinkedList<>();
        if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            args.add("java");
            args.add("-cp");
            args.add(instanceFile);

            // Keep the same env property pointing to the Java instance file so that it can be picked up
            // by the child process and manually added to classpath
            args.add(String.format("-D%s=%s", FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY, instanceFile));
            args.add("-Dlog4j.configurationFile=java_instance_log4j2.yml");
            args.add("-Dpulsar.log.dir=" + logDirectory);
            args.add("-Dpulsar.log.file=" + instanceConfig.getFunctionDetails().getName());
            if (instanceConfig.getFunctionDetails().getResources() != null) {
                Function.Resources resources = instanceConfig.getFunctionDetails().getResources();
                if (resources.getRam() != 0) {
                    args.add("-Xmx" + String.valueOf(resources.getRam()));
                }
            }
            args.add(JavaInstanceMain.class.getName());
            args.add("--jar");
            args.add(codeFile);
        } else if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.PYTHON) {
            args.add("python");
            args.add(instanceFile);
            args.add("--py");
            args.add(codeFile);
            args.add("--logging_directory");
            args.add(logDirectory);
            args.add("--logging_file");
            args.add(instanceConfig.getFunctionDetails().getName());
            // TODO:- Find a platform independent way of controlling memory for a python application
        }
        args.add("--instance_id");
        args.add(instanceConfig.getInstanceId());
        args.add("--function_id");
        args.add(instanceConfig.getFunctionId());
        args.add("--function_version");
        args.add(instanceConfig.getFunctionVersion());
        args.add("--tenant");
        args.add(instanceConfig.getFunctionDetails().getTenant());
        args.add("--namespace");
        args.add(instanceConfig.getFunctionDetails().getNamespace());
        args.add("--name");
        args.add(instanceConfig.getFunctionDetails().getName());
        args.add("--function_classname");
        args.add(instanceConfig.getFunctionDetails().getClassName());
        if (instanceConfig.getFunctionDetails().getLogTopic() != null &&
                !instanceConfig.getFunctionDetails().getLogTopic().isEmpty()) {
            args.add("--log_topic");
            args.add(instanceConfig.getFunctionDetails().getLogTopic());
        }
        args.add("--auto_ack");
        if (instanceConfig.getFunctionDetails().getAutoAck()) {
            args.add("true");
        } else {
            args.add("false");
        }

        args.add("--processing_guarantees");
        args.add(String.valueOf(instanceConfig.getFunctionDetails().getProcessingGuarantees()));
        args.add("--pulsar_serviceurl");
        args.add(pulsarServiceUrl);
        if (authConfig != null) {
            if (isNotBlank(authConfig.getClientAuthenticationPlugin())
                    && isNotBlank(authConfig.getClientAuthenticationParameters())) {
                args.add("--client_auth_plugin");
                args.add(authConfig.getClientAuthenticationPlugin());
                args.add("--client_auth_params");
                args.add(authConfig.getClientAuthenticationParameters());
            }
            args.add("--use_tls");
            args.add(Boolean.toString(authConfig.isUseTls()));
            args.add("--tls_allow_insecure");
            args.add(Boolean.toString(authConfig.isTlsAllowInsecureConnection()));
            args.add("--hostname_verification_enabled");
            args.add(Boolean.toString(authConfig.isTlsHostnameVerificationEnable()));
            if(isNotBlank(authConfig.getTlsTrustCertsFilePath())) {
                args.add("--tls_trust_cert_path");
                args.add(authConfig.getTlsTrustCertsFilePath());
            }
        }
        args.add("--max_buffered_tuples");
        args.add(String.valueOf(instanceConfig.getMaxBufferedTuples()));
        String userConfig = instanceConfig.getFunctionDetails().getUserConfig();
        if (userConfig != null && !userConfig.isEmpty()) {
            args.add("--user_config");
            args.add(userConfig);
        }
        args.add("--port");
        args.add(String.valueOf(instanceConfig.getPort()));

        // source related configs
        if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            if (!instanceConfig.getFunctionDetails().getSource().getClassName().isEmpty()) {
                args.add("--source_classname");
                args.add(instanceConfig.getFunctionDetails().getSource().getClassName());
            }
            String sourceConfigs = instanceConfig.getFunctionDetails().getSource().getConfigs();
            if (sourceConfigs != null && !sourceConfigs.isEmpty()) {
                args.add("--source_configs");
                args.add(sourceConfigs);
            }
            if (instanceConfig.getFunctionDetails().getSource().getTypeClassName() != null
                && !instanceConfig.getFunctionDetails().getSource().getTypeClassName().isEmpty()) {
                args.add("--source_type_classname");
                args.add(instanceConfig.getFunctionDetails().getSource().getTypeClassName());
            }
        }

        if (instanceConfig.getFunctionDetails().getSource().getTimeoutMs() > 0) {
            args.add("--source_timeout_ms");
            args.add(String.valueOf(instanceConfig.getFunctionDetails().getSource().getTimeoutMs()));
        }
        args.add("--source_subscription_type");
        args.add(instanceConfig.getFunctionDetails().getSource().getSubscriptionType().toString());
        args.add("--source_topics_serde_classname");
        args.add(new Gson().toJson(instanceConfig.getFunctionDetails().getSource().getTopicsToSerDeClassNameMap()));
        if (isNotBlank(instanceConfig.getFunctionDetails().getSource().getTopicsPattern())) {
            args.add("--topics_pattern");
            args.add(instanceConfig.getFunctionDetails().getSource().getTopicsPattern());
        }

        // sink related configs
        if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            if (!instanceConfig.getFunctionDetails().getSink().getClassName().isEmpty()) {
                args.add("--sink_classname");
                args.add(instanceConfig.getFunctionDetails().getSink().getClassName());
            }
            String sinkConfigs = instanceConfig.getFunctionDetails().getSink().getConfigs();
            if (sinkConfigs != null && !sinkConfigs.isEmpty()) {
                args.add("--sink_configs");
                args.add(sinkConfigs);
            }
            if (instanceConfig.getFunctionDetails().getSink().getTypeClassName() != null
                    && !instanceConfig.getFunctionDetails().getSink().getTypeClassName().isEmpty()) {
                args.add("--sink_type_classname");
                args.add(instanceConfig.getFunctionDetails().getSink().getTypeClassName());
            }
        }
        if (instanceConfig.getFunctionDetails().getSink().getTopic() != null
                && !instanceConfig.getFunctionDetails().getSink().getTopic().isEmpty()) {
            args.add("--sink_topic");
            args.add(instanceConfig.getFunctionDetails().getSink().getTopic());
        }
        if (instanceConfig.getFunctionDetails().getSink().getSerDeClassName() != null
                && !instanceConfig.getFunctionDetails().getSink().getSerDeClassName().isEmpty()) {
            args.add("--sink_serde_classname");
            args.add(instanceConfig.getFunctionDetails().getSink().getSerDeClassName());
        }

        // state storage configs
        if (null != stateStorageServiceUrl
            && instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            args.add("--state_storage_serviceurl");
            args.add(stateStorageServiceUrl);
        }
        return args;
    }

    /**
     * The core logic that initialize the thread container and executes the function
     */
    @Override
    public void start() {
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(() -> process.destroy()));
        startProcess();
        if (channel == null && stub == null) {
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", instancePort)
                    .usePlaintext(true)
                    .build();
            stub = InstanceControlGrpc.newFutureStub(channel);

            timer = Executors.newSingleThreadScheduledExecutor();
            timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    CompletableFuture<InstanceCommunication.HealthCheckResult> result = healthCheck();
                    try {
                        result.get();
                    } catch (Exception e) {
                        log.error("Health check failed for {}-{}",
                                instanceConfig.getFunctionDetails().getName(),
                                instanceConfig.getInstanceId(), e);
                    }
                }
            }, 30000, 30000, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void join() throws Exception {
        process.waitFor();
    }

    @Override
    public void stop() {
        if (timer != null) {
            timer.shutdown();
        }
        if (process != null) {
            process.destroy();
        }
        if (channel != null) {
            channel.shutdown();
        }
        channel = null;
        stub = null;
    }

    @Override
    public CompletableFuture<FunctionStatus> getFunctionStatus() {
        CompletableFuture<FunctionStatus> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<FunctionStatus> response = stub.getFunctionStatus(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<FunctionStatus>() {
            @Override
            public void onFailure(Throwable throwable) {
                FunctionStatus.Builder builder = FunctionStatus.newBuilder();
                builder.setRunning(false);
                if (deathException != null) {
                    builder.setFailureException(deathException.getMessage());
                } else {
                    builder.setFailureException(throwable.getMessage());
                }
                retval.complete(builder.build());
            }

            @Override
            public void onSuccess(InstanceCommunication.FunctionStatus t) {
                retval.complete(t);
            }
        });
        return retval;
    }

    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getAndResetMetrics() {
        CompletableFuture<InstanceCommunication.MetricsData> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<InstanceCommunication.MetricsData> response = stub.getAndResetMetrics(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<InstanceCommunication.MetricsData>() {
            @Override
            public void onFailure(Throwable throwable) {
                retval.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(InstanceCommunication.MetricsData t) {
                retval.complete(t);
            }
        });
        return retval;
    }

    public CompletableFuture<InstanceCommunication.HealthCheckResult> healthCheck() {
        CompletableFuture<InstanceCommunication.HealthCheckResult> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<InstanceCommunication.HealthCheckResult> response = stub.healthCheck(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<InstanceCommunication.HealthCheckResult>() {
            @Override
            public void onFailure(Throwable throwable) {
                retval.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(InstanceCommunication.HealthCheckResult t) {
                retval.complete(t);
            }
        });
        return retval;
    }

    private void startProcess() {
        deathException = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(processArgs);
            log.info("ProcessBuilder starting the process with args {}", String.join(" ", processBuilder.command()));
            process = processBuilder.start();
        } catch (Exception ex) {
            log.error("Starting process failed", ex);
            deathException = ex;
            return;
        }
        try {
            int exitValue = process.exitValue();
            log.error("Instance Process quit unexpectedly with return value " + exitValue);
            tryExtractingDeathException();
        } catch (IllegalThreadStateException ex) {
            log.info("Started process successfully");
        }
    }

    @Override
    public boolean isAlive() {
        if (process == null) {
            return false;
        }
        if (!process.isAlive()) {
            if (deathException == null) {
                tryExtractingDeathException();
            }
            return false;
        }
        return true;
    }

    private void tryExtractingDeathException() {
        InputStream errorStream = process.getErrorStream();
        try {
            byte[] errorBytes = new byte[errorStream.available()];
            errorStream.read(errorBytes);
            String errorMessage = new String(errorBytes);
            deathException = new RuntimeException(errorMessage);
            log.error("Extracted Process death exception", deathException);
        } catch (Exception ex) {
            deathException = ex;
            log.error("Error extracting Process death exception", deathException);
        }
    }
}
