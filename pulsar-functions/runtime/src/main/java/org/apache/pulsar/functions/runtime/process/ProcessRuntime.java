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

package org.apache.pulsar.functions.runtime.process;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceCache;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeUtils;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.FunctionCommon;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    private int metricsPort;
    @Getter
    private Throwable deathException;
    private ManagedChannel channel;
    private InstanceControlGrpc.InstanceControlFutureStub stub;
    private ScheduledFuture timer;
    private InstanceConfig instanceConfig;
    private final Long expectedHealthCheckInterval;
    private final SecretsProviderConfigurator secretsProviderConfigurator;
    private final String extraDependenciesDir;
    private final String narExtractionDirectory;
    private static final long GRPC_TIMEOUT_SECS = 5;
    private final String funcLogDir;

    ProcessRuntime(InstanceConfig instanceConfig,
                   String instanceFile,
                   String extraDependenciesDir,
                   String narExtractionDirectory,
                   String logDirectory,
                   String codeFile,
                   String pulsarServiceUrl,
                   String stateStorageServiceUrl,
                   AuthenticationConfig authConfig,
                   SecretsProviderConfigurator secretsProviderConfigurator,
                   Long expectedHealthCheckInterval,
                   String pulsarWebServiceUrl) throws Exception {
        this.instanceConfig = instanceConfig;
        this.instancePort = instanceConfig.getPort();
        this.metricsPort = instanceConfig.getMetricsPort();
        this.expectedHealthCheckInterval = expectedHealthCheckInterval;
        this.secretsProviderConfigurator = secretsProviderConfigurator;
        this.funcLogDir = RuntimeUtils.genFunctionLogFolder(logDirectory, instanceConfig);
        String logConfigFile = null;
        String secretsProviderClassName = secretsProviderConfigurator.getSecretsProviderClassName(instanceConfig.getFunctionDetails());
        String secretsProviderConfig = null;
        if (secretsProviderConfigurator.getSecretsProviderConfig(instanceConfig.getFunctionDetails()) != null) {
            secretsProviderConfig = new Gson().toJson(secretsProviderConfigurator.getSecretsProviderConfig(instanceConfig.getFunctionDetails()));
        }
        switch (instanceConfig.getFunctionDetails().getRuntime()) {
            case JAVA:
                String logConfigPath = System.getProperty("pulsar.functions.log.conf");
                if(log.isDebugEnabled()){
                    log.debug("The loaded value of pulsar.functions.log.conf is {}", logConfigPath);
                }
                // Added null check to prevent test failures
                if(logConfigPath != null && Files.exists(Paths.get(logConfigPath))){
                    logConfigFile = logConfigPath;
                } else { // Keeping existing file for backwards compatibility
                    logConfigFile = "java_instance_log4j2.xml";
                }
                break;
            case PYTHON:
                logConfigFile = System.getenv("PULSAR_HOME") + "/conf/functions-logging/logging_config.ini";
                break;
            case GO:
                break;
        }
        this.extraDependenciesDir = extraDependenciesDir;
        this.narExtractionDirectory = narExtractionDirectory;
        this.processArgs = RuntimeUtils.composeCmd(
            instanceConfig,
            instanceFile,
            // DONT SET extra dependencies here (for python or go runtime),
            // since process runtime is using Java ProcessBuilder,
            // we have to set the environment variable via ProcessBuilder
            FunctionDetails.Runtime.JAVA == instanceConfig.getFunctionDetails().getRuntime() ? extraDependenciesDir : null,
            logDirectory,
            codeFile,
            pulsarServiceUrl,
            stateStorageServiceUrl,
            authConfig,
            instanceConfig.getInstanceName(),
            instanceConfig.getPort(),
            expectedHealthCheckInterval,
            logConfigFile,
            secretsProviderClassName,
            secretsProviderConfig,
            false,
            null,
            null,
                narExtractionDirectory,
                null,
                pulsarWebServiceUrl);
    }

    /**
     * The core logic that initialize the process container and executes the function.
     */
    @Override
    public void start() {
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(() -> process.destroy()));

        // Note: we create the expected log folder before the function process logger attempts to create it
        // This is because if multiple instances are launched they can encounter a race condition creation of the dir.

        log.info("Creating function log directory {}", funcLogDir);

        try {
            Files.createDirectories(Paths.get(funcLogDir));
        } catch (IOException e) {
            log.info("Exception when creating log folder : {}",funcLogDir, e);
            throw new RuntimeException("Log folder creation error");
        }

        log.info("Created or found function log directory {}", funcLogDir);

        startProcess();
        if (channel == null && stub == null) {
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", instancePort)
                    .usePlaintext()
                    .build();
            stub = InstanceControlGrpc.newFutureStub(channel);

            timer = InstanceCache.getInstanceCache().getScheduledExecutorService().scheduleAtFixedRate(() -> {
                CompletableFuture<InstanceCommunication.HealthCheckResult> result = healthCheck();
                try {
                    result.get();
                } catch (Exception e) {
                    log.error("Health check failed for {}-{}",
                            instanceConfig.getFunctionDetails().getName(),
                            instanceConfig.getInstanceId(), e);
                }
            }, expectedHealthCheckInterval, expectedHealthCheckInterval, TimeUnit.SECONDS);
        }
    }

    @Override
    public void join() throws Exception {
        process.waitFor();
    }

    @Override
    public void stop() throws InterruptedException {
        if (timer != null) {
            timer.cancel(false);
        }
        if (channel != null) {
            channel.shutdown();
        }
        channel = null;
        stub = null;

        // kill process
        if (process != null) {
            process.destroy();
            int i = 0;
            // gracefully terminate at first
            while(process.isAlive()) {
                Thread.sleep(100);
                if (i > 100) {
                    break;
                }
                i++;
            }

            // forcibly kill after timeout
            if (process.isAlive()) {
                log.warn("Process for instance {} did not exit within timeout. Forcibly killing process...",
                        FunctionCommon.getFullyQualifiedInstanceId(
                                instanceConfig.getFunctionDetails().getTenant(),
                                instanceConfig.getFunctionDetails().getNamespace(),
                                instanceConfig.getFunctionDetails().getName(), instanceConfig.getInstanceId()));
                process.destroyForcibly();
            }
        }
    }

    @Override
    public CompletableFuture<FunctionStatus> getFunctionStatus(int instanceId) {
        CompletableFuture<FunctionStatus> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<FunctionStatus> response = stub.withDeadlineAfter(GRPC_TIMEOUT_SECS, TimeUnit.SECONDS).getFunctionStatus(Empty.newBuilder().build());
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
        }, MoreExecutors.directExecutor());
        return retval;
    }

    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getAndResetMetrics() {
        CompletableFuture<InstanceCommunication.MetricsData> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<InstanceCommunication.MetricsData> response = stub.withDeadlineAfter(GRPC_TIMEOUT_SECS, TimeUnit.SECONDS).getAndResetMetrics(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<InstanceCommunication.MetricsData>() {
            @Override
            public void onFailure(Throwable throwable) {
                retval.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(InstanceCommunication.MetricsData t) {
                retval.complete(t);
            }
        }, MoreExecutors.directExecutor());
        return retval;
    }

    @Override
    public CompletableFuture<Void> resetMetrics() {
        CompletableFuture<Void> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<Empty> response = stub.withDeadlineAfter(GRPC_TIMEOUT_SECS, TimeUnit.SECONDS).resetMetrics(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<Empty>() {
            @Override
            public void onFailure(Throwable throwable) {
                retval.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(Empty t) {
                retval.complete(null);
            }
        }, MoreExecutors.directExecutor());
        return retval;
    }

    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getMetrics(int instanceId) {
        CompletableFuture<InstanceCommunication.MetricsData> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<InstanceCommunication.MetricsData> response = stub.withDeadlineAfter(GRPC_TIMEOUT_SECS, TimeUnit.SECONDS).getMetrics(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<InstanceCommunication.MetricsData>() {
            @Override
            public void onFailure(Throwable throwable) {
                retval.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(InstanceCommunication.MetricsData t) {
                retval.complete(t);
            }
        }, MoreExecutors.directExecutor());
        return retval;
    }

    @Override
    public String getPrometheusMetrics() throws IOException {
        return RuntimeUtils.getPrometheusMetrics(metricsPort);
    }

    public CompletableFuture<InstanceCommunication.HealthCheckResult> healthCheck() {
        CompletableFuture<InstanceCommunication.HealthCheckResult> retval = new CompletableFuture<>();
        if (stub == null) {
            retval.completeExceptionally(new RuntimeException("Not alive"));
            return retval;
        }
        ListenableFuture<InstanceCommunication.HealthCheckResult> response = stub.withDeadlineAfter(GRPC_TIMEOUT_SECS, TimeUnit.SECONDS).healthCheck(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<InstanceCommunication.HealthCheckResult>() {
            @Override
            public void onFailure(Throwable throwable) {
                retval.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(InstanceCommunication.HealthCheckResult t) {
                retval.complete(t);
            }
        }, MoreExecutors.directExecutor());
        return retval;
    }

    private void startProcess() {
        deathException = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(processArgs).inheritIO();
            if (StringUtils.isNotEmpty(extraDependenciesDir)) {
                processBuilder.environment().put("PYTHONPATH", "${PYTHONPATH}:" + extraDependenciesDir);
            }
            secretsProviderConfigurator.configureProcessRuntimeSecretsProvider(processBuilder, instanceConfig.getFunctionDetails());
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
