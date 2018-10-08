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
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;

import java.io.InputStream;
import java.util.List;
import java.util.TimerTask;
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
    private final Long expectedHealthCheckInterval;
    private static final long GRPC_TIMEOUT_SECS = 5;

    ProcessRuntime(InstanceConfig instanceConfig,
                   String instanceFile,
                   String logDirectory,
                   String codeFile,
                   String pulsarServiceUrl,
                   String stateStorageServiceUrl,
                   AuthenticationConfig authConfig,
                   Long expectedHealthCheckInterval) throws Exception {
        this.instanceConfig = instanceConfig;
        this.instancePort = instanceConfig.getPort();
        this.expectedHealthCheckInterval = expectedHealthCheckInterval;
        this.processArgs = RuntimeUtils.composeArgs(instanceConfig, instanceFile, logDirectory, codeFile, pulsarServiceUrl, stateStorageServiceUrl,
                authConfig, instanceConfig.getInstanceName(), instanceConfig.getPort(), expectedHealthCheckInterval,
                "java_instance_log4j2.yml", false);
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
            }, expectedHealthCheckInterval, expectedHealthCheckInterval, TimeUnit.SECONDS);
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
        });
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
        });
        return retval;
    }

    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getMetrics() {
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
        });
        return retval;
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
        });
        return retval;
    }

    private void startProcess() {
        deathException = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(processArgs).inheritIO();
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
