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
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.CompletableFuture;

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
    private Exception startupException;
    private ManagedChannel channel;
    private InstanceControlGrpc.InstanceControlFutureStub stub;

    ProcessRuntime(InstanceConfig instanceConfig,
                   String instanceFile,
                   String logDirectory,
                   String codeFile,
                   String pulsarServiceUrl) {
        this.processArgs = composeArgs(instanceConfig, instanceFile, logDirectory, codeFile, pulsarServiceUrl);
    }

    private List<String> composeArgs(InstanceConfig instanceConfig,
                                     String instanceFile,
                                     String logDirectory,
                                     String codeFile,
                                     String pulsarServiceUrl) {
        List<String> args = new LinkedList<>();
        if (instanceConfig.getFunctionConfig().getRuntime() == Function.FunctionConfig.Runtime.JAVA) {
            args.add("java");
            args.add("-cp");
            args.add(instanceFile);
            args.add("-Dlog4j.configurationFile=java_instance_log4j2.yml");
            args.add("-Dpulsar.log.dir=" + logDirectory);
            args.add("-Dpulsar.log.file=" + instanceConfig.getFunctionConfig().getName());
            args.add("org.apache.pulsar.functions.runtime.JavaInstanceMain");
            args.add("--jar");
            args.add(codeFile);
        } else if (instanceConfig.getFunctionConfig().getRuntime() == Function.FunctionConfig.Runtime.PYTHON) {
            args.add("python");
            args.add(instanceFile);
            args.add("--py");
            args.add(codeFile);
            args.add("--logging_directory");
            args.add(logDirectory);
            args.add("--logging_file");
            args.add(instanceConfig.getFunctionConfig().getName());
        }
        args.add("--instance_id");
        args.add(instanceConfig.getInstanceId());
        args.add("--function_id");
        args.add(instanceConfig.getFunctionId());
        args.add("--function_version");
        args.add(instanceConfig.getFunctionVersion());
        args.add("--tenant");
        args.add(instanceConfig.getFunctionConfig().getTenant());
        args.add("--namespace");
        args.add(instanceConfig.getFunctionConfig().getNamespace());
        args.add("--name");
        args.add(instanceConfig.getFunctionConfig().getName());
        args.add("--function_classname");
        args.add(instanceConfig.getFunctionConfig().getClassName());
        if (instanceConfig.getFunctionConfig().getCustomSerdeInputsCount() > 0) {
            String sourceTopicString = "";
            String inputSerdeClassNameString = "";
            for (Map.Entry<String, String> entry : instanceConfig.getFunctionConfig().getCustomSerdeInputsMap().entrySet()) {
                if (sourceTopicString.isEmpty()) {
                    sourceTopicString = entry.getKey();
                } else {
                    sourceTopicString = sourceTopicString + "," + entry.getKey();
                }
                if (inputSerdeClassNameString.isEmpty()) {
                    inputSerdeClassNameString = entry.getValue();
                } else {
                    inputSerdeClassNameString = inputSerdeClassNameString + "," + entry.getValue();
                }
            }
            args.add("--custom_serde_source_topics");
            args.add(sourceTopicString);
            args.add("--custom_serde_classnames");
            args.add(inputSerdeClassNameString);
        }
        if (instanceConfig.getFunctionConfig().getInputsCount() > 0) {
            String sourceTopicString = "";
            for (String topicName : instanceConfig.getFunctionConfig().getInputsList()) {
                if (sourceTopicString.isEmpty()) {
                    sourceTopicString = topicName;
                } else {
                    sourceTopicString = sourceTopicString + "," + topicName;
                }
            }
            args.add("--source_topics");
            args.add(sourceTopicString);
        }
        args.add("--auto_ack");
        if (instanceConfig.getFunctionConfig().getAutoAck()) {
            args.add("true");
        } else {
            args.add("false");
        }
        if (instanceConfig.getFunctionConfig().getOutput() != null
                && !instanceConfig.getFunctionConfig().getOutput().isEmpty()) {
            args.add("--sink_topic");
            args.add(instanceConfig.getFunctionConfig().getOutput());
        }
        if (instanceConfig.getFunctionConfig().getOutputSerdeClassName() != null
                && !instanceConfig.getFunctionConfig().getOutputSerdeClassName().isEmpty()) {
            args.add("--output_serde_classname");
            args.add(instanceConfig.getFunctionConfig().getOutputSerdeClassName());
        }
        args.add("--processing_guarantees");
        if (instanceConfig.getFunctionConfig().getProcessingGuarantees() != null) {
            args.add(String.valueOf(instanceConfig.getFunctionConfig().getProcessingGuarantees()));
        } else {
            args.add("ATMOST_ONCE");
        }
        args.add("--pulsar_serviceurl");
        args.add(pulsarServiceUrl);
        args.add("--max_buffered_tuples");
        args.add(String.valueOf(instanceConfig.getMaxBufferedTuples()));
        Map<String, String> userConfig = instanceConfig.getFunctionConfig().getUserConfigMap();
        if (userConfig != null && !userConfig.isEmpty()) {
            args.add("--user_config");
            args.add(new Gson().toJson(userConfig));
        }
        instancePort = findAvailablePort();
        args.add("--port");
        args.add(String.valueOf(instancePort));

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
        }
    }

    @Override
    public void join() throws Exception {
        process.waitFor();
    }

    @Override
    public void stop() {
        process.destroy();
        channel.shutdown();
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
                if (startupException != null) {
                    builder.setFailureException(startupException.getMessage());
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

    private int findAvailablePort() {
        // The logic here is a little flaky. There is no guarantee that this
        // port returned will be available later on when the instance starts
        // TODO(sanjeev):- Fix this
        try {
            ServerSocket socket = new ServerSocket(0);
            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException ex){
            throw new RuntimeException("No free port found", ex);
        }
    }

    private void startProcess() {
        startupException = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(processArgs);
            log.info("ProcessBuilder starting the process with args {}", String.join(" ", processBuilder.command()));
            process = processBuilder.start();
        } catch (Exception ex) {
            log.error("Starting process failed", ex);
            startupException = ex;
            return;
        }
        try {
            int exitValue = process.exitValue();
            log.error("Instance Process quit unexpectedly with return value " + exitValue);
        } catch (IllegalThreadStateException ex) {
            log.info("Started process successfully");
        }
    }

    @Override
    public boolean isAlive() {
        return process != null && process.isAlive();
    }

    @Override
    public Exception getDeathException() {
        if (isAlive()) return null;
        if (startupException != null) return startupException;
        InputStream errorStream = process.getErrorStream();
        try {
            byte[] errorBytes = new byte[errorStream.available()];
            errorStream.read(errorBytes);
            String errorMessage = new String(errorBytes);
            startupException = new RuntimeException(errorMessage);
        } catch (Exception ex) {
            startupException = ex;
        }
        return startupException;
    }
}
