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

package org.apache.pulsar.functions.runtime.container;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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
class ProcessFunctionContainer implements FunctionContainer {

    // The thread that invokes the function
    @Getter
    private Process process;
    @Getter
    private final ProcessBuilder processBuilder;
    private final int instancePort;
    private Exception startupException;
    private ManagedChannel channel;
    private InstanceControlGrpc.InstanceControlFutureStub stub;
    private Timer alivenessTimer;
    private int alivenessCheckInterval;

    ProcessFunctionContainer(InstanceConfig instanceConfig,
                             int maxBufferedTuples,
                             String instanceFile,
                             String logDirectory,
                             String codeFile,
                             String pulsarServiceUrl,
                             int alivenessCheckInterval) {
        List<String> args = new LinkedList<>();
        if (instanceConfig.getFunctionConfig().getRuntime() == Function.FunctionConfig.Runtime.JAVA) {
            args.add("java");
            args.add("-cp");
            args.add(instanceFile);
            args.add("-Dlog4j.configurationFile=java_instance_log4j2.yml");
            args.add("-Dpulsar.log.dir=" + logDirectory);
            args.add("-Dpulsar.log.file=" + instanceConfig.getFunctionId());
            args.add("org.apache.pulsar.functions.runtime.instance.JavaInstanceMain");
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
            args.add(instanceConfig.getFunctionId());
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
        if (instanceConfig.getFunctionConfig().getSinkTopic() != null
                && !instanceConfig.getFunctionConfig().getSinkTopic().isEmpty()) {
            args.add("--sink_topic");
            args.add(instanceConfig.getFunctionConfig().getSinkTopic());
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
        args.add(String.valueOf(maxBufferedTuples));
        Map<String, String> userConfig = instanceConfig.getFunctionConfig().getUserConfigMap();
        if (userConfig != null && !userConfig.isEmpty()) {
            args.add("--user_config");
            args.add(new Gson().toJson(userConfig));
        }
        instancePort = findAvailablePort();
        args.add("--port");
        args.add(String.valueOf(instancePort));

        processBuilder = new ProcessBuilder(args);
        this.alivenessCheckInterval = alivenessCheckInterval;
    }

    /**
     * The core logic that initialize the thread container and executes the function
     */
    @Override
    public void start() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> process.destroy()));
        startProcess();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", instancePort)
                .usePlaintext(true)
                .build();
        stub = InstanceControlGrpc.newFutureStub(channel);
        alivenessTimer = new Timer();
        alivenessTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (!process.isAlive()) {
                    log.error("Process is no longer alive, Restarting...");
                    InputStream errorStream = process.getErrorStream();
                    try {
                        byte[] errorBytes = new byte[errorStream.available()];
                        errorStream.read(errorBytes);
                        String errorMessage = new String(errorBytes);
                        startupException = new RuntimeException(errorMessage);
                        log.error("ErrorStream was " + errorMessage);
                    } catch (Exception ex) {
                        startupException = ex;
                    }
                    startProcess();
                }
            }
        }, alivenessCheckInterval * 1000, alivenessCheckInterval * 1000);
    }

    @Override
    public void join() throws Exception {
        process.waitFor();
    }

    @Override
    public void stop() {
        process.destroy();
        channel.shutdown();
        alivenessTimer.cancel();
    }

    @Override
    public CompletableFuture<FunctionStatus> getFunctionStatus() {
        CompletableFuture<FunctionStatus> retval = new CompletableFuture<>();
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
        try {
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
}
