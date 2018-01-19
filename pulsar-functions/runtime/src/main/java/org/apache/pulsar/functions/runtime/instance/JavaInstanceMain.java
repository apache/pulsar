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

package org.apache.pulsar.functions.runtime.instance;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.StringConverter;
import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.container.InstanceConfig;
import org.apache.pulsar.functions.runtime.functioncache.FunctionCacheManagerImpl;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.stats.FunctionStats;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceMain {
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
    @Parameter(names = "--source_topics", description = "Input Topic Name\n", required = true)
    protected String sourceTopicName;
    @Parameter(names = "--sink_topic", description = "Output Topic Name\n")
    protected String sinkTopicName;

    @Parameter(names = "--input_serde_classnames", description = "Input SerDe\n", required = true)
    protected String inputSerdeClassName;

    @Parameter(names = "--output_serde_classname", description = "Output SerDe\n")
    protected String outputSerdeClassName;

    @Parameter(names = "--processing_guarantees", description = "Processing Guarantees\n", required = true)
    protected FunctionConfig.ProcessingGuarantees processingGuarantees;

    @Parameter(names = "--instance_id", description = "Instance Id\n", required = true)
    protected String instanceId;

    @Parameter(names = "--function_id", description = "Function Id\n", required = true)
    protected String functionId;

    @Parameter(names = "--function_version", description = "Function Version\n", required = true)
    protected String functionVersion;

    @Parameter(names = "--pulsar_serviceurl", description = "Pulsar Service Url\n", required = true)
    protected String pulsarServiceUrl;

    @Parameter(names = "--port", description = "Port to listen on\n", required = true)
    protected int port;

    @Parameter(names = "--max_buffered_tuples", description = "Maximum number of tuples to buffer\n", required = true)
    protected int maxBufferedTuples;

    @Parameter(names = "--user_config", description = "UserConfig\n")
    protected String userConfig;

    private Thread fnThread;
    private JavaInstanceRunnable javaInstanceRunnable;

    private Server server;

    public JavaInstanceMain() { }


    public void start() throws Exception {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionId(functionId);
        instanceConfig.setFunctionVersion(functionVersion);
        instanceConfig.setInstanceId(instanceId);
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        functionConfigBuilder.setTenant(tenant);
        functionConfigBuilder.setNamespace(namespace);
        functionConfigBuilder.setName(functionName);
        functionConfigBuilder.setClassName(className);
        String[] sourceTopics = sourceTopicName.split(",");
        String[] inputSerdeClassNames = inputSerdeClassName.split(",");
        if (sourceTopics.length != inputSerdeClassNames.length) {
            throw new RuntimeException("Error specifying inputs");
        }
        for (int i = 0; i < sourceTopics.length; ++i) {
            functionConfigBuilder.putInputs(sourceTopics[i], inputSerdeClassNames[i]);
        }
        if (outputSerdeClassName != null) {
            functionConfigBuilder.setOutputSerdeClassName(outputSerdeClassName);
        }
        if (sinkTopicName != null) {
            functionConfigBuilder.setSinkTopic(sinkTopicName);
        }
        functionConfigBuilder.setProcessingGuarantees(processingGuarantees);
        Map<String, String> userConfigMap = new HashMap<>();

        if (userConfig != null && !userConfig.isEmpty()) {
            String[] params = userConfig.split(",");
            for (String p : params) {
                String[] kv = p.split(":");
                if (kv.length == 2) {
                    userConfigMap.put(kv[0], kv[1]);
                }
            }
            functionConfigBuilder.putAllUserConfig(userConfigMap);
        }
        instanceConfig.setFunctionConfig(functionConfigBuilder.build());

        log.info("Starting JavaInstanceMain with {}", instanceConfig);
        this.javaInstanceRunnable = new JavaInstanceRunnable(
                instanceConfig,
                maxBufferedTuples,
                new FunctionCacheManagerImpl(),
                jarFile,
                PulsarClient.create(pulsarServiceUrl, new ClientConfiguration()));
        this.fnThread = new Thread(javaInstanceRunnable, FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()));

        server = ServerBuilder.forPort(port)
                .addService(new InstanceControlImpl(javaInstanceRunnable))
                .build()
                .start();
        log.info("JaveInstance Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    JavaInstanceMain.this.stop();
                } catch (Exception ex) {
                    System.err.println(ex);
                }
                System.err.println("*** server shut down");
            }
        });
        fnThread.start();
        server.awaitTermination();
    }

    private void stop() throws Exception {
        if (server != null) {
            server.shutdown();
        }
        fnThread.interrupt();
        fnThread.join();
    }

    public static void main(String[] args) throws Exception {
        JavaInstanceMain javaInstanceMain = new JavaInstanceMain();
        JCommander jcommander = new JCommander(javaInstanceMain);
        jcommander.setProgramName("JavaInstanceMain");

        // parse args by JCommander
        jcommander.parse(args);
        javaInstanceMain.start();
    }

    static class InstanceControlImpl extends InstanceControlGrpc.InstanceControlImplBase {
        private JavaInstanceRunnable javaInstanceRunnable;

        public InstanceControlImpl(JavaInstanceRunnable javaInstanceRunnable) {
            this.javaInstanceRunnable = javaInstanceRunnable;
        }

        @Override
        public void getFunctionStatus(Empty request, StreamObserver<InstanceCommunication.FunctionStatus> responseObserver) {
            FunctionStats stats = javaInstanceRunnable.getStats();
            String failureException = javaInstanceRunnable.getFailureException() != null
                    ? javaInstanceRunnable.getFailureException().getMessage() : "";
            InstanceCommunication.FunctionStatus response = InstanceCommunication.FunctionStatus.newBuilder()
                    .setRunning(true)
                    .setFailureException(failureException)
                    .setNumProcessed(stats.getTotalProcessed())
                    .setNumSuccessfullyProcessed(stats.getTotalSuccessfullyProcessed())
                    .setNumTimeouts(stats.getTotalTimeoutExceptions())
                    .setNumSystemExceptions(stats.getTotalSystemExceptions())
                    .setNumUserExceptions(stats.getTotalUserExceptions())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
