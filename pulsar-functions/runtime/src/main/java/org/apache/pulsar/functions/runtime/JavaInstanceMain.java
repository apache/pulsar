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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;

import java.lang.reflect.Type;
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

    @Parameter(names = "--sink_topic", description = "Output Topic Name\n")
    protected String sinkTopicName;

    @Parameter(names = "--custom_serde_source_topics", description = "Input Topics that need custom deserialization\n", required = false)
    protected String customSerdeSourceTopics;
    @Parameter(names = "--custom_serde_classnames", description = "Input SerDe\n", required = false)
    protected String customSerdeClassnames;
    @Parameter(names = "--source_topics", description = "Input Topics\n", required = false)
    protected String defaultSerdeSourceTopics;

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

    @Parameter(names = "--state_storage_serviceurl", description = "State Storage Service Url\n", required= false)
    protected String stateStorageServiceUrl;

    @Parameter(names = "--port", description = "Port to listen on\n", required = true)
    protected int port;

    @Parameter(names = "--max_buffered_tuples", description = "Maximum number of tuples to buffer\n", required = true)
    protected int maxBufferedTuples;

    @Parameter(names = "--user_config", description = "UserConfig\n")
    protected String userConfig;

    @Parameter(names = "--auto_ack", description = "Enable Auto Acking?\n")
    protected String autoAck = "true";

    private Server server;

    public JavaInstanceMain() { }


    public void start() throws Exception {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionId(functionId);
        instanceConfig.setFunctionVersion(functionVersion);
        instanceConfig.setInstanceId(instanceId);
        instanceConfig.setMaxBufferedTuples(maxBufferedTuples);
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        functionConfigBuilder.setTenant(tenant);
        functionConfigBuilder.setNamespace(namespace);
        functionConfigBuilder.setName(functionName);
        functionConfigBuilder.setClassName(className);
        if (defaultSerdeSourceTopics != null) {
            String[] sourceTopics = defaultSerdeSourceTopics.split(",");
            for (String sourceTopic : sourceTopics) {
                functionConfigBuilder.addInputs(sourceTopic);
            }
        }
        if (customSerdeSourceTopics != null && customSerdeClassnames != null) {
            String[] sourceTopics = customSerdeSourceTopics.split(",");
            String[] inputSerdeClassNames = customSerdeClassnames.split(",");
            if (sourceTopics.length != inputSerdeClassNames.length) {
                throw new RuntimeException("Error specifying inputs");
            }
            for (int i = 0; i < sourceTopics.length; ++i) {
                functionConfigBuilder.putCustomSerdeInputs(sourceTopics[i], inputSerdeClassNames[i]);
            }
        }
        if (outputSerdeClassName != null) {
            functionConfigBuilder.setOutputSerdeClassName(outputSerdeClassName);
        }
        if (sinkTopicName != null) {
            functionConfigBuilder.setOutput(sinkTopicName);
        }
        functionConfigBuilder.setProcessingGuarantees(processingGuarantees);
        if (autoAck.equals("true")) {
            functionConfigBuilder.setAutoAck(true);
        } else {
            functionConfigBuilder.setAutoAck(false);
        }
        if (userConfig != null && !userConfig.isEmpty()) {
            Type type = new TypeToken<Map<String, String>>(){}.getType();
            Map<String, String> userConfigMap = new Gson().fromJson(userConfig, type);
            functionConfigBuilder.putAllUserConfig(userConfigMap);
        }
        FunctionConfig functionConfig = functionConfigBuilder.build();
        instanceConfig.setFunctionConfig(functionConfig);

        ThreadRuntimeFactory containerFactory = new ThreadRuntimeFactory(
                "LocalRunnerThreadGroup",
                pulsarServiceUrl,
                stateStorageServiceUrl);

        RuntimeSpawner runtimeSpawner = new RuntimeSpawner(
                instanceConfig,
                jarFile,
                containerFactory,
                null,
                0);

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
        runtimeSpawner.join();
        log.info("RuntimeSpawner quit, shutting down JavaInstance");
        server.shutdown();
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
        private RuntimeSpawner runtimeSpawner;

        public InstanceControlImpl(RuntimeSpawner runtimeSpawner) {
            this.runtimeSpawner = runtimeSpawner;
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
    }
}
