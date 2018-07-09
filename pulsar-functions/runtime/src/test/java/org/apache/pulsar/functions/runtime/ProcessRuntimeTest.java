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

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link ThreadRuntime}.
 */
@Slf4j
public class ProcessRuntimeTest {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessRuntimeTest.class);

    private static final String TEST_TENANT = "test-function-tenant";
    private static final String TEST_NAMESPACE = "test-function-namespace";
    private static final String TEST_NAME = "test-function-container";
    private static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    static {
        topicsToSerDeClassName.put("persistent://sample/standalone/ns1/test_src", DefaultSerDe.class.getName());
    }

    private final ProcessRuntimeFactory factory;
    private final String userJarFile;
    private final String javaInstanceJarFile;
    private final String pythonInstanceFile;
    private final String pulsarServiceUrl;
    private final String stateStorageServiceUrl;
    private final String logDirectory;

    public ProcessRuntimeTest() {
        this.userJarFile = "/Users/user/UserJar.jar";
        this.javaInstanceJarFile = "/Users/user/JavaInstance.jar";
        this.pythonInstanceFile = "/Users/user/PythonInstance.py";
        this.pulsarServiceUrl = "pulsar://localhost:6670";
        this.stateStorageServiceUrl = "bk://localhost:4181";
        this.logDirectory = "Users/user/logs";
        this.factory = new ProcessRuntimeFactory(
            pulsarServiceUrl, stateStorageServiceUrl, null, javaInstanceJarFile, pythonInstanceFile, logDirectory);
    }

    @AfterMethod
    public void tearDown() {
        this.factory.close();
    }

    FunctionDetails createFunctionDetails(FunctionDetails.Runtime runtime) {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setRuntime(runtime);
        functionDetailsBuilder.setTenant(TEST_TENANT);
        functionDetailsBuilder.setNamespace(TEST_NAMESPACE);
        functionDetailsBuilder.setName(TEST_NAME);
        functionDetailsBuilder.setClassName("org.apache.pulsar.functions.utils.functioncache.AddFunction");
        functionDetailsBuilder.setSink(Function.SinkSpec.newBuilder()
                .setTopic(TEST_NAME + "-output")
                .setSerDeClassName("org.apache.pulsar.functions.runtime.serde.Utf8Serializer")
                .setClassName("org.pulsar.pulsar.TestSink")
                .setTypeClassName(String.class.getName())
                .build());
        functionDetailsBuilder.setLogTopic(TEST_NAME + "-log");
        functionDetailsBuilder.setSource(Function.SourceSpec.newBuilder()
                .setSubscriptionType(Function.SubscriptionType.FAILOVER)
                .putAllTopicsToSerDeClassName(topicsToSerDeClassName)
                .setTopicsPattern("persistent://tenant/ns/.*")
                .setClassName("org.pulsar.pulsar.TestSource")
                .setTypeClassName(String.class.getName()));
        return functionDetailsBuilder.build();
    }

    InstanceConfig createJavaInstanceConfig(FunctionDetails.Runtime runtime) {
        InstanceConfig config = new InstanceConfig();

        config.setFunctionDetails(createFunctionDetails(runtime));
        config.setFunctionId(java.util.UUID.randomUUID().toString());
        config.setFunctionVersion("1.0");
        config.setInstanceId(java.util.UUID.randomUUID().toString());
        config.setMaxBufferedTuples(1024);

        return config;
    }

    @Test
    public void testJavaConstructor() {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        ProcessRuntime container = factory.createContainer(config, userJarFile);
        List<String> args = container.getProcessArgs();
        assertEquals(args.size(), 56);
        String expectedArgs = "java -cp " + javaInstanceJarFile
                + " -Dpulsar.functions.java.instance.jar=" + javaInstanceJarFile
                + " -Dlog4j.configurationFile=java_instance_log4j2.yml "
                + "-Dpulsar.log.dir=" + logDirectory + "/functions" + " -Dpulsar.log.file=" + config.getFunctionDetails().getName()
                + " org.apache.pulsar.functions.runtime.JavaInstanceMain"
                + " --jar " + userJarFile + " --instance_id "
                + config.getInstanceId() + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion() + " --tenant " + config.getFunctionDetails().getTenant()
                + " --namespace " + config.getFunctionDetails().getNamespace()
                + " --name " + config.getFunctionDetails().getName()
                + " --function_classname " + config.getFunctionDetails().getClassName()
                + " --log_topic " + config.getFunctionDetails().getLogTopic()
                + " --auto_ack false"
                + " --processing_guarantees ATLEAST_ONCE"
                + " --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port " + args.get(35)
                + " --source_classname " + config.getFunctionDetails().getSource().getClassName()
                + " --source_type_classname " + config.getFunctionDetails().getSource().getTypeClassName()
                + " --source_subscription_type " + config.getFunctionDetails().getSource().getSubscriptionType().name()
                + " --source_topics_serde_classname " + new Gson().toJson(topicsToSerDeClassName)
                + " --topics_pattern " + config.getFunctionDetails().getSource().getTopicsPattern()
                + " --sink_classname " + config.getFunctionDetails().getSink().getClassName()
                + " --sink_type_classname " + config.getFunctionDetails().getSink().getTypeClassName()
                + " --sink_topic " + config.getFunctionDetails().getSink().getTopic()
                + " --sink_serde_classname " + config.getFunctionDetails().getSink().getSerDeClassName()
                + " --state_storage_serviceurl " + stateStorageServiceUrl;
        assertEquals(expectedArgs, String.join(" ", args));
    }

    @Test
    public void testPythonConstructor() {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.PYTHON);

        ProcessRuntime container = factory.createContainer(config, userJarFile);
        List<String> args = container.getProcessArgs();
        assertEquals(args.size(), 44);
        String expectedArgs = "python " + pythonInstanceFile
                + " --py " + userJarFile + " --logging_directory "
                + logDirectory + "/functions" + " --logging_file " + config.getFunctionDetails().getName() + " --instance_id "
                + config.getInstanceId() + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion() + " --tenant " + config.getFunctionDetails().getTenant()
                + " --namespace " + config.getFunctionDetails().getNamespace()
                + " --name " + config.getFunctionDetails().getName()
                + " --function_classname " + config.getFunctionDetails().getClassName()
                + " --log_topic " + config.getFunctionDetails().getLogTopic()
                + " --auto_ack false"
                + " --processing_guarantees ATLEAST_ONCE"
                + " --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port " + args.get(33)
                + " --source_subscription_type " + config.getFunctionDetails().getSource().getSubscriptionType().name()
                + " --source_topics_serde_classname " + new Gson().toJson(topicsToSerDeClassName)
                + " --topics_pattern " + config.getFunctionDetails().getSource().getTopicsPattern()
                + " --sink_topic " + config.getFunctionDetails().getSink().getTopic()
                + " --sink_serde_classname " + config.getFunctionDetails().getSink().getSerDeClassName();
        assertEquals(expectedArgs, String.join(" ", args));
    }

}
