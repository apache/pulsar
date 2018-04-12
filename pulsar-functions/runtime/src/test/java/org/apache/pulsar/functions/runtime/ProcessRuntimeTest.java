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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.runtime.ProcessRuntime;
import org.apache.pulsar.functions.runtime.ProcessRuntimeFactory;
import org.apache.pulsar.functions.runtime.ThreadRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;

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

    private final ProcessRuntimeFactory factory;
    private final String userJarFile;
    private final String javaInstanceJarFile;
    private final String pythonInstanceFile;
    private final String pulsarServiceUrl;
    private final String logDirectory;

    public ProcessRuntimeTest() {
        this.userJarFile = "/Users/user/UserJar.jar";
        this.javaInstanceJarFile = "/Users/user/JavaInstance.jar";
        this.pythonInstanceFile = "/Users/user/PythonInstance.py";
        this.pulsarServiceUrl = "pulsar://localhost:6670";
        this.logDirectory = "Users/user/logs";
        this.factory = new ProcessRuntimeFactory(
            pulsarServiceUrl, javaInstanceJarFile, pythonInstanceFile, logDirectory);
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
        functionDetailsBuilder.addInputs(TEST_NAME + "-input1");
        functionDetailsBuilder.addInputs(TEST_NAME + "-input2");
        functionDetailsBuilder.setOutput(TEST_NAME + "-output");
        functionDetailsBuilder.setOutputSerdeClassName("org.apache.pulsar.functions.runtime.serde.Utf8Serializer");
        functionDetailsBuilder.setLogTopic(TEST_NAME + "-log");
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
        assertEquals(args.size(), 43);
        args.remove(args.size() - 1);
        String expectedArgs = "java -cp " + javaInstanceJarFile + " -Dlog4j.configurationFile=java_instance_log4j2.yml "
                + "-Dpulsar.log.dir=" + logDirectory + "/functions" + " -Dpulsar.log.file=" + config.getFunctionDetails().getName()
                + " org.apache.pulsar.functions.runtime.JavaInstanceMain"
                + " --jar " + userJarFile + " --instance_id "
                + config.getInstanceId() + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion() + " --tenant " + config.getFunctionDetails().getTenant()
                + " --namespace " + config.getFunctionDetails().getNamespace()
                + " --name " + config.getFunctionDetails().getName()
                + " --function_classname " + config.getFunctionDetails().getClassName()
                + " --subscription_type " + config.getFunctionDetails().getSubscriptionType()
                + " --log_topic " + config.getFunctionDetails().getLogTopic()
                + " --input_topics " + TEST_NAME + "-input1," + TEST_NAME + "-input2"
                + " --auto_ack false"
                + " --output_topic " + config.getFunctionDetails().getOutput()
                + " --output_serde_classname " + config.getFunctionDetails().getOutputSerdeClassName()
                + " --processing_guarantees ATLEAST_ONCE"
                + " --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port";
        assertEquals(expectedArgs, String.join(" ", args));
    }

    @Test
    public void testPythonConstructor() {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.PYTHON);

        ProcessRuntime container = factory.createContainer(config, userJarFile);
        List<String> args = container.getProcessArgs();
        assertEquals(args.size(), 42);
        args.remove(args.size() - 1);
        String expectedArgs = "python " + pythonInstanceFile
                + " --py " + userJarFile + " --logging_directory "
                + logDirectory + "/functions" + " --logging_file " + config.getFunctionDetails().getName() + " --instance_id "
                + config.getInstanceId() + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion() + " --tenant " + config.getFunctionDetails().getTenant()
                + " --namespace " + config.getFunctionDetails().getNamespace()
                + " --name " + config.getFunctionDetails().getName()
                + " --function_classname " + config.getFunctionDetails().getClassName()
                + " --subscription_type " + config.getFunctionDetails().getSubscriptionType()
                + " --log_topic " + config.getFunctionDetails().getLogTopic()
                + " --input_topics " + TEST_NAME + "-input1," + TEST_NAME + "-input2"
                + " --auto_ack false"
                + " --output_topic " + config.getFunctionDetails().getOutput()
                + " --output_serde_classname " + config.getFunctionDetails().getOutputSerdeClassName()
                + " --processing_guarantees ATLEAST_ONCE"
                + " --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port";
        assertEquals(expectedArgs, String.join(" ", args));
    }

}
