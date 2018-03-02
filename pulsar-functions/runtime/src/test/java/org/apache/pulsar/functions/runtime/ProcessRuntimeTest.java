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
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
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

    FunctionConfig createFunctionConfig(FunctionConfig.Runtime runtime) {
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        functionConfigBuilder.setRuntime(runtime);
        functionConfigBuilder.setTenant(TEST_TENANT);
        functionConfigBuilder.setNamespace(TEST_NAMESPACE);
        functionConfigBuilder.setName(TEST_NAME);
        functionConfigBuilder.setClassName("org.apache.pulsar.functions.utils.functioncache.AddFunction");
        functionConfigBuilder.addInputs(TEST_NAME + "-source1");
        functionConfigBuilder.addInputs(TEST_NAME + "-source2");
        functionConfigBuilder.setOutput(TEST_NAME + "-sink");
        functionConfigBuilder.setOutputSerdeClassName("org.apache.pulsar.functions.runtime.serde.Utf8Serializer");
        return functionConfigBuilder.build();
    }

    InstanceConfig createJavaInstanceConfig(FunctionConfig.Runtime runtime) {
        InstanceConfig config = new InstanceConfig();

        config.setFunctionConfig(createFunctionConfig(runtime));
        config.setFunctionId(java.util.UUID.randomUUID().toString());
        config.setFunctionVersion("1.0");
        config.setInstanceId(java.util.UUID.randomUUID().toString());
        config.setMaxBufferedTuples(1024);

        return config;
    }

    @Test
    public void testJavaConstructor() {
        InstanceConfig config = createJavaInstanceConfig(FunctionConfig.Runtime.JAVA);

        ProcessRuntime container = factory.createContainer(config, userJarFile);
        List<String> args = container.getProcessArgs();
        assertEquals(args.size(), 39);
        args.remove(args.size() - 1);
        String expectedArgs = "java -cp " + javaInstanceJarFile + " -Dlog4j.configurationFile=java_instance_log4j2.yml "
                + "-Dpulsar.log.dir=" + logDirectory + " -Dpulsar.log.file=" + config.getFunctionConfig().getName()
                + " org.apache.pulsar.functions.runtime.JavaInstanceMain"
                + " --jar " + userJarFile + " --instance_id "
                + config.getInstanceId() + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion() + " --tenant " + config.getFunctionConfig().getTenant()
                + " --namespace " + config.getFunctionConfig().getNamespace()
                + " --name " + config.getFunctionConfig().getName()
                + " --function_classname " + config.getFunctionConfig().getClassName()
                + " --source_topics " + TEST_NAME + "-source1," + TEST_NAME + "-source2"
                + " --auto_ack false"
                + " --sink_topic " + config.getFunctionConfig().getOutput()
                + " --output_serde_classname " + config.getFunctionConfig().getOutputSerdeClassName()
                + " --processing_guarantees ATMOST_ONCE"
                + " --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port";
        assertEquals(expectedArgs, String.join(" ", args));
    }

    @Test
    public void testPythonConstructor() {
        InstanceConfig config = createJavaInstanceConfig(FunctionConfig.Runtime.PYTHON);

        ProcessRuntime container = factory.createContainer(config, userJarFile);
        List<String> args = container.getProcessArgs();
        assertEquals(args.size(), 38);
        args.remove(args.size() - 1);
        String expectedArgs = "python " + pythonInstanceFile
                + " --py " + userJarFile + " --logging_directory "
                + logDirectory + " --logging_file " + config.getFunctionConfig().getName() + " --instance_id "
                + config.getInstanceId() + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion() + " --tenant " + config.getFunctionConfig().getTenant()
                + " --namespace " + config.getFunctionConfig().getNamespace()
                + " --name " + config.getFunctionConfig().getName()
                + " --function_classname " + config.getFunctionConfig().getClassName()
                + " --source_topics " + TEST_NAME + "-source1," + TEST_NAME + "-source2"
                + " --auto_ack false"
                + " --sink_topic " + config.getFunctionConfig().getOutput()
                + " --output_serde_classname " + config.getFunctionConfig().getOutputSerdeClassName()
                + " --processing_guarantees ATMOST_ONCE"
                + " --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port";
        assertEquals(expectedArgs, String.join(" ", args));
    }

}
