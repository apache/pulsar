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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.fs.LimitsConfig;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.runtime.instance.JavaInstanceConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link ThreadFunctionContainer}.
 */
@Slf4j
public class ProcessFunctionContainerTest {

    private static final String TEST_TENANT = "test-function-tenant";
    private static final String TEST_NAMESPACE = "test-function-namespace";
    private static final String TEST_NAME = "test-function-container";

    private final ProcessFunctionContainerFactory factory;
    private final String userJarFile;
    private final String javaInstanceJarFile;
    private final String pulsarServiceUrl;
    private final String logDirectory;

    public ProcessFunctionContainerTest() {
        this.userJarFile = "/Users/user/UserJar.jar";
        this.javaInstanceJarFile = "/Users/user/JavaInstance.jar";
        this.pulsarServiceUrl = "pulsar://localhost:6670";
        this.logDirectory = "Users/user/logs";
        this.factory = new ProcessFunctionContainerFactory(
            1024, pulsarServiceUrl, javaInstanceJarFile, logDirectory);
    }

    @AfterMethod
    public void tearDown() {
        this.factory.close();
    }

    FunctionConfig createFunctionConfig() {
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        functionConfigBuilder.setTenant(TEST_TENANT);
        functionConfigBuilder.setNamespace(TEST_NAMESPACE);
        functionConfigBuilder.setName(TEST_NAME);
        functionConfigBuilder.setClassName("org.apache.pulsar.functions.runtime.functioncache.AddFunction");
        functionConfigBuilder.setSourceTopic(TEST_NAME + "-source");
        functionConfigBuilder.setSinkTopic(TEST_NAME + "-sink");
        functionConfigBuilder.setInputSerdeClassName("org.apache.pulsar.functions.runtime.serde.Utf8Serializer");
        functionConfigBuilder.setOutputSerdeClassName("org.apache.pulsar.functions.runtime.serde.Utf8Serializer");
        return functionConfigBuilder.build();
    }

    JavaInstanceConfig createJavaInstanceConfig() {
        JavaInstanceConfig config = new JavaInstanceConfig();

        config.setFunctionConfig(createFunctionConfig());
        config.setFunctionId(java.util.UUID.randomUUID().toString());
        config.setFunctionVersion("1.0");
        config.setInstanceId(java.util.UUID.randomUUID().toString());
        LimitsConfig limitsConfig = new LimitsConfig();
        limitsConfig.setMaxTimeMs(2000);
        limitsConfig.setMaxMemoryMb(2048);
        config.setLimitsConfig(limitsConfig);

        return config;
    }

    @Test
    public void testConstructor() {
        JavaInstanceConfig config = createJavaInstanceConfig();

        ProcessFunctionContainer container = factory.createContainer(config, userJarFile);
        List<String> args = container.getProcessBuilder().command();
        assertEquals(args.size(), 39);
        args.remove(args.size() - 1);
        String expectedArgs = "java -cp " + javaInstanceJarFile + " -Dlog4j.configurationFile=java_instance_log4j2.yml "
                + "-Dpulsar.log.dir=" + logDirectory + " -Dpulsar.log.file=" + config.getFunctionId()
                + " org.apache.pulsar.functions.runtime.instance.JavaInstanceMain --instance-id "
                + config.getInstanceId() + " --function-id " + config.getFunctionId()
                + " --function-version " + config.getFunctionVersion() + " --tenant " + config.getFunctionConfig().getTenant()
                + " --namespace " + config.getFunctionConfig().getNamespace()
                + " --name " + config.getFunctionConfig().getName()
                + " --function-classname " + config.getFunctionConfig().getClassName()
                + " --source-topic " + config.getFunctionConfig().getSourceTopic()
                + " --input-serde-classname " + config.getFunctionConfig().getInputSerdeClassName()
                + " --sink-topic " + config.getFunctionConfig().getSinkTopic()
                + " --output-serde-classname " + config.getFunctionConfig().getOutputSerdeClassName()
                + " --processing-guarantees ATMOST_ONCE --jar " + userJarFile
                + " --pulsar-serviceurl " + pulsarServiceUrl
                + " --max-buffered-tuples 1024 --port";
        assertEquals(expectedArgs, String.join(" ", args));
    }

}
