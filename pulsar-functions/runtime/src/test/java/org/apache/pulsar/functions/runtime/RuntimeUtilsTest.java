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

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.jose4j.json.internal.json_simple.JSONObject;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.testng.AssertJUnit.assertTrue;

@Slf4j
public class RuntimeUtilsTest {

    @Test
    public void testSplitRuntimeArgs() {
        String str1 = "-Xms314572800";
        String[] result = RuntimeUtils.splitRuntimeArgs(str1);
        Assert.assertEquals(result.length,1);
        Assert.assertEquals(result[0], str1);

        String str2 = "-Xms314572800 -Dbar=foo";
        result = RuntimeUtils.splitRuntimeArgs(str2);
        Assert.assertEquals(result.length,2);
        Assert.assertEquals(result[0], "-Xms314572800");
        Assert.assertEquals(result[1], "-Dbar=foo");

        String str3 = "-Xms314572800 -Dbar=foo -Dfoo=\"bar foo\"";
        result = RuntimeUtils.splitRuntimeArgs(str3);
        Assert.assertEquals(result.length,3);
        Assert.assertEquals(result[0], "-Xms314572800");
        Assert.assertEquals(result[1], "-Dbar=foo");
        Assert.assertEquals(result[2], "-Dfoo=\"bar foo\"");
    }

    @Test(dataProvider = "k8sRuntime")
    public void getGoInstanceCmd(boolean k8sRuntime) throws IOException {
        HashMap<String, String> goInstanceConfig;

        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setClusterName("kluster");
        instanceConfig.setInstanceId(3000);
        instanceConfig.setFunctionId("func-7734");
        instanceConfig.setFunctionVersion("1.0.0");
        instanceConfig.setMaxBufferedTuples(5);
        instanceConfig.setPort(1337);
        instanceConfig.setMetricsPort(60000);


        JSONObject userConfig = new JSONObject();
        userConfig.put("word-of-the-day", "der Weltschmerz");

        JSONObject secretsMap = new JSONObject();
        secretsMap.put("secret", "cake is a lie");

        Function.SourceSpec sources = Function.SourceSpec.newBuilder()
                .setCleanupSubscription(true)
                .setSubscriptionName("go-func-sub")
                .setTimeoutMs(500)
                .putInputSpecs("go-func-input", Function.ConsumerSpec.newBuilder().setIsRegexPattern(false).build())
                .build();

        Function.RetryDetails retryDetails = Function.RetryDetails.newBuilder()
                .setDeadLetterTopic("go-func-deadletter")
                .setMaxMessageRetries(1)
                .build();

        Function.Resources resources = Function.Resources.newBuilder()
                .setCpu(2)
                .setDisk(1024)
                .setRam(32)
                .build();

        Function.FunctionDetails functionDetails = Function.FunctionDetails.newBuilder()
                .setAutoAck(true)
                .setTenant("public")
                .setNamespace("default")
                .setName("go-func")
                .setLogTopic("go-func-log")
                .setProcessingGuarantees(Function.ProcessingGuarantees.ATLEAST_ONCE)
                .setRuntime(Function.FunctionDetails.Runtime.GO)
                .setSecretsMap(secretsMap.toJSONString())
                .setParallelism(1)
                .setSource(sources)
                .setRetryDetails(retryDetails)
                .setResources(resources)
                .setUserConfig(userConfig.toJSONString())
                .build();

        instanceConfig.setFunctionDetails(functionDetails);

        List<String> commands = RuntimeUtils.getGoInstanceCmd(instanceConfig, "config", "pulsar://localhost:6650", k8sRuntime);
        if (k8sRuntime) {
            goInstanceConfig = new ObjectMapper().readValue(commands.get(2).replaceAll("^\'|\'$", ""), HashMap.class);
        } else {
            goInstanceConfig = new ObjectMapper().readValue(commands.get(2), HashMap.class);
        }
        Assert.assertEquals(commands.toArray().length, 3);
        Assert.assertEquals(commands.get(0), "config");
        Assert.assertEquals(commands.get(1), "-instance-conf");
        Assert.assertEquals(goInstanceConfig.get("maxBufTuples"), 5);
        Assert.assertEquals(goInstanceConfig.get("maxMessageRetries"), 1);
        Assert.assertEquals(goInstanceConfig.get("killAfterIdleMs"), 0);
        Assert.assertEquals(goInstanceConfig.get("parallelism"), 1);
        Assert.assertEquals(goInstanceConfig.get("className"), "");
        Assert.assertEquals(goInstanceConfig.get("sourceSpecsTopic"), "go-func-input");
        Assert.assertEquals(goInstanceConfig.get("secretsMap"), secretsMap.toString());
        Assert.assertEquals(goInstanceConfig.get("sourceSchemaType"), "");
        Assert.assertEquals(goInstanceConfig.get("sinkSpecsTopic"), "");
        Assert.assertEquals(goInstanceConfig.get("clusterName"), "kluster");
        Assert.assertEquals(goInstanceConfig.get("nameSpace"), "default");
        Assert.assertEquals(goInstanceConfig.get("receiverQueueSize"), 0);
        Assert.assertEquals(goInstanceConfig.get("tenant"), "public");
        Assert.assertEquals(goInstanceConfig.get("ram"), 32);
        Assert.assertEquals(goInstanceConfig.get("logTopic"), "go-func-log");
        Assert.assertEquals(goInstanceConfig.get("processingGuarantees"), 0);
        Assert.assertEquals(goInstanceConfig.get("autoAck"), true);
        Assert.assertEquals(goInstanceConfig.get("regexPatternSubscription"), false);
        Assert.assertEquals(goInstanceConfig.get("pulsarServiceURL"), "pulsar://localhost:6650");
        Assert.assertEquals(goInstanceConfig.get("runtime"), 3);
        Assert.assertEquals(goInstanceConfig.get("cpu"), 2.0);
        Assert.assertEquals(goInstanceConfig.get("funcID"), "func-7734");
        Assert.assertEquals(goInstanceConfig.get("funcVersion"), "1.0.0");
        Assert.assertEquals(goInstanceConfig.get("disk"), 1024);
        Assert.assertEquals(goInstanceConfig.get("instanceID"), 3000);
        Assert.assertEquals(goInstanceConfig.get("cleanupSubscription"), true);
        Assert.assertEquals(goInstanceConfig.get("port"), 1337);
        Assert.assertEquals(goInstanceConfig.get("subscriptionType"), 0);
        Assert.assertEquals(goInstanceConfig.get("timeoutMs"), 500);
        Assert.assertEquals(goInstanceConfig.get("subscriptionName"), "go-func-sub");
        Assert.assertEquals(goInstanceConfig.get("name"), "go-func");
        Assert.assertEquals(goInstanceConfig.get("expectedHealthCheckInterval"), 0);
        Assert.assertEquals(goInstanceConfig.get("deadLetterTopic"), "go-func-deadletter");
        Assert.assertEquals(goInstanceConfig.get("userConfig"), userConfig.toString());
        Assert.assertEquals(goInstanceConfig.get("metricsPort"), 60000);
    }

    @DataProvider(name = "k8sRuntime")
    public static Object[][] k8sRuntimeFlag() {
        return new Object[][] {
                {
                        true
                },
                {
                        false
                }
        };
    }

    @Test(dataProvider = "k8sRuntime")
    public void getAdditionalJavaRuntimeArguments(boolean k8sRuntime) throws Exception {

        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setClusterName("kluster");
        instanceConfig.setInstanceId(3000);
        instanceConfig.setFunctionId("func-7734");
        instanceConfig.setFunctionVersion("1.0.0");
        instanceConfig.setMaxBufferedTuples(5);
        instanceConfig.setPort(1337);
        instanceConfig.setFunctionDetails(Function.FunctionDetails.newBuilder().build());
        instanceConfig.setAdditionalJavaRuntimeArguments(Arrays.asList("-XX:+ExitOnOutOfMemoryError"));

        List<String> cmd = RuntimeUtils.getCmd(instanceConfig, "instanceFile",
                "extraDependenciesDir", /* extra dependencies for running instances */
                "logDirectory",
                "originalCodeFileName",
                "pulsarServiceUrl",
                "stateStorageServiceUrl",
                AuthenticationConfig.builder().build(),
                "shardId",
                23,
                1234L,
                "logConfigFile",
                "secretsProviderClassName",
                "secretsProviderConfig",
                false,
                null,
                null,
                "narExtractionDirectory",
                "functionInstanceClassPath",
                false,
                "");

        log.info("cmd {}", cmd);

        assertTrue(cmd.contains("-XX:+ExitOnOutOfMemoryError"));

        // verify that the additional runtime arguments are passed before the Java class
        int indexJavaClass = cmd.indexOf("org.apache.pulsar.functions.instance.JavaInstanceMain");
        int indexAdditionalArguments = cmd.indexOf("-XX:+ExitOnOutOfMemoryError");
        assertTrue(indexJavaClass > 0);
        assertTrue(indexAdditionalArguments > 0);
        assertTrue(indexAdditionalArguments < indexJavaClass);
    }
}
