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
package org.apache.pulsar.functions.utils;

import com.google.gson.Gson;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link Reflections}.
 */
public class FunctionConfigUtilsTest {

    @Test
    public void testConvertBackFidelity() {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant("test-tenant");
        functionConfig.setNamespace("test-namespace");
        functionConfig.setName("test-function");
        functionConfig.setParallelism(1);
        functionConfig.setClassName(IdentityFunction.class.getName());
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(true).serdeClassName("test-serde").build());
        functionConfig.setInputSpecs(inputSpecs);
        functionConfig.setOutput("test-output");
        functionConfig.setOutputSerdeClassName("test-serde");
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        functionConfig.setRetainOrdering(false);
        functionConfig.setUserConfig(new HashMap<>());
        functionConfig.setAutoAck(true);
        functionConfig.setTimeoutMs(2000l);
        Function.FunctionDetails functionDetails = FunctionConfigUtils.convert(functionConfig, null);
        FunctionConfig convertedConfig = FunctionConfigUtils.convertFromDetails(functionDetails);
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(convertedConfig)
        );
    }

    @Test
    public void testConvertWindow() {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant("test-tenant");
        functionConfig.setNamespace("test-namespace");
        functionConfig.setName("test-function");
        functionConfig.setParallelism(1);
        functionConfig.setClassName(IdentityFunction.class.getName());
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(true).serdeClassName("test-serde").build());
        functionConfig.setInputSpecs(inputSpecs);
        functionConfig.setOutput("test-output");
        functionConfig.setOutputSerdeClassName("test-serde");
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        functionConfig.setRetainOrdering(false);
        functionConfig.setUserConfig(new HashMap<>());
        functionConfig.setAutoAck(true);
        functionConfig.setTimeoutMs(2000l);
        functionConfig.setWindowConfig(new WindowConfig().setWindowLengthCount(10));
        Function.FunctionDetails functionDetails = FunctionConfigUtils.convert(functionConfig, null);
        FunctionConfig convertedConfig = FunctionConfigUtils.convertFromDetails(functionDetails);
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(convertedConfig)
        );
    }

    @Test
    public void testFunctionConfigConvertFromDetails() {
        String name = "test1";
        String namespace = "ns1";
        String tenant = "tenant1";
        String classname = getClass().getName();
        int parallelism = 3;
        Map<String, String> userConfig = new HashMap<>();
        userConfig.put("key1", "val1");
        Function.ProcessingGuarantees processingGuarantees = Function.ProcessingGuarantees.EFFECTIVELY_ONCE;
        Function.FunctionDetails.Runtime runtime = Function.FunctionDetails.Runtime.JAVA;
        Function.SinkSpec sinkSpec = Function.SinkSpec.newBuilder().setTopic("sinkTopic1").build();
        Map<String, Function.ConsumerSpec> consumerSpecMap = new HashMap<>();
        consumerSpecMap.put("sourceTopic1", Function.ConsumerSpec.newBuilder()
                .setSchemaType(JSONSchema.class.getName()).build());
        Function.SourceSpec sourceSpec = Function.SourceSpec.newBuilder()
                .putAllInputSpecs(consumerSpecMap)
                .setSubscriptionType(Function.SubscriptionType.FAILOVER).build();
        boolean autoAck = true;
        String logTopic = "log-topic1";
        Function.Resources resources = Function.Resources.newBuilder().setCpu(1.5).setDisk(1024 * 20).setRam(1024 * 10).build();
        String packageUrl = "http://package.url";
        Map<String, String> secretsMap = new HashMap<>();
        secretsMap.put("secretConfigKey1", "secretConfigVal1");
        Function.RetryDetails retryDetails = Function.RetryDetails.newBuilder().setDeadLetterTopic("dead-letter-1").build();

        Function.FunctionDetails functionDetails = Function.FunctionDetails
                .newBuilder()
                .setNamespace(namespace)
                .setTenant(tenant)
                .setName(name)
                .setClassName(classname)
                .setParallelism(parallelism)
                .setUserConfig(new Gson().toJson(userConfig))
                .setProcessingGuarantees(processingGuarantees)
                .setRuntime(runtime)
                .setSink(sinkSpec)
                .setSource(sourceSpec)
                .setAutoAck(autoAck)
                .setLogTopic(logTopic)
                .setResources(resources)
                .setPackageUrl(packageUrl)
                .setSecretsMap(new Gson().toJson(secretsMap))
                .setRetryDetails(retryDetails)
                .build();

        FunctionConfig functionConfig = FunctionConfigUtils.convertFromDetails(functionDetails);

        assertEquals(functionConfig.getTenant(), tenant);
        assertEquals(functionConfig.getNamespace(), namespace);
        assertEquals(functionConfig.getName(), name);
        assertEquals(functionConfig.getClassName(), classname);
        assertEquals(functionConfig.getLogTopic(), logTopic);
        assertEquals(functionConfig.getResources().getCpu(), resources.getCpu());
        assertEquals(functionConfig.getResources().getDisk().longValue(), resources.getDisk());
        assertEquals(functionConfig.getResources().getRam().longValue(), resources.getRam());
        assertEquals(functionConfig.getOutput(), sinkSpec.getTopic());
        assertEquals(functionConfig.getInputSpecs().keySet(), sourceSpec.getInputSpecsMap().keySet());
    }
}
