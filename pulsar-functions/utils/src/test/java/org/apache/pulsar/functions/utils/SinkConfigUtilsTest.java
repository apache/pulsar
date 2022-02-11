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

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.config.validation.ConfigValidationAnnotations;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.pulsar.common.functions.FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE;
import static org.apache.pulsar.common.functions.FunctionConfig.ProcessingGuarantees.ATMOST_ONCE;
import static org.apache.pulsar.common.functions.FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/**
 * Unit test of {@link SinkConfigUtilsTest}.
 */
public class SinkConfigUtilsTest {

    private ConnectorDefinition defn;

    @Data
    @Accessors(chain = true)
    @NoArgsConstructor
    public static class TestSinkConfig {
        @ConfigValidationAnnotations.NotNull
        private String configParameter;
    }

    @Test
    public void testConvertBackFidelity() throws IOException  {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant("test-tenant");
        sinkConfig.setNamespace("test-namespace");
        sinkConfig.setName("test-source");
        sinkConfig.setParallelism(1);
        sinkConfig.setArchive("builtin://jdbc");
        sinkConfig.setSourceSubscriptionName("test-subscription");
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder()
                .isRegexPattern(true)
                .receiverQueueSize(532)
                .serdeClassName("test-serde")
                .poolMessages(true).build());
        sinkConfig.setInputs(Collections.singleton("test-input"));
        sinkConfig.setInputSpecs(inputSpecs);
        sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);

        Map<String, String> producerConfigs = new HashMap<>();
        producerConfigs.put("security.protocal", "SASL_PLAINTEXT");
        Map<String, Object> configs = new HashMap<>();
        configs.put("topic", "kafka");
        configs.put("bootstrapServers", "server-1,server-2");
        configs.put("producerConfigProperties", producerConfigs);

        sinkConfig.setConfigs(configs);
        sinkConfig.setRetainOrdering(false);
        sinkConfig.setRetainKeyOrdering(false);
        sinkConfig.setAutoAck(true);
        sinkConfig.setCleanupSubscription(false);
        sinkConfig.setTimeoutMs(2000l);
        sinkConfig.setRuntimeFlags("-DKerberos");
        sinkConfig.setCleanupSubscription(true);

        sinkConfig.setResources(Resources.getDefaultResources());

        Function.FunctionDetails functionDetails = SinkConfigUtils.convert(sinkConfig, new SinkConfigUtils.ExtractedSinkDetails(null, null));
        assertEquals(Function.SubscriptionType.SHARED, functionDetails.getSource().getSubscriptionType());
        SinkConfig convertedConfig = SinkConfigUtils.convertFromDetails(functionDetails);
        assertEquals(
                new Gson().toJson(convertedConfig),
                new Gson().toJson(sinkConfig)
        );

        sinkConfig.setRetainOrdering(true);
        sinkConfig.setRetainKeyOrdering(false);

        functionDetails = SinkConfigUtils.convert(sinkConfig, new SinkConfigUtils.ExtractedSinkDetails(null, null));
        assertEquals(Function.SubscriptionType.FAILOVER, functionDetails.getSource().getSubscriptionType());
        convertedConfig = SinkConfigUtils.convertFromDetails(functionDetails);
        assertEquals(
                new Gson().toJson(convertedConfig),
                new Gson().toJson(sinkConfig));

        sinkConfig.setRetainOrdering(false);
        sinkConfig.setRetainKeyOrdering(true);

        functionDetails = SinkConfigUtils.convert(sinkConfig, new SinkConfigUtils.ExtractedSinkDetails(null, null));
        assertEquals(Function.SubscriptionType.KEY_SHARED, functionDetails.getSource().getSubscriptionType());
        convertedConfig = SinkConfigUtils.convertFromDetails(functionDetails);
        assertEquals(
                new Gson().toJson(convertedConfig),
                new Gson().toJson(sinkConfig));
    }

    @Test
    public void testParseRetainOrderingField() throws IOException {
        List<Boolean> testcases = Lists.newArrayList(true, false, null);
        for (Boolean testcase : testcases) {
            SinkConfig sinkConfig = createSinkConfig();
            sinkConfig.setRetainOrdering(testcase);
            Function.FunctionDetails functionDetails = SinkConfigUtils.convert(sinkConfig, new SinkConfigUtils.ExtractedSinkDetails(null, null));
            SinkConfig result = SinkConfigUtils.convertFromDetails(functionDetails);
            assertEquals(result.getRetainOrdering(), testcase != null ? testcase : Boolean.valueOf(false));
        }
    }

    @Test
    public void testParseKeyRetainOrderingField() throws IOException {
        List<Boolean> testcases = Lists.newArrayList(true, false, null);
        for (Boolean testcase : testcases) {
            SinkConfig sinkConfig = createSinkConfig();
            sinkConfig.setRetainKeyOrdering(testcase);
            Function.FunctionDetails functionDetails = SinkConfigUtils.convert(sinkConfig, new SinkConfigUtils.ExtractedSinkDetails(null, null));
            SinkConfig result = SinkConfigUtils.convertFromDetails(functionDetails);
            assertEquals(result.getRetainKeyOrdering(), testcase != null ? testcase : Boolean.valueOf(false));
        }
    }

    @Test
    public void testParseProcessingGuaranteesField() throws IOException {
        List<FunctionConfig.ProcessingGuarantees> testcases = Lists.newArrayList(
                EFFECTIVELY_ONCE,
                ATMOST_ONCE,
                ATLEAST_ONCE,
                null
        );

        for (FunctionConfig.ProcessingGuarantees testcase : testcases) {
            SinkConfig sinkConfig = createSinkConfig();
            sinkConfig.setProcessingGuarantees(testcase);
            Function.FunctionDetails functionDetails = SinkConfigUtils.convert(sinkConfig, new SinkConfigUtils.ExtractedSinkDetails(null, null));
            SinkConfig result = SinkConfigUtils.convertFromDetails(functionDetails);
            assertEquals(result.getProcessingGuarantees(), testcase == null ? ATLEAST_ONCE : testcase);
        }
    }

    @Test
    public void testCleanSubscriptionField() throws IOException {
        List<Boolean> testcases = Lists.newArrayList(true, false, null);

        for (Boolean testcase : testcases) {
            SinkConfig sinkConfig = createSinkConfig();
            sinkConfig.setCleanupSubscription(testcase);
            Function.FunctionDetails functionDetails = SinkConfigUtils.convert(sinkConfig, new SinkConfigUtils.ExtractedSinkDetails(null, null));
            SinkConfig result = SinkConfigUtils.convertFromDetails(functionDetails);
            assertEquals(result.getCleanupSubscription(), testcase == null ? Boolean.valueOf(true) : testcase);
        }
    }

    @Test
    public void testMergeEqual() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createSinkConfig();
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
        assertEquals(
                new Gson().toJson(sinkConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Sink Names differ")
    public void testMergeDifferentName() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("name", "Different");
        SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Tenants differ")
    public void testMergeDifferentTenant() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("tenant", "Different");
        SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Namespaces differ")
    public void testMergeDifferentNamespace() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("namespace", "Different");
        SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
    }

    @Test
    public void testMergeDifferentClassName() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("className", "Different");
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
        assertEquals(
                mergedConfig.getClassName(),
                "Different"
        );
        mergedConfig.setClassName(sinkConfig.getClassName());
        assertEquals(
                new Gson().toJson(sinkConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Input Topics cannot be altered")
    public void testMergeDifferentInputs() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("topicsPattern", "Different");
        SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "isRegexPattern for input topic test-input cannot be altered")
    public void testMergeDifferentInputSpecWithRegexChange() {
        SinkConfig sinkConfig = createSinkConfig();
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(false).serdeClassName("my-serde").build());
        SinkConfig newSinkConfig = createUpdatedSinkConfig("inputSpecs", inputSpecs);
        SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
    }

    @Test
    public void testMergeDifferentInputSpec() {
        SinkConfig sinkConfig = createSinkConfig();
        sinkConfig.getInputSpecs().put("test-input", ConsumerConfig.builder().isRegexPattern(true).receiverQueueSize(1000).build());

        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(true).serdeClassName("test-serde").receiverQueueSize(58).build());
        SinkConfig newSinkConfig = createUpdatedSinkConfig("inputSpecs", inputSpecs);
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
        assertEquals(mergedConfig.getInputSpecs().get("test-input"), newSinkConfig.getInputSpecs().get("test-input"));

        // make sure original sinkConfig was not modified
        assertEquals(sinkConfig.getInputSpecs().get("test-input").getReceiverQueueSize().intValue(), 1000);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Processing Guarantees cannot be altered")
    public void testMergeDifferentProcessingGuarantees() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("processingGuarantees", EFFECTIVELY_ONCE);
        SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Retain Ordering cannot be altered")
    public void testMergeDifferentRetainOrdering() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("retainOrdering", true);
        SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Retain Key Ordering cannot be altered")
    public void testMergeDifferentRetainKeyOrdering() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("retainKeyOrdering", true);
        SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
    }

    @Test
    public void testMergeDifferentUserConfig() {
        SinkConfig sinkConfig = createSinkConfig();
        Map<String, String> myConfig = new HashMap<>();
        myConfig.put("MyKey", "MyValue");
        SinkConfig newSinkConfig = createUpdatedSinkConfig("configs", myConfig);
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
        assertEquals(
                mergedConfig.getConfigs(),
                myConfig
        );
        mergedConfig.setConfigs(sinkConfig.getConfigs());
        assertEquals(
                new Gson().toJson(sinkConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentSecrets() {
        SinkConfig sinkConfig = createSinkConfig();
        Map<String, String> mySecrets = new HashMap<>();
        mySecrets.put("MyKey", "MyValue");
        SinkConfig newSinkConfig = createUpdatedSinkConfig("secrets", mySecrets);
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
        assertEquals(
                mergedConfig.getSecrets(),
                mySecrets
        );
        mergedConfig.setSecrets(sinkConfig.getSecrets());
        assertEquals(
                new Gson().toJson(sinkConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "AutoAck cannot be altered")
    public void testMergeDifferentAutoAck() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("autoAck", false);
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Subscription Name cannot be altered")
    public void testMergeDifferentSubname() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("sourceSubscriptionName", "Different");
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
    }

    @Test
    public void testMergeDifferentParallelism() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("parallelism", 101);
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
        assertEquals(
                mergedConfig.getParallelism(),
                new Integer(101)
        );
        mergedConfig.setParallelism(sinkConfig.getParallelism());
        assertEquals(
                new Gson().toJson(sinkConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentResources() {
        SinkConfig sinkConfig = createSinkConfig();
        Resources resources = new Resources();
        resources.setCpu(0.3);
        resources.setRam(1232l);
        resources.setDisk(123456l);
        SinkConfig newSinkConfig = createUpdatedSinkConfig("resources", resources);
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
        assertEquals(
                mergedConfig.getResources(),
                resources
        );
        mergedConfig.setResources(sinkConfig.getResources());
        assertEquals(
                new Gson().toJson(sinkConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentTimeout() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("timeoutMs", 102l);
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
        assertEquals(
                mergedConfig.getTimeoutMs(),
                new Long(102l)
        );
        mergedConfig.setTimeoutMs(sinkConfig.getTimeoutMs());
        assertEquals(
                new Gson().toJson(sinkConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentCleanupSubscription() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newSinkConfig = createUpdatedSinkConfig("cleanupSubscription", false);
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newSinkConfig);
        assertFalse(mergedConfig.getCleanupSubscription());
        mergedConfig.setCleanupSubscription(sinkConfig.getCleanupSubscription());
        assertEquals(
                new Gson().toJson(sinkConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeRuntimeFlags() {
        SinkConfig sinkConfig = createSinkConfig();
        SinkConfig newFunctionConfig = createUpdatedSinkConfig("runtimeFlags", "-Dfoo=bar2");
        SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(sinkConfig, newFunctionConfig);
        assertEquals(
                mergedConfig.getRuntimeFlags(), "-Dfoo=bar2"
        );
        mergedConfig.setRuntimeFlags(sinkConfig.getRuntimeFlags());
        assertEquals(
                new Gson().toJson(sinkConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testValidateConfig() throws IOException {
        SinkConfig sinkConfig = createSinkConfig();

        // Good config
        sinkConfig.getConfigs().put("configParameter", "Test");
        SinkConfigUtils.validateSinkConfig(sinkConfig, TestSinkConfig.class);

        // Bad config
        sinkConfig.getConfigs().put("configParameter", null);
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> SinkConfigUtils.validateSinkConfig(sinkConfig, TestSinkConfig.class));
        assertTrue(e.getMessage().contains("Could not validate sink config: Field 'configParameter' cannot be null!"));
    }

    private SinkConfig createSinkConfig() {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant("test-tenant");
        sinkConfig.setNamespace("test-namespace");
        sinkConfig.setName("test-sink");
        sinkConfig.setParallelism(1);
        sinkConfig.setClassName(IdentityFunction.class.getName());
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(true).serdeClassName("test-serde").build());
        sinkConfig.setInputSpecs(inputSpecs);
        sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sinkConfig.setRetainOrdering(false);
        sinkConfig.setRetainKeyOrdering(false);
        sinkConfig.setConfigs(new HashMap<>());
        sinkConfig.setAutoAck(true);
        sinkConfig.setTimeoutMs(2000l);
        sinkConfig.setCleanupSubscription(true);
        sinkConfig.setArchive("DummyArchive.nar");
        sinkConfig.setCleanupSubscription(true);
        return sinkConfig;
    }

    private SinkConfig createUpdatedSinkConfig(String fieldName, Object fieldValue) {
        SinkConfig sinkConfig = createSinkConfig();
        Class<?> fClass = SinkConfig.class;
        try {
            Field chap = fClass.getDeclaredField(fieldName);
            chap.setAccessible(true);
            chap.set(sinkConfig, fieldValue);
        } catch (Exception e) {
            throw new RuntimeException("Something wrong with the test", e);
        }
        return sinkConfig;
    }

    @Test
    public void testPoolMessages() throws IOException {
        SinkConfig sinkConfig = createSinkConfig();
        Function.FunctionDetails functionDetails = SinkConfigUtils.convert(sinkConfig, new SinkConfigUtils.ExtractedSinkDetails(null, null));
        assertFalse(functionDetails.getSource().getInputSpecsMap().get("test-input").getPoolMessages());
        SinkConfig convertedConfig = SinkConfigUtils.convertFromDetails(functionDetails);
        assertFalse(convertedConfig.getInputSpecs().get("test-input").isPoolMessages());

        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder()
                .poolMessages(true).build());
        sinkConfig.setInputSpecs(inputSpecs);

        functionDetails = SinkConfigUtils.convert(sinkConfig, new SinkConfigUtils.ExtractedSinkDetails(null, null));
        assertTrue(functionDetails.getSource().getInputSpecsMap().get("test-input").getPoolMessages());
        convertedConfig = SinkConfigUtils.convertFromDetails(functionDetails);
        assertTrue(convertedConfig.getInputSpecs().get("test-input").isPoolMessages());
    }
}
