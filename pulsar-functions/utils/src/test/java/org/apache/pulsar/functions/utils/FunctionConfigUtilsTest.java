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

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.functions.*;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.functions.FunctionDefaultsMediatorImpl;
import org.apache.pulsar.functions.utils.functions.FunctionDefaultException;
import org.apache.pulsar.functions.utils.functions.InvalidFunctionDefaultException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.apache.pulsar.common.functions.FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE;
import static org.apache.pulsar.common.functions.FunctionConfig.Runtime.PYTHON;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit test of {@link Reflections}.
 */
@Slf4j
public class FunctionConfigUtilsTest {

    @Test
    public void testConvertBackFidelity() throws FunctionDefaultException {
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
        functionConfig.setRetainKeyOrdering(false);
        functionConfig.setForwardSourceMessageProperty(true);
        functionConfig.setUserConfig(new HashMap<>());
        functionConfig.setAutoAck(true);
        functionConfig.setTimeoutMs(2000l);
        functionConfig.setRuntimeFlags("-DKerberos");
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setMaxPendingMessages(100);
        producerConfig.setMaxPendingMessagesAcrossPartitions(1000);
        producerConfig.setUseThreadLocalProducers(true);
        producerConfig.setBatchBuilder("DEFAULT");
        producerConfig.setBatchingDisabled(false);
        producerConfig.setChunkingEnabled(false);
        producerConfig.setBlockIfQueueFullDisabled(false);
        producerConfig.setCompressionType(CompressionType.LZ4);
        producerConfig.setHashingScheme(HashingScheme.Murmur3_32Hash);
        producerConfig.setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        producerConfig.setBatchingMaxPublishDelay(12L);

        functionConfig.setProducerConfig(producerConfig);

        FunctionDefaultsMediatorImpl defaults = mock(FunctionDefaultsMediatorImpl.class);
        when(defaults.isBatchingDisabled()).thenReturn(false);
        when(defaults.isChunkingEnabled()).thenReturn(false);
        when(defaults.isBlockIfQueueFullDisabled()).thenReturn(false);
        when(defaults.getCompressionType()).thenReturn(CompressionType.LZ4);
        when(defaults.getCompressionTypeProto()).thenReturn(Function.CompressionType.LZ4);
        when(defaults.getHashingScheme()).thenReturn(HashingScheme.Murmur3_32Hash);
        when(defaults.getHashingSchemeProto()).thenReturn(Function.HashingScheme.MURMUR3_32HASH);
        when(defaults.getMessageRoutingMode()).thenReturn(MessageRoutingMode.CustomPartition);
        when(defaults.getMessageRoutingModeProto()).thenReturn(Function.MessageRoutingMode.CUSTOM_PARTITION);
        when(defaults.getBatchingMaxPublishDelay()).thenReturn(12L);

        Function.FunctionDetails functionDetails = FunctionConfigUtils.convert(functionConfig, null, defaults);
        FunctionConfig convertedConfig = FunctionConfigUtils.convertFromDetails(functionDetails);

        // add default resources
        functionConfig.setResources(Resources.getDefaultResources());
        // set default cleanupSubscription config
        functionConfig.setCleanupSubscription(true);
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(convertedConfig)
        );
    }

    @Test
    public void testConvertWindow() throws FunctionDefaultException {
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
        functionConfig.setRetainKeyOrdering(false);
        functionConfig.setForwardSourceMessageProperty(true);
        functionConfig.setUserConfig(new HashMap<>());
        functionConfig.setAutoAck(true);
        functionConfig.setTimeoutMs(2000l);
        functionConfig.setWindowConfig(new WindowConfig().setWindowLengthCount(10));
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setMaxPendingMessages(100);
        producerConfig.setMaxPendingMessagesAcrossPartitions(1000);
        producerConfig.setUseThreadLocalProducers(true);
        producerConfig.setBatchBuilder("KEY_BASED");
        producerConfig.setBatchingDisabled(false);
        producerConfig.setChunkingEnabled(false);
        producerConfig.setBlockIfQueueFullDisabled(false);
        producerConfig.setCompressionType(CompressionType.LZ4);
        producerConfig.setHashingScheme(HashingScheme.Murmur3_32Hash);
        producerConfig.setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        producerConfig.setBatchingMaxPublishDelay(12L);

        functionConfig.setProducerConfig(producerConfig);

        FunctionDefaultsMediatorImpl defaults = mock(FunctionDefaultsMediatorImpl.class);
        when(defaults.isBatchingDisabled()).thenReturn(false);
        when(defaults.isChunkingEnabled()).thenReturn(false);
        when(defaults.isBlockIfQueueFullDisabled()).thenReturn(false);
        when(defaults.getCompressionType()).thenReturn(CompressionType.LZ4);
        when(defaults.getCompressionTypeProto()).thenReturn(Function.CompressionType.LZ4);
        when(defaults.getHashingScheme()).thenReturn(HashingScheme.Murmur3_32Hash);
        when(defaults.getHashingSchemeProto()).thenReturn(Function.HashingScheme.MURMUR3_32HASH);
        when(defaults.getMessageRoutingMode()).thenReturn(MessageRoutingMode.CustomPartition);
        when(defaults.getMessageRoutingModeProto()).thenReturn(Function.MessageRoutingMode.CUSTOM_PARTITION);
        when(defaults.getBatchingMaxPublishDelay()).thenReturn(12L);

        Function.FunctionDetails functionDetails = FunctionConfigUtils.convert(functionConfig, null, defaults);
        FunctionConfig convertedConfig = FunctionConfigUtils.convertFromDetails(functionDetails);

        // add default resources
        functionConfig.setResources(Resources.getDefaultResources());
        // set default cleanupSubscription config
        functionConfig.setCleanupSubscription(true);
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(convertedConfig)
        );
    }

    @Test
    public void testMergeEqual() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createFunctionConfig();
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Function Names differ")
    public void testMergeDifferentName() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("name", "Different");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Tenants differ")
    public void testMergeDifferentTenant() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("tenant", "Different");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Namespaces differ")
    public void testMergeDifferentNamespace() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("namespace", "Different");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test
    public void testMergeDifferentClassName() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("className", "Different");
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getClassName(),
                "Different"
        );
        mergedConfig.setClassName(functionConfig.getClassName());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Input Topics cannot be altered")
    public void testMergeDifferentInputs() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("topicsPattern", "Different");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "isRegexPattern for input topic test-input cannot be altered")
    public void testMergeDifferentInputSpecWithRegexChange() {
        FunctionConfig functionConfig = createFunctionConfig();
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(false).serdeClassName("my-serde").build());
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("inputSpecs", inputSpecs);
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test
    public void testMergeDifferentInputSpec() {
        FunctionConfig functionConfig = createFunctionConfig();
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(true).serdeClassName("test-serde").receiverQueueSize(58).build());
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("inputSpecs", inputSpecs);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(mergedConfig.getInputSpecs().get("test-input"), newFunctionConfig.getInputSpecs().get("test-input"));
    }

    @Test
    public void testMergeDifferentLogTopic() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("logTopic", "Different");
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getLogTopic(),
                "Different"
        );
        mergedConfig.setLogTopic(functionConfig.getLogTopic());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeCleanupSubscription() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("cleanupSubscription", true);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertTrue(mergedConfig.getCleanupSubscription());

        newFunctionConfig = createUpdatedFunctionConfig("cleanupSubscription", false);
        mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertFalse(mergedConfig.getCleanupSubscription());

        newFunctionConfig = createUpdatedFunctionConfig("cleanupSubscription", true);
        mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertTrue(mergedConfig.getCleanupSubscription());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Processing Guarantees cannot be altered")
    public void testMergeDifferentProcessingGuarantees() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("processingGuarantees", EFFECTIVELY_ONCE);
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Retain Ordering cannot be altered")
    public void testMergeDifferentRetainOrdering() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("retainOrdering", true);
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Retain Key Ordering cannot be altered")
    public void testMergeDifferentRetainKeyOrdering() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("retainKeyOrdering", true);
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test
    public void testMergeDifferentUserConfig() {
        FunctionConfig functionConfig = createFunctionConfig();
        Map<String, String> myConfig = new HashMap<>();
        myConfig.put("MyKey", "MyValue");
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("userConfig", myConfig);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getUserConfig(),
                myConfig
        );
        mergedConfig.setUserConfig(functionConfig.getUserConfig());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentSecrets() {
        FunctionConfig functionConfig = createFunctionConfig();
        Map<String, String> mySecrets = new HashMap<>();
        mySecrets.put("MyKey", "MyValue");
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("secrets", mySecrets);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getSecrets(),
                mySecrets
        );
        mergedConfig.setSecrets(functionConfig.getSecrets());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Runtime cannot be altered")
    public void testMergeDifferentRuntime() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("runtime", PYTHON);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "AutoAck cannot be altered")
    public void testMergeDifferentAutoAck() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("autoAck", false);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test
    public void testMergeDifferentMaxMessageRetries() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("maxMessageRetries", 10);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getMaxMessageRetries(),
                new Integer(10)
        );
        mergedConfig.setMaxMessageRetries(functionConfig.getMaxMessageRetries());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentDeadLetterTopic() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("deadLetterTopic", "Different");
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getDeadLetterTopic(),
                "Different"
        );
        mergedConfig.setDeadLetterTopic(functionConfig.getDeadLetterTopic());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Subscription Name cannot be altered")
    public void testMergeDifferentSubname() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("subName", "Different");
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test
    public void testMergeDifferentParallelism() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("parallelism", 101);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getParallelism(),
                new Integer(101)
        );
        mergedConfig.setParallelism(functionConfig.getParallelism());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentResources() {
        FunctionConfig functionConfig = createFunctionConfig();
        Resources resources = new Resources();
        resources.setCpu(0.3);
        resources.setRam(1232l);
        resources.setDisk(123456l);
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("resources", resources);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getResources(),
                resources
        );
        mergedConfig.setResources(functionConfig.getResources());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentWindowConfig() {
        FunctionConfig functionConfig = createFunctionConfig();
        WindowConfig windowConfig = new WindowConfig();
        windowConfig.setSlidingIntervalCount(123);
        windowConfig.setSlidingIntervalDurationMs(123l);
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("windowConfig", windowConfig);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getWindowConfig(),
                windowConfig
        );
        mergedConfig.setWindowConfig(functionConfig.getWindowConfig());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentTimeout() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("timeoutMs", 102l);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getTimeoutMs(),
                new Long(102l)
        );
        mergedConfig.setTimeoutMs(functionConfig.getTimeoutMs());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeRuntimeFlags() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("runtimeFlags", "-Dfoo=bar2");
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
        assertEquals(
                mergedConfig.getRuntimeFlags(), "-Dfoo=bar2"
        );
        mergedConfig.setRuntimeFlags(functionConfig.getRuntimeFlags());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }
    @Test
    public void testMergeFunctionDefaults_validateUpdate_whenIgnoringExistingDefaults_ExistingConfigsAreNull(){
        FunctionConfig existingFunctionConfig = createFunctionConfig();

        ProducerConfig newProducerConfig = new ProducerConfig();
        newProducerConfig.setBatchingDisabled(true);
        FunctionConfig newFunctionConfig = createFunctionConfig();
        newFunctionConfig.setProducerConfig(newProducerConfig);

        FunctionConfig mergedConfig = FunctionConfigUtils
                .validateUpdate(existingFunctionConfig, newFunctionConfig, true);

        Assert.assertNotNull(mergedConfig.getProducerConfig());
        Assert.assertNotNull(existingFunctionConfig.getProducerConfig().getBatchingDisabled());
        Assert.assertNotNull(newFunctionConfig.getProducerConfig().getBatchingDisabled());
        Assert.assertEquals((boolean)existingFunctionConfig.getProducerConfig().getBatchingDisabled(), false);
        Assert.assertEquals((boolean)mergedConfig.getProducerConfig().getBatchingDisabled(), true);
        Assert.assertNull(mergedConfig.getProducerConfig().getBlockIfQueueFullDisabled());
        Assert.assertNull(mergedConfig.getProducerConfig().getChunkingEnabled());
        Assert.assertNull(mergedConfig.getProducerConfig().getCompressionType());
        Assert.assertNull(mergedConfig.getProducerConfig().getHashingScheme());
        Assert.assertNull(mergedConfig.getProducerConfig().getMessageRoutingMode());
        Assert.assertNull(mergedConfig.getProducerConfig().getBatchingMaxPublishDelay());
    }
    @Test
    public void testMergeFunctionDefaults_validateUpdate_whenNotIgnoringExistingDefaults_ExistingConfigsOverride(){
        FunctionConfig existingFunctionConfig = createFunctionConfig();

        ProducerConfig newProducerConfig = new ProducerConfig();
        newProducerConfig.setBatchingDisabled(true);
        FunctionConfig newFunctionConfig = createFunctionConfig();
        newFunctionConfig.setProducerConfig(newProducerConfig);

        FunctionConfig mergedConfig = FunctionConfigUtils
                .validateUpdate(existingFunctionConfig, newFunctionConfig, false);

        Assert.assertNotNull(mergedConfig.getProducerConfig());
        Assert.assertNotNull(existingFunctionConfig.getProducerConfig().getBatchingDisabled());
        Assert.assertNotNull(newFunctionConfig.getProducerConfig().getBatchingDisabled());
        Assert.assertEquals((boolean)existingFunctionConfig.getProducerConfig().getBatchingDisabled(), false);
        Assert.assertEquals((boolean)mergedConfig.getProducerConfig().getBatchingDisabled(), true);
        Assert.assertNotNull(mergedConfig.getProducerConfig().getBlockIfQueueFullDisabled());
        Assert.assertNotNull(mergedConfig.getProducerConfig().getChunkingEnabled());
        Assert.assertNotNull(mergedConfig.getProducerConfig().getCompressionType());
        Assert.assertNotNull(mergedConfig.getProducerConfig().getHashingScheme());
        Assert.assertNotNull(mergedConfig.getProducerConfig().getMessageRoutingMode());
        Assert.assertNotNull(mergedConfig.getProducerConfig().getBatchingMaxPublishDelay());

        Assert.assertEquals((boolean)mergedConfig.getProducerConfig().getBlockIfQueueFullDisabled(), false);
        Assert.assertEquals((boolean)mergedConfig.getProducerConfig().getChunkingEnabled(), false);
        Assert.assertEquals(mergedConfig.getProducerConfig().getCompressionType(), CompressionType.SNAPPY);
        Assert.assertEquals(mergedConfig.getProducerConfig().getHashingScheme(), HashingScheme.Murmur3_32Hash);
        Assert.assertEquals(mergedConfig.getProducerConfig().getMessageRoutingMode(), MessageRoutingMode.RoundRobinPartition);
        Assert.assertEquals((long)mergedConfig.getProducerConfig().getBatchingMaxPublishDelay(), 21L);
    }
    @Test
    public void testMergeFunctionDefaults_overrideBatchingDisabled(){
        FunctionConfig existingFunctionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfigDefaults("batchingDisabled", true);
        FunctionConfig mergedConfig = FunctionConfigUtils
                .validateUpdate(existingFunctionConfig, newFunctionConfig, false);
        Assert.assertEquals((boolean)mergedConfig.getProducerConfig().getBatchingDisabled(), true);
    }
    @Test
    public void testMergeFunctionDefaults_overrideChunkingEnabled(){
        FunctionConfig existingFunctionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfigDefaults("chunkingEnabled", true);
        FunctionConfig mergedConfig = FunctionConfigUtils
                .validateUpdate(existingFunctionConfig, newFunctionConfig, false);
        Assert.assertEquals((boolean)mergedConfig.getProducerConfig().getChunkingEnabled(), true);
    }
    @Test
    public void testMergeFunctionDefaults_overrideBlockIfQueueFullDisabled(){
        FunctionConfig existingFunctionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfigDefaults("blockIfQueueFullDisabled", true);
        FunctionConfig mergedConfig = FunctionConfigUtils
                .validateUpdate(existingFunctionConfig, newFunctionConfig, false);
        Assert.assertEquals((boolean)mergedConfig.getProducerConfig().getBlockIfQueueFullDisabled(), true);
    }
    @Test
    public void testMergeFunctionDefaults_overrideCompressionType(){
        FunctionConfig existingFunctionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfigDefaults("compressionType", CompressionType.ZLIB);
        FunctionConfig mergedConfig = FunctionConfigUtils
                .validateUpdate(existingFunctionConfig, newFunctionConfig, false);
        Assert.assertEquals(mergedConfig.getProducerConfig().getCompressionType(), CompressionType.ZLIB);
    }
    @Test
    public void testMergeFunctionDefaults_overrideHashingScheme(){
        FunctionConfig existingFunctionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfigDefaults("hashingScheme", HashingScheme.JavaStringHash);
        FunctionConfig mergedConfig = FunctionConfigUtils
                .validateUpdate(existingFunctionConfig, newFunctionConfig, false);
        Assert.assertEquals(mergedConfig.getProducerConfig().getHashingScheme(), HashingScheme.JavaStringHash);
    }
    @Test
    public void testMergeFunctionDefaults_overrideMessageRoutingMode(){
        FunctionConfig existingFunctionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfigDefaults("messageRoutingMode",
                MessageRoutingMode.SinglePartition);
        FunctionConfig mergedConfig = FunctionConfigUtils
                .validateUpdate(existingFunctionConfig, newFunctionConfig, false);
        Assert.assertEquals(mergedConfig.getProducerConfig().getMessageRoutingMode(),
                MessageRoutingMode.SinglePartition);
    }
    @Test
    public void testMergeFunctionDefaults_overrideBatchingMaxPublishDelay(){
        FunctionConfig existingFunctionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfigDefaults("batchingMaxPublishDelay", 5L);
        FunctionConfig mergedConfig = FunctionConfigUtils
                .validateUpdate(existingFunctionConfig, newFunctionConfig, false);
        Assert.assertEquals((long)mergedConfig.getProducerConfig().getBatchingMaxPublishDelay(), 5L);
    }

    private FunctionConfig createFunctionConfig() {
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
        functionConfig.setOutputSchemaType("json");
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        functionConfig.setRetainOrdering(false);
        functionConfig.setRetainKeyOrdering(false);
        functionConfig.setSubscriptionPosition(SubscriptionInitialPosition.Earliest);
        functionConfig.setBatchBuilder("DEFAULT");
        functionConfig.setForwardSourceMessageProperty(false);
        functionConfig.setUserConfig(new HashMap<>());
        functionConfig.setAutoAck(true);
        functionConfig.setTimeoutMs(2000l);
        functionConfig.setWindowConfig(new WindowConfig().setWindowLengthCount(10));
        functionConfig.setCleanupSubscription(true);
        functionConfig.setRuntimeFlags("-Dfoo=bar");

        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setBatchingDisabled(false);
        producerConfig.setChunkingEnabled(false);
        producerConfig.setBlockIfQueueFullDisabled(false);
        producerConfig.setCompressionType(CompressionType.SNAPPY);
        producerConfig.setHashingScheme(HashingScheme.Murmur3_32Hash);
        producerConfig.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        producerConfig.setBatchingMaxPublishDelay(21L);

        functionConfig.setProducerConfig(producerConfig);
        return functionConfig;
    }

    private FunctionConfig createUpdatedFunctionConfig(String fieldName, Object fieldValue) {
        FunctionConfig functionConfig = createFunctionConfig();
        Class<?> fClass = FunctionConfig.class;
        try {
            Field chap = fClass.getDeclaredField(fieldName);
            chap.setAccessible(true);
            chap.set(functionConfig, fieldValue);
        } catch (Exception e) {
            throw new RuntimeException("Something wrong with the test", e);
        }
        return functionConfig;
    }
    private FunctionConfig createUpdatedFunctionConfigDefaults(String fieldName, Object fieldValue) {
        FunctionConfig functionConfig = createFunctionConfig();
        ProducerConfig producerConfig = functionConfig.getProducerConfig();
        Class<?> fClass = ProducerConfig.class;
        try {
            Field chap = fClass.getDeclaredField(fieldName);
            chap.setAccessible(true);
            chap.set(producerConfig, fieldValue);
        } catch (Exception e) {
            throw new RuntimeException("Something wrong with the test", e);
        }
        return functionConfig;
    }

    @Test
    public void testDisableForwardSourceMessageProperty() throws InvalidProtocolBufferException, FunctionDefaultException {
        FunctionConfig config = new FunctionConfig();
        config.setTenant("test-tenant");
        config.setNamespace("test-namespace");
        config.setName("test-function");
        config.setParallelism(1);
        config.setClassName(IdentityFunction.class.getName());
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(true).serdeClassName("test-serde").build());
        config.setInputSpecs(inputSpecs);
        config.setOutput("test-output");
        config.setForwardSourceMessageProperty(true);
        FunctionConfigUtils.inferMissingArguments(config, false);
        assertNull(config.getForwardSourceMessageProperty());
        FunctionDefaultsMediatorImpl defaults = mock(FunctionDefaultsMediatorImpl.class);
        FunctionDetails details = FunctionConfigUtils.convert(config, FunctionConfigUtilsTest.class.getClassLoader(), defaults);
        assertFalse(details.getSink().getForwardSourceMessageProperty());
        String detailsJson = "'" + JsonFormat.printer().omittingInsignificantWhitespace().print(details) + "'";
        log.info("Function details : {}", detailsJson);
        assertFalse(detailsJson.contains("forwardSourceMessageProperty"));
    }

    @Test
    public void testFunctionConfigConvertFromDetails() throws InvalidFunctionDefaultException {
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
                .setSubscriptionType(Function.SubscriptionType.FAILOVER)
                .setCleanupSubscription(true)
                .build();
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
        assertEquals((Object) functionConfig.getResources().getCpu(), resources.getCpu());
        assertEquals(functionConfig.getResources().getDisk().longValue(), resources.getDisk());
        assertEquals(functionConfig.getResources().getRam().longValue(), resources.getRam());
        assertEquals(functionConfig.getOutput(), sinkSpec.getTopic());
        assertEquals(functionConfig.getInputSpecs().keySet(), sourceSpec.getInputSpecsMap().keySet());
        assertEquals(functionConfig.getCleanupSubscription().booleanValue(), sourceSpec.getCleanupSubscription());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Output Serde mismatch")
    public void testMergeDifferentSerde() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("outputSerdeClassName", "test-updated-serde");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Output Schema mismatch")
    public void testMergeDifferentOutputSchemaTypes() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("outputSchemaType", "avro");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, false);
    }
    @Test
    public void testMergeEqual_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createFunctionConfig();
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Function Names differ")
    public void testMergeDifferentName_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("name", "Different");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Tenants differ")
    public void testMergeDifferentTenant_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("tenant", "Different");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Namespaces differ")
    public void testMergeDifferentNamespace_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("namespace", "Different");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test
    public void testMergeDifferentClassName_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("className", "Different");
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getClassName(),
                "Different"
        );
        mergedConfig.setClassName(functionConfig.getClassName());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Input Topics cannot be altered")
    public void testMergeDifferentInputs_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("topicsPattern", "Different");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "isRegexPattern for input topic test-input cannot be altered")
    public void testMergeDifferentInputSpecWithRegexChange_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(false).serdeClassName("my-serde").build());
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("inputSpecs", inputSpecs);
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test
    public void testMergeDifferentInputSpec_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(true).serdeClassName("test-serde").receiverQueueSize(58).build());
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("inputSpecs", inputSpecs);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(mergedConfig.getInputSpecs().get("test-input"), newFunctionConfig.getInputSpecs().get("test-input"));
    }

    @Test
    public void testMergeDifferentLogTopic_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("logTopic", "Different");
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getLogTopic(),
                "Different"
        );
        mergedConfig.setLogTopic(functionConfig.getLogTopic());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeCleanupSubscription_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("cleanupSubscription", true);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertTrue(mergedConfig.getCleanupSubscription());

        newFunctionConfig = createUpdatedFunctionConfig("cleanupSubscription", false);
        mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertFalse(mergedConfig.getCleanupSubscription());

        newFunctionConfig = createUpdatedFunctionConfig("cleanupSubscription", true);
        mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertTrue(mergedConfig.getCleanupSubscription());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Processing Guarantees cannot be altered")
    public void testMergeDifferentProcessingGuarantees_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("processingGuarantees", EFFECTIVELY_ONCE);
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Retain Ordering cannot be altered")
    public void testMergeDifferentRetainOrdering_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("retainOrdering", true);
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Retain Key Ordering cannot be altered")
    public void testMergeDifferentRetainKeyOrdering_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("retainKeyOrdering", true);
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test
    public void testMergeDifferentUserConfig_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        Map<String, String> myConfig = new HashMap<>();
        myConfig.put("MyKey", "MyValue");
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("userConfig", myConfig);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getUserConfig(),
                myConfig
        );
        mergedConfig.setUserConfig(functionConfig.getUserConfig());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentSecrets_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        Map<String, String> mySecrets = new HashMap<>();
        mySecrets.put("MyKey", "MyValue");
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("secrets", mySecrets);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getSecrets(),
                mySecrets
        );
        mergedConfig.setSecrets(functionConfig.getSecrets());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Runtime cannot be altered")
    public void testMergeDifferentRuntime_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("runtime", PYTHON);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "AutoAck cannot be altered")
    public void testMergeDifferentAutoAck_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("autoAck", false);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test
    public void testMergeDifferentMaxMessageRetries_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("maxMessageRetries", 10);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getMaxMessageRetries(),
                new Integer(10)
        );
        mergedConfig.setMaxMessageRetries(functionConfig.getMaxMessageRetries());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentDeadLetterTopic_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("deadLetterTopic", "Different");
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getDeadLetterTopic(),
                "Different"
        );
        mergedConfig.setDeadLetterTopic(functionConfig.getDeadLetterTopic());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Subscription Name cannot be altered")
    public void testMergeDifferentSubname_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("subName", "Different");
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test
    public void testMergeDifferentParallelism_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("parallelism", 101);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getParallelism(),
                new Integer(101)
        );
        mergedConfig.setParallelism(functionConfig.getParallelism());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentResources_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        Resources resources = new Resources();
        resources.setCpu(0.3);
        resources.setRam(1232l);
        resources.setDisk(123456l);
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("resources", resources);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getResources(),
                resources
        );
        mergedConfig.setResources(functionConfig.getResources());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentWindowConfig_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        WindowConfig windowConfig = new WindowConfig();
        windowConfig.setSlidingIntervalCount(123);
        windowConfig.setSlidingIntervalDurationMs(123l);
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("windowConfig", windowConfig);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getWindowConfig(),
                windowConfig
        );
        mergedConfig.setWindowConfig(functionConfig.getWindowConfig());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentTimeout_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("timeoutMs", 102l);
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getTimeoutMs(),
                new Long(102l)
        );
        mergedConfig.setTimeoutMs(functionConfig.getTimeoutMs());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeRuntimeFlags_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("runtimeFlags", "-Dfoo=bar2");
        FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
        assertEquals(
                mergedConfig.getRuntimeFlags(), "-Dfoo=bar2"
        );
        mergedConfig.setRuntimeFlags(functionConfig.getRuntimeFlags());
        assertEquals(
                new Gson().toJson(functionConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Output Serde mismatch")
    public void testMergeDifferentSerde_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("outputSerdeClassName", "test-updated-serde");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Output Schema mismatch")
    public void testMergeDifferentOutputSchemaTypes_IgnoreExistingFunctionDefaults() {
        FunctionConfig functionConfig = createFunctionConfig();
        FunctionConfig newFunctionConfig = createUpdatedFunctionConfig("outputSchemaType", "avro");
        FunctionConfigUtils.validateUpdate(functionConfig, newFunctionConfig, true);
    }
}
