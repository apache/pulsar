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
package org.apache.pulsar.functions.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.functions.instance.ClusterFunctionProducerDefaults;
import org.apache.pulsar.functions.instance.ClusterFunctionProducerDefaultsProxy;
import org.apache.pulsar.functions.instance.FunctionDefaultsConfig;
import org.apache.pulsar.functions.instance.InvalidWorkerConfigDefaultException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.mockito.Mockito.*;

public class TestClusterFunctionProducerDefaults {
    @BeforeMethod
    public void setup() {

    }
    @Test
    public void WorkerConfig_FunctionDefaultsUnknown_ShouldLoadClusterDefaults() throws URISyntaxException, IOException, InvalidWorkerConfigDefaultException {
        // call WorkerConfig.load(..) on a resource file with the expected function default properties
        URL yamlUrl = getClass().getClassLoader().getResource("test-worker-config-function-producer-defaults.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());

        Function.ProducerSpec producerSpec = Function.ProducerSpec.newBuilder()
                .setBatching(Function.Batching.UNKNOWN_BATCHING)
                .setChunking(Function.Chunking.UNKNOWN_CHUNKING)
                .setBlockIfQueueFull(Function.BlockIfQueueFull.UNKNOWN_BLOCKING)
                .setCompressionType(Function.CompressionType.UNKNOWN_COMPRESSION)
                .setHashingScheme(Function.HashingScheme.UNKNOWN_HASHING)
                .setMessageRoutingMode(Function.MessageRoutingMode.UNKNOWN_ROUTING)
                .setBatchingMaxPublishDelay(0)  // This is the default case.
                .build();
        Function.SinkSpec sink = Function.SinkSpec.newBuilder()
                .setProducerSpec(producerSpec)
                .build();
        Function.FunctionDetails details = Function.FunctionDetails.newBuilder()
                .setSink(sink)
                .build();
        /*
            batchingEnabledDefault: false
            chunkingEnabledDefault: false
            blockIfQueueFullDefault: true
            compressionTypeDefault: ZLIB
            hashingSchemeDefault: JavaStringHash
            messageRoutingModeDefault: RoundRobinPartition
            batchingMaxPublishDelayDefault: 12
        */
        ClusterFunctionProducerDefaults defaults = wc.getClusterFunctionProducerDefaults();
        Logger mockLogger = mock(Logger.class);

        Assert.assertEquals(defaults.getBatchingEnabled(details, mockLogger), false);
        Assert.assertEquals(defaults.getChunkingEnabled(details, mockLogger), false);
        Assert.assertEquals(defaults.getBlockIfQueueFull(details, mockLogger), true);
        Assert.assertEquals(defaults.getCompressionType(details, mockLogger), CompressionType.ZLIB);
        Assert.assertEquals(defaults.getHashingScheme(details, mockLogger), HashingScheme.JavaStringHash);
        Assert.assertEquals(defaults.getMessageRoutingMode(details, mockLogger), MessageRoutingMode.RoundRobinPartition);
        Assert.assertEquals(defaults.getBatchingMaxPublishDelay(details, mockLogger), 12);

    }

    @Test
    public void ClusterFunctionProducerDefaultsProxy_FunctionDefaultsUnknown_ShouldLoadClusterDefaults() throws URISyntaxException, IOException, InvalidWorkerConfigDefaultException {
        // call WorkerConfig.load(..) on a resource file with the expected function default properties
        URL yamlUrl = getClass().getClassLoader().getResource("test-worker-config-function-producer-defaults.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());

        Function.ProducerSpec producerSpec = Function.ProducerSpec.newBuilder()
                .setBatching(Function.Batching.UNKNOWN_BATCHING)
                .setChunking(Function.Chunking.UNKNOWN_CHUNKING)
                .setBlockIfQueueFull(Function.BlockIfQueueFull.UNKNOWN_BLOCKING)
                .setCompressionType(Function.CompressionType.UNKNOWN_COMPRESSION)
                .setHashingScheme(Function.HashingScheme.UNKNOWN_HASHING)
                .setMessageRoutingMode(Function.MessageRoutingMode.UNKNOWN_ROUTING)
                .setBatchingMaxPublishDelay(0)  // This is the default case.
                .build();
        Function.SinkSpec sink = Function.SinkSpec.newBuilder()
                .setProducerSpec(producerSpec)
                .build();
        Function.FunctionDetails details = Function.FunctionDetails.newBuilder()
                .setSink(sink)
                .build();
        /*
            batchingEnabledDefault: false
            chunkingEnabledDefault: false
            blockIfQueueFullDefault: true
            compressionTypeDefault: ZLIB
            hashingSchemeDefault: JavaStringHash
            messageRoutingModeDefault: RoundRobinPartition
            batchingMaxPublishDelayDefault: 12
         */

        ClusterFunctionProducerDefaults defaults = wc.getClusterFunctionProducerDefaults();
        ClusterFunctionProducerDefaultsProxy defaultsProxy = new ClusterFunctionProducerDefaultsProxy(details, defaults);

        Assert.assertEquals(defaultsProxy.getBatchingEnabled(), false);
        Assert.assertEquals(defaultsProxy.getChunkingEnabled(), false);
        Assert.assertEquals(defaultsProxy.getBlockIfQueueFull(), true);
        Assert.assertEquals(defaultsProxy.getCompressionType(), CompressionType.ZLIB);
        Assert.assertEquals(defaultsProxy.getHashingScheme(), HashingScheme.JavaStringHash);
        Assert.assertEquals(defaultsProxy.getMessageRoutingMode(), MessageRoutingMode.RoundRobinPartition);
        Assert.assertEquals(defaultsProxy.getBatchingMaxPublishDelay(), 12);
    }
    @Test
    public void WorkerConfig_LoadsExpectedDefaults() throws URISyntaxException, IOException, InvalidWorkerConfigDefaultException {
        URL yamlUrl = getClass().getClassLoader().getResource("test-worker-config-function-producer-defaults.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());
        Assert.assertEquals(wc.getFunctionDefaults().isBatchingEnabled(), false);
        Assert.assertEquals(wc.getFunctionDefaults().isChunkingEnabled(), false);
        Assert.assertEquals(wc.getFunctionDefaults().isBlockIfQueueFull(), true);
        Assert.assertEquals(wc.getFunctionDefaults().getBatchingMaxPublishDelay(), 12);
        Assert.assertEquals(wc.getFunctionDefaults().getCompressionType(), "ZLIB");
        Assert.assertEquals(wc.getFunctionDefaults().getHashingScheme(), "JavaStringHash");
        Assert.assertEquals(wc.getFunctionDefaults().getMessageRoutingMode(), "RoundRobinPartition");
    }

    @Test
    public void WorkerConfig_Constructor_DoesNotThrowNullPointerException(){
        // This test is to help isolate a flaky test
        WorkerConfig workerConfig = new WorkerConfig();
    }
    @Test
    public void WorkerConfig_BuildProducerDefaults_DoesNotThrowNullPointerException() throws InvalidWorkerConfigDefaultException {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.buildProducerDefaults();
    }
    @Test
    public void WorkerConfig_Load_DoesNotThrowNullPointerException() throws InvalidWorkerConfigDefaultException, URISyntaxException, IOException {
        URL yamlUrl = getClass().getClassLoader().getResource("test-worker-config-function-producer-defaults.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());
    }
    @Test
    public void FunctionDetails_Builder_DoesNotThrowNullPointerException() {
        Function.ProducerSpec producerSpec = Function.ProducerSpec.newBuilder()
                .setBatching(Function.Batching.ENABLED_BATCHING)
                .setChunking(Function.Chunking.DISABLED_CHUNKING)
                .setBlockIfQueueFull(Function.BlockIfQueueFull.DISABLED_BLOCKING)
                .setCompressionType(Function.CompressionType.LZ4)
                .setHashingScheme(Function.HashingScheme.MURMUR3_32HASH)
                .setMessageRoutingMode(Function.MessageRoutingMode.CUSTOM_PARTITION)
                .setBatchingMaxPublishDelay(100)
                .build();
        Function.SinkSpec sink = Function.SinkSpec.newBuilder()
                .setProducerSpec(producerSpec)
                .build();
        Function.FunctionDetails details = Function.FunctionDetails.newBuilder()
                .setSink(sink)
                .build();
    }

    @Test
    public void WorkerConfig_FunctionDefaultsSpecified_ShouldOverrideClusterDefaults() throws URISyntaxException, IOException, InvalidWorkerConfigDefaultException {
        // call WorkerConfig.load(..) on a resource file with the expected function default properties
        URL yamlUrl = getClass().getClassLoader().getResource("test-worker-config-function-producer-defaults.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());
        Function.ProducerSpec producerSpec = Function.ProducerSpec.newBuilder()
                .setBatching(Function.Batching.ENABLED_BATCHING)
                .setChunking(Function.Chunking.DISABLED_CHUNKING)
                .setBlockIfQueueFull(Function.BlockIfQueueFull.DISABLED_BLOCKING)
                .setCompressionType(Function.CompressionType.LZ4)
                .setHashingScheme(Function.HashingScheme.MURMUR3_32HASH)
                .setMessageRoutingMode(Function.MessageRoutingMode.CUSTOM_PARTITION)
                .setBatchingMaxPublishDelay(100)
                .build();
        Function.SinkSpec sink = Function.SinkSpec.newBuilder()
                .setProducerSpec(producerSpec)
                .build();
        Function.FunctionDetails details = Function.FunctionDetails.newBuilder()
                .setSink(sink)
                .build();
        /*
            batchingEnabledDefault: false
            chunkingEnabledDefault: false
            blockIfQueueFullDefault: true
            compressionTypeDefault: ZLIB
            hashingSchemeDefault: JavaStringHash
            messageRoutingModeDefault: RoundRobinPartition
            batchingMaxPublishDelayDefault: 12
        */
        ClusterFunctionProducerDefaults defaults = wc.getClusterFunctionProducerDefaults();
        Logger mockLogger = mock(Logger.class);
        Assert.assertEquals(defaults.getBatchingEnabled(details, mockLogger), true);
        Assert.assertEquals(defaults.getChunkingEnabled(details, mockLogger), false);
        Assert.assertEquals(defaults.getBlockIfQueueFull(details, mockLogger), false);
        Assert.assertEquals(defaults.getCompressionType(details, mockLogger), CompressionType.LZ4);
        Assert.assertEquals(defaults.getHashingScheme(details, mockLogger), HashingScheme.Murmur3_32Hash);
        Assert.assertEquals(defaults.getMessageRoutingMode(details, mockLogger), MessageRoutingMode.CustomPartition);
        Assert.assertEquals(defaults.getBatchingMaxPublishDelay(details, mockLogger), 100);
    }

    @Test
    public void WorkerConfig_ThrowsNoSuchFieldException_InvalidCompressionType() {
        WorkerConfig workerConfig = new WorkerConfig();
        WorkerConfig.FunctionDefaults functionDefaults = new WorkerConfig.FunctionDefaults();
        functionDefaults.setCompressionType("super-invalid-value");
        workerConfig.setFunctionDefaults(functionDefaults);
        Assert.assertThrows(InvalidWorkerConfigDefaultException.class, () -> workerConfig.buildProducerDefaults());
    }
    @Test
    public void WorkerConfig_ThrowsNoSuchFieldException_InvalidHashingScheme() {
        WorkerConfig workerConfig = new WorkerConfig();
        WorkerConfig.FunctionDefaults functionDefaults = new WorkerConfig.FunctionDefaults();
        functionDefaults.setHashingScheme("super-invalid-value");
        workerConfig.setFunctionDefaults(functionDefaults);
        Assert.assertThrows(InvalidWorkerConfigDefaultException.class, () -> workerConfig.buildProducerDefaults());
    }
    @Test
    public void WorkerConfig_ThrowsNoSuchFieldException_InvalidMessageRoutingMode() {
        WorkerConfig workerConfig = new WorkerConfig();
        WorkerConfig.FunctionDefaults functionDefaults = new WorkerConfig.FunctionDefaults();
        functionDefaults.setMessageRoutingMode("super-invalid-value");
        workerConfig.setFunctionDefaults(functionDefaults);
        Assert.assertThrows(InvalidWorkerConfigDefaultException.class, () -> workerConfig.buildProducerDefaults());
    }
    @Test
    public void WorkerConfig_ThrowsNoSuchFieldException_InvalidBatchingMaxPublishDelay() {
        WorkerConfig workerConfig = new WorkerConfig();
        WorkerConfig.FunctionDefaults functionDefaults = new WorkerConfig.FunctionDefaults();
        functionDefaults.setBatchingMaxPublishDelay(-500);
        workerConfig.setFunctionDefaults(functionDefaults);
        Assert.assertThrows(InvalidWorkerConfigDefaultException.class, () -> workerConfig.buildProducerDefaults());
    }
    @Test
    public void WorkerConfig_DoesNotThrow_InvalidWorkerConfigDefaultException() throws InvalidWorkerConfigDefaultException {
        // This protects against regression of default values being removed
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.buildProducerDefaults();
    }
}
