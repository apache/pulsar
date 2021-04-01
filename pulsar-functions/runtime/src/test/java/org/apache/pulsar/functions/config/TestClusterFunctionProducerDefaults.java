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

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.functions.utils.functions.*;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.mockito.Mockito.*;

public class TestClusterFunctionProducerDefaults {
    @BeforeMethod
    public void setup() {

    }
    @Test
    public void FunctionDefaultsMediatorImpl_ShouldUseCorrectClusterOverrides()
            throws URISyntaxException, IOException, InvalidWorkerConfigDefaultException {
        // call WorkerConfig.load(..) on a resource file with the expected function default properties
        URL yamlUrl = getClass().getClassLoader()
                .getResource("test-worker-config-function-producer-defaults.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());

        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setBatchingDisabled(false);
        producerConfig.setChunkingEnabled(true);
        producerConfig.setBlockIfQueueFullDisabled(true);
        producerConfig.setCompressionType(CompressionType.LZ4);
        producerConfig.setHashingScheme(HashingScheme.Murmur3_32Hash);
        producerConfig.setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        producerConfig.setBatchingMaxPublishDelay(0L); // This is the default case, so it should be treated like a null
        /*
            functionDefaults:
              batchingDisabled: true
              chunkingEnabled: false
              blockIfQueueFullDisabled: false
              batchingMaxPublishDelay: 12
              compressionType: ZLIB
              hashingScheme: JavaStringHash
              messageRoutingMode: RoundRobinPartition
        */
        ClusterFunctionProducerDefaults defaults = wc.getClusterFunctionProducerDefaults();

        FunctionDefaultsMediatorImpl mediator = new FunctionDefaultsMediatorImpl(defaults, producerConfig);

        Assert.assertEquals(mediator.isBatchingDisabled(), false);
        Assert.assertEquals(mediator.isChunkingEnabled(), true);
        Assert.assertEquals(mediator.isBlockIfQueueFullDisabled(), true);
        Assert.assertEquals(mediator.getCompressionType(), CompressionType.LZ4);
        Assert.assertEquals(mediator.getHashingScheme(), HashingScheme.Murmur3_32Hash);
        Assert.assertEquals(mediator.getMessageRoutingMode(), MessageRoutingMode.CustomPartition);
        Assert.assertEquals((long)mediator.getBatchingMaxPublishDelay(), 12L);

    }

    @Test
    public void ClusterFunctionProducerDefaultsProxy_FunctionDefaultsUnknown_ShouldLoadClusterDefaults() throws URISyntaxException, IOException, InvalidWorkerConfigDefaultException {
        // call WorkerConfig.load(..) on a resource file with the expected function default properties
        URL yamlUrl = getClass().getClassLoader().getResource("test-worker-config-function-producer-defaults.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());

        Function.ProducerSpec producerSpec = Function.ProducerSpec.newBuilder()
                .setBatchingDisabled(false)
                .setChunkingEnabled(false)
                .setBlockIfQueueFullDisabled(false)
                .setCompressionType(Function.CompressionType.LZ4)
                .setHashingScheme(Function.HashingScheme.MURMUR3_32HASH)
                .setMessageRoutingMode(Function.MessageRoutingMode.CUSTOM_PARTITION)
                .setBatchingMaxPublishDelay(0L)  // This is the default case.
                .build();

        ProducerDefaultsFromProtobufConverter converter = new ProducerDefaultsFromProtobufConverter(producerSpec);
        ProducerConfig producerConfig = converter.getProducerConfig();

        ClusterFunctionProducerDefaults defaults = wc.getClusterFunctionProducerDefaults();

        FunctionDefaultsMediatorImpl defaultsConfig = new FunctionDefaultsMediatorImpl(defaults, producerConfig);

        Assert.assertEquals(defaultsConfig.isBatchingDisabled(), false);
        Assert.assertEquals(defaultsConfig.isChunkingEnabled(), false);
        Assert.assertEquals(defaultsConfig.isBlockIfQueueFullDisabled(), false);
        Assert.assertEquals(defaultsConfig.getCompressionType(), CompressionType.LZ4);
        Assert.assertEquals(defaultsConfig.getHashingScheme(), HashingScheme.Murmur3_32Hash);
        Assert.assertEquals(defaultsConfig.getMessageRoutingMode(), MessageRoutingMode.CustomPartition);
        Assert.assertEquals((long)defaultsConfig.getBatchingMaxPublishDelay(), 12L);
    }
    @Test
    public void WorkerConfig_LoadsExpectedDefaults() throws URISyntaxException, IOException, InvalidWorkerConfigDefaultException {
        URL yamlUrl = getClass().getClassLoader().getResource("test-worker-config-function-producer-defaults.yml");
        /*
        functionDefaults:
            batchingDisabled: true
            chunkingEnabled: false
            blockIfQueueFullDisabled: false
            batchingMaxPublishDelay: 12
            compressionType: ZLIB
            hashingScheme: JavaStringHash
            messageRoutingMode: RoundRobinPartition
        */
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());
        Assert.assertEquals(wc.getClusterFunctionProducerDefaults().isBatchingDisabled(), true);
        Assert.assertEquals(wc.getClusterFunctionProducerDefaults().isChunkingEnabled(), false);
        Assert.assertEquals(wc.getClusterFunctionProducerDefaults().isBlockIfQueueFullDisabled(), false);
        Assert.assertEquals(wc.getClusterFunctionProducerDefaults().getBatchingMaxPublishDelay(), 12L);
        Assert.assertEquals(wc.getClusterFunctionProducerDefaults().getCompressionType(), CompressionType.ZLIB);
        Assert.assertEquals(wc.getClusterFunctionProducerDefaults().getHashingScheme(), HashingScheme.JavaStringHash);
        Assert.assertEquals(wc.getClusterFunctionProducerDefaults().getMessageRoutingMode(), MessageRoutingMode.RoundRobinPartition);
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
                .setBatchingDisabled(false)
                .setChunkingEnabled(false)
                .setBlockIfQueueFullDisabled(false)
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
        /*functionDefaults:
        batchingDisabled: true
        chunkingEnabled: false
        blockIfQueueFullDisabled: false
        batchingMaxPublishDelay: 12
        compressionType: ZLIB
        hashingScheme: JavaStringHash
        messageRoutingMode: RoundRobinPartition*/
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());
        ProducerConfig config = ProducerConfig.builder()
                .batchingDisabled(false)
                .chunkingEnabled(false)
                .blockIfQueueFullDisabled(false)
                .compressionType(CompressionType.LZ4)
                .hashingScheme(HashingScheme.Murmur3_32Hash)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .batchingMaxPublishDelay(100L)
                .build();

        ClusterFunctionProducerDefaults defaults = wc.getClusterFunctionProducerDefaults();

        FunctionDefaultsMediatorImpl functionDefaults = new FunctionDefaultsMediatorImpl(defaults, config);
        Assert.assertEquals(functionDefaults.isBatchingDisabled(), false);
        Assert.assertEquals(functionDefaults.isChunkingEnabled(), false);
        Assert.assertEquals(functionDefaults.isBlockIfQueueFullDisabled(), false);
        Assert.assertEquals(functionDefaults.getCompressionType(), CompressionType.LZ4);
        Assert.assertEquals(functionDefaults.getHashingScheme(), HashingScheme.Murmur3_32Hash);
        Assert.assertEquals(functionDefaults.getMessageRoutingMode(), MessageRoutingMode.CustomPartition);
        Assert.assertEquals((long)functionDefaults.getBatchingMaxPublishDelay(), 100L);
    }
/*
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
    }*/
    @Test
    public void WorkerConfig_DoesNotThrow_InvalidWorkerConfigDefaultException() throws InvalidWorkerConfigDefaultException {
        // This protects against regression of default values being removed
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.buildProducerDefaults();
    }

    @Test
    public void ConfigureFunctionDefaults_ProducerDefaultsAreNull_GettersReturnExpectedResults(){
        ClusterFunctionProducerDefaults producerDefaults = mock(ClusterFunctionProducerDefaults.class);
        ProducerConfig config = mock(ProducerConfig.class);
        when(producerDefaults.isBatchingDisabled()).thenReturn(false);
        when(producerDefaults.isChunkingEnabled()).thenReturn(false);
        when(producerDefaults.isBlockIfQueueFullDisabled()).thenReturn(false);
        when(producerDefaults.getCompressionType()).thenReturn(CompressionType.LZ4);
        when(producerDefaults.getHashingScheme()).thenReturn(HashingScheme.Murmur3_32Hash);
        when(producerDefaults.getMessageRoutingMode()).thenReturn(MessageRoutingMode.CustomPartition);
        when(producerDefaults.getBatchingMaxPublishDelay()).thenReturn(12L);

        when(config.getBatchingDisabled()).thenReturn(null);
        when(config.getChunkingEnabled()).thenReturn(null);
        when(config.getBlockIfQueueFullDisabled()).thenReturn(null);
        when(config.getCompressionType()).thenReturn(null);
        when(config.getHashingScheme()).thenReturn(null);
        when(config.getMessageRoutingMode()).thenReturn(null);
        when(config.getBatchingMaxPublishDelay()).thenReturn(null);

        FunctionDefaultsMediatorImpl clusterFunctionDefaults = new FunctionDefaultsMediatorImpl(producerDefaults, config);

        Assert.assertEquals(clusterFunctionDefaults.isBatchingDisabled(), false);
        Assert.assertEquals(clusterFunctionDefaults.isChunkingEnabled(), false);
        Assert.assertEquals(clusterFunctionDefaults.isBlockIfQueueFullDisabled(), false);
        Assert.assertEquals(clusterFunctionDefaults.getCompressionType(), CompressionType.LZ4);
        Assert.assertEquals(clusterFunctionDefaults.isBatchingDisabled(), false);
        Assert.assertEquals(clusterFunctionDefaults.getHashingScheme(), HashingScheme.Murmur3_32Hash);
        Assert.assertEquals(clusterFunctionDefaults.getMessageRoutingMode(), MessageRoutingMode.CustomPartition);
        Assert.assertEquals((long)clusterFunctionDefaults.getBatchingMaxPublishDelay(), 12L);
    }

    @Test
    public void ConfigureFunctionDefaults_ProducerDefaultsAreNotNull_GettersReturnExpectedResults() {
        ClusterFunctionProducerDefaults producerDefaults = mock(ClusterFunctionProducerDefaults.class);
        ProducerConfig config = mock(ProducerConfig.class);
        when(producerDefaults.isBatchingDisabled()).thenReturn(true);
        when(producerDefaults.isChunkingEnabled()).thenReturn(true);
        when(producerDefaults.isBlockIfQueueFullDisabled()).thenReturn(true);
        when(producerDefaults.getCompressionType()).thenReturn(CompressionType.NONE);
        when(producerDefaults.getHashingScheme()).thenReturn(HashingScheme.JavaStringHash);
        when(producerDefaults.getMessageRoutingMode()).thenReturn(MessageRoutingMode.SinglePartition);
        when(producerDefaults.getBatchingMaxPublishDelay()).thenReturn(12L);

        when(config.getBatchingDisabled()).thenReturn(false);
        when(config.getChunkingEnabled()).thenReturn(false);
        when(config.getBlockIfQueueFullDisabled()).thenReturn(false);
        when(config.getCompressionType()).thenReturn(CompressionType.LZ4);
        when(config.getHashingScheme()).thenReturn(HashingScheme.Murmur3_32Hash);
        when(config.getMessageRoutingMode()).thenReturn(MessageRoutingMode.CustomPartition);
        when(config.getBatchingMaxPublishDelay()).thenReturn(10L);

        FunctionDefaultsMediatorImpl clusterFunctionDefaults = new FunctionDefaultsMediatorImpl(producerDefaults, config);

        Assert.assertEquals(clusterFunctionDefaults.isBatchingDisabled(), false);
        Assert.assertEquals(clusterFunctionDefaults.isChunkingEnabled(), false);
        Assert.assertEquals(clusterFunctionDefaults.isBlockIfQueueFullDisabled(), false);
        Assert.assertEquals(clusterFunctionDefaults.getCompressionType(), CompressionType.LZ4);
        Assert.assertEquals(clusterFunctionDefaults.getHashingScheme(), HashingScheme.Murmur3_32Hash);
        Assert.assertEquals(clusterFunctionDefaults.getMessageRoutingMode(), MessageRoutingMode.CustomPartition);
        Assert.assertEquals((long)clusterFunctionDefaults.getBatchingMaxPublishDelay(), 10L);
    }
}
