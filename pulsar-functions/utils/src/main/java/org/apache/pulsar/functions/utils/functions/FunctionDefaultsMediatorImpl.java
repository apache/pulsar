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
package org.apache.pulsar.functions.utils.functions;

import lombok.AllArgsConstructor;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.functions.proto.Function;

@AllArgsConstructor
public class FunctionDefaultsMediatorImpl implements FunctionDefaultsMediator{
    private ClusterFunctionProducerDefaults producerDefaults;
    private ProducerConfig producerConfig;

    public boolean isBatchingDisabled(){
        // We must rely on a null check to determine if we should get the default from WorkerConfig
        // Otherwise, unexpected behavior could occur if we get the value from WorkerConfig
        // when the operator intentionally set a different value in the FunctionConfig.
        if (producerConfig.getBatchingDisabled() == null){
            return producerDefaults.isBatchingDisabled();
        }
        return producerConfig.getBatchingDisabled();
    }
    public boolean isChunkingEnabled(){
        if (producerConfig.getChunkingEnabled() == null){
            return producerDefaults.isChunkingEnabled();
        }
        return producerConfig.getChunkingEnabled();
    }
    public boolean isBlockIfQueueFullDisabled(){
        if (producerConfig.getBlockIfQueueFullDisabled() == null){
            return producerDefaults.isBlockIfQueueFullDisabled();
        }
        return producerConfig.getBlockIfQueueFullDisabled();
    }
    public CompressionType getCompressionType(){
        if (producerConfig.getCompressionType() == null){
            return producerDefaults.getCompressionType();
        }
        return producerConfig.getCompressionType();
    }
    public Function.CompressionType getCompressionTypeProto() {
        CompressionType compressionType = this.getCompressionType();
        switch (compressionType){
            case NONE:
                return Function.CompressionType.NONE;
            case ZLIB:
                return Function.CompressionType.ZLIB;
            case ZSTD:
                return Function.CompressionType.ZSTD;
            case SNAPPY:
                return Function.CompressionType.SNAPPY;
            case LZ4:
            default:
                return Function.CompressionType.LZ4;
        }
    }
    public HashingScheme getHashingScheme(){
        if (producerConfig.getHashingScheme() == null){
            return producerDefaults.getHashingScheme();
        }
        return producerConfig.getHashingScheme();
    }
    public Function.HashingScheme getHashingSchemeProto()  {
        HashingScheme hashingScheme = this.getHashingScheme();
        switch (hashingScheme){
            case JavaStringHash:
                return Function.HashingScheme.JAVA_STRING_HASH;
            case Murmur3_32Hash:
            default:
                return Function.HashingScheme.MURMUR3_32HASH;
        }
    }
    public MessageRoutingMode getMessageRoutingMode(){
        if (producerConfig.getMessageRoutingMode() == null){
            return producerDefaults.getMessageRoutingMode();
        }
        return producerConfig.getMessageRoutingMode();
    }
    public Function.MessageRoutingMode getMessageRoutingModeProto()  {
        MessageRoutingMode routingMode = this.getMessageRoutingMode();
        switch (routingMode){
            case SinglePartition:
                return Function.MessageRoutingMode.SINGLE_PARTITION;
            case RoundRobinPartition:
                return Function.MessageRoutingMode.ROUND_ROBIN_PARTITION;
            case CustomPartition:
            default:
                return Function.MessageRoutingMode.CUSTOM_PARTITION;
        }
    }
    public Long getBatchingMaxPublishDelay(){
        if (producerConfig.getBatchingMaxPublishDelay() == null || producerConfig.getBatchingMaxPublishDelay() == 0){
            return producerDefaults.getBatchingMaxPublishDelay();
        }
        return producerConfig.getBatchingMaxPublishDelay();
    }
}
