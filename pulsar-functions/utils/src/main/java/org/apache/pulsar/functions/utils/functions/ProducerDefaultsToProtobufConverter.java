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
import lombok.Getter;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.functions.proto.Function;

@Getter
@AllArgsConstructor
public class ProducerDefaultsToProtobufConverter {
    private ProducerConfig config;

    public Function.CompressionType getCompressionType() throws InvalidFunctionDefaultException {
        CompressionType compressionType = config.getCompressionType();
        switch (compressionType){
            case LZ4:
                return Function.CompressionType.LZ4;
            case NONE:
                return Function.CompressionType.NONE;
            case ZLIB:
                return Function.CompressionType.ZLIB;
            case ZSTD:
                return Function.CompressionType.ZSTD;
            case SNAPPY:
                return Function.CompressionType.SNAPPY;
            default:
                throw new InvalidFunctionDefaultException("compressionType", compressionType.name());
        }
    }
    public Function.HashingScheme getHashingScheme() throws InvalidFunctionDefaultException {
        HashingScheme hashingScheme = config.getHashingScheme();
        switch (hashingScheme){
            case Murmur3_32Hash:
                return Function.HashingScheme.MURMUR3_32HASH;
            case JavaStringHash:
                return Function.HashingScheme.JAVA_STRING_HASH;
            default:
                throw new InvalidFunctionDefaultException("hashingScheme", hashingScheme.name());
        }
    }
    public Function.MessageRoutingMode getMessageRoutingMode() throws InvalidFunctionDefaultException {
        MessageRoutingMode messageRoutingMode = config.getMessageRoutingMode();
        switch (messageRoutingMode){
            case CustomPartition:
                return Function.MessageRoutingMode.CUSTOM_PARTITION;
            case SinglePartition:
                return Function.MessageRoutingMode.SINGLE_PARTITION;
            case RoundRobinPartition:
                return Function.MessageRoutingMode.ROUND_ROBIN_PARTITION;
            default:
                throw new InvalidFunctionDefaultException("messageRoutingMode", messageRoutingMode.name());
        }
    }
    public boolean getBatchingDisabled(){
        return config.getBatchingDisabled();
    }
    public boolean getBatchingEnabled(){
        return !this.getBatchingDisabled();
    }
    public boolean getChunkingDisabled(){
        return config.getChunkingEnabled();
    }
    public boolean getBlockIfQueueFullDisabled(){
        return config.getBlockIfQueueFullDisabled();
    }
    public boolean getBlockIfQueueFullEnabled(){
        return !this.getBlockIfQueueFullDisabled();
    }
    public long getBatchingMaxPublishDelay(){
        return config.getBatchingMaxPublishDelay();
    }
}
