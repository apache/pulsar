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

import lombok.Getter;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.functions.proto.Function;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Getter
public class ClusterFunctionProducerDefaults {

    // Has a method that accepts a FunctionDetails and performs the logic to determine each actual final default.

    // Could it provide the builder wrapper for the client in Sink? Does that builder use the same interface as pulsar.producerbuilder?

    private boolean batchingDisabled;
    private boolean chunkingEnabled;
    private boolean blockIfQueueFullDisabled;
    private CompressionType compressionType;
    private HashingScheme hashingScheme;
    private MessageRoutingMode messageRoutingMode;

    private long batchingMaxPublishDelay;

    public ClusterFunctionProducerDefaults(boolean batchingDisabled, boolean chunkingEnabled, boolean blockIfQueueFullDisabled,
                                           String compressionType, String hashingScheme, String messageRoutingMode,
                                           int batchingMaxPublishDelay) throws InvalidWorkerConfigDefaultException {
        this.batchingDisabled = batchingDisabled;
        this.chunkingEnabled = chunkingEnabled;
        this.blockIfQueueFullDisabled = blockIfQueueFullDisabled;
        this.setCompressionType(compressionType);
        this.setHashingScheme(hashingScheme);
        this.setMessageRoutingMode(messageRoutingMode);
        this.setBatchingMaxPublishDelay(batchingMaxPublishDelay);
    }
    // We could also validate that chunking is disabled if batching is enabled, etc.

    public void setBatchingMaxPublishDelay(int batchingMaxPublishDelay) throws InvalidWorkerConfigDefaultException {
        if(batchingMaxPublishDelay < 0){
            throw new InvalidWorkerConfigDefaultException("batchingMaxPublishDelay", Integer.toString(batchingMaxPublishDelay));
        }
        this.batchingMaxPublishDelay = batchingMaxPublishDelay;
    }

    public void setCompressionType(String compressionType) throws InvalidWorkerConfigDefaultException {
        switch(compressionType.toUpperCase(Locale.ROOT).replace("_", "").replace("-","")){
            case "NONE":
                this.compressionType = CompressionType.NONE;
                break;
            case "LZ4":
                this.compressionType = CompressionType.LZ4;
                break;
            case "ZLIB":
                this.compressionType = CompressionType.ZLIB;
                break;
            case "ZSTD":
                this.compressionType = CompressionType.ZSTD;
                break;
            case "SNAPPY":
                this.compressionType = CompressionType.SNAPPY;
                break;
            default:
                throw new InvalidWorkerConfigDefaultException("compressionType", compressionType);
        }
    }

    public void setHashingScheme(String hashingScheme) throws InvalidWorkerConfigDefaultException {
        switch(hashingScheme.toUpperCase(Locale.ROOT).replace("_", "").replace("-","")){
            case "JAVASTRINGHASH":
                this.hashingScheme = HashingScheme.JavaStringHash;
                break;
            case "MURMUR332HASH":
                this.hashingScheme = HashingScheme.Murmur3_32Hash;
                break;
            default:
               throw new InvalidWorkerConfigDefaultException("hashingScheme", hashingScheme);
                //break;
        }
    }

    public void setMessageRoutingMode(String messageRoutingMode) throws InvalidWorkerConfigDefaultException {
        switch(messageRoutingMode.toUpperCase(Locale.ROOT).replace("_", "").replace("-","")){
            case "SINGLEPARTITION":
                this.messageRoutingMode = MessageRoutingMode.SinglePartition;
                break;
            case "ROUNDROBINPARTITION":
                this.messageRoutingMode = MessageRoutingMode.RoundRobinPartition;
                break;
            case "CUSTOMPARTITION":
                this.messageRoutingMode = MessageRoutingMode.CustomPartition;
                break;
            default:
                throw new InvalidWorkerConfigDefaultException("messageRoutingMode", messageRoutingMode);
        }
    }

   /* public boolean getBatchingEnabled(Function.FunctionDetails functionDetails, Logger instanceLog) {
        if (functionDetails.getSink() != null && functionDetails.getSink().getProducerSpec() != null
                && functionDetails.getSink().getProducerSpec().getBatchingDisabled() != null) {
            boolean batchingEnabled;
            Function.Batching Batching = functionDetails.getSink().getProducerSpec().getBatchingDisabled();
            switch (Batching) {
                case UNKNOWN_BATCHING:
                    batchingEnabled = this.batchingDisabled;
                    break;
                case DISABLED_BATCHING:
                    batchingEnabled = false;
                    break;
                case ENABLED_BATCHING:
                    batchingEnabled = true;
                    break;
                default:
                    instanceLog.error("ERROR: The value provided for batchingEnabled as a function default does not exist in ClusterFunctionProducerDefaults.java." +
                            "Defaulting to cluster default.");
                    batchingEnabled = this.batchingDisabled;
                    break;
            }
            return batchingEnabled;
        } else {
            return this.batchingDisabled;
        }
    }
    public boolean getChunkingEnabled(Function.FunctionDetails functionDetails, Logger instanceLog) {
        if (functionDetails.getSink() != null && functionDetails.getSink().getProducerSpec() != null
                && functionDetails.getSink().getProducerSpec().getChunking() != null) {
            boolean chunkingEnabled;
            Function.Chunking Chunking = functionDetails.getSink().getProducerSpec().getChunking();
            switch (Chunking) {
                case UNKNOWN_CHUNKING:
                    chunkingEnabled = this.chunkingEnabled;
                    break;
                case DISABLED_CHUNKING:
                    chunkingEnabled = false;
                    break;
                case ENABLED_CHUNKING:
                    chunkingEnabled = true;
                    break;
                default:
                    instanceLog.error("ERROR: The value provided for chunkingEnabled as a function default does not exist in ClusterFunctionProducerDefaults.java. " +
                            "Defaulting to cluster default.");
                    chunkingEnabled = this.chunkingEnabled;
                    break;
            }
            return chunkingEnabled;
        } else {
            return this.chunkingEnabled;
        }
    }
    public boolean getBlockIfQueueFull(Function.FunctionDetails functionDetails, Logger instanceLog) {
        if (functionDetails.getSink() != null && functionDetails.getSink().getProducerSpec() != null
                && functionDetails.getSink().getProducerSpec().getBlockIfQueueFull() != null) {
            boolean blockIfQueueFull;
            Function.BlockIfQueueFull functionBlockIfQueueFull = functionDetails.getSink().getProducerSpec().getBlockIfQueueFull();
            switch (functionBlockIfQueueFull) {
                case UNKNOWN_BLOCKING:
                    blockIfQueueFull = this.blockIfQueueFullDisabled;
                    break;
                case DISABLED_BLOCKING:
                    blockIfQueueFull = false;
                    break;
                case ENABLED_BLOCKING:
                    blockIfQueueFull = true;
                    break;
                default:
                    instanceLog.error("ERROR: The value provided for blockIfQueueFull as a function default does not exist in ClusterFunctionProducerDefaults.java");
                    blockIfQueueFull = this.blockIfQueueFullDisabled;
                    break;
            }
            return blockIfQueueFull;
        } else {
            return this.blockIfQueueFullDisabled;
        }
    }
    public CompressionType getCompressionType(Function.FunctionDetails functionDetails, Logger instanceLog) {
        if (functionDetails.getSink() != null && functionDetails.getSink().getProducerSpec() != null
                && functionDetails.getSink().getProducerSpec().getCompressionType() != null) {
            CompressionType compressionType;
            Function.CompressionType functionCompression = functionDetails.getSink().getProducerSpec().getCompressionType();
            switch (functionCompression) {
                case UNKNOWN_COMPRESSION:
                    compressionType = this.compressionType;
                    break;
                case NONE:
                    compressionType = CompressionType.NONE;
                    break;
                case LZ4:
                    compressionType = CompressionType.LZ4;
                    break;
                case ZLIB:
                    compressionType = CompressionType.ZLIB;
                    break;
                case ZSTD:
                    compressionType = CompressionType.ZSTD;
                    break;
                case SNAPPY:
                    compressionType = CompressionType.SNAPPY;
                    break;
                default:
                    instanceLog.error("ERROR: The value provided for compressionType as a function default does not exist in ClusterFunctionProducerDefaults.java");
                    compressionType = this.compressionType;
                    break;
            }
            return compressionType;
        } else {
            return this.compressionType;
        }
    }

    public HashingScheme getHashingScheme(Function.FunctionDetails functionDetails, Logger instanceLog) {
        if (functionDetails.getSink() != null && functionDetails.getSink().getProducerSpec() != null
                && functionDetails.getSink().getProducerSpec().getHashingScheme() != null) {
            HashingScheme hashingScheme;
            Function.HashingScheme functionHashingScheme = functionDetails.getSink().getProducerSpec().getHashingScheme();
            switch (functionHashingScheme) {
                case UNKNOWN_HASHING:
                    hashingScheme = this.hashingScheme;
                    break;
                case JAVA_STRING_HASH:
                    hashingScheme = HashingScheme.JavaStringHash;
                    break;
                case MURMUR3_32HASH:
                    hashingScheme = HashingScheme.Murmur3_32Hash;
                    break;
                default:
                    instanceLog.error("ERROR: The value provided for hashingScheme as a function default does not exist in ClusterFunctionProducerDefaults.java");
                    hashingScheme = this.hashingScheme;
                    break;
            }
            return hashingScheme;
        } else {
            return this.hashingScheme;
        }
    }
    public MessageRoutingMode getMessageRoutingMode(Function.FunctionDetails functionDetails, Logger instanceLog) {
        if (functionDetails.getSink() != null && functionDetails.getSink().getProducerSpec() != null
                && functionDetails.getSink().getProducerSpec().getMessageRoutingMode() != null) {
            MessageRoutingMode routingMode;
            Function.MessageRoutingMode functionRoutingMode = functionDetails.getSink().getProducerSpec().getMessageRoutingMode();
            switch (functionRoutingMode) {
                case UNKNOWN_ROUTING:
                    routingMode = this.messageRoutingMode;
                    break;
                case SINGLE_PARTITION:
                    routingMode = MessageRoutingMode.SinglePartition;
                    break;
                case ROUND_ROBIN_PARTITION:
                    routingMode = MessageRoutingMode.RoundRobinPartition;
                    break;
                case CUSTOM_PARTITION:
                    routingMode = MessageRoutingMode.CustomPartition;
                    break;
                default:
                    instanceLog.error("ERROR: The value provided for messageRoutingMode as a function default does not exist in ClusterFunctionProducerDefaults.java");
                    routingMode = this.messageRoutingMode;
                    break;
            }
            return routingMode;
        } else {
            return this.messageRoutingMode;
        }
    }
    public int getBatchingMaxPublishDelay(Function.FunctionDetails functionDetails, Logger instanceLog) {
        if (functionDetails.getSink() != null && functionDetails.getSink().getProducerSpec() != null
                && functionDetails.getSink().getProducerSpec().getBatchingMaxPublishDelay() != 0){
            // If .getBatchingMaxPublishDelay() != 0, then we consider that the function HAS overridden the cluster default
            return functionDetails.getSink().getProducerSpec().getBatchingMaxPublishDelay();
        }
        return this.batchingMaxPublishDelay;
    }
    public Map<String, Object> getFunctionDefaults(Function.FunctionDetails functionDetails, Logger instanceLog) {
        HashMap<String, Object> map = new HashMap<>();
        map.put("enableBatching", this.getBatchingEnabled(functionDetails, instanceLog));
        map.put("enableChunking", this.getChunkingEnabled(functionDetails, instanceLog));
        map.put("blockIfQueueFull", this.getBlockIfQueueFull(functionDetails, instanceLog));
        map.put("compressionType", this.getCompressionType(functionDetails, instanceLog));
        map.put("hashingScheme", this.getHashingScheme(functionDetails, instanceLog));
        map.put("messageRoutingMode", this.getMessageRoutingMode(functionDetails, instanceLog));
        map.put("batchingMaxPublishDelay", this.getBatchingMaxPublishDelay(functionDetails, instanceLog));
        return map;
    }*/

}
