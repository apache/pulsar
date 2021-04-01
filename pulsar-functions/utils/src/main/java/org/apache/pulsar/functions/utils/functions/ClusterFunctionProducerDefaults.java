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
                                           long batchingMaxPublishDelay) throws InvalidWorkerConfigDefaultException {
        this.batchingDisabled = batchingDisabled;
        this.chunkingEnabled = chunkingEnabled;
        this.blockIfQueueFullDisabled = blockIfQueueFullDisabled;
        this.setCompressionType(compressionType);
        this.setHashingScheme(hashingScheme);
        this.setMessageRoutingMode(messageRoutingMode);
        this.setBatchingMaxPublishDelay(batchingMaxPublishDelay);
    }
    // We could also validate that chunking is disabled if batching is enabled, etc.

    public void setBatchingMaxPublishDelay(long batchingMaxPublishDelay) throws InvalidWorkerConfigDefaultException {
        if(batchingMaxPublishDelay < 0){
            throw new InvalidWorkerConfigDefaultException("batchingMaxPublishDelay", Long.toString(batchingMaxPublishDelay));
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
}
