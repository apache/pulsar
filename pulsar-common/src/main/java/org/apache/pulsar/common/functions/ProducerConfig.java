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
package org.apache.pulsar.common.functions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;

/**
 * Configuration of the producer inside the function.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class ProducerConfig {
    private Integer maxPendingMessages;
    private Integer maxPendingMessagesAcrossPartitions;
    private Boolean useThreadLocalProducers;
    private CryptoConfig cryptoConfig;
    private String batchBuilder;

    public Boolean batchingDisabled;
    public Boolean chunkingEnabled;
    public Boolean blockIfQueueFullDisabled;
    public CompressionType compressionType;
    public HashingScheme hashingScheme;
    public MessageRoutingMode messageRoutingMode;
    public Long batchingMaxPublishDelay;
    public Boolean getBatchingEnabled(){
        return !this.getBatchingDisabled();
    }
    public Boolean getBlockIfQueueFullEnabled() { return !this.getBlockIfQueueFullDisabled();}
    public ProducerConfig merge(ProducerConfig newConfig){
        ProducerConfig mergedConfig = new ProducerConfig();
        if(newConfig == null) {
            mergedConfig = this;
        }
        else {
            if(newConfig.getBatchingDisabled() != null){
                mergedConfig.setBatchingDisabled(newConfig.getBatchingDisabled());
            } else {
                mergedConfig.setBatchingDisabled(this.getBatchingDisabled());
            }
            if(newConfig.getChunkingEnabled() != null){
                mergedConfig.setChunkingEnabled(newConfig.getChunkingEnabled());
            } else {
                mergedConfig.setChunkingEnabled(this.getChunkingEnabled());
            }
            if(newConfig.getBlockIfQueueFullDisabled() != null){
                mergedConfig.setBlockIfQueueFullDisabled(newConfig.getBlockIfQueueFullDisabled());
            } else {
                mergedConfig.setBlockIfQueueFullDisabled(this.getBlockIfQueueFullDisabled());
            }
            if(newConfig.getCompressionType() != null){
                mergedConfig.setCompressionType(newConfig.getCompressionType());
            } else {
                mergedConfig.setCompressionType(this.getCompressionType());
            }
            if(newConfig.getHashingScheme() != null){
                mergedConfig.setHashingScheme(newConfig.getHashingScheme());
            } else {
                mergedConfig.setHashingScheme(this.getHashingScheme());
            }
            if (newConfig.getMessageRoutingMode() != null) {
                mergedConfig.setMessageRoutingMode(newConfig.getMessageRoutingMode());
            } else {
                mergedConfig.setMessageRoutingMode(this.getMessageRoutingMode());
            }
            if(newConfig.getBatchingMaxPublishDelay() != null){
                mergedConfig.setBatchingMaxPublishDelay(newConfig.getBatchingMaxPublishDelay());
            } else {
                mergedConfig.setBatchingMaxPublishDelay(this.getBatchingMaxPublishDelay());
            }
        }
        return mergedConfig;
    }
}
