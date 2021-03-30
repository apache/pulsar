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
    /**
     * Used to override cluster defaults for function producing behavior.
     * These are nullable for backwards compatibility (so we don't force
     * users to set these values.) These must be nullable because if
     * we assume that a particular value (e.g. true/false) means
     * they didn't specify an override, if they did explicitly
     * specify that value and we assume they didn't, we would
     * load the value from WorkerConfig, which could result
     * in unexpected behavior that could require redeployment
     * of functions to fix.
     */
    public Boolean batchingDisabled;
    public Boolean chunkingEnabled;
    public Boolean blockIfQueueFullDisabled;
    public CompressionType compressionType;
    public HashingScheme hashingScheme;
    public MessageRoutingMode messageRoutingMode;
    public Integer batchingMaxPublishDelay;
}
