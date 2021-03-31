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
package org.apache.pulsar.functions.instance;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.functions.utils.functions.ClusterFunctionProducerDefaults;
import org.apache.pulsar.functions.utils.functions.InvalidWorkerConfigDefaultException;

import java.io.Serializable;

@Data
@Accessors(chain = true)
public class FunctionDefaultsConfig implements Serializable {
    @FieldContext(
            doc = "Disables default message batching between functions"
    )
    protected boolean batchingDisabled = false;
    @FieldContext(
            doc = "Enables default message chunking between functions"
    )
    protected boolean chunkingEnabled = false;
    @FieldContext(
            doc = "Disables default behavior to block when message queue is full"
    )
    protected boolean blockIfQueueFullDisabled = true;
    @FieldContext(
            doc = "Default compression type"
    )
    protected String compressionType = "LZ4";
    @FieldContext(
            doc = "Default hashing scheme"
    )
    protected String hashingScheme = "Murmur3_32Hash";
    @FieldContext(
            doc = "Default hashing scheme"
    )
    protected String messageRoutingMode = "CustomPartition";
    @FieldContext(
            doc = "Default max publish delay (in milliseconds) when message batching is enabled"
    )
    protected int batchingMaxPublishDelay = 10;

    public ClusterFunctionProducerDefaults buildProducerDefaults() throws InvalidWorkerConfigDefaultException {
        return new ClusterFunctionProducerDefaults(this.isBatchingDisabled(),
                this.isChunkingEnabled(), this.isBlockIfQueueFullDisabled(), this.getCompressionType(),
                this.getHashingScheme(), this.getMessageRoutingMode(), this.getBatchingMaxPublishDelay());
    }

    public FunctionDefaultsConfig() {
    }
}