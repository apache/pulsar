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

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.functions.proto.Function;

public interface FunctionDefaultsMediator {
    boolean isBatchingDisabled();
    boolean isChunkingEnabled();
    boolean isBlockIfQueueFullDisabled();
    CompressionType getCompressionType();
    Function.CompressionType getCompressionTypeProto();
    HashingScheme getHashingScheme();
    Function.HashingScheme getHashingSchemeProto();
    MessageRoutingMode getMessageRoutingMode();
    Function.MessageRoutingMode getMessageRoutingModeProto();
    Long getBatchingMaxPublishDelay();
}
