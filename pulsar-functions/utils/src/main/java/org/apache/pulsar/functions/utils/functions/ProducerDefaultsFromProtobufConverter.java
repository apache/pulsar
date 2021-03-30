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
import org.apache.pulsar.functions.proto.Function;

@Getter
@AllArgsConstructor
public class ProducerDefaultsFromProtobufConverter {
    private Function.ProducerSpec spec;

    public CompressionType getCompressionType() {
        Function.CompressionType compressionType = spec.getCompressionType();
        switch (compressionType){
            case NONE:
                return CompressionType.NONE;
            case ZLIB:
                return CompressionType.ZLIB;
            case ZSTD:
                return CompressionType.ZSTD;
            case SNAPPY:
                return CompressionType.SNAPPY;
            case LZ4:
            default:
                return CompressionType.LZ4;
        }
    }
    public HashingScheme getHashingScheme() {
        Function.HashingScheme hashingScheme = spec.getHashingScheme();
        switch (hashingScheme){
            case JAVA_STRING_HASH:
                return HashingScheme.JavaStringHash;
            case MURMUR3_32HASH:
            default:
                return HashingScheme.Murmur3_32Hash;
        }
    }
    public MessageRoutingMode getMessageRoutingMode() {
        Function.MessageRoutingMode messageRoutingMode = spec.getMessageRoutingMode();
        switch (messageRoutingMode){
            case SINGLE_PARTITION:
                return MessageRoutingMode.SinglePartition;
            case ROUND_ROBIN_PARTITION:
                return MessageRoutingMode.RoundRobinPartition;
            case CUSTOM_PARTITION:
            default:
                return MessageRoutingMode.CustomPartition;

        }
    }
}
