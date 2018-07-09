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
package org.apache.pulsar.common.api;

import java.util.Map;
import java.util.Optional;

import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
public class EncryptionContext {

    private Map<String, EncryptionKey> keys;
    private byte[] param;
    private Map<String, String> metadata;
    private String algorithm;
    private CompressionType compressionType;
    private int uncompressedMessageSize;
    private Optional<Integer> batchSize;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EncryptionKey {
        private byte[] keyValue;
        private Map<String, String> metadata;
    }

}